﻿module Application.MasterPDBActor

open Akkling
open Domain.MasterPDB
open Domain.OracleInstance
open Application.PendingRequest
open Application.Oracle
open Akka.Actor
open Domain.MasterPDBVersion
open Application.Parameters
open Application.Common

type Command =
| GetState // responds with StateResult
| GetInternalState // responds with MasterPDB
| SetInternalState of MasterPDB // no response
| PrepareForModification of WithRequestId<int, string> // responds with WithRequestId<PrepareForModificationResult>
| Commit of WithRequestId<string, string> // responds with WithRequestId<EditionDone>
| Rollback of WithRequestId<string> // responds with WithRequestId<EditionDone>
| SnapshotVersion of WithRequestId<int, string> // responds with WithRequest<SnapshotResult>
| CollectGarbage // no response

type PrepareForModificationResult = 
| Prepared of MasterPDB
| PreparationFailure of string * string

type StateResult = Result<Application.DTO.MasterPDB.MasterPDBState, string>
let stateOk state : StateResult = Ok state
let stateError error : StateResult = Error error

type EditionDone = Result<MasterPDB, string>

type SnapshotResult = Result<string * int * string, string>

type private Collaborators = {
    MasterPDBVersionActors: Map<int, IActorRef<MasterPDBVersionActor.Command>>
    OracleLongTaskExecutor: IActorRef<OracleLongTaskExecutor.Command>
    OracleDiskIntensiveTaskExecutor : IActorRef<OracleDiskIntensiveActor.Command>
}

let private getOrSpawnVersionActor parameters (oracleAPI:IOracleAPI) (masterPDBName:string) (version:MasterPDBVersion) collaborators ctx =
    let versionActorMaybe = collaborators.MasterPDBVersionActors |> Map.tryFind version.Number
    match versionActorMaybe with
    | Some versionActor -> collaborators, versionActor
    | None -> 
        let versionActor = 
            ctx |> MasterPDBVersionActor.spawn 
                parameters
                oracleAPI
                collaborators.OracleLongTaskExecutor
                collaborators.OracleDiskIntensiveTaskExecutor
                masterPDBName
                version
        
        { collaborators with MasterPDBVersionActors = collaborators.MasterPDBVersionActors.Add(version.Number, versionActor) }, 
        versionActor

type private State = {
    MasterPDB: MasterPDB
    PreviousMasterPDB: MasterPDB option
    Requests: RequestMap<Command>
    Collaborators: Collaborators
    EditionOperationInProgress: bool
    Repository: IMasterPDBRepository
}

let private masterPDBActorBody 
    (parameters:Parameters) 
    (oracleAPI:IOracleAPI)
    (instance:OracleInstance) 
    oracleLongTaskExecutor 
    oracleDiskIntensiveTaskExecutor 
    (initialRepository:IMasterPDBRepository) 
    (initialMasterPDB:MasterPDB)
    (previousMasterPDB:MasterPDB option)
    (ctx : Actor<_>) =

    let editionPDBName = initialMasterPDB.Name

    let rec loop state = actor {

        let masterPDB = state.MasterPDB
        let requests = state.Requests
        let collaborators = state.Collaborators
        let manifestPath = Domain.MasterPDB.manifestPath instance.MasterPDBManifestsPath masterPDB.Name

        if (state.PreviousMasterPDB.IsNone || state.PreviousMasterPDB.Value <> masterPDB) then
            ctx.Log.Value.Debug("Persisted modified master PDB {pdb}", masterPDB.Name)
            return! loop { state with Repository = state.Repository.Put masterPDB; PreviousMasterPDB = Some masterPDB }
        else

        ctx.Log.Value.Debug("Number of pending requests : {0}", requests.Count)
        let! (msg:obj) = ctx.Receive()
        
        match msg with
        | :? LifecycleEvent as event ->
            match event with
            | LifecycleEvent.PreStart ->
                ctx.Log.Value.Debug("Checking integrity...")
                let! editionPDBExists = oracleAPI.PDBExists editionPDBName
                match editionPDBExists, masterPDB.LockState.IsSome with
                | Ok true, false ->
                    ctx.Log.Value.Warning("Master PDB is not locked whereas its edition PDB exists on server => deleting the PDB...")
                    let result = oracleAPI.DeletePDB editionPDBName |> Async.RunSynchronously
                    result |> Result.mapError (fun error -> ctx.Log.Value.Error("Could not delete edition PDB {pdb} : {1}", editionPDBName, error.Message)) |> ignore
                    return! loop state
                | Ok false, true ->
                    ctx.Log.Value.Warning("Master PDB is declared as locked whereas its edition PDB does not exist on server => unlocked it")
                    return! loop { state with MasterPDB = { masterPDB with LockState = None } }
                | Ok _, _->
                    ctx.Log.Value.Debug("Integrity OK.")
                    return! loop state
                | Error error, _ ->
                    ctx.Log.Value.Error("Cannot check integrity : {0}", error)
                    return! loop state
            | _ ->
                return! unhandled()

        | :? Command as command -> 
            match command with
            | GetState -> 
                let sender = ctx.Sender().Retype<StateResult>()
                sender <! stateOk (masterPDB |> Application.DTO.MasterPDB.toDTO)
                return! loop state

            | GetInternalState ->
                ctx.Sender() <! masterPDB
                return! loop state

            | SetInternalState newMasterPDB ->
                return! loop { state with MasterPDB = newMasterPDB }

            | PrepareForModification (requestId, version, user) as command ->
                let sender = ctx.Sender().Retype<WithRequestId<PrepareForModificationResult>>()

                if masterPDB |> isLocked then
                    sender <! (requestId, PreparationFailure (masterPDB.Name, sprintf "PDB %s is already locked" masterPDB.Name))
                    return! loop state
                elif not <| UserRights.canLockPDB masterPDB (UserRights.normalUser user) then
                    sender <! (requestId, PreparationFailure (masterPDB.Name, sprintf "user %s is not authorized to lock PDB %s" user masterPDB.Name))
                    return! loop state
                else
                    let latestVersion = masterPDB |> getLatestAvailableVersion
                    if (latestVersion.Number <> version) then 
                        sender <! (requestId, PreparationFailure (masterPDB.Name, sprintf "version %d is not the latest version (%d) of \"%s\"" version latestVersion.Number masterPDB.Name))
                        return! loop state
                    elif (state.EditionOperationInProgress) then
                        sender <! (requestId, PreparationFailure (masterPDB.Name, sprintf "PDB %s has a pending edition operation in progress" masterPDB.Name))
                        return! loop state
                    else
                        let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                        oracleDiskIntensiveTaskExecutor <! OracleDiskIntensiveActor.ImportPDB (Some requestId, (manifestPath version), instance.MasterPDBDestPath, editionPDBName)
                        return! loop { state with Requests = newRequests; EditionOperationInProgress = true }

            | Commit (requestId, unlocker, _) ->
                let sender = ctx.Sender().Retype<WithRequestId<EditionDone>>()
                let lockInfoMaybe = masterPDB.LockState
                match lockInfoMaybe with
                | None -> 
                    sender <! (requestId, Error (sprintf "the master PDB %s is not being edited" masterPDB.Name))
                    return! loop state
                | Some lockInfo ->
                    if not <| UserRights.canUnlockPDB lockInfo (UserRights.normalUser unlocker) then
                        sender <! (requestId, Error (sprintf "user %s is not authorized to unlock PDB %s" unlocker masterPDB.Name))
                        return! loop state
                    elif (state.EditionOperationInProgress) then
                        sender <! (requestId, Error (sprintf "PDB %s has a pending edition operation in progress" masterPDB.Name))
                        return! loop state
                    else
                        let manifest = manifestPath (getNextAvailableVersion masterPDB)
                        oracleLongTaskExecutor <! OracleLongTaskExecutor.ExportPDB (Some requestId, manifest, editionPDBName)
                        let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                        return! loop { state with Requests = newRequests; EditionOperationInProgress = true }

            | Rollback (requestId, unlocker) ->
                let sender = ctx.Sender().Retype<WithRequestId<EditionDone>>()
                let lockInfoMaybe = masterPDB.LockState
                match lockInfoMaybe with
                | None -> 
                    sender <! (requestId, Error (sprintf "the master PDB %s is not being edited" masterPDB.Name))
                    return! loop state
                | Some lockInfo ->
                    if not <| UserRights.canUnlockPDB lockInfo (UserRights.normalUser unlocker) then
                        sender <! (requestId, Error (sprintf "user %s is not authorized to unlock PDB %s" unlocker masterPDB.Name))
                        return! loop state
                    elif (state.EditionOperationInProgress) then
                        sender <! (requestId, Error (sprintf "PDB %s has a pending edition operation in progress" masterPDB.Name))
                        return! loop state
                    else
                        let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                        oracleLongTaskExecutor <! OracleLongTaskExecutor.DeletePDB (Some requestId, editionPDBName)
                        return! loop { state with Requests = newRequests; EditionOperationInProgress = true }
            
            | SnapshotVersion (requestId, versionNumber, snapshotName) ->
                let sender = ctx.Sender().Retype<WithRequestId<SnapshotResult>>()
                let versionMaybe = masterPDB.Versions.TryFind(versionNumber)
                match versionMaybe with
                | None -> 
                    sender <! (requestId, Error (sprintf "version %d of master PDB %s does not exist" versionNumber masterPDB.Name))
                    return! loop state
                | Some version -> 
                    let newCollabs, versionActor = getOrSpawnVersionActor parameters oracleAPI masterPDB.Name version collaborators ctx
                    let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                    versionActor <! MasterPDBVersionActor.Snapshot (requestId, (manifestPath versionNumber), instance.SnapshotSourcePDBDestPath, snapshotName, instance.SnapshotPDBDestPath)
                    return! loop { state with Requests = newRequests; Collaborators = newCollabs }

            | CollectGarbage ->
                let! sourceVersionPDBsMaybe = oracleAPI.GetPDBNamesLike (sprintf "%s_V%%_%%" masterPDB.Name)
                match sourceVersionPDBsMaybe with
                | Ok sourceVersionPDBs -> 
                    ctx.Log.Value.Info("Garbage collection of PDB {pdb} requested", masterPDB.Name)
                    let regex = System.Text.RegularExpressions.Regex((sprintf "^%s_V([\\d]+)_.+$" masterPDB.Name))
                    let garbageVersion collabs sourceVersionPDB = 
                        let ok, version = System.Int32.TryParse(regex.Replace(sourceVersionPDB, "$1"))
                        if ok then 
                            let versionPDBMaybe = masterPDB.Versions |> Map.tryFind version
                            match versionPDBMaybe with
                            | Some versionPDB -> 
                                let newCollabs, versionActor = getOrSpawnVersionActor parameters oracleAPI masterPDB.Name versionPDB collabs ctx
                                versionActor <! MasterPDBVersionActor.CollectGarbage
                                newCollabs
                            | None -> 
                                ctx.Log.Value.Error("Cannot garbage PDB {0} because it does not correspond to a PDB version of {pdb}", sourceVersionPDB, masterPDB.Name)
                                collabs
                        else
                            ctx.Log.Value.Error("PDB {0} has not a valid PDB version name", sourceVersionPDB)
                            collabs
                    let newCollabs = sourceVersionPDBs |> List.fold garbageVersion collaborators
                    return! loop { state with Collaborators = newCollabs }
                | Error error ->
                    ctx.Log.Value.Error("Unexpected error while garbaging {pdb} : {0}", masterPDB.Name, error)
                    return! loop state

        | :? MasterPDBVersionActor.CommandToParent as commandToParent->
            match commandToParent with
            | MasterPDBVersionActor.KillVersion version ->
                let versionActorMaybe = collaborators.MasterPDBVersionActors |> Map.tryFind version
                match versionActorMaybe with
                | Some versionActor -> 
                    versionActor <! MasterPDBVersionActor.HaraKiri
                    let newCollabs = { collaborators with MasterPDBVersionActors = collaborators.MasterPDBVersionActors.Remove(version) }
                    return! loop { state with Collaborators = newCollabs }
                | None -> 
                    ctx.Log.Value.Error("cannot find actor for PDB {pdb} version {pdbversion}", masterPDB.Name, version)
                    return! loop state

        | :? OraclePDBResultWithReqId as requestResponse ->

            let (requestId, result) = requestResponse
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId

            match requestMaybe with
            | None -> 
                ctx.Log.Value.Error("internal error : request {requestId} not found", requestId)
                return! loop state

            | Some request -> 
                match request.Command with
                | PrepareForModification (_, _, locker) -> 
                    let sender = request.Requester.Retype<WithRequestId<PrepareForModificationResult>>()
                    match result with
                    | Ok _ ->
                        let newMasterPDB = masterPDB |> lock locker
                        sender <! (requestId, Prepared newMasterPDB)
                        return! loop { state with MasterPDB = newMasterPDB; Requests = newRequests; EditionOperationInProgress = false }
                    | Error error ->
                        sender <! (requestId, PreparationFailure (masterPDB.Name, error.Message))
                        return! loop { state with Requests = newRequests; EditionOperationInProgress = false }

                | Commit (_, unlocker, comment) ->
                    let sender = request.Requester.Retype<WithRequestId<EditionDone>>()
                    match result with
                    | Ok _ ->
                        let newMasterPDBMaybe = masterPDB |> addVersionToMasterPDB unlocker comment |> unlock
                        match newMasterPDBMaybe with
                        | Ok newMasterPDB ->
                            sender <! (requestId, Ok newMasterPDB)
                            return! loop { state with MasterPDB = newMasterPDB; Requests = newRequests; EditionOperationInProgress = false }
                        | Error error -> 
                            sender <! (requestId, Error (sprintf "cannot unlock %s : %s" masterPDB.Name error))
                            return! loop { state with Requests = newRequests; EditionOperationInProgress = false }
                    | Error error ->
                        sender <! (requestId, Error (sprintf "cannot commit %s : %s" masterPDB.Name error.Message))
                        return! loop { state with Requests = newRequests; EditionOperationInProgress = false }

                | Rollback _ ->
                    let sender = request.Requester.Retype<WithRequestId<EditionDone>>()
                    match result with
                    | Ok _ ->
                        let newMasterPDBMaybe = masterPDB |> unlock
                        match newMasterPDBMaybe with
                        | Ok newMasterPDB ->
                            sender <! (requestId, Ok newMasterPDB)
                            return! loop { state with MasterPDB = newMasterPDB; Requests = newRequests; EditionOperationInProgress = false }
                        | Error error -> 
                            sender <! (requestId, Error (sprintf "cannot unlock %s : %s" masterPDB.Name error))
                            return! loop { state with Requests = newRequests; EditionOperationInProgress = false }
                    | Error error ->
                        sender <! (requestId, Error (sprintf "cannot rollback %s : %s" masterPDB.Name error.Message))
                        return! loop { state with Requests = newRequests; EditionOperationInProgress = false }

                | SnapshotVersion (_, versionNumber, snapshotName) ->
                    let sender = request.Requester.Retype<WithRequestId<SnapshotResult>>()
                    match result with
                    | Ok _ ->
                        sender <! (requestId, Ok (masterPDB.Name, versionNumber, snapshotName))
                    | Error error ->
                        sender <! (requestId, Error error.Message)
                    return! loop { state with Requests = newRequests }

                | _ -> failwithf "Fatal error"

        | _ -> return! unhandled()
    }

    let collaborators = { 
        MasterPDBVersionActors = Map.empty
        OracleLongTaskExecutor = oracleLongTaskExecutor
        OracleDiskIntensiveTaskExecutor = oracleDiskIntensiveTaskExecutor 
    }

    loop { 
        MasterPDB = initialMasterPDB
        PreviousMasterPDB = previousMasterPDB
        Requests = Map.empty
        Collaborators = collaborators
        EditionOperationInProgress = false 
        Repository = initialRepository
    }

let private masterPDBActorName (masterPDB:string) = Common.ActorName (sprintf "MasterPDB='%s'" (masterPDB.ToUpper() |> System.Uri.EscapeDataString))

let spawn 
        parameters 
        oracleAPI 
        (instance:OracleInstance) 
        (longTaskExecutor:IActorRef<Application.OracleLongTaskExecutor.Command>) 
        (oracleDiskIntensiveTaskExecutor:IActorRef<Application.OracleDiskIntensiveActor.Command>) 
        (getRepository:OracleInstance -> string -> IMasterPDBRepository)
        name
        (actorFactory:IActorRefFactory) =
    
    let initialRepository = getRepository instance name
    let initialMasterPDB = initialRepository.Get()

    let (Common.ActorName actorName) = masterPDBActorName name
    
    Akkling.Spawn.spawn actorFactory actorName 
        <| props (
            masterPDBActorBody 
                parameters
                oracleAPI
                instance 
                longTaskExecutor 
                oracleDiskIntensiveTaskExecutor 
                initialRepository
                initialMasterPDB
                (Some initialMasterPDB)
        )

let spawnNew
        parameters 
        oracleAPI 
        (instance:OracleInstance) 
        (longTaskExecutor:IActorRef<Application.OracleLongTaskExecutor.Command>) 
        (oracleDiskIntensiveTaskExecutor:IActorRef<Application.OracleDiskIntensiveActor.Command>) 
        (newRepository:OracleInstance -> MasterPDB -> IMasterPDBRepository)
        (masterPDB:MasterPDB)
        (actorFactory:IActorRefFactory) =
    
    let initialRepository = newRepository instance masterPDB

    let (Common.ActorName actorName) = masterPDBActorName masterPDB.Name
    
    Akkling.Spawn.spawn actorFactory actorName 
        <| props (
            masterPDBActorBody 
                parameters
                oracleAPI
                instance 
                longTaskExecutor 
                oracleDiskIntensiveTaskExecutor 
                initialRepository
                masterPDB
                None
        )

