module Application.MasterPDBActor

open Akkling
open Domain.MasterPDB
open Domain.OracleInstance
open Application.PendingRequest
open Application.Oracle
open Akka.Actor
open Domain.MasterPDBVersion
open Application.Parameters
open Application.Common
open Domain.Common.Exceptional
open Domain.Common
open Domain.MasterPDBWorkingCopy

type Command =
| GetState // responds with StateResult
| GetInternalState // responds with MasterPDB
| SetInternalState of MasterPDB // no response
| GetEditionInfo // responds with EditionInfoResult
| PrepareForModification of WithRequestId<int, string> // responds with WithRequestId<PrepareForModificationResult>
| Commit of WithRequestId<string, string> // responds with WithRequestId<EditionCommitted>
| Rollback of WithRequestId<string> // responds with WithRequestId<EditionRolledBack>
| CreateWorkingCopy of WithRequestId<int, string, bool, bool> // responds with WithRequest<CreateWorkingCopyResult>
| DeleteWorkingCopy of WithRequestId<MasterPDBWorkingCopy> // responds with OraclePDBResultWithReqId
| CreateWorkingCopyOfEdition of WithRequestId<string, bool> // WithRequest<CreateWorkingCopyResult>
| CollectVersionsGarbage of int list // no response
| DeleteVersion of int // responds with DeleteVersionResult
| SwitchLock // responds with Result<bool,string>

type PrepareForModificationResult = 
| Prepared of string * MasterPDB * string * string * (string * string) list
| PreparationFailure of string * string

type StateResult = Result<Application.DTO.MasterPDB.MasterPDBDTO, string>
let stateOk state : StateResult = Ok state
let stateError error : StateResult = Error error

type EditionInfoResult = Result<Application.DTO.MasterPDB.MasterPDBEditionDTO, string>

type EditionCommitted = Result<string * MasterPDB * int, string>
type EditionRolledBack = Result<string * MasterPDB, string>

type CreateWorkingCopyResult = Result<string * int * string, string>

type DeleteVersionResult = Result<string * string * int, string> // instance * pdb * version

type private Collaborators = {
    MasterPDBVersionActors: Map<int, IActorRef<MasterPDBVersionActor.Command>>
    MasterPDBEditionActor: IActorRef<MasterPDBEditionActor.Command> option
    OracleShortTaskExecutor: IActorRef<OracleShortTaskExecutor.Command>
    OracleLongTaskExecutor: IActorRef<OracleLongTaskExecutor.Command>
    OracleDiskIntensiveTaskExecutor : IActorRef<OracleDiskIntensiveActor.Command>
}

let private getOrSpawnVersionActor parameters instance (masterPDBName:string) (version:MasterPDBVersion) collaborators ctx =
    let versionActorMaybe = collaborators.MasterPDBVersionActors |> Map.tryFind version.VersionNumber
    match versionActorMaybe with
    | Some versionActor -> collaborators, versionActor
    | None -> 
        let versionActor = 
            ctx |> MasterPDBVersionActor.spawn 
                parameters
                instance
                collaborators.OracleShortTaskExecutor
                collaborators.OracleLongTaskExecutor
                collaborators.OracleDiskIntensiveTaskExecutor
                masterPDBName
                version
        
        { collaborators with MasterPDBVersionActors = collaborators.MasterPDBVersionActors.Add(version.VersionNumber, versionActor) }, 
        versionActor

let private getOrSpawnEditionActor parameters instance (editionPDBName:string) collaborators ctx =
        let editionActor = collaborators.MasterPDBEditionActor |> Option.defaultWith (fun () ->
            ctx |> MasterPDBEditionActor.spawn 
                parameters
                instance
                collaborators.OracleShortTaskExecutor
                collaborators.OracleLongTaskExecutor
                collaborators.OracleDiskIntensiveTaskExecutor
                editionPDBName)
        
        { collaborators with MasterPDBEditionActor = Some editionActor }, 
        editionActor

type private State = {
    MasterPDB: MasterPDB
    PreviousMasterPDB: MasterPDB
    Requests: RequestMap<Command>
    Collaborators: Collaborators
    EditionOperationInProgress: bool
    Repository: IMasterPDBRepository
}

let private masterPDBActorBody 
    (parameters:Parameters) 
    (instance:OracleInstance) 
    oracleShortTaskExecutor
    oracleLongTaskExecutor 
    oracleDiskIntensiveTaskExecutor 
    (initialRepository:IMasterPDBRepository) 
    (ctx : Actor<obj>) =

    let initialMasterPDB = initialRepository.Get()
    let editionPDBName = masterPDBEditionName initialMasterPDB.Name

    let rec loop state =

        let masterPDB = state.MasterPDB
        let requests = state.Requests
        let collaborators = state.Collaborators
        let manifestFromVersion = Domain.MasterPDBVersion.manifestFile masterPDB.Name

        if requests.Count > 0 then ctx.Log.Value.Debug("Number of pending requests : {0}", requests.Count)

        if state.PreviousMasterPDB <> masterPDB then
            ctx.Log.Value.Debug("Persisted modified master PDB {pdb}", masterPDB.Name)
            loop { state with Repository = state.Repository.Put masterPDB; PreviousMasterPDB = masterPDB }
        
        else actor {

            let! msg = ctx.Receive()
        
            match msg with
            | :? LifecycleEvent as event ->
                match event with
                | LifecycleEvent.PreStart ->
                    ctx.Log.Value.Info("Checking integrity of master PDB {pdb}...", masterPDB.Name)
                    let pdbExists pdb : Async<Exceptional<bool>> = oracleShortTaskExecutor <? OracleShortTaskExecutor.PDBExists pdb
                    let! editionPDBExists = pdbExists editionPDBName
                    match editionPDBExists, masterPDB.EditionState.IsSome with
                    | Ok true, false ->
                        ctx.Log.Value.Warning("Master PDB {pdb} is not locked whereas its edition PDB exists on server => locked it (anonymous editor)", masterPDB.Name)
                        return! loop { state with MasterPDB = { masterPDB with EditionState = Some (newEditionInfo "anonymous") } }
                    | Ok false, true ->
                        ctx.Log.Value.Warning("Master PDB {pdb} is declared as locked whereas its edition PDB does not exist on server => unlocked it", masterPDB.Name)
                        return! loop { state with MasterPDB = { masterPDB with EditionState = None } }
                    | Ok _, _->
                        ctx.Log.Value.Info("Integrity of master PDB {pdb} OK.", masterPDB.Name)
                        return! loop state
                    | Error error, _ ->
                        ctx.Log.Value.Error("Cannot check integrity of master PDB {pdb} : {error}", masterPDB.Name, error)
                        return! loop state
                | _ ->
                    return! loop state

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

                | GetEditionInfo ->
                    let sender = ctx.Sender().Retype<EditionInfoResult>()
                    let result:EditionInfoResult = 
                        match masterPDB.EditionState with
                        | Some _ ->
                            Application.DTO.MasterPDB.toMasterPDBEditionDTO instance masterPDB |> Ok
                        | None -> sprintf "master PDB %s is not being edited" masterPDB.Name |> Error 
                    sender <! result
                    return! loop state

                | PrepareForModification (requestId, version, user) as command ->
                    let sender = ctx.Sender().Retype<WithRequestId<PrepareForModificationResult>>()

                    if masterPDB |> isLockedForEdition then
                        sender <! (requestId, PreparationFailure (masterPDB.Name, sprintf "PDB %s is already locked" masterPDB.Name))
                        return! loop state
                    elif masterPDB.EditionDisabled then
                        sender <! (requestId, PreparationFailure (masterPDB.Name, sprintf "editing PDB %s is disabled" masterPDB.Name))
                        return! loop state
                    elif not <| UserRights.canLockPDB masterPDB (UserRights.normalUser user) then
                        sender <! (requestId, PreparationFailure (masterPDB.Name, sprintf "user %s is not authorized to lock PDB %s" user masterPDB.Name))
                        return! loop state
                    else
                        let latestVersion = masterPDB |> getLatestAvailableVersionNumber
                        if (latestVersion <> version) then 
                            sender <! (requestId, PreparationFailure (masterPDB.Name, sprintf "version %d is not the latest version (%d) of \"%s\"" version latestVersion masterPDB.Name))
                            return! loop state
                        elif (state.EditionOperationInProgress) then
                            sender <! (requestId, PreparationFailure (masterPDB.Name, sprintf "PDB %s has a pending edition operation in progress" masterPDB.Name))
                            return! loop state
                        else
                            let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                            oracleDiskIntensiveTaskExecutor <! OracleDiskIntensiveActor.ImportPDB (Some requestId, (manifestFromVersion version), instance.MasterPDBDestPath, editionPDBName)
                            return! loop { state with Requests = newRequests; EditionOperationInProgress = true }

                | Commit (requestId, unlocker, _) ->
                    let sender = ctx.Sender().Retype<WithRequestId<EditionCommitted>>()
                    let lockInfoMaybe = masterPDB.EditionState
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
                            let manifest = manifestFromVersion (getNextAvailableVersion masterPDB)
                            oracleLongTaskExecutor <! OracleLongTaskExecutor.ExportPDB (Some requestId, manifest, editionPDBName)
                            let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                            return! loop { state with Requests = newRequests; EditionOperationInProgress = true }

                | Rollback (requestId, unlocker) ->
                    let sender = ctx.Sender().Retype<WithRequestId<EditionRolledBack>>()
                    let lockInfoMaybe = masterPDB.EditionState
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
            
                | CreateWorkingCopy (requestId, versionNumber, name, snapshot, durable) ->
                    let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()
                    let versionMaybe = masterPDB.Versions |> Map.tryFind versionNumber
                    match versionMaybe with
                    | None -> 
                        sender <! (requestId, Error (sprintf "version %d of master PDB %s does not exist" versionNumber masterPDB.Name |> exn))
                        return! loop state
                    | Some version ->
                        if version.Deleted then
                            sender <! (requestId, Error (sprintf "version %d of master PDB %s is deleted" versionNumber masterPDB.Name |> exn))
                            return! loop state
                        else
                            let newCollabs, versionActor = getOrSpawnVersionActor parameters instance masterPDB.Name version collaborators ctx
                            versionActor <<! MasterPDBVersionActor.CreateWorkingCopy (requestId, name, snapshot, durable)
                            return! loop { state with Collaborators = newCollabs }

                | DeleteWorkingCopy (requestId, workingCopy) ->
                    let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()
                    match workingCopy.Source with
                    | SpecificVersion versionNumber ->
                        let version = masterPDB.Versions |> Map.tryFind versionNumber
                        match version with
                        | None -> 
                            sender <! (requestId, Error (sprintf "version %d of master PDB %s does not exist" versionNumber masterPDB.Name |> exn))
                            return! loop state
                        | Some version ->
                            let newCollabs, versionActor = getOrSpawnVersionActor parameters instance masterPDB.Name version collaborators ctx
                            versionActor <<! MasterPDBVersionActor.DeleteWorkingCopy (requestId, workingCopy)
                            return! loop { state with Collaborators = newCollabs }
                    | Edition ->
                        if state.EditionOperationInProgress then
                            sender <! (requestId, Error (sprintf "PDB %s has a pending edition operation in progress" masterPDB.Name |> exn))
                            return! loop state
                        else
                            let newCollabs, editionActor = getOrSpawnEditionActor parameters instance editionPDBName state.Collaborators ctx
                            editionActor <<! MasterPDBEditionActor.DeleteWorkingCopy (requestId, workingCopy)
                            return! loop { state with Collaborators = newCollabs }

                | CreateWorkingCopyOfEdition (requestId, workingCopyName, durable) ->
                    let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()
                    let lockInfoMaybe = masterPDB.EditionState
                    match lockInfoMaybe with
                    | None -> 
                        sender <! (requestId, Error (sprintf "the master PDB %s is not being edited" masterPDB.Name |> exn))
                        return! loop state
                    | Some _ ->
                        if state.EditionOperationInProgress then
                            sender <! (requestId, Error (sprintf "PDB %s has a pending edition operation in progress" masterPDB.Name |> exn))
                            return! loop state
                        else
                            let newCollabs, editionActor = getOrSpawnEditionActor parameters instance editionPDBName state.Collaborators ctx
                            editionActor <<! MasterPDBEditionActor.CreateWorkingCopy (requestId, workingCopyName, durable)
                            return! loop { state with Collaborators = newCollabs }

                | CollectVersionsGarbage versions ->
                    ctx.Log.Value.Info("Garbage collection of versions of PDB {pdb} requested", masterPDB.Name)
                    let collectVersionGarbage collabs version =
                        let versionPDBMaybe = masterPDB.Versions |> Map.tryFind version
                        match versionPDBMaybe with
                        | Some versionPDB -> 
                            let newCollabs, versionActor = getOrSpawnVersionActor parameters instance masterPDB.Name versionPDB collabs ctx
                            versionActor <! MasterPDBVersionActor.CollectGarbage
                            newCollabs
                        | None -> 
                            ctx.Log.Value.Warning("Cannot garbage version {0} because it is not a version of {pdb}", version, masterPDB.Name)
                            collabs
                    let newCollabs = versions |> List.fold collectVersionGarbage state.Collaborators
                    return! loop { state with Collaborators = newCollabs }

                | DeleteVersion version ->
                    let sender = ctx.Sender().Retype<DeleteVersionResult>()
                    let result = state.MasterPDB |> deleteVersion version
                    match result with
                    | Ok newMasterPDB ->
                        sender <! Ok (instance.Name, masterPDB.Name, version)
                        collaborators.MasterPDBVersionActors 
                        |> Map.tryFind version 
                        |> Option.map (fun versionActor -> versionActor <! MasterPDBVersionActor.Delete)
                        |> ignore
                        return! loop { state with MasterPDB = newMasterPDB }
                    | Error error ->
                        sender <! Error error
                        return! loop state
            
                | SwitchLock ->
                    let sender = ctx.Sender().Retype<Result<bool,string>>()
                    let newLock = not state.MasterPDB.EditionDisabled
                    sender <! Ok newLock
                    return! loop { state with MasterPDB = { state.MasterPDB with EditionDisabled = newLock } }

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
                        | Ok editionPDB ->
                            let newMasterPDB = masterPDB |> lockForEdition locker
                            let editionPDBService = sprintf "%s%s/%s" instance.Server (oracleInstancePortString instance.Port) editionPDB
                            let schemaLogons = newMasterPDB.Schemas |> List.map (fun schema -> (schema.Type, sprintf "%s/%s@%s" schema.User schema.Password editionPDBService))
                            sender <! (requestId, Prepared (instance.Name, newMasterPDB, editionPDB, editionPDBService, schemaLogons))
                            return! loop { state with MasterPDB = newMasterPDB; Requests = newRequests; EditionOperationInProgress = false }
                        | Error error ->
                            sender <! (requestId, PreparationFailure (masterPDB.Name, error.Message))
                            return! loop { state with Requests = newRequests; EditionOperationInProgress = false }

                    | Commit (_, unlocker, comment) ->
                        let sender = request.Requester.Retype<WithRequestId<EditionCommitted>>()
                        match result with
                        | Ok _ ->
                            let newMasterPDBMaybe = masterPDB |> unlock |> Result.map (addVersionToMasterPDB unlocker comment)
                            match newMasterPDBMaybe with
                            | Ok (newMasterPDB, newVersion) ->
                                sender <! (requestId, Ok (instance.Name, newMasterPDB, newVersion))
                                return! loop { state with MasterPDB = newMasterPDB; Requests = newRequests; EditionOperationInProgress = false }
                            | Error error -> 
                                sender <! (requestId, Error (sprintf "cannot unlock %s : %s" masterPDB.Name error))
                                return! loop { state with Requests = newRequests; EditionOperationInProgress = false }
                        | Error error ->
                            sender <! (requestId, Error (sprintf "cannot commit %s : %s" masterPDB.Name error.Message))
                            return! loop { state with Requests = newRequests; EditionOperationInProgress = false }

                    | Rollback _ ->
                        let sender = request.Requester.Retype<WithRequestId<EditionRolledBack>>()
                        match result with
                        | Ok _ ->
                            let newMasterPDBMaybe = masterPDB |> unlock
                            match newMasterPDBMaybe with
                            | Ok newMasterPDB ->
                                sender <! (requestId, Ok (instance.Name, newMasterPDB))
                                return! loop { state with MasterPDB = newMasterPDB; Requests = newRequests; EditionOperationInProgress = false }
                            | Error error -> 
                                sender <! (requestId, Error (sprintf "cannot unlock %s : %s" masterPDB.Name error))
                                return! loop { state with Requests = newRequests; EditionOperationInProgress = false }
                        | Error error ->
                            sender <! (requestId, Error (sprintf "cannot rollback %s : %s" masterPDB.Name error.Message))
                            return! loop { state with Requests = newRequests; EditionOperationInProgress = false }

                    | _ -> 
                        ctx.Log.Value.Error("Unknown message received")
                        return! loop state

            | _ -> return! loop state
        
        }

    let collaborators = { 
        MasterPDBVersionActors = Map.empty
        MasterPDBEditionActor = None
        OracleShortTaskExecutor = oracleShortTaskExecutor
        OracleLongTaskExecutor = oracleLongTaskExecutor
        OracleDiskIntensiveTaskExecutor = oracleDiskIntensiveTaskExecutor 
    }

    loop { 
        MasterPDB = initialMasterPDB
        PreviousMasterPDB = initialMasterPDB
        Requests = Map.empty
        Collaborators = collaborators
        EditionOperationInProgress = false 
        Repository = initialRepository
    }

let private masterPDBActorName (masterPDB:string) = 
    Common.ActorName 
        (sprintf "MasterPDB='%s'" (masterPDB.ToUpper() |> System.Uri.EscapeDataString))

let spawn
        parameters
        (instance:OracleInstance)
        (shortTaskExecutor:IActorRef<Application.OracleShortTaskExecutor.Command>)
        (longTaskExecutor:IActorRef<Application.OracleLongTaskExecutor.Command>)
        (oracleDiskIntensiveTaskExecutor:IActorRef<Application.OracleDiskIntensiveActor.Command>)
        (getRepository:OracleInstance -> string -> IMasterPDBRepository)
        (name:string)
        (actorFactory:IActorRefFactory) =
    
    let name = name.ToUpper()

    let initialRepository = getRepository instance name

    let (Common.ActorName actorName) = masterPDBActorName name
    
    Akkling.Spawn.spawn actorFactory actorName
        <| props (
            masterPDBActorBody
                parameters
                instance
                shortTaskExecutor
                longTaskExecutor
                oracleDiskIntensiveTaskExecutor
                initialRepository
        )

let spawnNew
        parameters 
        (instance:OracleInstance) 
        (shortTaskExecutor:IActorRef<Application.OracleShortTaskExecutor.Command>)
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
                instance
                shortTaskExecutor
                longTaskExecutor
                oracleDiskIntensiveTaskExecutor
                (initialRepository.Put masterPDB)
        )

