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
| PrepareForModification of WithRequestId<int, string> // responds with WithRequestId<PrepareForModificationResult>
| Commit of WithRequestId<string, string> // responds with WithRequestId<EditionCommitted>
| Rollback of WithRequestId<string> // responds with WithRequestId<EditionRolledBack>
| CreateWorkingCopy of WithRequestId<int, string, bool, bool, bool> // responds with WithRequest<CreateWorkingCopyResult>
| DeleteWorkingCopy of WithRequestId<string, int> // responds with OraclePDBResultWithReqId
| CreateWorkingCopyOfEdition of WithRequestId<string, bool, bool> // WithRequest<CreateWorkingCopyResult>
| DeleteWorkingCopyOfEdition of WithRequestId<string> // responds with OraclePDBResultWithReqId
| CollectGarbage // no response

type PrepareForModificationResult = 
| Prepared of MasterPDB * string * string * (string * string) list
| PreparationFailure of string * string

type StateResult = Result<Application.DTO.MasterPDB.MasterPDBDTO, string>
let stateOk state : StateResult = Ok state
let stateError error : StateResult = Error error

type EditionCommitted = Result<MasterPDB * int, string>
type EditionRolledBack = Result<MasterPDB, string>

type CreateWorkingCopyResult = Result<string * int * string, string>

type private Collaborators = {
    MasterPDBVersionActors: Map<int, IActorRef<MasterPDBVersionActor.Command>>
    OracleShortTaskExecutor: IActorRef<OracleShortTaskExecutor.Command>
    OracleLongTaskExecutor: IActorRef<OracleLongTaskExecutor.Command>
    OracleDiskIntensiveTaskExecutor : IActorRef<OracleDiskIntensiveActor.Command>
}

let private getOrSpawnVersionActor parameters instance (masterPDBName:string) (version:MasterPDBVersion) collaborators ctx =
    let versionActorMaybe = collaborators.MasterPDBVersionActors |> Map.tryFind version.Number
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
        
        { collaborators with MasterPDBVersionActors = collaborators.MasterPDBVersionActors.Add(version.Number, versionActor) }, 
        versionActor

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
    let editionPDBName = sprintf "%s_EDITION" initialMasterPDB.Name

    let rec loop state = actor {

        let masterPDB = state.MasterPDB
        let requests = state.Requests
        let collaborators = state.Collaborators
        let manifestFromVersion = Domain.MasterPDBVersion.manifestFile masterPDB.Name

        if state.PreviousMasterPDB <> masterPDB then
            ctx.Log.Value.Debug("Persisted modified master PDB {pdb}", masterPDB.Name)
            return! loop { state with Repository = state.Repository.Put masterPDB; PreviousMasterPDB = masterPDB }
        else

        if requests.Count > 0 then ctx.Log.Value.Debug("Number of pending requests : {0}", requests.Count)
        let! msg = ctx.Receive()
        
        try
        match msg with
        | :? LifecycleEvent as event ->
            match event with
            | LifecycleEvent.PreStart ->
                ctx.Log.Value.Info("Checking integrity of {pdb}...", masterPDB.Name)
                let pdbExists pdb : Async<Exceptional<bool>> = oracleShortTaskExecutor <? OracleShortTaskExecutor.PDBExists pdb
                let! editionPDBExists = pdbExists editionPDBName
                match editionPDBExists, masterPDB.EditionState.IsSome with
                | Ok true, false ->
                    ctx.Log.Value.Warning("Master PDB {pdb} is not locked whereas its edition PDB exists on server => deleting the PDB...", masterPDB.Name)
                    let deletePDB pdb : Exceptional<string> = 
                        oracleLongTaskExecutor <? OracleLongTaskExecutor.DeletePDB (None, pdb)
                        |> runWithin parameters.LongTimeout id (fun () -> sprintf "PDB %s cannot be deleted : timeout exceeded" pdb |> exn |> Error)
                    deletePDB editionPDBName |> ignore
                    return! loop state
                | Ok false, true ->
                    ctx.Log.Value.Warning("Master PDB {pdb} is declared as locked whereas its edition PDB does not exist on server => unlocked it", masterPDB.Name)
                    return! loop { state with MasterPDB = { masterPDB with EditionState = None } }
                | Ok _, _->
                    ctx.Log.Value.Info("Integrity of {pdb} OK.", masterPDB.Name)
                    return! loop state
                | Error error, _ ->
                    ctx.Log.Value.Error("Cannot check integrity of {pdb} : {error}", masterPDB.Name, error)
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
            
            | CreateWorkingCopy (requestId, versionNumber, name, snapshot, durable, force) ->
                let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()
                let versionMaybe = masterPDB.Versions |> Map.tryFind versionNumber
                match versionMaybe with
                | None -> 
                    sender <! (requestId, Error (sprintf "version %d of master PDB %s does not exist" versionNumber masterPDB.Name |> exn))
                    return! loop state
                | Some version -> 
                    let newCollabs, versionActor = getOrSpawnVersionActor parameters instance masterPDB.Name version collaborators ctx
                    versionActor <<! MasterPDBVersionActor.CreateWorkingCopy (requestId, name, snapshot, durable, force)
                    return! loop { state with Collaborators = newCollabs }

            | CreateWorkingCopyOfEdition (requestId, workingCopyName, durable, force) ->
                let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()
                let lockInfoMaybe = masterPDB.EditionState
                match lockInfoMaybe with
                | None -> 
                    sender <! (requestId, Error (sprintf "the master PDB %s is not being edited" masterPDB.Name |> exn))
                    return! loop state
                | Some _ ->
                    if (state.EditionOperationInProgress) then
                        sender <! (requestId, Error (sprintf "PDB %s has a pending edition operation in progress" masterPDB.Name |> exn))
                        return! loop state
                    else
                        let editionActor = MasterPDBEditionActor.spawn parameters instance oracleShortTaskExecutor oracleLongTaskExecutor oracleDiskIntensiveTaskExecutor editionPDBName ctx
                        editionActor <<! MasterPDBEditionActor.CreateWorkingCopy (requestId, workingCopyName, durable, force)
                        retype editionActor <! Akka.Actor.PoisonPill.Instance
                        return! loop state

            | CollectGarbage ->
                ctx.Log.Value.Info("Garbage collection of PDB {pdb} requested", masterPDB.Name)
                let garbageCollector = MasterPDBGarbageCollector.spawn parameters oracleShortTaskExecutor oracleLongTaskExecutor masterPDB ctx
                garbageCollector <! MasterPDBGarbageCollector.CollectGarbage
                retype garbageCollector <! Akka.Actor.PoisonPill.Instance
                return loop state

            | DeleteWorkingCopy (requestId, pdb, version) ->
                let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()
                let workingCopy = masterPDB |> workingCopyOfVersion version pdb
                match workingCopy with
                | Some workingCopy ->
                    let garbageCollector = MasterPDBGarbageCollector.spawn parameters oracleShortTaskExecutor oracleLongTaskExecutor masterPDB ctx
                    garbageCollector <<! MasterPDBGarbageCollector.DeleteWorkingCopy (requestId, workingCopy)
                    retype garbageCollector <! Akka.Actor.PoisonPill.Instance
                | None ->
                    sender <! (requestId, Error (sprintf "version %d of master PDB %s has not been copied as %s" version masterPDB.Name pdb |> exn))
                return! loop state

            | DeleteWorkingCopyOfEdition (requestId, pdb) ->
                let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()
                let workingCopy = masterPDB |> workingCopyOfEdition pdb
                match workingCopy with
                | Some workingCopy ->
                    let garbageCollector = MasterPDBGarbageCollector.spawn parameters oracleShortTaskExecutor oracleLongTaskExecutor masterPDB ctx
                    garbageCollector <<! MasterPDBGarbageCollector.DeleteWorkingCopy (requestId, workingCopy)
                    retype garbageCollector <! Akka.Actor.PoisonPill.Instance
                | None ->
                    sender <! (requestId, Error (sprintf "edition of master PDB %s has not been copied as %s" masterPDB.Name pdb |> exn))
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

        | :? MasterPDBGarbageCollector.CommandToParent as commandToParent ->
            match commandToParent with
            | MasterPDBGarbageCollector.CollectVersionsGarbage sourceVersionPDBs ->
                // Get existing snapshot-source PDBs and try to delete them
                let regex = System.Text.RegularExpressions.Regex((sprintf "^%s_V([\\d]+)_.+$" masterPDB.Name))
                let garbageVersion collabs sourceVersionPDB = 
                    let ok, version = System.Int32.TryParse(regex.Replace(sourceVersionPDB, "$1"))
                    if ok then 
                        let versionPDBMaybe = masterPDB.Versions |> Map.tryFind version
                        match versionPDBMaybe with
                        | Some versionPDB -> 
                            let newCollabs, versionActor = getOrSpawnVersionActor parameters instance masterPDB.Name versionPDB collabs ctx
                            versionActor <! MasterPDBVersionActor.CollectGarbage
                            newCollabs
                        | None -> 
                            ctx.Log.Value.Error("Cannot garbage PDB {0} because it does not correspond to a PDB version of {pdb}", sourceVersionPDB, masterPDB.Name)
                            collabs
                    else
                        ctx.Log.Value.Error("PDB {0} has not a valid PDB version name", sourceVersionPDB)
                        collabs
                let newCollabs = sourceVersionPDBs |> List.fold garbageVersion state.Collaborators
                return! loop { state with Collaborators = newCollabs }

            | MasterPDBGarbageCollector.WorkingCopiesDeleted deletionResults ->
                let deletedWorkingCopies, undeletedWorkingCopies = deletionResults |> List.partition (fun (_, errorMaybe) -> errorMaybe |> Option.isNone)
                undeletedWorkingCopies |> List.iter (fun (wc, error) ->
                    ctx.Log.Value.Warning("Cannot delete working copy {workingCopy} : {error}", wc.Name, error.Value.Message)
                )
                let deletedWorkingCopies = deletedWorkingCopies |> List.map (fun (wc, _) -> wc.Name) |> Set.ofList
                return! loop { state with MasterPDB = { state.MasterPDB with WorkingCopies = state.MasterPDB.WorkingCopies |> Map.filter (fun name _ -> not (deletedWorkingCopies |> Set.contains name)) } }
                

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
                        sender <! (requestId, Prepared (newMasterPDB, editionPDB, editionPDBService, schemaLogons))
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
                            sender <! (requestId, Ok (newMasterPDB, newVersion))
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
                            sender <! (requestId, Ok newMasterPDB)
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
        
        with ex -> 
            ctx.Log.Value.Error("Unexpected error : {0}", ex)
            return! loop state
    }

    let collaborators = { 
        MasterPDBVersionActors = Map.empty
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

let private masterPDBActorName (masterPDB:string) = Common.ActorName (sprintf "MasterPDB='%s'" (masterPDB.ToUpper() |> System.Uri.EscapeDataString))

let spawn
        parameters
        (instance:OracleInstance)
        (shortTaskExecutor:IActorRef<Application.OracleShortTaskExecutor.Command>)
        (longTaskExecutor:IActorRef<Application.OracleLongTaskExecutor.Command>)
        (oracleDiskIntensiveTaskExecutor:IActorRef<Application.OracleDiskIntensiveActor.Command>)
        (getRepository:OracleInstance -> string -> IMasterPDBRepository)
        name
        (actorFactory:IActorRefFactory) =
    
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

