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
    MasterPDBEditionActor: IActorRef<MasterPDBEditionActor.Command> option
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

let private getOrSpawnEditionActor parameters instance (editionPDBName:string) collaborators ctx =
    match collaborators.MasterPDBEditionActor with
    | Some editionActor -> collaborators, editionActor
    | None -> 
        let editionActor = 
            ctx |> MasterPDBEditionActor.spawn 
                parameters
                instance
                collaborators.OracleShortTaskExecutor
                collaborators.OracleLongTaskExecutor
                collaborators.OracleDiskIntensiveTaskExecutor
                editionPDBName
        
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
    let editionPDBName = sprintf "%s_EDITION" initialMasterPDB.Name
    let pdbExists pdb : Async<Exceptional<bool>> = oracleShortTaskExecutor <? OracleShortTaskExecutor.PDBExists pdb
    let deletePDB pdb : Async<Exceptional<string>> = oracleLongTaskExecutor <? OracleLongTaskExecutor.DeletePDB (None, pdb)
    let getPDBNamesLike like : Async<Exceptional<string list>> = oracleShortTaskExecutor <? OracleShortTaskExecutor.GetPDBNamesLike like

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
                let! editionPDBExists = pdbExists editionPDBName
                match editionPDBExists, masterPDB.EditionState.IsSome with
                | Ok true, false ->
                    ctx.Log.Value.Warning("Master PDB {pdb} is not locked whereas its edition PDB exists on server => deleting the PDB...", masterPDB.Name)
                    let _ = 
                        deletePDB editionPDBName
                        |> runWithin parameters.LongTimeout id (fun () -> sprintf "Could not delete edition PDB %s : timeout exceeded" editionPDBName |> exn |> Error)
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
                let sender = ctx.Sender().Retype<WithRequestId<CreateWorkingCopyResult>>()
                let versionMaybe = masterPDB.Versions |> Map.tryFind versionNumber
                match versionMaybe with
                | None -> 
                    sender <! (requestId, Error (sprintf "version %d of master PDB %s does not exist" versionNumber masterPDB.Name))
                    return! loop state
                | Some version -> 
                    let newCollabs, versionActor = getOrSpawnVersionActor parameters instance masterPDB.Name version collaborators ctx
                    let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                    versionActor <! MasterPDBVersionActor.CreateWorkingCopy (requestId, name, snapshot, durable, force)
                    return! loop { state with Requests = newRequests; Collaborators = newCollabs }

            | CreateWorkingCopyOfEdition (requestId, workingCopyName, durable, force) ->
                let sender = ctx.Sender().Retype<WithRequestId<CreateWorkingCopyResult>>()
                let lockInfoMaybe = masterPDB.EditionState
                match lockInfoMaybe with
                | None -> 
                    sender <! (requestId, Error (sprintf "the master PDB %s is not being edited" masterPDB.Name))
                    return! loop state
                | Some _ ->
                    if (state.EditionOperationInProgress) then
                        sender <! (requestId, Error (sprintf "PDB %s has a pending edition operation in progress" masterPDB.Name))
                        return! loop state
                    else
                        let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                        let newCollabs, editionActor = getOrSpawnEditionActor parameters instance editionPDBName state.Collaborators ctx
                        editionActor <! MasterPDBEditionActor.CreateWorkingCopy (requestId, workingCopyName, durable, force)
                        return! loop { state with Requests = newRequests; Collaborators = newCollabs }

            | CollectGarbage ->
                ctx.Log.Value.Info("Garbage collection of PDB {pdb} requested", masterPDB.Name)
                let oldWCs = masterPDB.WorkingCopies |> List.choose (fun wc -> 
                    match wc.Lifetime with 
                    | Temporary expiry -> if (expiry <= System.DateTime.Now) then Some wc else None 
                    | _ -> None
                )
                oldWCs 
                |> List.choose (fun wc -> match wc.Source with | SpecificVersion version -> Some (wc.Name, version) | _ -> None)
                |> List.iter (fun (pdb, version) -> retype ctx.Self <! DeleteWorkingCopy (newRequestId(), pdb, version))
                oldWCs 
                |> List.choose (fun wc -> match wc.Source with | Edition -> Some wc.Name | _ -> None)
                |> List.iter (fun pdb -> retype ctx.Self <! DeleteWorkingCopyOfEdition (newRequestId(), pdb))
                // TODO: call versionActor <! MasterPDBVersionActor.CollectGarbage
                return! loop state

            | DeleteWorkingCopy (requestId, pdb, version) ->
                let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()
                let versionMaybe = masterPDB.Versions |> Map.tryFind version
                match versionMaybe with
                | None -> 
                    sender <! (requestId, Error (sprintf "version %d of master PDB %s does not exist" version masterPDB.Name |> exn))
                    return! loop state
                | Some pdbVersion -> 
                    if masterPDB |> isVersionCopiedAs version pdb then
                        let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                        let newCollabs, versionActor = getOrSpawnVersionActor parameters instance masterPDB.Name pdbVersion collaborators ctx
                        versionActor <! MasterPDBVersionActor.DeleteWorkingCopy (requestId, pdb)
                        return! loop { state with Requests = newRequests; Collaborators = newCollabs }
                    else
                        sender <! (requestId, Error (sprintf "version %d of master PDB %s has not been copied as %s" version masterPDB.Name pdb |> exn))
                        return! loop state

            | DeleteWorkingCopyOfEdition (requestId, pdb) ->
                let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()
                if masterPDB |> isEditionCopiedAs pdb then
                    let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                    let newCollabs, editionActor = getOrSpawnEditionActor parameters instance editionPDBName state.Collaborators ctx
                    editionActor <! MasterPDBEditionActor.DeleteWorkingCopy (requestId, pdb)
                    return! loop { state with Requests = newRequests; Collaborators = newCollabs }
                else
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

        | :? MasterPDBEditionActor.CommandToParent as commandToParent->
            match commandToParent with
            | MasterPDBEditionActor.KillEdition ->
                let versionActorMaybe = collaborators.MasterPDBEditionActor
                match versionActorMaybe with
                | Some versionActor -> 
                    versionActor <! MasterPDBEditionActor.HaraKiri
                    let newCollabs = { collaborators with MasterPDBEditionActor = None }
                    return! loop { state with Collaborators = newCollabs }
                | None -> 
                    ctx.Log.Value.Error("cannot find edition actor for PDB {pdb}", masterPDB.Name)
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

                | CreateWorkingCopy (_, versionNumber, name, snapshot, durable, _) ->
                    let sender = request.Requester.Retype<WithRequestId<CreateWorkingCopyResult>>()
                    match result with
                    | Ok _ ->
                        sender <! (requestId, Ok (masterPDB.Name, versionNumber, name))
                        let wc = 
                            if durable then 
                                newDurableWorkingCopy "userTODO" (SpecificVersion versionNumber) name // TODO
                            else    
                                newTempWorkingCopy parameters.GarbageCollectionDelay "userTODO" (SpecificVersion versionNumber) name // TODO
                        return! loop { state with Requests = newRequests; MasterPDB = { state.MasterPDB with WorkingCopies = wc :: state.MasterPDB.WorkingCopies } }
                    | Error error ->
                        sender <! (requestId, Error error.Message)
                        return! loop { state with Requests = newRequests }

                | CreateWorkingCopyOfEdition (_, name, durable, _) ->
                    let sender = request.Requester.Retype<WithRequestId<CreateWorkingCopyResult>>()
                    match result with
                    | Ok _ ->
                        sender <! (requestId, Ok (masterPDB.Name, 0, name))
                        let wc = 
                            if durable then 
                                newDurableWorkingCopy "userTODO" Edition name // TODO
                            else    
                                newTempWorkingCopy parameters.GarbageCollectionDelay "userTODO" Edition name // TODO
                        return! loop { state with Requests = newRequests; MasterPDB = { state.MasterPDB with WorkingCopies = wc :: state.MasterPDB.WorkingCopies } }
                    | Error error ->
                        sender <! (requestId, Error error.Message)
                        return! loop { state with Requests = newRequests }

                | DeleteWorkingCopy (_, pdb, version) ->
                    if request.Requester <> ctx.Self then retype request.Requester <! requestResponse else ()
                    match result with
                    | Ok _ ->
                        let chooser (wc:MasterPDBWorkingCopy) = 
                            if wc.Name = pdb then
                                match wc.Source with
                                | SpecificVersion v -> if v = version then None else Some wc
                                | _ -> Some wc
                            else
                                Some wc
                        return! loop { state with Requests = newRequests; MasterPDB = { state.MasterPDB with WorkingCopies = state.MasterPDB.WorkingCopies |> List.choose chooser } }
                    | Error _ ->
                        return! loop { state with Requests = newRequests }

                | DeleteWorkingCopyOfEdition (_, pdb) ->
                    if request.Requester <> ctx.Self then retype request.Requester <! requestResponse else ()
                    match result with
                    | Ok _ ->
                        let chooser (wc:MasterPDBWorkingCopy) = 
                            if wc.Name = pdb then
                                match wc.Source with
                                | Edition -> None
                                | _ -> Some wc
                            else
                                Some wc
                        return! loop { state with Requests = newRequests; MasterPDB = { state.MasterPDB with WorkingCopies = state.MasterPDB.WorkingCopies |> List.choose chooser } }
                    | Error _ ->
                        return! loop { state with Requests = newRequests }

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

