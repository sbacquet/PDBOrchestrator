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

type User = Application.UserRights.User
type private RequestValidation = Domain.Common.Validation.Validation<RequestId, string>

type Command =
| GetState // responds with StateResult
| GetInternalState // responds with MasterPDB
| SetInternalState of MasterPDB // no response
| GetEditionInfo // responds with EditionInfoResult
| PrepareForModification of WithRequestId<int, User> // responds with WithRequestId<PrepareForModificationResult>
| Commit of WithRequestId<User, string> // responds with WithRequestId<EditionCommitted>
| Rollback of WithRequestId<User> // responds with WithRequestId<EditionRolledBack>
| CreateWorkingCopy of WithRequestRef<int, string, bool, bool, bool> // responds with WithRequest<CreateWorkingCopyResult>
| CreateWorkingCopyOfEdition of WithRequestId<string, bool, bool> // WithRequest<CreateWorkingCopyResult>
| CollectVersionsGarbage of int list // no response
| AddVersion of MasterPDBVersion // responds with AddVersionResult
| DeleteVersion of int // responds with DeleteVersionResult
| SwitchLock // responds with Result<bool,string>

type PrepareForModificationResult = 
| Prepared of string * MasterPDB * string * string * (string * string) list
| PreparationFailure of string * string

type StateResult = Result<Application.DTO.MasterPDB.MasterPDBDTO, string>
let stateOk state : StateResult = Ok state
let stateError error : StateResult = Error error

type EditionInfoResult = Result<Application.DTO.MasterPDB.MasterPDBEditionDTO, string>

type EditionCommitted = Result<string * MasterPDB * MasterPDBVersion, string>
type EditionRolledBack = Result<string * MasterPDB, string>

type CreateWorkingCopyResult = Result<string * int * string, string>

type DeleteVersionResult = Result<unit, string>
type AddVersionResult = Result<unit, string>

type private Collaborators = {
    MasterPDBVersionActors: Map<int, IActorRef<MasterPDBVersionActor.Command>>
    OracleShortTaskExecutor: IActorRef<OracleShortTaskExecutor.Command>
    OracleLongTaskExecutor: IActorRef<OracleLongTaskExecutor.Command>
    OracleDiskIntensiveTaskExecutor : IActorRef<OracleDiskIntensiveActor.Command>
}

let private getOrSpawnVersionActor parameters instance (masterPDB:MasterPDB) (version:MasterPDBVersion) collaborators workingCopyFactory ctx =
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
                workingCopyFactory
                masterPDB.Name
                masterPDB.Schemas
                version
        
        { collaborators with MasterPDBVersionActors = collaborators.MasterPDBVersionActors.Add(version.VersionNumber, versionActor) }, 
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
    (workingCopyFactory:IActorRef<Application.WorkingCopyFactoryActor.Command>)
    (initialRepository:IMasterPDBRepository) 
    (ctx : Actor<obj>) =

    let initialMasterPDB = initialRepository.Get()
    let editionPDBName = masterPDBEditionName initialMasterPDB.Name
    let valid requestId =
        ctx.Sender() <! RequestValidation.Valid requestId

    let rec loop state =

        let masterPDB = state.MasterPDB
        let requests = state.Requests
        let collaborators = state.Collaborators
        let manifestFromVersion = Domain.MasterPDBVersion.manifestFile masterPDB.Name
        let invalid error = actor {
            ctx.Sender() <! RequestValidation.Invalid [ error ]
            return! loop state
        }

        actor {

            if state.PreviousMasterPDB <> masterPDB then
                ctx.Log.Value.Debug("Persisted modified master PDB {pdb}", masterPDB.Name)
                return! loop { state with Repository = state.Repository.Put masterPDB; PreviousMasterPDB = masterPDB }
            else 
            
            let count = requests.Count in if count > 0 then ctx.Log.Value.Debug("Number of pending requests : {0}", count)

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
                        return! loop { state with MasterPDB = { masterPDB with EditionState = Some (newEditionInfo Application.UserRights.anonymousUserName) } }
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
                    elif not <| UserRights.canLockPDB masterPDB user then
                        sender <! (requestId, PreparationFailure (masterPDB.Name, sprintf "user %s is not authorized to lock PDB %s" user.Name masterPDB.Name))
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
                            oracleDiskIntensiveTaskExecutor <! 
                                OracleDiskIntensiveActor.ImportPDB (
                                    Some requestId, 
                                    (manifestFromVersion version), 
                                    instance.MasterPDBDestPath, 
                                    true, 
                                    (usersAndPasswords masterPDB.Schemas), 
                                    editionPDBName)
                            return! loop { state with Requests = newRequests; EditionOperationInProgress = true }

                | Commit (requestId, unlocker, _) ->
                    let sender = ctx.Sender().Retype<WithRequestId<EditionCommitted>>()
                    let lockInfoMaybe = masterPDB.EditionState
                    match lockInfoMaybe with
                    | None -> 
                        sender <! (requestId, Error (sprintf "the master PDB %s is not being edited" masterPDB.Name))
                        return! loop state
                    | Some lockInfo ->
                        if not <| UserRights.canUnlockPDB lockInfo unlocker then
                            sender <! (requestId, Error (sprintf "user %s is not authorized to unlock PDB %s" unlocker.Name masterPDB.Name))
                            return! loop state
                        elif (state.EditionOperationInProgress) then
                            sender <! (requestId, Error (sprintf "PDB %s has a pending edition operation in progress" masterPDB.Name))
                            return! loop state
                        else
                            let manifest = manifestFromVersion (getNextAvailableVersion masterPDB)
                            oracleLongTaskExecutor <! OracleLongTaskExecutor.ExportPDB (Some requestId, manifest, (userNames masterPDB.Schemas), editionPDBName)
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
                        if not <| UserRights.canUnlockPDB lockInfo unlocker then
                            sender <! (requestId, Error (sprintf "user %s is not authorized to unlock PDB %s" unlocker.Name masterPDB.Name))
                            return! loop state
                        elif (state.EditionOperationInProgress) then
                            sender <! (requestId, Error (sprintf "PDB %s has a pending edition operation in progress" masterPDB.Name))
                            return! loop state
                        else
                            let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                            oracleDiskIntensiveTaskExecutor <! OracleDiskIntensiveActor.DeletePDB (Some requestId, editionPDBName)
                            return! loop { state with Requests = newRequests; EditionOperationInProgress = true }
            
                | CreateWorkingCopy (requestRef, versionNumber, name, snapshot, durable, force) ->
                    let versionMaybe = masterPDB.Versions |> Map.tryFind versionNumber
                    match versionMaybe with
                    | None -> 
                        return! invalid <| sprintf "version %d of master PDB %s does not exist on instance %s" versionNumber masterPDB.Name instance.Name
                    | Some version ->
                        if version.Deleted then
                            return! invalid <| sprintf "version %d of master PDB %s is deleted on instance %s" versionNumber masterPDB.Name instance.Name
                        else
                            let (requestId, requester) = requestRef
                            valid requestId
                            let newCollabs, versionActor = getOrSpawnVersionActor parameters instance masterPDB version collaborators workingCopyFactory ctx
                            versionActor.Tell(MasterPDBVersionActor.CreateWorkingCopy (requestId, name, snapshot, durable, force), untyped requester)
                            return! loop { state with Collaborators = newCollabs }

                | CreateWorkingCopyOfEdition (requestId, workingCopyName, durable, force) ->
                    let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()
                    let lockInfoMaybe = masterPDB.EditionState
                    match lockInfoMaybe with
                    | None -> 
                        sender <! (requestId, Error (sprintf "the master PDB %s is not being edited" masterPDB.Name |> exn))
                        return! loop state
                    | Some _ ->
                        if state.EditionOperationInProgress then
                            sender <! (requestId, Error (sprintf "PDB %s has a pending edition operation in progress" masterPDB.Name |> exn))
                        else
                            workingCopyFactory <<! 
                                WorkingCopyFactoryActor.CreateWorkingCopyOfEdition(
                                    Some requestId, 
                                    editionPDBName, 
                                    (instance |> getWorkingCopyFolder durable), 
                                    workingCopyName, 
                                    durable, 
                                    force, 
                                    (userNames masterPDB.Schemas)
                                )
                        return! loop state

                | CollectVersionsGarbage versions ->
                    ctx.Log.Value.Info("Garbage collection of versions of PDB {pdb} requested", masterPDB.Name)
                    let collectVersionGarbage collabs version =
                        match masterPDB.Versions |> Map.tryFind version with
                        | Some version -> 
                            let newCollabs, versionActor = getOrSpawnVersionActor parameters instance masterPDB version collabs workingCopyFactory ctx
                            versionActor <! MasterPDBVersionActor.CollectGarbage
                            newCollabs
                        | None -> 
                            ctx.Log.Value.Warning("Cannot garbage version {0} because it is not a version of {pdb}", version, masterPDB.Name)
                            collabs
                    let latestVersion = masterPDB |> getLatestAvailableVersionNumber
                    let newCollabs =
                        versions
                        |> List.filter ((<>) latestVersion)
                        |> List.fold collectVersionGarbage state.Collaborators
                    return! loop { state with Collaborators = newCollabs }

                | AddVersion version ->
                    let sender = ctx.Sender().Retype<AddVersionResult>()
                    let result = masterPDB |> addVersionToMasterPDB version
                    match result with
                    | Ok newMasterPDB ->
                        sender <! Ok ()
                        return! loop { state with MasterPDB = newMasterPDB }
                    | Error error ->
                        sender <! Error error
                        return! loop state

                | DeleteVersion version ->
                    let sender = ctx.Sender().Retype<DeleteVersionResult>()
                    let result = state.MasterPDB |> deleteVersion version
                    match result with
                    | Ok newMasterPDB ->
                        sender <! Ok ()
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
                            let newMasterPDB = masterPDB |> lockForEdition locker.Name
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
                            let newMasterPDBMaybe = masterPDB |> unlock |> Result.map (addNewVersionToMasterPDB unlocker.Name comment)
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
        (workingCopyFactory:IActorRef<Application.WorkingCopyFactoryActor.Command>)
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
                workingCopyFactory
                initialRepository
        )

let spawnNew
        parameters 
        (instance:OracleInstance) 
        (shortTaskExecutor:IActorRef<Application.OracleShortTaskExecutor.Command>)
        (longTaskExecutor:IActorRef<Application.OracleLongTaskExecutor.Command>) 
        (oracleDiskIntensiveTaskExecutor:IActorRef<Application.OracleDiskIntensiveActor.Command>)
        (workingCopyFactory:IActorRef<Application.WorkingCopyFactoryActor.Command>)
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
                workingCopyFactory
                (initialRepository.Put masterPDB)
        )

