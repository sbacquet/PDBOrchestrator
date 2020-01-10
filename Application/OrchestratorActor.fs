module Application.OrchestratorActor

open Akkling
open Application.OracleInstanceActor
open Domain.Orchestrator
open Application.PendingRequest
open Application.UserPendingRequest
open Domain.Common.Validation
open Application.Common
open Application.DTO.MasterPDB

type OnInstance<'T> = WithUser<string, 'T>
type OnInstance<'T1, 'T2> = WithUser<string, 'T1, 'T2>
type OnInstance<'T1, 'T2, 'T3> = WithUser<string, 'T1, 'T2, 'T3>
type OnInstance<'T1, 'T2, 'T3, 'T4> = WithUser<string, 'T1, 'T2, 'T3, 'T4>
type OnInstance<'T1, 'T2, 'T3, 'T4, 'T5> = WithUser<string, 'T1, 'T2, 'T3, 'T4, 'T5>
type OnInstance<'T1, 'T2, 'T3, 'T4, 'T5, 'T6> = WithUser<string, 'T1, 'T2, 'T3, 'T4, 'T5, 'T6>

type RequestValidation = Validation<RequestId, string>

type Command =
| GetState // responds with Application.DTO.Orchestrator
| GetInstanceState of string // responds with Application.OracleInstanceActor.StateResult
| GetMasterPDBState of string * string // responds with Application.MasterPDBActor.StateResult
| GetMasterPDBEditionInfo of string // responds with Application.MasterPDBActor.EditionInfoResult
| CreateMasterPDB of WithUser<CreateMasterPDBParams> // responds with RequestValidation
| PrepareMasterPDBForModification of WithUser<string, int> // responds with RequestValidation
| CommitMasterPDB of WithUser<string, string> // responds with RequestValidation
| RollbackMasterPDB of WithUser<string> // responds with RequestValidation
| CreateWorkingCopy of OnInstance<string, int, string, bool, bool, bool> // responds with RequestValidation
| CreateWorkingCopyOfEdition of WithUser<string, string, bool, bool> // responds with RequestValidation
| DeleteWorkingCopy of OnInstance<string> // responds with RequestValidation
| ExtendWorkingCopy of string * string // responds with Result<MasterPDBWorkingCopy, string>
| GetRequest of RequestId // responds with WithRequestId<RequestStatus>
| GetDumpTransferInfo of string // responds with Result<Application.OracleInstanceActor.DumpTransferInfo,string>

type AdminCommand =
| GetPendingChanges // responds with Result<PendingChanges option,string> = Result<pending changes if any, error>
| EnterMaintenanceMode // responds with bool = mode changes
| EnterNormalMode // responds with bool = mode changed
| IsMaintenanceMode // responds with bool = is maintenance mode
| CollectGarbage // no response
| Synchronize of string // responds with OracleInstanceActor.StateSet
| SetPrimaryOracleInstance of string // responds with Result<string, string*string> = Result<new instance, (error, current instance)>

let private pendingChangeCommandFilter mapper = function
| GetState
| GetInstanceState _
| GetMasterPDBState _
| GetMasterPDBEditionInfo _
| CreateWorkingCopy _
| DeleteWorkingCopy _
| ExtendWorkingCopy _
| GetDumpTransferInfo _
| GetRequest _ 
  -> false
| CreateMasterPDB (user, _)
| PrepareMasterPDBForModification (user, _, _)
| CommitMasterPDB (user, _, _)
| RollbackMasterPDB (user, _)
| CreateWorkingCopyOfEdition (user, _, _, _, _) 
  -> mapper user

type PendingChanges = {
    Commands : Command list
    OpenMasterPDBs : (string * EditionInfoDTO) list
}

let consPendingChanges commands openMasterPDBs = { Commands = commands |> Seq.toList; OpenMasterPDBs = openMasterPDBs }

type private Collaborators = {
    OracleInstanceActors: Map<string, IActorRef<obj>>
}

type CompletedRequestData =
| PDBName of string
| PDBVersion of int
| PDBService of string
| OracleInstance of string
| SchemaLogon of string * string
| ResourceLink of string

type RequestResult =
| CompletedOk of string * CompletedRequestData list
| CompletedWithError of string

type RequestStatus = 
| NotFound
| Pending
| Done of RequestResult * System.TimeSpan

let completedOk dataList message = CompletedOk (message, dataList)

let private spawnCollaborators parameters getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo state (ctx : Actor<_>) = 
    let spawnInstance = OracleInstanceActor.spawn parameters getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo ctx
    let collaborators = {
        OracleInstanceActors =
            state.OracleInstanceNames 
            |> List.map (fun instanceName -> 
                instanceName, spawnInstance instanceName
               )
            |> Map.ofList
    }
    collaborators.OracleInstanceActors |> Map.iter (fun _ actor -> actor |> monitor ctx |> ignore)
    collaborators

type private State = {
    Orchestrator : Orchestrator
    PreviousOrchestrator : Orchestrator
    Collaborators : Collaborators
    PendingRequests :PendingUserRequestMap<Command>
    CompletedRequests : CompletedUserRequestMap<RequestResult>
    InMaintenanceMode : bool
    Repository : IOrchestratorRepository
}

let describeCommand = function
| GetState ->
    "get PDB orchestrator state"
| GetInstanceState instance ->
    sprintf "get state of Oracle instance \"%s\"" instance
| GetMasterPDBState (instance, pdb) ->
    sprintf "get state of master PDB \"%s\" in Oracle instance \"%s\"" pdb instance
| GetMasterPDBEditionInfo pdb ->
    sprintf "get edition info of master PDB \"%s\"" pdb
| CreateMasterPDB (user, parameters) ->
    sprintf "create master PDB \"%s\" from dump \"%s\"" parameters.Name parameters.Dump
| PrepareMasterPDBForModification (user, pdb, version) ->
    sprintf "prepare master PDB \"%s\" for modifications" pdb
| CommitMasterPDB (user, pdb, comment) ->
    sprintf "commit modifications done in master PDB \"%s\"" pdb
| RollbackMasterPDB (user, pdb) ->
    sprintf "roll back modifications done in \"%s\"" pdb
| CreateWorkingCopy (user, instance, pdb, version, name, snapshot, durable, force) ->
    sprintf "create a %s working copy (%s) named \"%s\" of master PDB \"%s\" version %d on instance \"%s\"" (if durable then "durable" else "temporary") (if snapshot then "snapshot" else "clone") name pdb version instance
| DeleteWorkingCopy (user, instance, name) ->
    sprintf "delete a working copy named \"%s\" on instance \"%s\"" name instance
| CreateWorkingCopyOfEdition (user, masterPDB, wcName, durable, force) ->
    sprintf "create a %s working copy (clone) named \"%s\" of master PDB \"%s\" edition" (if durable then "durable" else "temporary") wcName masterPDB
| ExtendWorkingCopy (instance, name) ->
    sprintf "extend lifetime of working copy %s on instance %s" name instance
| GetRequest requestId ->
    sprintf "get request from id %O" requestId
| GetDumpTransferInfo instance ->
    sprintf "get dump transfer info for Oracle instance \"%s\"" instance

let describeAdminCommand = function
| GetPendingChanges ->
    "get pending changes"
| EnterMaintenanceMode ->
    "set maintenance mode"
| EnterNormalMode ->
    "set normal mode"
| IsMaintenanceMode ->
    "is maintenance mode ?"
| CollectGarbage ->
    "collect garbage"
| Synchronize withInstance ->
    sprintf "synchronize \"%s\" instance with primary" withInstance
| SetPrimaryOracleInstance instance ->
    sprintf "switch primary instance to \"%s\"" instance

let private orchestratorActorBody (parameters:Application.Parameters.Parameters) getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo (repository:IOrchestratorRepository) (ctx : Actor<_>) =

    let logRequest id command = ctx.Log.Value.Info("<< Command {0} : {1}", id, describeCommand command)
    let logRequestResponse id command completedRequest = 
        match completedRequest.Status with
        | CompletedOk _ -> 
            ctx.Log.Value.Info(
                ">> Command {0} completed in {1} s. ({2})", 
                id, 
                completedRequest.Duration.TotalSeconds, 
                describeCommand command)
        | CompletedWithError _ -> 
            ctx.Log.Value.Info(
                ">> Command {0} completed with error in {1} s. ({2})", 
                id, 
                completedRequest.Duration.TotalSeconds, 
                describeCommand command)
    let requestDone state = completeUserRequest logRequestResponse state.PendingRequests state.CompletedRequests

    let rec loop state = 
        actor {
            if state.PreviousOrchestrator <> state.Orchestrator then
                ctx.Log.Value.Debug("Persisted modified orchestrator")
                return! loop { state with Repository = state.Repository.Put state.Orchestrator; PreviousOrchestrator = state.Orchestrator }
            else

            if state.PendingRequests.Count > 0 then ctx.Log.Value.Debug("Number of pending requests : {0}", state.PendingRequests.Count)
            if state.CompletedRequests.Count > 0 then ctx.Log.Value.Debug("Number of completed requests : {0}", state.CompletedRequests.Count)

            let! (msg:obj) = ctx.Receive()

            try
                match msg with
                | :? Command as command ->
                    return! command |> handleCommand state

                | :? AdminCommand as command ->
                    return! command |> handleAdminCommand state

                | :? WithRequestId<MasterPDBCreationResult> as response ->
                    return! response |> handleResponseToCreateMasterPDB state

                | :? WithRequestId<MasterPDBActor.PrepareForModificationResult> as response ->
                    return! response |> handleResponseToPrepareMasterPDBForModification state

                | :? WithRequestId<MasterPDBActor.EditionCommitted> as response ->
                    return! response |> handleResponseToMasterPDBEditionCommit state

                | :? WithRequestId<MasterPDBActor.EditionRolledBack> as response ->
                    return! response |> handleResponseToMasterPDBEditionRollback state

                | :? WithRequestId<OracleInstanceActor.CreateWorkingCopyResult> as response ->
                    return! response |> handleResponseToCreateWorkingCopy state

                | :? Application.Oracle.OraclePDBResultWithReqId as response ->
                    return! response |> handleResponseToDeleteWorkingCopy state

                | Terminated (child, _, _) -> // Message sent when an Oracle instance actor died (could not initialize)
                    return! child |> terminatedOracleInstanceActor state
                
                | _ -> 
                    return! loop state

            with ex -> 
                ctx.Log.Value.Error("Unexpected error : {0}", ex)
                return! loop state
        }

    and handleCommand state command = 
        let sender = ctx.Sender().Retype<RequestValidation>()
        let pendingChangeCommandAcceptable user = UserRights.isAdmin (UserRights.normalUser user)
        actor {
            // Check if command is compatible with maintenance mode
            if state.InMaintenanceMode && pendingChangeCommandFilter (not << pendingChangeCommandAcceptable) command then
                sender <! RequestValidation.Invalid [ "the command cannot be run in maintenance mode" ]
                return! loop state
            else

            let getInstanceName instanceName = if instanceName = "primary" then state.Orchestrator.PrimaryInstance else instanceName
            
            match command with
            | GetState ->
                let! orchestratorDTO = state.Orchestrator |> DTO.Orchestrator.toDTO (state.Collaborators.OracleInstanceActors |> Map.map (fun _ a -> a.Retype<OracleInstanceActor.Command>()))
                ctx.Sender() <! orchestratorDTO
                return! loop state

            | GetInstanceState instanceName ->
                let sender = ctx.Sender().Retype<Application.OracleInstanceActor.StateResult>()
                let instanceName = getInstanceName instanceName
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance:IActorRef<OracleInstanceActor.Command> = retype state.Collaborators.OracleInstanceActors.[instanceName]
                    instance <<! OracleInstanceActor.GetState
                | None ->
                    sender <! OracleInstanceActor.stateError (sprintf "cannot find Oracle instance %s" instanceName)
                return! loop state

            | GetMasterPDBState (instanceName, pdb) ->
                let sender = ctx.Sender().Retype<MasterPDBActor.StateResult>()
                let instanceName = getInstanceName instanceName
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance:IActorRef<OracleInstanceActor.Command> = retype state.Collaborators.OracleInstanceActors.[instanceName]
                    instance <<! OracleInstanceActor.GetMasterPDBState pdb
                | None ->
                    sender <! MasterPDBActor.stateError (sprintf "cannot find Oracle instance %s" instanceName)
                return! loop state

            | GetMasterPDBEditionInfo pdb ->
                let sender = ctx.Sender().Retype<MasterPDBActor.EditionInfoResult>()
                let instanceName = getInstanceName "primary"
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance:IActorRef<OracleInstanceActor.Command> = retype state.Collaborators.OracleInstanceActors.[instanceName]
                    instance <<! OracleInstanceActor.GetMasterPDBEditionInfo pdb
                | None ->
                    let error:MasterPDBActor.EditionInfoResult = sprintf "cannot find Oracle instance %s" instanceName |> Error
                    sender <! error
                return! loop state

            | CreateMasterPDB (user, parameters) ->
                let primaryInstance = state.Collaborators.OracleInstanceActors.[state.Orchestrator.PrimaryInstance]
                let requestId = newRequestId()
                let newPendingRequests = state.PendingRequests |> registerUserRequest logRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.CreateMasterPDB (requestId, parameters)
                sender <! Valid requestId
                return! loop { state with PendingRequests = newPendingRequests }

            | PrepareMasterPDBForModification (user, pdb, version) ->
                let primaryInstance = state.Collaborators.OracleInstanceActors.[state.Orchestrator.PrimaryInstance]
                let requestId = newRequestId()
                let newPendingRequests = state.PendingRequests |> registerUserRequest logRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.PrepareMasterPDBForModification (requestId, pdb, version, user)
                sender <! Valid requestId
                return! loop { state with PendingRequests = newPendingRequests }

            | CommitMasterPDB (user, pdb, comment) ->
                let primaryInstance = state.Collaborators.OracleInstanceActors.[state.Orchestrator.PrimaryInstance]
                let requestId = newRequestId()
                let newPendingRequests = state.PendingRequests |> registerUserRequest logRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.CommitMasterPDB (requestId, pdb, user, comment)
                sender <! Valid requestId
                return! loop { state with PendingRequests = newPendingRequests }

            | RollbackMasterPDB (user, pdb) ->
                let primaryInstance = state.Collaborators.OracleInstanceActors.[state.Orchestrator.PrimaryInstance]
                let requestId = newRequestId()
                let newPendingRequests = state.PendingRequests |> registerUserRequest logRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.RollbackMasterPDB (requestId, user, pdb)
                sender <! Valid requestId
                return! loop { state with PendingRequests = newPendingRequests }

            | CreateWorkingCopy (user, instanceName, masterPDBName, versionNumber, wcName, snapshot, durable, force) ->
                let instanceName = getInstanceName instanceName
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance = state.Collaborators.OracleInstanceActors.[instanceName]
                    let requestId = newRequestId()
                    let newPendingRequests = state.PendingRequests |> registerUserRequest logRequest requestId command user
                    retype instance <! Application.OracleInstanceActor.CreateWorkingCopy (requestId, user, masterPDBName, versionNumber, wcName, snapshot, durable, force)
                    sender <! Valid requestId
                    return! loop { state with PendingRequests = newPendingRequests }
                | None ->
                    sender <! RequestValidation.Invalid [ sprintf "cannot find Oracle instance %s" instanceName ]
                    return! loop state

            | DeleteWorkingCopy (user, instanceName, wcName) ->
                let instanceName = getInstanceName instanceName
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance = state.Collaborators.OracleInstanceActors.[instanceName]
                    let requestId = newRequestId()
                    let newPendingRequests = state.PendingRequests |> registerUserRequest logRequest requestId command user
                    retype instance <! Application.OracleInstanceActor.DeleteWorkingCopy (requestId, wcName)
                    sender <! Valid requestId
                    return! loop { state with PendingRequests = newPendingRequests }
                | None ->
                    sender <! RequestValidation.Invalid [ sprintf "cannot find Oracle instance %s" instanceName ]
                    return! loop state

            | CreateWorkingCopyOfEdition (user, masterPDBName, wcName, durable, force) ->
                let instanceName = getInstanceName "primary"
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance = state.Collaborators.OracleInstanceActors.[instanceName]
                    let requestId = newRequestId()
                    let newPendingRequests = state.PendingRequests |> registerUserRequest logRequest requestId command user
                    retype instance <! Application.OracleInstanceActor.CreateWorkingCopyOfEdition (requestId, user, masterPDBName, wcName, durable, force)
                    sender <! Valid requestId
                    return! loop { state with PendingRequests = newPendingRequests }
                | None ->
                    sender <! RequestValidation.Invalid [ sprintf "cannot find Oracle instance %s" instanceName ]
                    return! loop state

            | ExtendWorkingCopy (instanceName, name) ->
                let sender = ctx.Sender().Retype<Result<Domain.MasterPDBWorkingCopy.MasterPDBWorkingCopy, string>>()
                let instanceName = getInstanceName instanceName
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance:IActorRef<OracleInstanceActor.Command> = retype state.Collaborators.OracleInstanceActors.[instanceName]
                    instance <<! OracleInstanceActor.ExtendWorkingCopy name
                | None ->
                    sender <! (sprintf "cannot find Oracle instance %s" instanceName |> Error)
                return! loop state

            | GetRequest requestId ->
                let sender = ctx.Sender().Retype<WithRequestId<RequestStatus>>()
                let requestMaybe = state.PendingRequests |> Map.tryFind requestId
                match requestMaybe with
                | Some _ ->
                    sender <! (requestId, Pending)
                    return! loop state
                | None -> 
                    let requestMaybe = state.CompletedRequests |> Map.tryFind requestId
                    match requestMaybe with
                    | None -> 
                        sender <! (requestId, NotFound)
                        return! loop state
                    | Some request ->
                        sender <! (requestId, Done (request.Status, request.Duration))
                        ctx.Log.Value.Debug("Request {requestId} completed => removed from the list", requestId)
                        return! loop { state with CompletedRequests = state.CompletedRequests |> Map.remove requestId }

            | GetDumpTransferInfo instanceName ->
                let sender = ctx.Sender().Retype<Result<OracleInstanceActor.DumpTransferInfo, string>>()
                let instanceName = getInstanceName instanceName
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance:IActorRef<OracleInstanceActor.Command> = retype state.Collaborators.OracleInstanceActors.[instanceName]
                    let! (transferInfo:OracleInstanceActor.DumpTransferInfo) = instance <? OracleInstanceActor.GetDumpTransferInfo
                    sender <! Ok transferInfo
                | None ->
                    sender <! (sprintf "cannot find Oracle instance %s" instanceName |> Error)
                return! loop state
        }

    and handleAdminCommand state command = 
        ctx.Log.Value.Info("Received admin command : {0}", describeAdminCommand command)
        let getPendingChanges () = async {
            let pendingChangeCommands = 
                state.PendingRequests 
                |> Map.toSeq
                |> Seq.map (fun (_, request) -> request.Command)
                |> Seq.filter (pendingChangeCommandFilter (fun _ -> true))
                
            let primaryInstance:IActorRef<OracleInstanceActor.Command> = retype state.Collaborators.OracleInstanceActors.[state.Orchestrator.PrimaryInstance]
            let! (primaryInstanceState:OracleInstanceActor.StateResult) = primaryInstance <? OracleInstanceActor.GetState
            return
                primaryInstanceState 
                |> Result.map (fun state -> 
                    state.MasterPDBs 
                    |> List.filter (fun pdb -> pdb.EditionState |> Option.isSome)
                    |> List.map (fun pdb -> pdb.Name, pdb.EditionState.Value)
                    )
                |> Result.map (fun openMasterPDBs ->
                    if (openMasterPDBs.IsEmpty && Seq.isEmpty pendingChangeCommands) then    
                        None
                    else
                        Some <| consPendingChanges pendingChangeCommands openMasterPDBs
                    )
        }
        actor {
            match command with
            | GetPendingChanges ->
                let! result = getPendingChanges()
                ctx.Sender() <! result
                return! loop state

            | EnterMaintenanceMode ->
                if (state.InMaintenanceMode) then
                    ctx.Sender() <! false
                    ctx.Log.Value.Warning("The server is already in maintenance mode.")
                    return! loop state
                else
                    ctx.Sender() <! true
                    ctx.Log.Value.Warning("The server is now in maintenance mode.")
                    return! loop { state with InMaintenanceMode = true }

            | IsMaintenanceMode ->
                ctx.Sender() <! state.InMaintenanceMode
                return! loop state

            | CollectGarbage ->
                ctx.Log.Value.Info("Garbage collection requested")
                state.Collaborators.OracleInstanceActors |> Map.iter (fun _ actor -> retype actor <! OracleInstanceActor.CollectGarbage)
                return! loop state

            | EnterNormalMode ->
                if (state.InMaintenanceMode) then
                    ctx.Sender() <! true
                    ctx.Log.Value.Info("The server is now in normal mode.")
                    return! 
                        loop { state with InMaintenanceMode = false }
                else
                    ctx.Log.Value.Info("The server is already in normal mode.")
                    ctx.Sender() <! false
                    return! loop state

            | Synchronize targetInstance ->
                if not state.InMaintenanceMode then
                    ctx.Sender() <! stateError "the server must be in maintenance mode"
                else
                    match state.Orchestrator |> containsOracleInstance targetInstance with
                    | None ->
                        ctx.Sender() <! stateError (sprintf "cannot find Oracle instance %s" targetInstance)
                    | Some targetInstance ->
                        let pendingChangesMaybe = getPendingChanges() |> runWithinElseDefaultError parameters.ShortTimeout
                        match pendingChangesMaybe with
                        | Ok (Ok None) ->
                            let primaryInstance = state.Collaborators.OracleInstanceActors.[state.Orchestrator.PrimaryInstance]
                            let target = state.Collaborators.OracleInstanceActors.[targetInstance]
                            retype primaryInstance <<! TransferInternalState target
                        | Ok (Ok (Some _)) ->
                            ctx.Sender() <! stateError "primary instance has pending changes"
                        | Ok (Error error)
                        | Error error ->
                            ctx.Sender() <! stateError (sprintf "cannot get pending changes : %s" error)
                return! loop state

            | SetPrimaryOracleInstance newPrimary ->
                let sender = ctx.Sender().Retype<Result<string,string*string>>()
                if not state.InMaintenanceMode then
                    sender <! Error ("the server must be in maintenance mode", state.Orchestrator.PrimaryInstance)
                    return! loop state
                elif (state.Orchestrator.PrimaryInstance = newPrimary) then
                    sender <! Error (sprintf "%s is already the primary Oracle instance" newPrimary, state.Orchestrator.PrimaryInstance)
                    return! loop state
                else
                    match state.Orchestrator |> containsOracleInstance newPrimary with
                    | None ->
                        sender <! Error (sprintf "cannot find Oracle instance %s" newPrimary, state.Orchestrator.PrimaryInstance)
                        return! loop state
                    | Some newPrimary ->
                        let! pendingChangesMaybe = getPendingChanges()
                        match pendingChangesMaybe with
                        | Ok None ->
                            sender <! Ok newPrimary
                            return! loop { state with Orchestrator = { state.Orchestrator with PrimaryInstance = newPrimary } }
                        | Ok (Some _) ->
                            sender <! Error ("primary instance has pending changes", state.Orchestrator.PrimaryInstance)
                            return! loop state
                        | Error error ->
                            sender <! Error (sprintf "cannot get pending changes : %s" error, state.Orchestrator.PrimaryInstance)
                            return! loop state
         }

    and handleResponseToCreateMasterPDB state response =
        let (requestId, result) = response
        let requestMaybe = state.PendingRequests |> Map.tryFind requestId
        actor {
            match requestMaybe with
            | None -> 
                ctx.Log.Value.Error("internal error : request {requestId} not found", requestId)
                return! loop state
            | Some request ->
                let status = 
                    match result with
                    | InvalidRequest errors -> 
                        CompletedWithError (sprintf "Invalid request : %s." (System.String.Join("; ", errors |> List.toArray)))
                    | MasterPDBCreationFailure (instance, pdb, error) -> 
                        CompletedWithError (sprintf "Error while creating master PDB %s on Oracle instance %s : %s." pdb instance error)
                    | MasterPDBCreated (instance, pdb) ->
                        sprintf "Master PDB %s created successfully on Oracle instance %s." pdb.Name instance
                        |> completedOk [ 
                            PDBName pdb.Name
                            ResourceLink (sprintf "/instances/%s/master-pdbs/%s" instance pdb.Name)
                        ]
                let (newPendingRequests, newCompletedRequests) = requestDone state request status
                return! loop { state with PendingRequests = newPendingRequests; CompletedRequests = newCompletedRequests }
        }

    and handleResponseToPrepareMasterPDBForModification state response =
        let (requestId, result) = response
        let requestMaybe = state.PendingRequests |> Map.tryFind requestId
        actor {
            match requestMaybe with
            | None -> 
                ctx.Log.Value.Error("internal error : request {requestId} not found", requestId)
                return! loop state
            | Some request ->
                let status = 
                    match result with
                    | MasterPDBActor.Prepared (instance, pdb, editionPDB, editionPDBService, schemas) -> 
                        let schemasData = schemas |> List.map SchemaLogon
                        sprintf "Master PDB %s prepared successfully for edition." pdb.Name 
                        |> completedOk ([ 
                            PDBName editionPDB
                            PDBService editionPDBService
                            ResourceLink (sprintf "/instances/%s/master-pdbs/%s/edition" instance pdb.Name)
                           ] @ schemasData)
                    | MasterPDBActor.PreparationFailure (pdb, error) -> 
                        sprintf "Error while preparing master PDB %s for edition : %s." pdb error |> CompletedWithError
                let (newPendingRequests, newCompletedRequests) = requestDone state request status
                return! loop { state with PendingRequests = newPendingRequests; CompletedRequests = newCompletedRequests }
        }

    and handleResponseToMasterPDBEditionCommit state response =
        let (requestId, result) = response
        let requestMaybe = state.PendingRequests |> Map.tryFind requestId
        actor {
            match requestMaybe with
            | None -> 
                ctx.Log.Value.Error("internal error : request {requestId} not found", requestId)
                return! loop state
            | Some request ->
                let status = 
                    match result with
                    | Ok (instance, pdb, newVersion) -> 
                        sprintf "Master PDB %s committed successfully (new version %d created) on Oracle instance %s." pdb.Name newVersion instance
                        |> completedOk [ 
                            PDBName pdb.Name
                            PDBVersion newVersion
                            ResourceLink (sprintf "/instances/%s/master-pdbs/%s/versions/%d" instance pdb.Name newVersion)
                        ]
                    | Error error -> 
                        sprintf "Error while committing master PDB : %s." error |> CompletedWithError
                let (newPendingRequests, newCompletedRequests) = requestDone state request status
                return! loop { state with PendingRequests = newPendingRequests; CompletedRequests = newCompletedRequests }
        }

    and handleResponseToMasterPDBEditionRollback state response =
        let (requestId, result) = response
        let requestMaybe = state.PendingRequests |> Map.tryFind requestId
        actor {
            match requestMaybe with
            | None -> 
                ctx.Log.Value.Error("internal error : request {requestId} not found", requestId)
                return! loop state
            | Some request ->
                let status = 
                    match result with
                    | Ok (instance, pdb) -> 
                        let latestVersion = pdb |> Domain.MasterPDB.getLatestAvailableVersionNumber
                        sprintf "Master PDB %s rolled back successfully to version %d on Oracle instance %s." pdb.Name latestVersion instance
                        |> completedOk [ 
                            PDBName pdb.Name 
                            ResourceLink (sprintf "/instances/%s/master-pdbs/%s/versions/%d" instance pdb.Name latestVersion)
                        ]
                    | Error error -> 
                        sprintf "Error while rolling back master PDB : %s." error |> CompletedWithError
                let (newPendingRequests, newCompletedRequests) = requestDone state request status
                return! loop { state with PendingRequests = newPendingRequests; CompletedRequests = newCompletedRequests }
        }
    
    and handleResponseToCreateWorkingCopy state response =
        let (requestId, result) = response
        let requestMaybe = state.PendingRequests |> Map.tryFind requestId
        actor {
            match requestMaybe with
            | None -> 
                ctx.Log.Value.Error("internal error : request {requestId} not found", requestId)
                return! loop state
            | Some request ->
                let status = 
                    match result with
                    | Ok (pdb, versionNumber, workingCopyName, workingCopyService, oracleInstance) -> 
                        sprintf "Working copy of PDB %s version %d created successfully with name %s." pdb versionNumber workingCopyName
                        |> completedOk [ 
                            PDBName workingCopyName
                            PDBService workingCopyService
                            OracleInstance oracleInstance
                            ResourceLink (sprintf "/instances/%s/working-copies/%s" oracleInstance workingCopyName)
                        ]
                    | Error error -> 
                        sprintf "Error while creating working copy : %s." error |> CompletedWithError
                let (newPendingRequests, newCompletedRequests) = requestDone state request status
                return! loop { state with PendingRequests = newPendingRequests; CompletedRequests = newCompletedRequests }
        }

    and handleResponseToDeleteWorkingCopy state response =
        let (requestId, result) = response
        let requestMaybe = state.PendingRequests |> Map.tryFind requestId
        actor {
            match requestMaybe with
            | None -> 
                ctx.Log.Value.Error("internal error : request {requestId} not found", requestId)
                return! loop state
            | Some request ->
                let status = 
                    match result with
                    | Ok wcName -> 
                        sprintf "Working copy %s deleted successfully." wcName
                        |> completedOk [ PDBName wcName ]
                    | Error error -> 
                        sprintf "Error while deleting working copy : %s." error.Message |> CompletedWithError
                let (newPendingRequests, newCompletedRequests) = requestDone state request status
                return! loop { state with PendingRequests = newPendingRequests; CompletedRequests = newCompletedRequests }
        }

    and terminatedOracleInstanceActor state oracleInstanceActor =
        let terminatedInstanceNameMaybe = state.Collaborators.OracleInstanceActors |> Map.tryFindKey (fun _ actorRef -> oracleInstanceActor = actorRef)
        let newCollabs = 
            terminatedInstanceNameMaybe
            |> Option.map (fun name -> 
                ctx.Log.Value.Warning("Actor for Oracle instance {0} was terminated => disabled it", name)
                { state.Collaborators with OracleInstanceActors = state.Collaborators.OracleInstanceActors |> Map.remove name })
            |> Option.defaultValue state.Collaborators
        let newInstances = newCollabs.OracleInstanceActors |> Map.toList |> List.map fst
        actor {
            if not (newInstances |> List.contains state.Orchestrator.PrimaryInstance) then
                ctx.Log.Value.Error("Primary Oracle instance is down => stopping the orchestrator...")
                return! Stop
            else
                return! loop { state with Collaborators = newCollabs; Orchestrator = { state.Orchestrator with OracleInstanceNames = newInstances } }
        }
    
    let initialState = repository.Get()
    let collaborators = ctx |> spawnCollaborators parameters getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo initialState

    loop { 
        Orchestrator = initialState
        PreviousOrchestrator = initialState
        Collaborators = collaborators
        PendingRequests = Map.empty
        CompletedRequests = Map.empty
        InMaintenanceMode = false 
        Repository = repository
    }


let [<Literal>]cOrchestratorActorName = "Orchestrator"

let spawn parameters getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo (repository:IOrchestratorRepository) actorFactory =
    let actor = 
        Akkling.Spawn.spawn actorFactory cOrchestratorActorName 
        <| props (orchestratorActorBody parameters getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo repository)
    actor.Retype<Command>()
