module Application.OrchestratorActor

open Akkling
open Application.OracleInstanceActor
open Domain.Orchestrator
open Application.PendingRequest
open Application.UserPendingRequest
open Domain.Common.Validation
open Application.Common
open Application.DTO.MasterPDB
open Domain.MasterPDBWorkingCopy
open Domain.Common

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
| GetInstanceBasicState of string // responds with Application.OracleInstanceActor.BasicStateResult
| GetMasterPDBState of string * string // responds with Application.MasterPDBActor.StateResult
| GetMasterPDBEditionInfo of string // responds with Application.MasterPDBActor.EditionInfoResult
| CreateMasterPDB of WithUser<CreateMasterPDBParams> // responds with RequestValidation
| PrepareMasterPDBForModification of WithUser<string, int> // responds with RequestValidation
| CommitMasterPDB of WithUser<string, string> // responds with RequestValidation
| RollbackMasterPDB of WithUser<string> // responds with RequestValidation
| CreateWorkingCopy of OnInstance<string, int, string, bool, bool, bool> // responds with RequestValidation
| CreateWorkingCopyOfEdition of WithUser<string, string, bool, bool> // responds with RequestValidation
| DeleteWorkingCopy of OnInstance<string, bool> // responds with RequestValidation
| ExtendWorkingCopy of string * string // responds with Result<MasterPDBWorkingCopy, string>
| GetRequest of RequestId // responds with WithRequestId<RequestStatus>
| DeleteRequest of RequestId // no response
| GetDumpTransferInfo of string // responds with Result<Application.OracleInstanceActor.DumpTransferInfo,string>

type AdminCommand =
| GetPendingChanges // responds with Result<PendingChanges option,string> = Result<pending changes if any, error>
| GetPendingCommands // responds with Command list
| EnterMaintenanceMode // responds with bool = mode changes
| EnterNormalMode // responds with bool = mode changed
| IsMaintenanceMode // responds with bool = is maintenance mode
| CollectGarbage // no response
| Synchronize of string // responds with OracleInstanceActor.StateSet
| SetPrimaryOracleInstance of string // responds with Result<string, string*string> = Result<new instance, (error, current instance)>
| DeleteMasterPDBVersion of string * int * bool // responds with DeleteVersionResult
| CollectInstanceGarbage of string // no response
| SwitchLock of string // responds with Result<bool,string>
| MasterPDBVersionSynchronizedWithPrimary of string * string * int // responds with Application.MasterPDBActor.DeleteVersionResult

let private pendingChangeCommandFilter mapper = function
// Commands that do not impact Oracle instances and master PDBs
| GetState
| GetInstanceState _
| GetInstanceBasicState _
| GetMasterPDBState _
| GetMasterPDBEditionInfo _
| ExtendWorkingCopy _
| GetDumpTransferInfo _
| GetRequest _ 
| DeleteRequest _
    -> false
// Commands that impact Oracle instances and master PDBs
| CreateMasterPDB (user, _)
| PrepareMasterPDBForModification (user, _, _)
| CommitMasterPDB (user, _, _)
| RollbackMasterPDB (user, _)
    -> mapper user
// Requests for durable copies are considered as pending changes,
// so that there is no risk they are lost when switching servers (server is then in maintenance mode)
// Only temporary copies can be created when in maintenance mode, so that continuous integration is not blocked.
| CreateWorkingCopy (user, _, _, _, _, _, durable, _)
| CreateWorkingCopyOfEdition (user, _, _, durable, _) 
| DeleteWorkingCopy (user, _, _, durable)
    -> durable && mapper user

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

let completedOk dataList message = CompletedOk (message, dataList)

type RequestStatus = 
| NotFound
| Pending
| Done of RequestResult * System.TimeSpan

type DeleteVersionResult = Result<(string * string) list, string> // instances where version was not deleted

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
| GetInstanceBasicState instance ->
    sprintf "get basic state of Oracle instance \"%s\"" instance
| GetMasterPDBState (instance, pdb) ->
    sprintf "get state of master PDB \"%s\" in Oracle instance \"%s\"" pdb instance
| GetMasterPDBEditionInfo pdb ->
    sprintf "get edition info of master PDB \"%s\"" pdb
| CreateMasterPDB (_, parameters) ->
    sprintf "create master PDB \"%s\" from dump \"%s\"" parameters.Name parameters.Dump
| PrepareMasterPDBForModification (_, pdb, version) ->
    sprintf "prepare master PDB \"%s\" version %d for modifications" pdb version
| CommitMasterPDB (_, pdb, _) ->
    sprintf "commit modifications done in master PDB \"%s\"" pdb
| RollbackMasterPDB (_, pdb) ->
    sprintf "roll back modifications done in \"%s\"" pdb
| CreateWorkingCopy (_, instance, pdb, version, name, _, durable, force) ->
    sprintf "create%s a %s working copy named \"%s\" of master PDB \"%s\" version %d on Oracle instance \"%s\"" (if force then " (force)" else "") (Lifetime.text durable) name pdb version instance
| DeleteWorkingCopy (_, instance, name, durable) ->
    sprintf "delete a %s working copy named \"%s\" on Oracle instance \"%s\"" (Lifetime.text durable) name instance
| CreateWorkingCopyOfEdition (_, masterPDB, wcName, durable, force) ->
    sprintf "create%s a %s working copy (clone) named \"%s\" of master PDB \"%s\" edition" (if force then " (force)" else "") (Lifetime.text durable) wcName masterPDB
| ExtendWorkingCopy (instance, name) ->
    sprintf "extend lifetime of working copy %s on Oracle instance %s" name instance
| GetRequest requestId ->
    sprintf "get request from id %O" requestId
| DeleteRequest requestId ->
    sprintf "delete request with id %O" requestId
| GetDumpTransferInfo instance ->
    sprintf "get dump transfer info for Oracle instance \"%s\"" instance

let describeAdminCommand = function
| GetPendingChanges ->
    "get pending changes"
| GetPendingCommands ->
    "get pending commands"
| EnterMaintenanceMode ->
    "set maintenance mode"
| EnterNormalMode ->
    "set normal mode"
| IsMaintenanceMode ->
    "is maintenance mode ?"
| CollectGarbage ->
    "collect garbage of all Oracle instances"
| CollectInstanceGarbage instance ->
    sprintf "collect garbage of Oracle instance %s" instance
| Synchronize withInstance ->
    sprintf "synchronize \"%s\" Oracle instance with primary" withInstance
| SetPrimaryOracleInstance instance ->
    sprintf "switch primary Oracle instance to \"%s\"" instance
| DeleteMasterPDBVersion (pdb, version, force) ->
    sprintf "delete version %d of master PDB \"%s\"" version pdb
| SwitchLock pdb ->
    sprintf "switch lock of master PDB %s" pdb
| MasterPDBVersionSynchronizedWithPrimary (instance, pdb, version) ->
    sprintf "synchronized version %d of master PDB \"%s\" on instance \"%s\"" version pdb instance

let private orchestratorActorBody (parameters:Application.Parameters.Parameters) getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo (repository:IOrchestratorRepository) (ctx : Actor<_>) =

    let registerUserRequest = 
        let logRequest id command (user:User) = ctx.Log.Value.Info("<< Command {requestId} from user {user} : {command}", id, user.Name, describeCommand command)
        registerUserRequest logRequest

    let requestDone state = 
        let logRequestResponse id command completedRequest = 
            match completedRequest.Status with
            | CompletedOk _ -> 
                ctx.Log.Value.Info(
                    ">> Command {requestId} completed in {duration} s. ({description})", 
                    id, 
                    completedRequest.Duration.TotalSeconds, 
                    describeCommand command)
            | CompletedWithError error -> 
                ctx.Log.Value.Info(
                    ">> Command {requestId} completed with error in {duration} s. ({description})", 
                    id, 
                    completedRequest.Duration.TotalSeconds, 
                    describeCommand command)
                ctx.Log.Value.Info("Error in command {requestId} was : {error}", id, error)
        completeUserRequest logRequestResponse state.PendingRequests state.CompletedRequests

    let deleteRequest state =
        let logRequestDeleted id command = 
            ctx.Log.Value.Warning(
                "^^ Command {requestId} deleted. ({description})", 
                id, 
                describeCommand command)
        deletePendingRequest logRequestDeleted state.PendingRequests state.CompletedRequests

    let instanceActor state instanceName : IActorRef<OracleInstanceActor.Command> = 
        retype state.Collaborators.OracleInstanceActors.[instanceName]

    let primaryInstance state = instanceActor state state.Orchestrator.PrimaryInstance

    let getInstanceName state instanceName = if instanceName = "primary" then state.Orchestrator.PrimaryInstance else instanceName

    let rec loop state = 

        actor {

            if state.PreviousOrchestrator <> state.Orchestrator then
                ctx.Log.Value.Debug("Persisted modified orchestrator")
                return! loop { state with Repository = state.Repository.Put state.Orchestrator; PreviousOrchestrator = state.Orchestrator }
            else 

            if ctx.Log.Value.IsDebugEnabled then
                let count = state.PendingRequests |> alivePendingRequests |> Seq.length in 
                    if count > 0 then ctx.Log.Value.Debug("Number of pending requests : {0}", count)
                let count = state.CompletedRequests.Count in 
                    if count > 0 then ctx.Log.Value.Debug("Number of completed requests : {0}", count)
            
            let! (msg:obj) = ctx.Receive()

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
        
        }

    and handleCommand state command = 
        let sender = ctx.Sender().Retype<RequestValidation>()
        let pendingChangeCommandAcceptable user = UserRights.isAdmin user
        let instanceActor = instanceActor state
        let primaryInstance = primaryInstance state
        let getInstanceName = getInstanceName state
        let validateWorkingCopyName (name:string) : Validation<string, string> =
            let name = name.ToUpper()
            let regex = [
                System.Text.RegularExpressions.Regex("^.+_EDITION$")
                System.Text.RegularExpressions.Regex("^.+_V\\d{3}_.+$")
            ]
            if regex |> List.exists (fun regex -> regex.IsMatch(name)) then
                Invalid [ sprintf "%s is not a valid working copy PDB name : \"*_EDITION\" and \"*_Vnnn_*\" are reserved" name ]
            else
                Valid name
        let instancesMatching (user:User) = function
        | "primary" -> [ state.Orchestrator.PrimaryInstance ]
        | "auto" -> state.Orchestrator.OracleInstanceNames |> user.getOracleInstanceAffinity
        | instanceName -> [ instanceName ]

        let acknowledge instanceName user validateRequest = actor {
            let requestId = newRequestId()
            let requestRef:RequestRef = (requestId, ctx.Self)
            let foldInstance instanceValidation instanceName : Validation<string, string> =
                match instanceValidation with
                | Valid _ -> instanceValidation
                | Invalid errors ->
                    let instance = instanceActor instanceName
                    let validationResult : Result<RequestValidation, string> = instance <? (validateRequest requestRef) |> runWithinElseDefaultError parameters.ShortTimeout
                    match validationResult with
                    | Ok validation ->
                        match validation with
                        | RequestValidation.Valid _ -> Valid instanceName
                        | RequestValidation.Invalid errs -> Invalid (errs @ errors)
                    | Error error -> Invalid ((sprintf "instance %s : %s" instanceName error) :: errors)
            let instancesOrderedByAffinity = instancesMatching user instanceName
            let instanceValidation = instancesOrderedByAffinity |> List.fold foldInstance (Invalid [])
            match instanceValidation with
            | Valid instanceName -> 
                ctx.Log.Value.Debug("Request {0} run on instance {1}", requestId, instanceName)
                retype (ctx.Sender()) <! RequestValidation.Valid requestId
                let newPendingRequests = state.PendingRequests |> registerUserRequest requestId command user
                return! loop { state with PendingRequests = newPendingRequests }
            | Invalid errors ->
                retype (ctx.Sender()) <! RequestValidation.Invalid errors
                return! loop state
        }

        actor {
            // Check if command is compatible with maintenance mode
            if state.InMaintenanceMode && pendingChangeCommandFilter (not << pendingChangeCommandAcceptable) command then
                sender <! RequestValidation.Invalid [ "the command cannot be run in maintenance mode" ]
                return! loop state
            else

            match command with
            | GetState ->
                let orchestratorDTO = state.Orchestrator |> DTO.Orchestrator.toDTO
                ctx.Sender() <! orchestratorDTO
                return! loop state

            | GetInstanceState instanceName ->
                let sender = ctx.Sender().Retype<Application.OracleInstanceActor.StateResult>()
                let instanceName = getInstanceName instanceName
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance = instanceActor instanceName
                    instance <<! OracleInstanceActor.GetState
                | None ->
                    sender <! OracleInstanceActor.stateError (sprintf "cannot find Oracle instance %s" instanceName)
                return! loop state

            | GetInstanceBasicState instanceName ->
                let sender = ctx.Sender().Retype<Application.OracleInstanceActor.BasicStateResult>()
                let instanceName = getInstanceName instanceName
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance = instanceActor instanceName
                    instance <<! OracleInstanceActor.GetBasicState
                | None ->
                    sender <! Error (sprintf "cannot find Oracle instance %s" instanceName)
                return! loop state

            | GetMasterPDBState (instanceName, pdb) ->
                let sender = ctx.Sender().Retype<MasterPDBActor.StateResult>()
                let instanceName = getInstanceName instanceName
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance = instanceActor instanceName
                    instance <<! OracleInstanceActor.GetMasterPDBState pdb
                | None ->
                    sender <! MasterPDBActor.stateError (sprintf "cannot find Oracle instance %s" instanceName)
                return! loop state

            | GetMasterPDBEditionInfo pdb ->
                let sender = ctx.Sender().Retype<MasterPDBActor.EditionInfoResult>()
                let instanceName = getInstanceName "primary"
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance = instanceActor instanceName
                    instance <<! OracleInstanceActor.GetMasterPDBEditionInfo pdb
                | None ->
                    let error:MasterPDBActor.EditionInfoResult = sprintf "cannot find Oracle instance %s" instanceName |> Error
                    sender <! error
                return! loop state

            | CreateMasterPDB (user, parameters) ->
                let requestId = newRequestId()
                let newPendingRequests = state.PendingRequests |> registerUserRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.CreateMasterPDB (requestId, parameters)
                sender <! Valid requestId
                return! loop { state with PendingRequests = newPendingRequests }

            | PrepareMasterPDBForModification (user, pdb, version) ->
                let requestId = newRequestId()
                let newPendingRequests = state.PendingRequests |> registerUserRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.PrepareMasterPDBForModification (requestId, pdb, version, user)
                sender <! Valid requestId
                return! loop { state with PendingRequests = newPendingRequests }

            | CommitMasterPDB (user, pdb, comment) ->
                let requestId = newRequestId()
                let newPendingRequests = state.PendingRequests |> registerUserRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.CommitMasterPDB (requestId, pdb, user, comment)
                sender <! Valid requestId
                return! loop { state with PendingRequests = newPendingRequests }

            | RollbackMasterPDB (user, pdb) ->
                let requestId = newRequestId()
                let newPendingRequests = state.PendingRequests |> registerUserRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.RollbackMasterPDB (requestId, user, pdb)
                sender <! Valid requestId
                return! loop { state with PendingRequests = newPendingRequests }

            | CreateWorkingCopy (user, instanceName, masterPDBName, versionNumber, wcName, snapshot, durable, force) ->
                let validatedName = validateWorkingCopyName wcName
                match validatedName with
                | Valid wcName ->
                    return! acknowledge instanceName user (fun requestRef -> Application.OracleInstanceActor.CreateWorkingCopy (requestRef, user, masterPDBName, versionNumber, wcName, snapshot, durable, force))
                | Invalid errors ->
                    sender <! RequestValidation.Invalid errors
                    return! loop state

            | DeleteWorkingCopy (user, instanceName, wcName, durable) ->
                let instanceName = getInstanceName instanceName
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance = instanceActor instanceName
                    let requestId = newRequestId()
                    let newPendingRequests = state.PendingRequests |> registerUserRequest requestId command user
                    instance <! Application.OracleInstanceActor.DeleteWorkingCopy (requestId, wcName, durable)
                    sender <! Valid requestId
                    return! loop { state with PendingRequests = newPendingRequests }
                | None ->
                    sender <! RequestValidation.Invalid [ sprintf "cannot find Oracle instance %s" instanceName ]
                    return! loop state

            | CreateWorkingCopyOfEdition (user, masterPDBName, wcName, durable, force) ->
                let validatedName = validateWorkingCopyName wcName
                match validatedName with
                | Valid wcName ->
                    let instanceName = getInstanceName "primary"
                    match state.Orchestrator |> containsOracleInstance instanceName with
                    | Some instanceName ->
                        let instance = instanceActor instanceName
                        let requestId = newRequestId()
                        let newPendingRequests = state.PendingRequests |> registerUserRequest requestId command user
                        instance <! Application.OracleInstanceActor.CreateWorkingCopyOfEdition (requestId, user, masterPDBName, wcName, durable, force)
                        sender <! Valid requestId
                        return! loop { state with PendingRequests = newPendingRequests }
                    | None ->
                        sender <! RequestValidation.Invalid [ sprintf "cannot find Oracle instance %s" instanceName ]
                        return! loop state
                | Invalid errors ->
                    sender <! RequestValidation.Invalid errors
                    return! loop state

            | ExtendWorkingCopy (instanceName, name) ->
                let sender = ctx.Sender().Retype<Result<Domain.MasterPDBWorkingCopy.MasterPDBWorkingCopy, string>>()
                let instanceName = getInstanceName instanceName
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance = instanceActor instanceName
                    instance <<! OracleInstanceActor.ExtendWorkingCopy name
                    return! loop state
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

            | DeleteRequest requestId ->
                let (newPendingRequests, newCompletedRequests) = requestId |> deleteRequest state
                return! loop { state with PendingRequests = newPendingRequests; CompletedRequests = newCompletedRequests }

            | GetDumpTransferInfo instanceName ->
                let sender = ctx.Sender().Retype<Result<OracleInstanceActor.DumpTransferInfo, string>>()
                let instanceName = getInstanceName instanceName
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance = instanceActor instanceName
                    let! (transferInfo:OracleInstanceActor.DumpTransferInfo) = instance <? OracleInstanceActor.GetDumpTransferInfo
                    sender <! Ok transferInfo
                    return! loop state
                | None ->
                    sender <! (sprintf "cannot find Oracle instance %s" instanceName |> Error)
                    return! loop state
        }

    and handleAdminCommand state command = 
        ctx.Log.Value.Info("Received admin command : {description}", describeAdminCommand command)

        let instanceActor = instanceActor state
        let primaryInstance = primaryInstance state
        let getInstanceName = getInstanceName state

        let getPendingCommands filter =
            state.PendingRequests 
            |> alivePendingRequests
            |> Seq.map (fun request -> request.Command)
            |> Seq.filter filter
        let getPendingChanges () = async {
            let pendingChangeCommands = getPendingCommands (pendingChangeCommandFilter (fun _ -> true))
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

            | GetPendingCommands ->
                let sender = ctx.Sender().Retype<Command list>()
                let commands = getPendingCommands (fun _ -> true) |> Seq.toList
                sender <! commands
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

            | CollectInstanceGarbage instanceName ->
                let instanceName = getInstanceName instanceName
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    let instance = instanceActor instanceName
                    instance <! OracleInstanceActor.CollectGarbage
                    return! loop state
                | None ->
                    ctx.Log.Value.Warning("Cannot find Oracle instance {instance}.", instanceName)
                    return! loop state

            | CollectGarbage ->
                ctx.Log.Value.Info("Garbage collection of all Oracle instances requested.")
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
                            ctx.Sender() <! stateError "primary Oracle instance has pending changes"
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
                            sender <! Error ("primary Oracle instance has pending changes", state.Orchestrator.PrimaryInstance)
                            return! loop state
                        | Error error ->
                            sender <! Error (sprintf "cannot get pending changes : %s" error, state.Orchestrator.PrimaryInstance)
                            return! loop state

            | DeleteMasterPDBVersion (pdb, version, force) ->
                let sender = ctx.Sender().Retype<DeleteVersionResult>()
                // Delete on primary instance first
                let res:Result<Application.MasterPDBActor.DeleteVersionResult, string> = 
                    primaryInstance <? OracleInstanceActor.DeleteMasterPDBVersion (pdb, version, force) |> runWithinElseDefaultError parameters.ShortTimeout
                match res with
                | Ok (Ok _) ->
                    // Now try to delete on other instances (not blocking)
                    let otherInstances = 
                        state.Orchestrator.OracleInstanceNames
                        |> List.filter (fun name -> name <> state.Orchestrator.PrimaryInstance)
                    let res = 
                        otherInstances
                        |> List.map (fun name ->
                            let x:Async<Application.MasterPDBActor.DeleteVersionResult> = instanceActor name <? OracleInstanceActor.DeleteMasterPDBVersion (pdb, version, force)
                            x
                            |> AsyncResult.mapError (fun error -> (name, error))
                            |> AsyncValidation.ofAsyncResult
                           )
                        |> AsyncValidation.sequenceP
                        |> runWithinElseDefaultError parameters.ShortTimeout
                    match res with
                    | Ok (Valid _) -> sender <! Ok []
                    | Ok (Invalid instanceErrors) -> sender <! Ok instanceErrors
                    | Error error -> sender <! Ok [ "?", error ]

                | Ok (Error error) -> sender <! Error error
                | Error error -> sender <! Error error

                return! loop state

            | SwitchLock pdb ->
                primaryInstance <<! OracleInstanceActor.SwitchLock pdb
                return! loop state

            | MasterPDBVersionSynchronizedWithPrimary (instanceName, pdbName, versionNumber) ->
                let sender = ctx.Sender().Retype<Application.MasterPDBActor.AddVersionResult>()
                match state.Orchestrator |> containsOracleInstance instanceName with
                | Some instanceName ->
                    if instanceName = state.Orchestrator.PrimaryInstance then
                        sender <! Error "primary instance cannot synchronize with itself"
                    else
                        let instance = instanceActor instanceName
                        let res:Result<Application.MasterPDBActor.StateResult,string> = primaryInstance <? OracleInstanceActor.GetMasterPDBState pdbName |> runWithinElseDefaultError parameters.ShortTimeout
                        match res with
                        | Ok (Ok pdbDTO) ->
                            match pdbDTO.Versions |> List.tryFind (fun v -> v.VersionNumber = versionNumber) with
                            | Some version ->
                                instance <<! OracleInstanceActor.AddMasterPDBVersion (pdbName, version |> Application.DTO.MasterPDBVersion.fromDTO)
                            | None ->
                                sender <! (Error <| sprintf "cannot find version %d of master PDB %s on Oracle instance %s" versionNumber pdbName instanceName)
                        | Ok (Error error) ->
                            sender <! Error error
                        | Error error ->
                            sender <! (Error <| sprintf "cannot get master PDB %s on Oracle instance %s : %s" pdbName instanceName error)
                | None ->
                    sender <! (Error <| sprintf "cannot find Oracle instance %s" instanceName)
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
                        CompletedWithError <| sprintf "Invalid request : %s." (System.String.Join("; ", errors |> List.toArray))
                    | MasterPDBCreationFailure (instance, pdb, error) -> 
                        CompletedWithError <| sprintf "Error while creating master PDB %s on Oracle instance %s : %s." pdb instance error
                    | MasterPDBCreated (instance, pdb) ->
                        completedOk [ 
                            PDBName pdb.Name
                            ResourceLink (sprintf "/instances/%s/master-pdbs/%s" instance pdb.Name)
                        ]
                        <| sprintf "Master PDB %s created successfully on Oracle instance %s." pdb.Name instance
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
                        completedOk ([ 
                            PDBName editionPDB
                            PDBService editionPDBService
                            ResourceLink (sprintf "/instances/%s/master-pdbs/%s/edition" instance pdb.Name)
                        ] @ schemasData)
                        <| sprintf "Master PDB %s prepared successfully for edition." pdb.Name 
                    | MasterPDBActor.PreparationFailure (pdb, error) -> 
                        CompletedWithError <| sprintf "Error while preparing master PDB %s for edition : %s." pdb error
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
                        completedOk [ 
                            PDBName pdb.Name
                            PDBVersion newVersion.VersionNumber
                            ResourceLink (sprintf "/instances/%s/master-pdbs/%s/versions/%d" instance pdb.Name newVersion.VersionNumber)
                        ]
                        <| sprintf "Master PDB %s committed successfully (new version %d created) on Oracle instance %s." pdb.Name newVersion.VersionNumber instance
                    | Error error -> 
                        CompletedWithError <| sprintf "Error while committing master PDB : %s." error
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
                        completedOk [ 
                            PDBName pdb.Name 
                            ResourceLink (sprintf "/instances/%s/master-pdbs/%s/versions/%d" instance pdb.Name latestVersion)
                        ]
                        <| sprintf "Master PDB %s rolled back successfully to version %d on Oracle instance %s." pdb.Name latestVersion instance
                    | Error error -> 
                        CompletedWithError <| sprintf "Error while rolling back master PDB : %s." error
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
                        completedOk [ 
                            PDBName workingCopyName
                            PDBService workingCopyService
                            OracleInstance oracleInstance
                            ResourceLink (sprintf "/instances/%s/working-copies/%s" oracleInstance workingCopyName)
                        ] 
                        <| match versionNumber with
                           | 0 -> sprintf "Working copy of edition of PDB %s created successfully with name %s." pdb workingCopyName
                           | _ -> sprintf "Working copy of PDB %s version %d created successfully with name %s." pdb versionNumber workingCopyName

                    | Error error -> 
                        CompletedWithError <| sprintf "Error while creating working copy : %s." error
                let (newPendingRequests, newCompletedRequests) = requestDone state request status
                return! loop { state with PendingRequests = newPendingRequests; CompletedRequests = newCompletedRequests }
        }

    and handleResponseToDeleteWorkingCopy state response =
        let (requestId, result) = response
        let requestMaybe = state.PendingRequests |> Map.tryFind requestId
        actor {
            match requestMaybe with
            | None -> 
                ctx.Log.Value.Error("Internal error : request {requestId} not found.", requestId)
                return! loop state
            | Some request ->
                let status = 
                    match result with
                    | Ok wcName -> 
                        completedOk [ PDBName wcName ] <| sprintf "Working copy %s deleted successfully." wcName
                    | Error error -> 
                        CompletedWithError <| sprintf "Error while deleting working copy : %s." error.Message
                let (newPendingRequests, newCompletedRequests) = requestDone state request status
                return! loop { state with PendingRequests = newPendingRequests; CompletedRequests = newCompletedRequests }
        }

    and terminatedOracleInstanceActor state oracleInstanceActor =
        let terminatedInstanceNameMaybe = state.Collaborators.OracleInstanceActors |> Map.tryFindKey (fun _ actorRef -> oracleInstanceActor = actorRef)
        let newCollabs = 
            terminatedInstanceNameMaybe
            |> Option.map (fun name -> 
                ctx.Log.Value.Warning("Actor for Oracle instance {instance} was terminated => disabled it", name)
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
