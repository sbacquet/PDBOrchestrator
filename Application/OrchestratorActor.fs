﻿module Application.OrchestratorActor

open Akkling
open Application.OracleInstanceActor
open Domain.Orchestrator
open Application.PendingRequest
open Application.UserPendingRequest
open Domain.Common.Validation
open Application.Common
open Domain.OracleInstance

type OnInstance<'T> = WithUser<string, 'T>
type OnInstance<'T1, 'T2> = WithUser<string, 'T1, 'T2>
type OnInstance<'T1, 'T2, 'T3> = WithUser<string, 'T1, 'T2, 'T3>

type RequestValidation = Validation<RequestId, string>

type Command =
| GetState // responds with Application.DTO.Orchestrator
| GetInstanceState of string // responds with Application.OracleInstanceActor.StateResult
| GetMasterPDBState of string * string // responds with Application.MasterPDBActor.StateResult
| CreateMasterPDB of WithUser<CreateMasterPDBParams> // responds with RequestValidation
| PrepareMasterPDBForModification of WithUser<string, int> // responds with RequestValidation
| CommitMasterPDB of WithUser<string, string> // responds with RequestValidation
| RollbackMasterPDB of WithUser<string> // responds with RequestValidation
| SnapshotMasterPDBVersion of OnInstance<string, int, string> // responds with RequestValidation
| GetRequest of RequestId // responds with WithRequestId<RequestStatus>

type AdminCommand =
| GetPendingChanges // responds with Result<PendingChanges option,string>
| EnterReadOnlyMode // responds with bool
| EnterNormalMode // responds with bool
| IsReadOnlyMode // responds with bool
| CollectGarbage // no response
| Synchronize of string // responds with OracleInstanceActor.StateSet
| SetPrimaryOracleInstance of string

let private pendingChangeCommandFilter mapper = function
| GetState
| GetInstanceState _
| GetMasterPDBState _
| SnapshotMasterPDBVersion _
| GetRequest _ ->
    false
| CreateMasterPDB (user, _)
| PrepareMasterPDBForModification (user, _, _)
| CommitMasterPDB (user, _, _)
| RollbackMasterPDB (user, _) ->
    mapper user

type PendingChanges = {
    Commands : Command list
    OpenMasterPDBs : (string * Domain.MasterPDB.LockInfo) list
}

let consPendingChanges commands openMasterPDBs = { Commands = commands |> Seq.toList; OpenMasterPDBs = openMasterPDBs }

type private Collaborators = {
    OracleInstanceActors: Map<string, IActorRef<obj>>
}

type RequestStatus = 
| NotFound
| Pending
| CompletedOk of string
| CompletedWithError of string

let private spawnCollaborators parameters getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo state (ctx : Actor<_>) = 
    let spawnInstance = OracleInstanceActor.spawn parameters getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo ctx
    {
        OracleInstanceActors =
            state.OracleInstanceNames 
            |> List.map (fun instanceName -> 
                instanceName, spawnInstance instanceName
               )
            |> Map.ofList
    }

type private State = {
    Orchestrator : Orchestrator
    PreviousOrchestrator : Orchestrator
    Collaborators : Collaborators
    PendingRequests :PendingUserRequestMap<Command>
    CompletedRequests : CompletedUserRequestMap<RequestStatus>
    ReadOnly : bool
    Repository : IOrchestratorRepository
}

let private orchestratorActorBody (parameters:Application.Parameters.Parameters) getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo repository initialState (ctx : Actor<_>) =

    let rec loop state = actor {
        
        let orchestrator = state.Orchestrator
        let collaborators = state.Collaborators
        let pendingRequests = state.PendingRequests
        let completedRequests = state.CompletedRequests
        let readOnly = state.ReadOnly

        if (state.PreviousOrchestrator <> orchestrator) then
            ctx.Log.Value.Debug("Persisted modified orchestrator")
            return! loop { state with Repository = state.Repository.Put orchestrator; PreviousOrchestrator = orchestrator }
        else

        ctx.Log.Value.Debug("Number of pending requests : {0}", pendingRequests.Count)
        ctx.Log.Value.Debug("Number of completed requests : {0}", completedRequests.Count)

        let! (msg:obj) = ctx.Receive()

        match msg with
        | :? Command as command ->
            let sender = ctx.Sender().Retype<RequestValidation>()

            // Check if command is compatible with maintenance mode
            let pendingChangeCommandAcceptable user = UserRights.isAdmin (UserRights.normalUser user)
            if (readOnly && pendingChangeCommandFilter (not << pendingChangeCommandAcceptable) command) then
                sender <! RequestValidation.Invalid [ "the command cannot be run in maintenance mode" ]
                return! loop state
            
            else // Do not remove this line !!

            let getInstanceName instanceName = if instanceName = "primary" then orchestrator.PrimaryInstance else instanceName
            
            match command with
            | GetState ->
                let! orchestratorDTO = orchestrator |> DTO.Orchestrator.toDTO (collaborators.OracleInstanceActors |> Map.map (fun _ a -> a.Retype<OracleInstanceActor.Command>()))
                ctx.Sender() <! orchestratorDTO
                return! loop state

            | GetInstanceState instanceName ->
                let sender = ctx.Sender().Retype<Application.OracleInstanceActor.StateResult>()
                let instanceName = getInstanceName instanceName
                if (orchestrator.OracleInstanceNames |> List.contains instanceName) then 
                    let instance:IActorRef<OracleInstanceActor.Command> = retype collaborators.OracleInstanceActors.[instanceName]
                    instance <<! OracleInstanceActor.GetState
                else
                    sender <! OracleInstanceActor.stateError (sprintf "cannot find Oracle instance %s" instanceName)
                return! loop state

            | GetMasterPDBState (instanceName, pdb) ->
                let sender = ctx.Sender().Retype<MasterPDBActor.StateResult>()
                let instanceName = getInstanceName instanceName
                if (orchestrator.OracleInstanceNames |> List.contains instanceName) then 
                    let instance:IActorRef<OracleInstanceActor.Command> = retype collaborators.OracleInstanceActors.[instanceName]
                    instance <<! OracleInstanceActor.GetMasterPDBState pdb
                else
                    sender <! MasterPDBActor.stateError (sprintf "cannot find Oracle instance %s" instanceName)
                return! loop state

            | CreateMasterPDB (user, parameters) ->
                let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryInstance]
                let requestId = newRequestId()
                let newPendingRequests = pendingRequests |> registerUserRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.CreateMasterPDB (requestId, parameters)
                sender <! Valid requestId
                return! loop { state with PendingRequests = newPendingRequests }

            | PrepareMasterPDBForModification (user, pdb, version) ->
                let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryInstance]
                let requestId = newRequestId()
                let newPendingRequests = pendingRequests |> registerUserRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.PrepareMasterPDBForModification (requestId, pdb, version, user)
                sender <! Valid requestId
                return! loop { state with PendingRequests = newPendingRequests }

            | CommitMasterPDB (user, pdb, comment) ->
                let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryInstance]
                let requestId = newRequestId()
                let newPendingRequests = pendingRequests |> registerUserRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.CommitMasterPDB (requestId, pdb, user, comment)
                sender <! Valid requestId
                return! loop { state with PendingRequests = newPendingRequests }

            | RollbackMasterPDB (user, pdb) ->
                let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryInstance]
                let requestId = newRequestId()
                let newPendingRequests = pendingRequests |> registerUserRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.RollbackMasterPDB (requestId, user, pdb)
                sender <! Valid requestId
                return! loop { state with PendingRequests = newPendingRequests }

            | SnapshotMasterPDBVersion (user, instanceName, masterPDBName, versionNumber, snapshotName) ->
                let instanceName = getInstanceName instanceName
                if (orchestrator.OracleInstanceNames |> List.contains instanceName) then 
                    let instance = collaborators.OracleInstanceActors.[instanceName]
                    let requestId = newRequestId()
                    let newPendingRequests = pendingRequests |> registerUserRequest requestId command user
                    retype instance <! Application.OracleInstanceActor.SnapshotMasterPDBVersion (requestId, masterPDBName, versionNumber, snapshotName)
                    sender <! Valid requestId
                    return! loop { state with PendingRequests = newPendingRequests }
                else
                    sender <! RequestValidation.Invalid [ sprintf "cannot find Oracle instance %s" instanceName ]
                    return! loop state

            | GetRequest requestId ->
                let sender = ctx.Sender().Retype<WithRequestId<RequestStatus>>()
                let requestMaybe = pendingRequests |> Map.tryFind requestId
                match requestMaybe with
                | Some _ ->
                    sender <! (requestId, Pending)
                    return! loop state
                | None -> 
                    let requestMaybe = completedRequests |> Map.tryFind requestId
                    match requestMaybe with
                    | None -> 
                        sender <! (requestId, NotFound)
                        return! loop state
                    | Some request ->
                        sender <! (requestId, request.Status)
                        logDebugf ctx "Request %s completed => removed from the list" (requestId.ToString())
                        return! loop { state with CompletedRequests = completedRequests |> Map.remove requestId }

        | :? AdminCommand as command ->
            let getPendingChanges () = async {
                let pendingChangeCommands = 
                    pendingRequests 
                    |> Map.toSeq
                    |> Seq.map (fun (_, request) -> request.Command)
                    |> Seq.filter (pendingChangeCommandFilter (fun _ -> true))
                
                let primaryInstance:IActorRef<OracleInstanceActor.Command> = retype collaborators.OracleInstanceActors.[orchestrator.PrimaryInstance]
                let! (primaryInstanceState:OracleInstanceActor.StateResult) = primaryInstance <? OracleInstanceActor.GetState
                return
                    primaryInstanceState 
                    |> Result.map (fun state -> 
                        state.MasterPDBs 
                        |> List.filter (fun pdb -> pdb.LockState |> Option.isSome)
                        |> List.map (fun pdb -> pdb.Name, pdb.LockState.Value)
                        )
                    |> Result.map (fun openMasterPDBs ->
                        if (openMasterPDBs.IsEmpty && Seq.isEmpty pendingChangeCommands) then    
                            None
                        else
                            Some <| consPendingChanges pendingChangeCommands openMasterPDBs
                        )
            }
            match command with
            | GetPendingChanges ->
                let! result = getPendingChanges()
                ctx.Sender() <! result
                return! loop state

            | EnterReadOnlyMode ->
                if (state.ReadOnly) then
                    ctx.Sender() <! false
                    ctx.Log.Value.Warning("The server is already in maintenance mode.")
                    return! loop state
                else
                    ctx.Sender() <! true
                    ctx.Log.Value.Warning("The server is now in maintenance mode.")
                    return! loop { state with ReadOnly = true }

            | IsReadOnlyMode ->
                ctx.Sender() <! readOnly
                return! loop state

            | CollectGarbage ->
                ctx.Log.Value.Info("Garbage collection requested")
                collaborators.OracleInstanceActors |> Map.iter (fun _ actor -> retype actor <! OracleInstanceActor.CollectGarbage)
                return! loop state

            | EnterNormalMode ->
                if (state.ReadOnly) then
                    ctx.Sender() <! true
                    ctx.Log.Value.Info("The server is now in normal mode.")
                    return! 
                        loop { state with ReadOnly = false }
                else
                    ctx.Log.Value.Info("The server is already in normal mode.")
                    ctx.Sender() <! false
                    return! loop state

            | Synchronize targetInstance ->
                if not state.ReadOnly then
                    ctx.Sender() <! stateError "the server must be in maintenance mode"
                elif not (orchestrator.OracleInstanceNames |> List.contains targetInstance) then
                    ctx.Sender() <! stateError (sprintf "cannot find Oracle instance %s" targetInstance)
                else
                    let pendingChangesMaybe = getPendingChanges() |> runWithinElseDefaultError parameters.ShortTimeout
                    match pendingChangesMaybe with
                    | Ok (Ok None) ->
                        let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryInstance]
                        let target = collaborators.OracleInstanceActors.[targetInstance]
                        retype primaryInstance <<! TransferInternalState target
                    | Ok (Ok (Some _)) ->
                        ctx.Sender() <! stateError "primary instance has pending changes"
                    | Ok (Error error)
                    | Error error ->
                        ctx.Sender() <! stateError (sprintf "cannot get pending changes : %s" error)
                return! loop state

            | SetPrimaryOracleInstance newPrimary ->
                let sender = ctx.Sender().Retype<Result<string,string>>()
                if not state.ReadOnly then
                    sender <! Error "the server must be in maintenance mode"
                    return! loop state
                elif (orchestrator.PrimaryInstance = newPrimary) then
                    sender <! Error (sprintf "%s is already the primary Oracle instance" newPrimary)
                    return! loop state
                elif not (orchestrator.OracleInstanceNames |> List.contains newPrimary) then
                    sender <! Error (sprintf "cannot find Oracle instance %s" newPrimary)
                    return! loop state
                else
                    let! pendingChangesMaybe = getPendingChanges()
                    match pendingChangesMaybe with
                    | Ok None ->
                        sender <! Ok newPrimary
                        return! loop { state with Orchestrator = { orchestrator with PrimaryInstance = newPrimary } }
                    | Ok (Some _) ->
                        sender <! Error "primary instance has pending changes"
                        return! loop state
                    | Error error ->
                        sender <! Error (sprintf "cannot get pending changes : %s" error)
                        return! loop state

        | :? WithRequestId<MasterPDBCreationResult> as responseToCreateMasterPDB ->
            let (requestId, result) = responseToCreateMasterPDB
            let requestMaybe = pendingRequests |> Map.tryFind requestId
            match requestMaybe with
            | None -> 
                logErrorf ctx "internal error : request %s not found" (requestId.ToString())
                return! loop state
            | Some request ->
                let status = 
                    match result with
                    | InvalidRequest errors -> 
                        CompletedWithError (sprintf "invalid request : %s" (System.String.Join("; ", errors |> List.toArray)))
                    | MasterPDBCreationFailure (pdb, error) -> 
                        CompletedWithError (sprintf "error while creating master PDB %s : %s" pdb error)
                    | MasterPDBCreated pdb ->
                        CompletedOk (sprintf "master PDB %s created successfully" pdb.Name)
                let (newPendingRequests, newCompletedRequests) = completeUserRequest request status pendingRequests completedRequests
                return! loop { state with PendingRequests = newPendingRequests; CompletedRequests = newCompletedRequests }

        | :? WithRequestId<MasterPDBActor.PrepareForModificationResult> as responseToPrepareMasterPDBForModification ->
            let (requestId, result) = responseToPrepareMasterPDBForModification
            let requestMaybe = pendingRequests |> Map.tryFind requestId
            match requestMaybe with
            | None -> 
                logErrorf ctx "internal error : request %s not found" (requestId.ToString())
                return! loop state
            | Some request ->
                let status = 
                    match result with
                    | MasterPDBActor.Prepared pdb -> CompletedOk (sprintf "master PDB %s prepared successfully for edition" pdb.Name)
                    | MasterPDBActor.PreparationFailure (pdb, error) -> CompletedWithError (sprintf "error while preparing master PDB %s for edition : %s" pdb error)
                let (newPendingRequests, newCompletedRequests) = completeUserRequest request status pendingRequests completedRequests
                return! loop { state with PendingRequests = newPendingRequests; CompletedRequests = newCompletedRequests }

        | :? WithRequestId<MasterPDBActor.EditionDone> as responseToMasterPDBEdition ->
            let (requestId, result) = responseToMasterPDBEdition
            let requestMaybe = pendingRequests |> Map.tryFind requestId
            match requestMaybe with
            | None -> 
                logErrorf ctx "internal error : request %s not found" (requestId.ToString())
                return! loop state
            | Some request ->
                let status = 
                    match result with
                    | Ok pdb -> CompletedOk (sprintf "master PDB %s unlocked successfully" pdb.Name)
                    | Error error -> CompletedWithError (sprintf "error while unlocking master PDB : %s" error)
                let (newPendingRequests, newCompletedRequests) = completeUserRequest request status pendingRequests completedRequests
                return! loop { state with PendingRequests = newPendingRequests; CompletedRequests = newCompletedRequests }

        | :? WithRequestId<MasterPDBActor.SnapshotResult> as responseToSnapshotMasterPDBVersion ->
            let (requestId, result) = responseToSnapshotMasterPDBVersion
            let requestMaybe = pendingRequests |> Map.tryFind requestId
            match requestMaybe with
            | None -> 
                logErrorf ctx "internal error : request %s not found" (requestId.ToString())
                return! loop state
            | Some request ->
                let status = 
                    match result with
                    | Ok (pdb, versionNumber, snapshotName) -> CompletedOk (sprintf "version %d of master PDB %s snapshoted successfully with name %s" versionNumber pdb snapshotName)
                    | Error error -> CompletedWithError (sprintf "error while snapshoting master PDB version : %s" error)
                let (newPendingRequests, newCompletedRequests) = completeUserRequest request status pendingRequests completedRequests
                return! loop { state with PendingRequests = newPendingRequests; CompletedRequests = newCompletedRequests }

        | _ -> return! loop state
    }

    let collaborators = ctx |> spawnCollaborators parameters getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo initialState

    loop { 
        Orchestrator = initialState
        PreviousOrchestrator = initialState
        Collaborators = collaborators
        PendingRequests = Map.empty
        CompletedRequests = Map.empty
        ReadOnly = false 
        Repository = repository
    }


let [<Literal>]cOrchestratorActorName = "Orchestrator"

let spawn parameters getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo (repository:IOrchestratorRepository) actorFactory =
    let initialState = repository.Get()
    let actor = 
        Akkling.Spawn.spawn actorFactory cOrchestratorActorName 
        <| props (orchestratorActorBody parameters getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo repository initialState)
    actor.Retype<Command>()
