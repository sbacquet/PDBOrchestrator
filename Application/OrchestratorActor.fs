﻿module Application.OrchestratorActor

open Akkling
open Application.OracleInstanceActor
open Domain.Orchestrator
open Application.PendingRequest
open Application.UserPendingRequest
open Domain.Common.Validation
open Application.Common

type OnInstance<'T> = WithUser<string, 'T>
type OnInstance<'T1, 'T2> = WithUser<string, 'T1, 'T2>
type OnInstance<'T1, 'T2, 'T3> = WithUser<string, 'T1, 'T2, 'T3>

type RequestValidation = Validation<RequestId, string>

type Command =
| GetState // responds with Application.DTO.Orchestrator
| GetInstanceState of string // responds with Application.OracleInstanceActor.StateResult
| GetMasterPDBState of string * string // responds with Application.MasterPDBActor.StateResult
| Synchronize of string // responds with OracleInstanceActor.StateSet
| CreateMasterPDB of WithUser<CreateMasterPDBParams> // responds with RequestValidation
| PrepareMasterPDBForModification of WithUser<string, int> // responds with RequestValidation
| CommitMasterPDB of WithUser<string, string> // responds with RequestValidation
| RollbackMasterPDB of WithUser<string> // responds with RequestValidation
| SnapshotMasterPDBVersion of OnInstance<string, int, string> // responds with RequestValidation
| GetRequest of RequestId // responds with WithRequestId<RequestStatus>

type AdminCommand =
| GetPendingChanges // responds with Result<Option<PendingChanges>,string>
| EnterReadOnlyMode // responds with bool=true
| ExitReadOnlyMode // responds with bool=false
| IsReadOnlyMode // responds with bool
| CollectGarbage // no response

let private pendingChangeCommandFilter = function
| GetState
| GetInstanceState _
| GetMasterPDBState _
| SnapshotMasterPDBVersion _
| GetRequest _
| Synchronize _ -> false
| CreateMasterPDB _
| PrepareMasterPDBForModification _
| CommitMasterPDB _
| RollbackMasterPDB _ -> true

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

let private spawnCollaborators parameters getOracleAPI (oracleInstanceRepo:#IOracleInstanceRepository) getMasterPDBRepo state (ctx : Actor<_>) = {
    OracleInstanceActors =
        state.OracleInstanceNames 
        |> List.map (fun instanceName -> 
            let instance = oracleInstanceRepo.Get instanceName
            instanceName, ctx |> OracleInstanceActor.spawn parameters (getOracleAPI instance) (getMasterPDBRepo instance) instance
           )
        |> Map.ofList
}

type private State = {
    Orchestrator : Orchestrator
    PendingRequests :PendingUserRequestMap<Command>
    CompletedRequests : CompletedUserRequestMap<RequestStatus>
    ReadOnly : bool
}

let private orchestratorActorBody parameters getOracleAPI (oracleInstanceRepo:#IOracleInstanceRepository) getMasterPDBRepo initialState (ctx : Actor<_>) =

    let collaborators = ctx |> spawnCollaborators parameters getOracleAPI oracleInstanceRepo getMasterPDBRepo initialState

    let rec loop state = actor {
        
        let orchestrator = state.Orchestrator
        let pendingRequests = state.PendingRequests
        let completedRequests = state.CompletedRequests
        let readOnly = state.ReadOnly

        ctx.Log.Value.Debug("Number of pending requests : {0}", pendingRequests.Count)
        ctx.Log.Value.Debug("Number of completed requests : {0}", completedRequests.Count)

        let! (msg:obj) = ctx.Receive()

        match msg with
        | :? Command as command ->
            let sender = ctx.Sender().Retype<RequestValidation>()
            if (readOnly && pendingChangeCommandFilter command) then
                sender <! RequestValidation.Invalid [ "this command cannot be accepted in read-only mode" ]
                return! loop state
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

            | Synchronize targetInstance ->
                if (orchestrator.OracleInstanceNames |> List.contains targetInstance) then
                    let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryInstance]
                    let target = collaborators.OracleInstanceActors.[targetInstance]
                    retype primaryInstance <<! TransferInternalState target
                else
                    ctx.Sender() <! stateError (sprintf "cannot find Oracle instance %s" targetInstance)
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
                    sender <! Invalid [ sprintf "cannot find Oracle instance %s" instanceName ]
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
            match command with
            | GetPendingChanges ->
                let pendingChangeCommands = 
                    pendingRequests 
                    |> Map.toSeq
                    |> Seq.map (fun (_, request) -> request.Command)
                    |> Seq.filter pendingChangeCommandFilter
                
                let primaryInstance:IActorRef<OracleInstanceActor.Command> = retype collaborators.OracleInstanceActors.[orchestrator.PrimaryInstance]
                let! (primaryInstanceState:OracleInstanceActor.StateResult) = primaryInstance <? OracleInstanceActor.GetState
                let result = 
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
                ctx.Sender() <! result
                return! loop state

            | EnterReadOnlyMode ->
                ctx.Sender() <! true
                return! loop { state with ReadOnly = true }

            | ExitReadOnlyMode ->
                ctx.Sender() <! false
                return! loop { state with ReadOnly = false }

            | IsReadOnlyMode ->
                ctx.Sender() <! readOnly
                return! loop state

            | CollectGarbage ->
                ctx.Log.Value.Info("Garbage collection requested")
                collaborators.OracleInstanceActors |> Map.iter (fun _ actor -> retype actor <! OracleInstanceActor.CollectGarbage)
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
                    | MasterPDBCreationFailure error -> 
                        CompletedWithError error
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
                    | MasterPDBActor.PreparationFailure error -> CompletedWithError (sprintf "error while preparing master PDB for edition : %s" error)
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

    loop { Orchestrator = initialState; PendingRequests = Map.empty; CompletedRequests = Map.empty; ReadOnly = false }

let [<Literal>]cOrchestratorActorName = "Orchestrator"

let spawn parameters getOracleAPI (oracleInstanceRepo:#IOracleInstanceRepository) getMasterPDBRepo initialState actorFactory =
    let actor = 
        Akkling.Spawn.spawn actorFactory cOrchestratorActorName 
        <| props (orchestratorActorBody parameters getOracleAPI oracleInstanceRepo getMasterPDBRepo initialState)
    actor.Retype<Command>()
