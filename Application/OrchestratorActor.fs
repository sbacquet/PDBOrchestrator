module Application.OrchestratorActor

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

type Collaborators = {
    OracleInstanceActors: Map<string, IActorRef<obj>>
}

type RequestStatus = 
| NotFound
| Pending
| CompletedOk of string
| CompletedWithError of string

let spawnCollaborators getOracleAPI (oracleInstanceRepo:#IOracleInstanceRepository) getMasterPDBRepo state (ctx : Actor<_>) = {
    OracleInstanceActors =
        state.OracleInstanceNames 
        |> List.map (fun instanceName -> 
            let instance = oracleInstanceRepo.Get instanceName
            instanceName, ctx |> OracleInstanceActor.spawn getOracleAPI (getMasterPDBRepo instance) instance
           )
        |> Map.ofList
}

let createMasterPDBError error : MasterPDBCreationResult = InvalidRequest [ error ]

let orchestratorActorBody getOracleAPI (oracleInstanceRepo:#IOracleInstanceRepository) getMasterPDBRepo initialState (ctx : Actor<_>) =

    let collaborators = ctx |> spawnCollaborators getOracleAPI oracleInstanceRepo getMasterPDBRepo initialState

    let rec loop (orchestrator : Orchestrator) (pendingRequests:PendingUserRequestMap<Command>) (completedRequests : CompletedUserRequestMap<RequestStatus>) = actor {

        logDebugf ctx "Number of pending requests : %d" pendingRequests.Count
        logDebugf ctx "Number of completed requests : %d" completedRequests.Count
        let! (msg:obj) = ctx.Receive()

        match msg with
        | :? Command as command ->
            let sender = ctx.Sender().Retype<RequestValidation>()
            match command with
            | GetState ->
                let! state = orchestrator |> DTO.Orchestrator.toDTO (collaborators.OracleInstanceActors |> Map.map (fun _ a -> a.Retype<OracleInstanceActor.Command>()))
                ctx.Sender() <! state
                return! loop orchestrator pendingRequests completedRequests

            | GetInstanceState instanceName ->
                let sender = ctx.Sender().Retype<Application.OracleInstanceActor.StateResult>()
                if (orchestrator.OracleInstanceNames |> List.contains instanceName) then 
                    let instance:IActorRef<OracleInstanceActor.Command> = retype collaborators.OracleInstanceActors.[instanceName]
                    instance <<! OracleInstanceActor.GetState
                else
                    sender <! OracleInstanceActor.stateError (sprintf "cannot find Oracle instance %s" instanceName)
                return! loop orchestrator pendingRequests completedRequests

            | GetMasterPDBState (instanceName, pdb) ->
                let sender = ctx.Sender().Retype<MasterPDBActor.StateResult>()
                if (orchestrator.OracleInstanceNames |> List.contains instanceName) then 
                    let instance:IActorRef<OracleInstanceActor.Command> = retype collaborators.OracleInstanceActors.[instanceName]
                    instance <<! OracleInstanceActor.GetMasterPDBState pdb
                else
                    sender <! MasterPDBActor.stateError (sprintf "cannot find Oracle instance %s" instanceName)
                return! loop orchestrator pendingRequests completedRequests

            | Synchronize targetInstance ->
                if (orchestrator.OracleInstanceNames |> List.contains targetInstance) then
                    let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryInstance]
                    let target = collaborators.OracleInstanceActors.[targetInstance]
                    retype primaryInstance <<! TransferInternalState target
                else
                    ctx.Sender() <! stateError (sprintf "cannot find Oracle instance %s" targetInstance)
                return! loop orchestrator pendingRequests completedRequests

            | CreateMasterPDB (user, parameters) ->
                let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryInstance]
                let requestId = newRequestId()
                let newPendingRequests = pendingRequests |> registerUserRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.CreateMasterPDB (requestId, parameters)
                sender <! Valid requestId
                return! loop orchestrator newPendingRequests completedRequests

            | PrepareMasterPDBForModification (user, pdb, version) ->
                let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryInstance]
                let requestId = newRequestId()
                let newPendingRequests = pendingRequests |> registerUserRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.PrepareMasterPDBForModification (requestId, pdb, version, user)
                sender <! Valid requestId
                return! loop orchestrator newPendingRequests completedRequests

            | CommitMasterPDB (user, pdb, comment) ->
                let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryInstance]
                let requestId = newRequestId()
                let newPendingRequests = pendingRequests |> registerUserRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.CommitMasterPDB (requestId, pdb, user, comment)
                sender <! Valid requestId
                return! loop orchestrator newPendingRequests completedRequests

            | RollbackMasterPDB (user, pdb) ->
                let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryInstance]
                let requestId = newRequestId()
                let newPendingRequests = pendingRequests |> registerUserRequest requestId command user
                retype primaryInstance <! Application.OracleInstanceActor.RollbackMasterPDB (requestId, pdb)
                sender <! Valid requestId
                return! loop orchestrator newPendingRequests completedRequests

            | SnapshotMasterPDBVersion (user, instanceName, masterPDBName, versionNumber, snapshotName) ->
                if (orchestrator.OracleInstanceNames |> List.contains instanceName) then 
                    let instance = collaborators.OracleInstanceActors.[instanceName]
                    let requestId = newRequestId()
                    let newPendingRequests = pendingRequests |> registerUserRequest requestId command user
                    retype instance <! Application.OracleInstanceActor.SnapshotMasterPDBVersion (requestId, masterPDBName, versionNumber, snapshotName)
                    sender <! Valid requestId
                    return! loop orchestrator newPendingRequests completedRequests
                else
                    sender <! Invalid [ sprintf "cannot find Oracle instance %s" instanceName ]
                    return! loop orchestrator pendingRequests completedRequests

            | GetRequest requestId ->
                let sender = ctx.Sender().Retype<WithRequestId<RequestStatus>>()
                let requestMaybe = pendingRequests |> Map.tryFind requestId
                match requestMaybe with
                | Some _ ->
                    sender <! (requestId, Pending)
                    return! loop orchestrator pendingRequests completedRequests
                | None -> 
                    let requestMaybe = completedRequests |> Map.tryFind requestId
                    match requestMaybe with
                    | None -> 
                        sender <! (requestId, NotFound)
                        return! loop orchestrator pendingRequests completedRequests
                    | Some request ->
                        sender <! (requestId, request.Status)
                        logDebugf ctx "Request %s completed => removed from the list" (requestId.ToString())
                        return! loop orchestrator pendingRequests (completedRequests |> Map.remove requestId)

        | :? WithRequestId<MasterPDBCreationResult> as responseToCreateMasterPDB ->
            let (requestId, result) = responseToCreateMasterPDB
            let requestMaybe = pendingRequests |> Map.tryFind requestId
            match requestMaybe with
            | None -> 
                logErrorf ctx "internal error : request %s not found" (requestId.ToString())
                return! loop orchestrator pendingRequests completedRequests
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
                return! loop orchestrator newPendingRequests newCompletedRequests

        | :? WithRequestId<MasterPDBActor.PrepareForModificationResult> as responseToPrepareMasterPDBForModification ->
            let (requestId, result) = responseToPrepareMasterPDBForModification
            let requestMaybe = pendingRequests |> Map.tryFind requestId
            match requestMaybe with
            | None -> 
                logErrorf ctx "internal error : request %s not found" (requestId.ToString())
                return! loop orchestrator pendingRequests completedRequests
            | Some request ->
                let status = 
                    match result with
                    | MasterPDBActor.Locked _ -> CompletedWithError "internal error"
                    | MasterPDBActor.Prepared pdb -> CompletedOk (sprintf "master PDB %s prepared successfully for edition" pdb.Name)
                    | MasterPDBActor.PreparationFailure error -> CompletedWithError (sprintf "error while preparing master PDB for edition : %s" error)
                let (newPendingRequests, newCompletedRequests) = completeUserRequest request status pendingRequests completedRequests
                return! loop orchestrator newPendingRequests newCompletedRequests

        | :? WithRequestId<MasterPDBActor.EditionDone> as responseToRollbackMasterPDB ->
            let (requestId, result) = responseToRollbackMasterPDB
            let requestMaybe = pendingRequests |> Map.tryFind requestId
            match requestMaybe with
            | None -> 
                logErrorf ctx "internal error : request %s not found" (requestId.ToString())
                return! loop orchestrator pendingRequests completedRequests
            | Some request ->
                let status = 
                    match result with
                    | Ok pdb -> CompletedOk (sprintf "master PDB %s unlocked successfully" pdb.Name)
                    | Error error -> CompletedWithError (sprintf "error while unlocking master PDB : %s" error)
                let (newPendingRequests, newCompletedRequests) = completeUserRequest request status pendingRequests completedRequests
                return! loop orchestrator newPendingRequests newCompletedRequests

        | :? WithRequestId<MasterPDBActor.SnapshotResult> as responseToSnapshotMasterPDBVersion ->
            let (requestId, result) = responseToSnapshotMasterPDBVersion
            let requestMaybe = pendingRequests |> Map.tryFind requestId
            match requestMaybe with
            | None -> 
                logErrorf ctx "internal error : request %s not found" (requestId.ToString())
                return! loop orchestrator pendingRequests completedRequests
            | Some request ->
                let status = 
                    match result with
                    | Ok (pdb, versionNumber, snapshotName) -> CompletedOk (sprintf "version %d of master PDB %s snapshoted successfully with name %s" versionNumber pdb snapshotName)
                    | Error error -> CompletedWithError (sprintf "error while snapshoting master PDB version : %s" error)
                let (newPendingRequests, newCompletedRequests) = completeUserRequest request status pendingRequests completedRequests
                return! loop orchestrator newPendingRequests newCompletedRequests

        | _ -> return! loop orchestrator pendingRequests completedRequests
    }
    loop initialState Map.empty Map.empty

let [<Literal>]cOrchestratorActorName = "Orchestrator"

let spawn getOracleAPI (oracleInstanceRepo:#IOracleInstanceRepository) getMasterPDBRepo initialState actorFactory =
    let actor = 
        Akkling.Spawn.spawn actorFactory cOrchestratorActorName 
        <| props (orchestratorActorBody getOracleAPI oracleInstanceRepo getMasterPDBRepo initialState)
    actor.Retype<Command>()
