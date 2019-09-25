﻿module Application.OrchestratorActor

open Akkling
open Application.OracleInstanceActor
open Domain.Orchestrator
open Application.PendingRequest

type OnInstance<'C> = string * 'C

type InstanceResult<'R> = Result<'R, string>

type Command =
| Synchronize of string
| GetState // responds with Application.DTO.Orchestrator
| CreateMasterPDB of WithRequestId<CreateMasterPDBParams> // responds with WithRequestId<MasterPDBCreationResult>
| PrepareMasterPDBForModification of WithRequestId<string, int, string> // responds with WithRequestId<MasterPDBActor.PrepareForModificationResult>
| RollbackMasterPDB of WithRequestId<string> // responds with WithRequestId<MasterPDBActor.RollbackResult>
| SnapshotMasterPDBVersion of WithRequestId<string, string, int, string> // responds with WithRequest<MasterPDBActor.SnapshotResult>

type Collaborators = {
    OracleInstanceActors: Map<string, IActorRef<obj>>
}

// TODO : try/catch ActorNotFoundException and return option
let spawnCollaborators getOracleAPI getInstance getMasterPDBRepo state (ctx : Actor<_>) = {
    OracleInstanceActors =
        state.OracleInstanceNames 
        |> List.map (fun instanceName -> 
            instanceName,
            ctx |> OracleInstanceActor.spawn getOracleAPI (getMasterPDBRepo instanceName) (getInstance instanceName))
        |> Map.ofList
}

let createMasterPDBError error : MasterPDBCreationResult = InvalidRequest [ error ]

let orchestratorActorBody getOracleAPI getInstance getMasterPDBRepo initialState (ctx : Actor<_>) =
    let collaborators = ctx |> spawnCollaborators getOracleAPI getInstance getMasterPDBRepo initialState
    let rec loop (orchestrator : Orchestrator) = actor {
        let! msg = ctx.Receive()
        match msg with
        | Synchronize targetInstance ->
            if (orchestrator.OracleInstanceNames |> List.contains targetInstance) then
                let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryServer]
                let target = collaborators.OracleInstanceActors.[targetInstance]
                retype primaryInstance <<! TransferInternalState target
            else
                ctx.Sender() <! stateSetError (sprintf "cannot find actor of instance %s" targetInstance)
            return! loop orchestrator
        | GetState ->
            let! state = orchestrator |> DTO.Orchestrator.toDTO (collaborators.OracleInstanceActors |> Map.map (fun _ a -> a.Retype<OracleInstanceActor.Command>()))
            ctx.Sender() <! state
            return! loop orchestrator
        | CreateMasterPDB parameters ->
            let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryServer]
            retype primaryInstance <<! Application.OracleInstanceActor.CreateMasterPDB parameters
            return! loop orchestrator
        | PrepareMasterPDBForModification parameters ->
            let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryServer]
            retype primaryInstance <<! Application.OracleInstanceActor.PrepareMasterPDBForModification parameters
            return! loop orchestrator
        | RollbackMasterPDB parameters ->
            let primaryInstance = collaborators.OracleInstanceActors.[orchestrator.PrimaryServer]
            retype primaryInstance <<! Application.OracleInstanceActor.RollbackMasterPDB parameters
            return! loop orchestrator
        | SnapshotMasterPDBVersion (requestId, instanceName, masterPDBName, versionNumber, snapshotName) ->
            let sender = ctx.Sender().Retype<WithRequestId<Application.MasterPDBActor.SnapshotResult>>()
            let instanceMaybe = collaborators.OracleInstanceActors |> Map.tryFind instanceName
            match instanceMaybe with
            | None -> sender <! (requestId, Error (sprintf "cannot find Oracle instance %s" instanceName))
            | Some instance ->
                retype instance <<! Application.OracleInstanceActor.SnapshotMasterPDBVersion (requestId, masterPDBName, versionNumber, snapshotName)
            return! loop orchestrator
    }
    loop initialState

let [<Literal>]cOrchestratorActorName = "Orchestrator"

let spawn getOracleAPI getInstance getMasterPDBRepo initialState actorFactory =
    Akkling.Spawn.spawn actorFactory cOrchestratorActorName <| props (orchestratorActorBody getOracleAPI getInstance getMasterPDBRepo initialState)
