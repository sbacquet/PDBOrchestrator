module Application.OrchestratorActor

open Akkling
open Application.OracleInstanceActor
open Domain.Orchestrator
open Akka.Actor

type OnInstance<'C> = string * 'C

type InstanceResult<'R> = Result<'R, string>

type Command =
| Synchronize of string
| GetState
| CreateMasterPDB of (* withParams : *) CreateMasterPDBParams // returns MasterPDBCreationResult

// TODO : try/catch ActorNotFoundException and return option
let spawnCollaborators getInstance getInstanceState getOracleAPI state (ctx : Actor<_>) =
    state.OracleInstanceNames 
    |> List.map (fun instanceName -> instanceName, ctx |> OracleInstanceActor.spawn getInstance getInstanceState getOracleAPI instanceName)
    |> Map.ofList

let createMasterPDBError error : MasterPDBCreationResult = InvalidRequest [ error ]

let orchestratorActorBody getInstance getInstanceState getOracleAPI initialState (ctx : Actor<_>) =
    let collaborators = ctx |> spawnCollaborators getInstance getInstanceState getOracleAPI initialState
    let rec loop (state : OrchestratorState) = actor {
        let! msg = ctx.Receive()
        match msg with
        | Synchronize targetServer ->
            let primaryInstanceMaybe = collaborators.TryFind(state.PrimaryServer)
            match primaryInstanceMaybe with
            | Some primaryInstance -> retype primaryInstance <<! TransferState targetServer
            | None -> ctx.Sender() <! stateSetError (sprintf "cannot find actor of primary instance %s" state.PrimaryServer)
            return! loop state
        | GetState ->
            ctx.Sender() <! state
            return! loop state
        | CreateMasterPDB parameters ->
            let primaryInstanceMaybe = collaborators.TryFind(state.PrimaryServer)
            match primaryInstanceMaybe with
            | Some primaryInstance -> retype primaryInstance <<! Application.OracleInstanceActor.CreateMasterPDB parameters
            | None -> ctx.Sender() <! createMasterPDBError (sprintf "cannot find actor of primary instance %s" state.PrimaryServer)
    }
    loop initialState

let [<Literal>]cOrchestratorActorName = "Orchestrator"

let spawn getInstance getInstanceState getOracleAPI initialState actorFactory =
    Akkling.Spawn.spawn actorFactory cOrchestratorActorName <| props (orchestratorActorBody getInstance getInstanceState getOracleAPI initialState)
