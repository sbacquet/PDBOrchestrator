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
let spawnChildActors getInstance getInstanceState getOracleAPI state (ctx : Actor<_>) =
    state.OracleInstanceNames |> List.iter (fun instanceName ->
        ctx |> OracleInstanceActor.spawn getInstance getInstanceState getOracleAPI instanceName |> ignore
    )

let createMasterPDBError error : MasterPDBCreationResult = InvalidRequest [ error ]

let orchestratorActorBody getInstance getInstanceState getOracleAPI initialState (ctx : Actor<_>) =
    let rec loop (state : OrchestratorState) = actor {
        let primaryInstanceMaybe = lazy(ctx |> Common.resolveActor (oracleInstanceActorName state.PrimaryServer))
        let! msg = ctx.Receive()
        match msg with
        | Synchronize targetServer ->
            match primaryInstanceMaybe.Value with
            | Ok primaryInstance -> primaryInstance <<! TransferState targetServer
            | Error error -> ctx.Sender() <! stateSetError error
            return! loop state
        | GetState ->
            ctx.Sender() <! state
            return! loop state
        | CreateMasterPDB parameters ->
            match primaryInstanceMaybe.Value with
            | Ok primaryInstance -> primaryInstance <<! Application.OracleInstanceActor.CreateMasterPDB parameters
            | Error error -> ctx.Sender() <! createMasterPDBError error
    }
    ctx |> spawnChildActors getInstance getInstanceState getOracleAPI initialState
    loop initialState

let [<Literal>]cOrchestratorActorName = "Orchestrator"

let spawn getInstance getInstanceState getOracleAPI initialState actorFactory =
    Akkling.Spawn.spawn actorFactory cOrchestratorActorName <| props (orchestratorActorBody getInstance getInstanceState getOracleAPI initialState)

let getOrchestratorActor (ctx : Actor<_>) = Common.resolveActor (Common.ActorName cOrchestratorActorName) ctx

