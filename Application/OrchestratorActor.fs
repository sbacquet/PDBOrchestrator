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

let orchestratorActorBody getInstance getInstanceState getOracleAPI initialState (ctx : Actor<_>) =
    let rec loop (state : OrchestratorState) = actor {
        let primaryInstance = lazy(ctx |> Common.resolveActor (oracleInstanceActorName state.PrimaryServer))
        let! msg = ctx.Receive()
        match msg with
        | Synchronize targetServer ->
            primaryInstance.Value <<! TransferState targetServer
            return! loop state
        | GetState ->
            ctx.Sender() <! state
            return! loop state
        | CreateMasterPDB parameters ->
            retype primaryInstance.Value <<! Application.OracleInstanceActor.CreateMasterPDB parameters
    }
    ctx |> spawnChildActors getInstance getInstanceState getOracleAPI initialState
    loop initialState

let [<Literal>]cOrchestratorActorName = "Orchestrator"

let spawn getInstance getInstanceState getOracleAPI initialState actorFactory =
    Akkling.Spawn.spawn actorFactory cOrchestratorActorName <| props (orchestratorActorBody getInstance getInstanceState getOracleAPI initialState)

let getOrchestratorActor (ctx : Actor<_>) = Common.resolveActor (Common.ActorName cOrchestratorActorName) ctx

