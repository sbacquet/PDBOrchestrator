module Application.OrchestratorActor

open Akkling
open Application.OracleInstanceActor
open Domain.Orchestrator

type Command =
| Synchronize of string
| GetState

let primaryServerActor (ctx : Actor<_>) name =
    (select ctx name).ResolveOne(System.TimeSpan.FromSeconds(1.))
    |> Async.RunSynchronously 

let spawnChildActors getInstance getInstanceState getOracleAPI state (ctx : Actor<_>) =
    state.OracleInstanceNames |> List.iter (fun instanceName ->
        spawn ctx instanceName <| props (oracleInstanceActorBody getInstance getInstanceState getOracleAPI instanceName) |> ignore
    )

let orchestratorActorBody getInstance getInstanceState getOracleAPI initialState (ctx : Actor<_>) =
    let rec loop (state : OrchestratorState) = actor {
        let! msg = ctx.Receive()
        match msg with
        | Synchronize targetServer ->
            let primaryServer = primaryServerActor ctx state.PrimaryServer
            primaryServer <<! TransferState targetServer
            return! loop state
        | GetState ->
            ctx.Sender() <! state
            return! loop state
            
    }
    ctx |> spawnChildActors getInstance getInstanceState getOracleAPI initialState
    loop initialState
