module Application.OrchestratorActor

open Akkling
open Application.OracleServerActor
open Domain.OrchestratorState

type Command =
| Synchronize of string
| GetState

let primaryServerActor (ctx : Actor<_>) name =
    (select ctx name).ResolveOne(System.TimeSpan.FromSeconds(1.))
    |> Async.RunSynchronously 

let spawnChildActors (ctx : Actor<_>) state getInstanceState =
    state.OracleInstances |> List.iter (fun instance ->
        spawn ctx instance.Name <| props (oracleServerActorBody (getInstanceState instance.Name)) |> ignore
    )

let orchestratorActorBody initialState getInstanceState (ctx : Actor<_>) =
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
    spawnChildActors ctx initialState getInstanceState
    loop initialState
