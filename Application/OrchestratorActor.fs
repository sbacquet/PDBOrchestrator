module Application.OrchestratorActor

open Akkling
open Application.StateActor

type Command =
| Synchronize of string

type Event =
| Synchronized

let primaryServerActor (ctx : Actor<_>) name =
    (select ctx name).ResolveOne(System.TimeSpan.FromSeconds(1.))
    |> Async.RunSynchronously 


let orchestratorActorBody initialState (ctx : Actor<_>) =
    let rec loop (state : Domain.OrchestratorState.OrchestratorState) = actor {
        let! (msg : obj) = ctx.Receive()
        match msg with
        | :? Command as mess ->
            match mess with
            | Synchronize targetServer ->
                let primaryServer = primaryServerActor ctx state.PrimaryServer
                primaryServer <! TransferState targetServer
                return! loop state
        | :? Application.StateActor.StateEvent as stateMessage ->
            match stateMessage with
            | StateSet ->
                ctx.Sender() <! Synchronized
            | _ -> return! unhandled()
        | _ -> return! unhandled()
    }
    loop initialState
