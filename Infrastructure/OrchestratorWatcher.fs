module Infrastructure.OrchestratorWatcher

open Akkling

let private orchestratorWatcherBody orchestrator (ctx : Actor<_>) =

    orchestrator |> monitor ctx |> ignore

    let rec loop () = actor {
        let! msg = ctx.Receive()

        match msg with
        | Terminated _ ->
            ctx.Log.Value.Error("FATAL: orchestrator is down => stopping the server...")
            ctx.System.Terminate() |> Async.AwaitTask |> Async.Start
            return! Stop
        | _ -> return! loop ()
    }

    loop ()

let spawn orchestrator actorFactory =
    Akkling.Spawn.spawnAnonymous actorFactory <| props (orchestratorWatcherBody orchestrator)
