module Application.Common

open Akkling

type ActorName = ActorName of string

let resolveSiblingActor (ActorName name) ctx =
    (select ctx <| sprintf "../%s" name).ResolveOne(System.TimeSpan.FromSeconds(1.))
    |> Async.RunSynchronously 
