module Application.Common

open Akkling
open Akka.Actor

type ActorName = ActorName of string

let resolveActor (ActorName name) (ctx:Actor<_>) =
    try
        let actor = 
            (select ctx name).ResolveOne(System.TimeSpan.FromSeconds(1.))
            |> Async.RunSynchronously 
        if actor.Path.Address = Akka.Actor.Address.AllSystems then 
            Error (sprintf @"unresolvable actor name ""%s""" name) 
        else 
            Ok actor
    with 
    | _ -> Error (sprintf @"cannot find any actor with name ""%s"" under ""%s""" name ctx.Self.Path.Name)

let resolveSiblingActor (ActorName name) = resolveActor (ActorName (sprintf "../%s" name))
