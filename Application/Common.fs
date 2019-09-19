module Application.Common

open Akkling

type ActorName = ActorName of string

let resolveActor (ActorName name) ctx =
    let actor = 
        (select ctx name).ResolveOne(System.TimeSpan.FromSeconds(1.))
        |> Async.RunSynchronously 
    if actor.Path.Address = Akka.Actor.Address.AllSystems then failwithf @"unresolvable actor name ""%s""" name
    actor

let resolveSiblingActor (ActorName name) = resolveActor (ActorName (sprintf "../%s" name))
