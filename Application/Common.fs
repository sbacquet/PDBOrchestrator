module Application.Common

open Akkling
open Akka.Actor

type ActorName = ActorName of string

let runWithinElseTimeoutException timeout cont = Async.RunSynchronously(cont, timeout)

let runWithin timeout ok error cont = 
    try
        cont |> runWithinElseTimeoutException timeout |> ok
    with
    | :? System.TimeoutException -> error()

let runWithinElseDefault timeout defaultValue cont = runWithin timeout id (fun () -> defaultValue) cont

let runWithinElseError timeout error cont = runWithin timeout Ok (fun () -> Error error) cont

let runWithinElseDefaultError timeout cont = runWithinElseError timeout "operation timed out" cont

let resolveActor (ActorName name) (ctx:Actor<_>) =
    try
        let actor = 
            (select ctx name).ResolveOne(System.TimeSpan.FromSeconds(1.))
            |> runWithinElseTimeoutException 1100 
        if actor.Path.Address = Akka.Actor.Address.AllSystems then 
            Error (sprintf @"unresolvable actor name ""%s""" name) 
        else 
            Ok actor
    with 
    | _ -> Error (sprintf @"cannot find any actor with name ""%s"" under ""%s""" name ctx.Self.Path.Name)

let resolveSiblingActor (ActorName name) = resolveActor (ActorName (sprintf "../%s" name))

type IRepository<'K, 'T> =
    abstract member Get : 'K -> 'T
    abstract member Put : 'K -> 'T -> IRepository<'K, 'T>

type IMasterPDBRepository = IRepository<string, Domain.MasterPDB.MasterPDB>

type IOracleInstanceRepository = IRepository<string, Domain.OracleInstance.OracleInstance>

type IOrchestratorRepository = IRepository<string, Domain.Orchestrator.Orchestrator>
