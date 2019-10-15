module Application.Common

open Akkling
open Akka.Actor
open System

type ActorName = ActorName of string

let runWithinElseTimeoutException (timeout:System.TimeSpan option) cont = 
    Async.RunSynchronously(cont, match timeout with | Some t -> (int)t.TotalMilliseconds | None -> -1)

let runWithin timeout ok error cont = 
    try
        cont |> runWithinElseTimeoutException timeout |> ok
    with
    | :? System.TimeoutException -> error()

let runWithinElseDefault timeout defaultValue cont = runWithin timeout id (fun () -> defaultValue) cont

let runWithinElseError timeout error cont = runWithin timeout Ok (fun () -> Error error) cont

let runWithinElseDefaultError timeout cont = runWithinElseError timeout "operation timed out" cont

type IRepository<'K, 'T> =
    abstract member Get : 'K -> 'T
    abstract member Put : 'K -> 'T -> IRepository<'K, 'T>

type IMasterPDBRepository = IRepository<string, Domain.MasterPDB.MasterPDB>

type IOracleInstanceRepository = IRepository<string, Domain.OracleInstance.OracleInstance>

type IOrchestratorRepository = IRepository<string, Domain.Orchestrator.Orchestrator>
