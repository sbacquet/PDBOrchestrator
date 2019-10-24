module Application.OracleDiskIntensiveActor

open Akkling
open Application.PendingRequest
open Application.Oracle
open Akka.Routing
open Application.Parameters

type Command =
| ImportPDB of WithOptionalRequestId<string, string, string>

let private oracleDiskIntensiveTaskExecutorBody (oracleAPI : IOracleAPI) (ctx : Actor<Command>) =

    let stopWatch = System.Diagnostics.Stopwatch()

    let rec loop () = actor {

        let! n = ctx.Receive()

        match n with
        | ImportPDB (requestId, manifest, dest, name) -> 
            stopWatch.Restart()
            let! result = oracleAPI.ImportPDB manifest dest name
            stopWatch.Stop()
            result |> Result.map (fun pdb -> ctx.Log.Value.Info("PDB {PDB} imported in {0} s", pdb, stopWatch.Elapsed.TotalSeconds)) |> ignore
            match requestId with
            | Some reqId -> ctx.Sender() <! (reqId, result)
            | None -> ctx.Sender() <! result
            return! loop ()
    }
    loop ()

let [<Literal>]cOracleLongTaskExecutorName = "OracleDiskIntensiveTaskExecutor"

let spawn (parameters:Parameters) oracleAPI actorFactory =
    Akkling.Spawn.spawn actorFactory cOracleLongTaskExecutorName 
    <| { props (oracleDiskIntensiveTaskExecutorBody oracleAPI) 
            with Router = Some (upcast SmallestMailboxPool(parameters.NumberOfOracleDiskIntensiveTaskExecutors)) }
