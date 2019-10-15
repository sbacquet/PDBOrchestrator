module Application.OracleDiskIntensiveActor

open Akkling
open Application.PendingRequest
open Application.Oracle
open Akka.Routing
open Application.GlobalParameters

type Command =
| ImportPDB of WithRequestId<string, string, string>

let oracleDiskIntensiveTaskExecutorBody (oracleAPI : IOracleAPI) (ctx : Actor<Command>) =

    let stopWatch = System.Diagnostics.Stopwatch()

    let rec loop () = actor {
        let! n = ctx.Receive()
        match n with
        | ImportPDB (requestId, manifest, dest, name) -> 
            stopWatch.Restart()
            let! result = oracleAPI.ImportPDB manifest dest name
            stopWatch.Stop()
            result |> Result.map (fun pdb -> ctx.Log.Value.Info("PDB {PDB} imported in {0} s", pdb, stopWatch.Elapsed.TotalSeconds)) |> ignore
            ctx.Sender() <! (requestId, result)
            return! loop ()
    }
    loop ()

let [<Literal>]cOracleLongTaskExecutorName = "OracleDiskIntensiveTaskExecutor"

let spawn (parameters:GlobalParameters) oracleAPI actorFactory =
    Akkling.Spawn.spawn actorFactory cOracleLongTaskExecutorName 
    <| { props (oracleDiskIntensiveTaskExecutorBody oracleAPI) 
            with Router = Some (upcast SmallestMailboxPool(parameters.NumberOfOracleDiskIntensiveTaskExecutors)) }
