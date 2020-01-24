module Application.OracleDiskIntensiveActor

open Akkling
open Application.PendingRequest
open Application.Oracle
open Akka.Routing
open Application.Parameters

type Command =
| ImportPDB of WithOptionalRequestId<string, string, string>
| ClonePDB of WithOptionalRequestId<string, string, string> // responds with OraclePDBResultWithReqId

let private oracleDiskIntensiveTaskExecutorBody (oracleAPI : IOracleAPI) (ctx : Actor<Command>) =

    let stopWatch = System.Diagnostics.Stopwatch()

    let rec loop () =

        actor {

        let! command = ctx.Receive()

        match command with
        | ImportPDB (requestId, manifest, dest, name) -> 
            stopWatch.Restart()
            let! result = oracleAPI.ImportPDB manifest dest name
            stopWatch.Stop()
            result |> Result.map (fun pdb -> ctx.Log.Value.Info("PDB {PDB} imported in {0} s", pdb, stopWatch.Elapsed.TotalSeconds)) |> ignore
            match requestId with
            | Some reqId -> ctx.Sender() <! (reqId, result)
            | None -> ctx.Sender() <! result
            return! loop ()

        | ClonePDB (requestId, from, dest, name) -> 
            stopWatch.Restart()
            let! result = oracleAPI.ClonePDB from dest name
            stopWatch.Stop()
            result |> Result.map (fun clone -> ctx.Log.Value.Info("Clone {PDB} created in {0} s", clone, stopWatch.Elapsed.TotalSeconds)) |> ignore
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
