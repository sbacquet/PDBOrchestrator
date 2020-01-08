module Application.OracleLongTaskExecutor

open Akkling
open Application.PendingRequest
open Application.Oracle
open Akka.Routing
open Application.Parameters
open Domain.Common

type CreatePDBFromDumpParams = {
    Name: string
    DumpPath:string
    Schemas: string list
    TargetSchemas: (string * string) list
}

type Command =
| CreatePDBFromDump of WithOptionalRequestId<CreatePDBFromDumpParams> // responds with OraclePDBResultWithReqId
| ClosePDB of WithOptionalRequestId<string> // responds with OraclePDBResultWithReqId
| SnapshotPDB of WithOptionalRequestId<string, string, string> // responds with OraclePDBResultWithReqId
| ExportPDB of WithOptionalRequestId<string, string> // responds with OraclePDBResultWithReqId
| DeletePDB of WithOptionalRequestId<string> // responds with OraclePDBResultWithReqId

let private oracleLongTaskExecutorBody (parameters:Parameters) (oracleAPI : IOracleAPI) (ctx : Actor<Command>) =

    let stopWatch = System.Diagnostics.Stopwatch()

    let rec loop () = actor {

        let! n = ctx.Receive()

        match n with
        | CreatePDBFromDump (requestId, pars) -> 
            let! result = 
                oracleAPI.NewPDBFromDump 
                    parameters.VeryLongTimeout 
                    pars.Name
                    pars.DumpPath 
                    pars.Schemas 
                    pars.TargetSchemas 
            match requestId with
            | Some reqId -> ctx.Sender() <! (reqId, result)
            | None -> ctx.Sender() <! result
            return! loop ()

        | ClosePDB (requestId, name) -> 
            let! result = oracleAPI.ClosePDB name
            match requestId with
            | Some reqId -> ctx.Sender() <! (reqId, result)
            | None -> ctx.Sender() <! result
            return! loop ()

        | SnapshotPDB (requestId, from, dest, name) -> 
            stopWatch.Restart()
            let! result = oracleAPI.SnapshotPDB from dest name
            stopWatch.Stop()
            result |> Result.map (fun snap -> ctx.Log.Value.Info("Snapshot {snapshot} created in {0} s", snap, stopWatch.Elapsed.TotalSeconds)) |> ignore
            match requestId with
            | Some reqId -> ctx.Sender() <! (reqId, result)
            | None -> ctx.Sender() <! result
            return! loop ()

        | ExportPDB (requestId, manifest, name) -> 
            let! result = oracleAPI.ExportPDB manifest name
            match requestId with
            | Some reqId -> ctx.Sender() <! (reqId, result)
            | None -> ctx.Sender() <! result
            return! loop ()

        | DeletePDB (requestId, name) -> 
            let! result = name |> oracleAPI.DeletePDB
            match requestId with
            | Some reqId -> ctx.Sender() <! (reqId, result)
            | None -> ctx.Sender() <! result
            return! loop ()
    }
    loop ()

let [<Literal>]cOracleLongTaskExecutorName = "OracleLongTaskExecutor"

let spawn (parameters:Parameters) oracleAPI actorFactory =
    Akkling.Spawn.spawn actorFactory cOracleLongTaskExecutorName 
    <| { props (oracleLongTaskExecutorBody parameters oracleAPI) 
            with Router = Some (upcast SmallestMailboxPool(parameters.NumberOfOracleLongTaskExecutors)) }
