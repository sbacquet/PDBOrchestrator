module Application.OracleLongTaskExecutor

open Akkling
open Application.PendingRequest
open Application.Oracle
open Akka.Routing
open Application.GlobalParameters

type CreatePDBFromDumpParams = {
    Name: string
    AdminUserName: string
    AdminUserPassword: string
    Destination: string 
    DumpPath:string
    Schemas: string list
    TargetSchemas: (string * string) list
    Directory:string
}

type Command =
| CreatePDBFromDump of WithOptionalRequestId<CreatePDBFromDumpParams> // responds with OraclePDBResultWithReqId
| ClosePDB of WithOptionalRequestId<string> // responds with OraclePDBResultWithReqId
| SnapshotPDB of WithOptionalRequestId<string, string, string> // responds with OraclePDBResultWithReqId
| ExportPDB of WithOptionalRequestId<string, string> // responds with OraclePDBResultWithReqId
| DeletePDB of WithOptionalRequestId<string> // responds with OraclePDBResultWithReqId

let private newManifestName (pdb:string) version =
    sprintf "%s_V%03d.XML" (pdb.ToUpper()) version

let private oracleLongTaskExecutorBody (oracleAPI : IOracleAPI) (ctx : Actor<Command>) =

    let stopWatch = System.Diagnostics.Stopwatch()

    let rec loop () = actor {

        let! n = ctx.Receive()

        match n with
        | CreatePDBFromDump (requestId, parameters) -> 
            let! result = oracleAPI.NewPDBFromDump parameters.AdminUserName parameters.AdminUserPassword parameters.Destination parameters.DumpPath parameters.Schemas parameters.TargetSchemas parameters.Directory (newManifestName parameters.Name 1) parameters.Name
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
            let elapsed = stopWatch.Elapsed
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
            let! result = oracleAPI.DeletePDB name
            match requestId with
            | Some reqId -> ctx.Sender() <! (reqId, result)
            | None -> ctx.Sender() <! result
            return! loop ()
    }
    loop ()

let [<Literal>]cOracleLongTaskExecutorName = "OracleLongTaskExecutor"

let spawn (parameters:GlobalParameters) oracleAPI actorFactory =
    Akkling.Spawn.spawn actorFactory cOracleLongTaskExecutorName 
    <| { props (oracleLongTaskExecutorBody oracleAPI) 
            with Router = Some (upcast SmallestMailboxPool(parameters.NumberOfOracleLongTaskExecutors)) }
