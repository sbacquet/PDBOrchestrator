module Application.OracleLongTaskExecutor

open Akkling
open Application.PendingRequest
open Application.Oracle
open Akka.Routing

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
| CreatePDBFromDump of WithRequestId<CreatePDBFromDumpParams>
| ClosePDB of WithRequestId<string>
| SnapshotPDB of WithRequestId<string, string, string>
| ExportPDB of WithRequestId<string, string>
| DeletePDB of WithRequestId<string>

let newManifestName (pdb:string) version =
    sprintf "%s_V%03d.XML" (pdb.ToUpper()) version

let oracleLongTaskExecutorBody (oracleAPI : IOracleAPI) (ctx : Actor<Command>) =

    let stopWatch = System.Diagnostics.Stopwatch()

    let rec loop () = actor {
        let! n = ctx.Receive()
        match n with
        | CreatePDBFromDump (requestId, parameters) -> 
            let! result = oracleAPI.NewPDBFromDump parameters.AdminUserName parameters.AdminUserPassword parameters.Destination parameters.DumpPath parameters.Schemas parameters.TargetSchemas parameters.Directory (newManifestName parameters.Name 1) parameters.Name
            ctx.Sender() <! (requestId, result)
            return! loop ()
        | ClosePDB (requestId, name) -> 
            let! result = oracleAPI.ClosePDB name
            ctx.Sender() <! (requestId, result)
            return! loop ()
        | SnapshotPDB (requestId, from, dest, name) -> 
            stopWatch.Restart()
            let! result = oracleAPI.SnapshotPDB from dest name
            stopWatch.Stop()
            result |> Result.map (fun snap -> ctx.Log.Value.Info("Snapshot {snapshot} created in {0} s", snap, stopWatch.Elapsed.TotalSeconds)) |> ignore
            let elapsed = stopWatch.Elapsed
            ctx.Sender() <! (requestId, result)
            return! loop ()
        | ExportPDB (requestId, manifest, name) -> 
            let! result = oracleAPI.ExportPDB manifest name
            ctx.Sender() <! (requestId, result)
            return! loop ()
        | DeletePDB (requestId, name) -> 
            let! result = oracleAPI.DeletePDB name
            ctx.Sender() <! (requestId, result)
            return! loop ()
    }
    loop ()

let [<Literal>]cOracleLongTaskExecutorName = "OracleLongTaskExecutor"
let numberOfOracleLongTaskExecutors = 3 // TODO : config

let spawn oracleAPI actorFactory =
    Akkling.Spawn.spawn actorFactory cOracleLongTaskExecutorName 
    <| { props (oracleLongTaskExecutorBody oracleAPI) with Router = Some (upcast SmallestMailboxPool(numberOfOracleLongTaskExecutors)) }

let getOracleLongTaskExecutor (ctx : Actor<_>) = Common.resolveActor (Common.ActorName cOracleLongTaskExecutorName) ctx
