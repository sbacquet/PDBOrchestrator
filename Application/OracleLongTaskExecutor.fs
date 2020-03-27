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
| OpenPDB of WithOptionalRequestId<string,bool> // responds with OraclePDBResultWithReqId
| ClosePDB of WithOptionalRequestId<string> // responds with OraclePDBResultWithReqId
| SnapshotPDB of WithOptionalRequestId<string, string, string> // responds with OraclePDBResultWithReqId
| ExportPDB of WithOptionalRequestId<string, string> // responds with OraclePDBResultWithReqId
| DeleteSnapshotPDB of WithOptionalRequestId<string> // responds with OraclePDBResultWithReqId

let private oracleLongTaskExecutorBody (parameters:Parameters) (oracleAPI : IOracleAPI) (ctx : Actor<Command>) =

    let rec loop () =
        
        actor {

        let! command = ctx.Receive()

        match command with
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

        | OpenPDB (requestId, name, readWrite) -> 
            let! result = oracleAPI.OpenPDB readWrite name
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
            let! result = oracleAPI.SnapshotPDB from dest name
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

        | DeleteSnapshotPDB (requestId, name) -> 
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
