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
| SnapshotPDB of WithOptionalRequestId<string, string> // responds with OraclePDBResultWithReqId
| ExportPDB of WithOptionalRequestId<string, string> // responds with OraclePDBResultWithReqId
| DeletePDB of WithOptionalRequestId<string> // responds with OraclePDBResultWithReqId
| GarbageWorkingCopies of Domain.OracleInstance.OracleInstance // no response

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

        | SnapshotPDB (requestId, from, name) -> 
            stopWatch.Restart()
            let! result = oracleAPI.SnapshotPDB from name
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
            let! validation = name |> oracleAPI.DeletePDBWithSnapshots None
            let result = 
                match validation with
                | Validation.Valid _ -> Ok name
                | Validation.Invalid errors -> Error (errors |> List.map (fun ex -> ex.Message) |> String.concat "; " |> exn)
            match requestId with
            | Some reqId -> ctx.Sender() <! (reqId, result)
            | None -> ctx.Sender() <! result
            return! loop ()

        | GarbageWorkingCopies instance ->
            // Warning : here we are deleting working copies that could currently be accessed by version actors
            let! deleteResult = 
                oracleAPI.GetOldPDBsFromFolder parameters.GarbageCollectionDelay instance.WorkingCopyDestPath
                |> AsyncValidation.ofAsyncResult
                |> AsyncValidation.bind (AsyncValidation.traverseS (oracleAPI.DeletePDB >> AsyncValidation.ofAsyncResult))
            deleteResult |> Validation.mapErrors (fun errors -> let message = System.String.Join("; ", errors |> List.map (fun ex -> ex.Message)) in ctx.Log.Value.Warning(message); List.empty) |> ignore
            ctx.Log.Value.Info("Garbage collection of instance {instance} done.", instance.Name)
            return! loop ()

    }
    loop ()

let [<Literal>]cOracleLongTaskExecutorName = "OracleLongTaskExecutor"

let spawn (parameters:Parameters) oracleAPI actorFactory =
    Akkling.Spawn.spawn actorFactory cOracleLongTaskExecutorName 
    <| { props (oracleLongTaskExecutorBody parameters oracleAPI) 
            with Router = Some (upcast SmallestMailboxPool(parameters.NumberOfOracleLongTaskExecutors)) }
