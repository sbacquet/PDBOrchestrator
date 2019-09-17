module Application.OracleLongTaskExecutor

open Akkling
open Application.PendingRequest
open Application.Oracle
open Application.PendingRequest

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
| ImportPDB of System.Guid * string * string * string
| SnapshotPDB of System.Guid * string * string * string
| ExportPDB of System.Guid * string * string
| DeletePDB of System.Guid * string


let oracleLogTaskExecutorBody (ctx : Actor<Command>) =
    let rec loop () = actor {
        let! n = ctx.Receive()
        match n with
        | CreatePDBFromDump (requestId, parameters) -> 
            let result : OraclePDBResult = Ok parameters.Name // TODO (inject OracleAPI)
            let response : WithRequestId<OraclePDBResult> = (requestId, result)
            ctx.Sender() <! response
            return! loop ()
        | _ -> return! unhandled()
    }
    loop ()

