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
| ImportPDB of WithRequestId<string, string, string>
| SnapshotPDB of WithRequestId<string, string, string>
| ExportPDB of WithRequestId<string, string>
| DeletePDB of WithRequestId<string>

let newManifestName (pdb:string) version =
    sprintf "%s_v%03d" (pdb.ToUpper()) version

let oracleLongTaskExecutorBody (oracleAPI : OracleAPI) (ctx : Actor<Command>) =
    let rec loop () = actor {
        let! n = ctx.Receive()
        match n with
        | CreatePDBFromDump (requestId, parameters) -> 
            let result : OraclePDBResult = oracleAPI.NewPDBFromDump parameters.AdminUserName parameters.AdminUserPassword parameters.Destination parameters.DumpPath parameters.Schemas parameters.TargetSchemas parameters.Directory (newManifestName parameters.Name 1) parameters.Name
            let response : WithRequestId<OraclePDBResult> = (requestId, result)
            ctx.Sender() <! response
            return! loop ()
        | _ -> return! unhandled()
    }
    loop ()

