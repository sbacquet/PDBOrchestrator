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
            let result = oracleAPI.NewPDBFromDump parameters.AdminUserName parameters.AdminUserPassword parameters.Destination parameters.DumpPath parameters.Schemas parameters.TargetSchemas parameters.Directory (newManifestName parameters.Name 1) parameters.Name
            ctx.Sender() <! (requestId, result)
            return! loop ()
        | ClosePDB (requestId, name) -> 
            let result = oracleAPI.ClosePDB name
            ctx.Sender() <! (requestId, result)
            return! loop ()
        | ImportPDB (requestId, manifest, dest, name) -> 
            let result = oracleAPI.ImportPDB manifest dest name
            ctx.Sender() <! (requestId, result)
            return! loop ()
        | SnapshotPDB (requestId, from, dest, name) -> 
            let result = oracleAPI.ImportPDB from dest name
            ctx.Sender() <! (requestId, result)
            return! loop ()
        | ExportPDB (requestId, manifest, name) -> 
            let result = oracleAPI.ExportPDB manifest name
            ctx.Sender() <! (requestId, result)
            return! loop ()
        | DeletePDB (requestId, name) -> 
            let result = oracleAPI.DeletePDB name
            ctx.Sender() <! (requestId, result)
            return! loop ()
    }
    loop ()

