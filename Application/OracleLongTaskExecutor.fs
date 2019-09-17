module Application.OracleLongTaskExecutor

open Akkling
open Application.PendingRequest
open Application.Oracle

type CreatePDBFromDumpParams = {
    Name: string
    AdminUserName: string
    AdminUserPassword: string
    Destination: string 
    DumpPath:string
    Schemas: string list
    TargetSchemas: (string * string) list
    Directory:string
    Callback: Akka.Actor.IActorRef -> OraclePDBResult -> unit
}

type Command =
| CreatePDBFromDump of CreatePDBFromDumpParams
| ClosePDB of System.Guid * string
| ImportPDB of System.Guid * string * string * string
| SnapshotPDB of System.Guid * string * string * string
| ExportPDB of System.Guid * string * string
| DeletePDB of System.Guid * string


let oracleLogTaskExecutorBody (ctx : Actor<Command>) =
    let rec loop () = actor {
        let! n = ctx.Receive()
        match n with
        | CreatePDBFromDump parameters -> 
            let result : OraclePDBResult = Ok parameters.Name // TODO (inject OracleAPI)
            result |> parameters.Callback (untyped (ctx.Sender()))
            return! loop ()
        | _ -> return! unhandled()
    }
    loop ()

