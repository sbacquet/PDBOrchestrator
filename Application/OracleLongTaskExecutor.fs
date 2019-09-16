module Application.OracleLongTaskExecutor

open Akkling

type Command =
| CreatePDB of string * string * string * string
| ClosePDB of System.Guid * string
| ImportPDB of System.Guid * string * string * string
| SnapshotPDB of System.Guid * string * string * string
| ExportPDB of System.Guid * string * string
| DeletePDB of System.Guid * string


let oracleLogTaskExecutorBody (ctx : Actor<_>) =
    let rec loop () = actor {
        let! n = ctx.Receive()
        match n with
        | CreatePDB (a, b, c, name) -> 
            let result : Application.Oracle.OraclePDBResult = Ok name // TODO (inject OracleAPI)
            ctx.Sender() <! result
            return! loop ()
        | _ -> return! unhandled()
    }
    loop ()

