module Application.OracleDiskIntensiveActor

open Akkling
open Application.PendingRequest
open Application.Oracle

type Command =
| ImportPDB of WithRequestId<string, string, string>

let oracleDiskIntensiveTaskExecutorBody (oracleAPI : IOracleAPI) (ctx : Actor<Command>) =
    let rec loop () = actor {
        let! n = ctx.Receive()
        match n with
        | ImportPDB (requestId, manifest, dest, name) -> 
            let! result = oracleAPI.ImportPDB manifest dest name
            ctx.Sender() <! (requestId, result)
            return! loop ()
    }
    loop ()

let [<Literal>]cOracleLongTaskExecutorName = "OracleDiskIntensiveTaskExecutor"

let spawn oracleAPI actorFactory =
    Akkling.Spawn.spawn actorFactory cOracleLongTaskExecutorName <| props (oracleDiskIntensiveTaskExecutorBody oracleAPI)
