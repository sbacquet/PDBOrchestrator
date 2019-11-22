module Application.OracleShortTaskExecutor

open Akkling
open Application.PendingRequest
open Application.Oracle
open Akka.Routing
open Application.Parameters
open Domain.Common

type Command =
| PDBExists of string
| GetPDBFilesFolder of string
| GetPDBNamesLike of string

let private oracleShortTaskExecutorBody (parameters:Parameters) (oracleAPI : IOracleAPI) (ctx : Actor<Command>) =

    let stopWatch = System.Diagnostics.Stopwatch()

    let rec loop () = actor {

        let! n = ctx.Receive()

        match n with
        | PDBExists pdb ->
            let! exists = oracleAPI.PDBExists pdb
            ctx.Sender() <! exists

        | GetPDBFilesFolder pdb ->
            let! folder = oracleAPI.GetPDBFilesFolder pdb
            ctx.Sender() <! folder

        | GetPDBNamesLike like ->
            let! names = oracleAPI.GetPDBNamesLike like
            ctx.Sender() <! names
    }
    loop ()

let [<Literal>]cOracleShortTaskExecutorName = "OracleShortTaskExecutor"

let spawn (parameters:Parameters) oracleAPI actorFactory =
    Akkling.Spawn.spawn actorFactory cOracleShortTaskExecutorName 
    <| { props (oracleShortTaskExecutorBody parameters oracleAPI) 
            with Router = Some (upcast SmallestMailboxPool(parameters.NumberOfOracleShortTaskExecutors)) }
