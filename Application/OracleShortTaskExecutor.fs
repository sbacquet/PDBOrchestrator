module Application.OracleShortTaskExecutor

open Akkling
open Application.Oracle
open Akka.Routing
open Application.Parameters

type Command =
| PDBExists of string
| GetPDBFilesFolder of string
| GetPDBNamesLike of string

let private oracleShortTaskExecutorBody (parameters:Parameters) (oracleAPI : IOracleAPI) (ctx : Actor<Command>) =

    let rec loop () =

        actor {

        let! command = ctx.Receive()

        match command with
        | PDBExists pdb ->
            let! exists = oracleAPI.PDBExists pdb
            ctx.Sender() <! exists
            return! loop ()

        | GetPDBFilesFolder pdb ->
            let! folder = oracleAPI.GetPDBFilesFolder pdb
            ctx.Sender() <! folder
            return! loop ()

        | GetPDBNamesLike like ->
            let! names = oracleAPI.GetPDBNamesLike like
            ctx.Sender() <! names
            return! loop ()
        
        }
    
    loop ()

let [<Literal>]cOracleShortTaskExecutorName = "OracleShortTaskExecutor"

let spawn (parameters:Parameters) oracleAPI actorFactory =
    Akkling.Spawn.spawn actorFactory cOracleShortTaskExecutorName 
    <| { props (oracleShortTaskExecutorBody parameters oracleAPI) 
            with Router = Some (upcast SmallestMailboxPool(parameters.NumberOfOracleShortTaskExecutors)) }
