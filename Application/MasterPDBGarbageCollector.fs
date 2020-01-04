module Application.MasterPDBGarbageCollector

open Akkling
open Akka.Actor
open Application.Parameters
open Application.PendingRequest
open Domain.Common.Exceptional
open Application.Common
open Domain.MasterPDBWorkingCopy

type Command =
| CollectGarbage // responds with CommandToParent.WorkingCopiesDeleted
| DeleteWorkingCopy of WithRequestId<MasterPDBWorkingCopy> // responds with OraclePDBResultWithReqId

type CommandToParent =
| CollectVersionsGarbage of string list
| WorkingCopiesDeleted of (MasterPDBWorkingCopy * exn option) list

let private masterPDBGarbageCollectorBody 
    (parameters:Parameters)
    (oracleShortTaskExecutor:IActorRef<Application.OracleShortTaskExecutor.Command>)
    (oracleLongTaskExecutor:IActorRef<OracleLongTaskExecutor.Command>) 
    (masterPDB:Domain.MasterPDB.MasterPDB)
    (ctx : Actor<Command>) =

    let deletePDB pdb : Exceptional<string> = 
        oracleLongTaskExecutor <? OracleLongTaskExecutor.DeletePDB (None, pdb)
        |> runWithin parameters.LongTimeout id (fun () -> sprintf "PDB %s cannot be deleted : timeout exceeded" pdb |> exn |> Error)
    let getPDBNamesLike like : Exceptional<string list> = 
        oracleShortTaskExecutor <? OracleShortTaskExecutor.GetPDBNamesLike like
        |> runWithin parameters.ShortTimeout id (fun () -> "cannot get PDB names : timeout exceeded" |> exn |> Error)
    let toErrorMaybe result = match result with | Ok _ -> None | Error error -> Some error

    let rec loop () = actor {

        let! command = ctx.Receive()

        match command with
        | CollectGarbage ->
            ctx.Log.Value.Info("Garbage collection of PDB {pdb} requested", masterPDB.Name)
            // Delete expired working copies
            let deletionResults =
                masterPDB.WorkingCopies
                |> Map.toList |> List.map snd
                |> List.choose (fun wc -> 
                    match wc.Lifetime with 
                    | Temporary expiry -> if (expiry <= System.DateTime.Now) then Some wc else None 
                    | _ -> None
                )
                |> List.map (fun wc -> (wc, deletePDB wc.Name |> toErrorMaybe))
            ctx.Sender() <! WorkingCopiesDeleted deletionResults
            
            let sourceVersionPDBsMaybe = getPDBNamesLike (sprintf "%s_V%%_%s" masterPDB.Name parameters.ServerInstanceName)
            match sourceVersionPDBsMaybe with
            | Ok sourceVersionPDBs -> 
                if not (sourceVersionPDBs |> List.isEmpty) then
                    ctx.Parent() <! CollectVersionsGarbage sourceVersionPDBs
            | Error error -> 
                ctx.Log.Value.Error(error.ToString())
            return! loop ()

        | DeleteWorkingCopy (requestId, workingCopy) ->
            let result = deletePDB workingCopy.Name
            ctx.Parent() <! WorkingCopiesDeleted [ (workingCopy, result |> toErrorMaybe) ]
            ctx.Sender() <! (requestId, result)
            return! loop ()

    }

    loop ()

let spawn parameters shortTaskExecutor longTaskExecutor (masterPDB:Domain.MasterPDB.MasterPDB) (actorFactory:IActorRefFactory) =

    (Akkling.Spawn.spawn actorFactory "garbageCollector"
        <| props (
            masterPDBGarbageCollectorBody 
                parameters
                shortTaskExecutor
                longTaskExecutor 
                masterPDB
        )).Retype<Command>()

