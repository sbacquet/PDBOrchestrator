module Application.OracleInstanceGarbageCollector

open Akkling
open Akka.Actor
open Application.Parameters
open Application.PendingRequest
open Domain.Common.Exceptional
open Application.Common
open Domain.MasterPDBWorkingCopy
open Domain.Common
open Domain.OracleInstance
open Application.Oracle

type Command =
| CollectGarbage // responds with CommandToParent.WorkingCopiesDeleted
| DeleteWorkingCopy of WithRequestId<string> // responds with OraclePDBResultWithReqId

type CommandToParent =
| CollectVersionsGarbage of (string * int list)
| WorkingCopiesDeleted of (MasterPDBWorkingCopy * exn option) list

let private oracleInstanceGarbageCollectorBody 
    (parameters:Parameters)
    (oracleShortTaskExecutor:IActorRef<Application.OracleShortTaskExecutor.Command>)
    (oracleLongTaskExecutor:IActorRef<OracleLongTaskExecutor.Command>) 
    (workingCopyFactory:IActorRef<WorkingCopyFactoryActor.Command>)
    (instance:OracleInstance)
    (ctx : Actor<Command>) =

    let pdbExists pdb : Exceptional<bool> = 
        oracleShortTaskExecutor <? OracleShortTaskExecutor.PDBExists pdb
        |> runWithin parameters.ShortTimeout id (fun () -> sprintf "cannot check if PDB %s exists: timeout exceeded" pdb |> exn |> Error)
    let deleteWorkingCopy (wc:MasterPDBWorkingCopy) : OraclePDBResult = result {
        let! exists = pdbExists wc.Name
        if exists then
            return! 
                workingCopyFactory <? WorkingCopyFactoryActor.DeleteWorkingCopy (None, wc)
                |> runWithin parameters.VeryLongTimeout id (fun () -> sprintf "PDB %s cannot be deleted : timeout exceeded" wc.Name |> exn |> Error)
        else
            return wc.Name
    }
    let getPDBNamesLike like : Exceptional<string list> = 
        oracleShortTaskExecutor <? OracleShortTaskExecutor.GetPDBNamesLike like
        |> runWithin parameters.ShortTimeout id (fun () -> "cannot get PDB names : timeout exceeded" |> exn |> Error)
    let toErrorMaybe result = match result with | Ok _ -> None | Error error -> Some error

    let rec loop () = actor {

        let! command = ctx.Receive()

        match command with
        | CollectGarbage ->
            ctx.Log.Value.Info("Garbage collection of Oracle instance {instance} requested", instance.Name)
            // Delete expired working copies
            let deletionResults =
                instance.WorkingCopies
                |> Map.toList |> List.map snd
                |> List.filter isExpired
                |> List.map (fun wc -> (wc, deleteWorkingCopy wc |> toErrorMaybe))
            ctx.Sender() <! WorkingCopiesDeleted deletionResults
            
            let collectMasterPDBGarbage masterPDBName =
                let sourceVersionPDBsMaybe = getPDBNamesLike (sprintf "%s_V%%_%s" masterPDBName parameters.ServerInstanceName)
                match sourceVersionPDBsMaybe with
                | Ok sourceVersionPDBs -> 
                    if not (sourceVersionPDBs |> List.isEmpty) then
                        let regex = System.Text.RegularExpressions.Regex((sprintf "^%s_V(\\d{3})_.+$" masterPDBName))
                        let parseResults = sourceVersionPDBs |> List.map (fun sourceVersionPDB -> System.Int32.TryParse(regex.Replace(sourceVersionPDB, "$1")))
                        let versions = parseResults |> List.filter fst |> List.map snd
                        if not (versions |> List.isEmpty) then
                            ctx.Parent() <! CollectVersionsGarbage (masterPDBName, versions)
                | Error error -> 
                    ctx.Log.Value.Error(error.ToString())
            instance.MasterPDBs |> List.iter collectMasterPDBGarbage
            return! stop () // short-lived actor

        | DeleteWorkingCopy (requestId, workingCopy) ->
            ctx.Log.Value.Warning("Deleting unregistered working copy {pdb} on instance {instance} requested", workingCopy, instance.Name)
            workingCopyFactory <<! Application.WorkingCopyFactoryActor.DeleteUnregisteredWorkingCopy(Some requestId, workingCopy)
            return! stop () // short-lived actor

    }

    loop ()

let spawn parameters shortTaskExecutor longTaskExecutor workingCopyFactory (instance:OracleInstance) (actorFactory:IActorRefFactory) =

    (Akkling.Spawn.spawnAnonymous actorFactory
        <| props (
            oracleInstanceGarbageCollectorBody
                parameters
                shortTaskExecutor
                longTaskExecutor
                workingCopyFactory
                instance
        )).Retype<Command>()

