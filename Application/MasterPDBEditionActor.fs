module Application.MasterPDBEditionActor

open Akkling
open Akka.Actor
open Application.PendingRequest
open Application.Oracle
open Application.Parameters
open Domain.Common
open Domain.Common.Exceptional
open Domain.OracleInstance
open Application.Common
open Domain.MasterPDBWorkingCopy

type Command =
| CreateWorkingCopy of WithRequestId<string, bool, bool> // responds with OraclePDBResultWithReqId
| DeleteWorkingCopy of WithRequestId<MasterPDBWorkingCopy> // responds with OraclePDBResultWithReqId

let private masterPDBEditionActorBody 
    (parameters:Parameters)
    (instance:OracleInstance) 
    (oracleShortTaskExecutor:IActorRef<OracleShortTaskExecutor.Command>) 
    (workingCopyFactory:IActorRef<Application.WorkingCopyFactoryActor.Command>)
    (editionPDBName:string)
    (ctx : Actor<Command>) =

    let pdbExists pdb : Exceptional<bool> = 
        oracleShortTaskExecutor <? OracleShortTaskExecutor.PDBExists pdb
        |> runWithin parameters.ShortTimeout id (fun () -> sprintf "cannot check if PDB %s exists: timeout exceeded" pdb |> exn |> Error)
    let isTempWorkingCopy (pdb:string) : Exceptional<bool> = result {
        let! (folder:string option) = 
            oracleShortTaskExecutor <? OracleShortTaskExecutor.GetPDBFilesFolder pdb
            |> runWithin parameters.ShortTimeout id (fun () -> "cannot get files folder : timeout exceeded" |> exn |> Error)
        return folder |> Option.map (fun folder -> folder |> isTemporaryWorkingCopyFolder instance) |> Option.defaultValue false
    }
    let isDurableWorkingCopy (pdb:string) : Exceptional<bool> = result {
        let! (folder:string option) = 
            oracleShortTaskExecutor <? OracleShortTaskExecutor.GetPDBFilesFolder pdb
            |> runWithin parameters.ShortTimeout id (fun () -> "cannot get files folder : timeout exceeded" |> exn |> Error)
        return folder |> Option.map (fun folder -> folder |> isDurableWorkingCopyFolder instance) |> Option.defaultValue false
    }

    let rec loop () = 
        
        actor {

        let! command = ctx.Receive()
        let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()

        match command with
        | CreateWorkingCopy (requestId, workingCopyName, durable, force) -> 
            let result = result {
                let! wcExists = pdbExists workingCopyName
                // if the working copy already exists and not forcing, keep it if same durability
                if wcExists && not force then
                    let! isDurable = isDurableWorkingCopy workingCopyName
                    if isDurable <> durable then
                        return! Error <| (sprintf "working copy %s already exists but for a different durability (%s)" workingCopyName (lifetimeText isDurable) |> exn)
                    else
                        return Some workingCopyName
                else
                    let! deleteFirst = result {
                        if wcExists then
                            let! canDelete = 
                                if durable then Ok true
                                else isTempWorkingCopy workingCopyName
                            return!
                                if canDelete then
                                    Ok true
                                else
                                    Error <| (sprintf "PDB %s exists and is not a temporary working copy, so cannot be overwritten" workingCopyName |> exn)
                        else
                            return false
                    }
                    workingCopyFactory <<! WorkingCopyFactoryActor.CreateWorkingCopyOfEdition(Some requestId, editionPDBName, (instance |> getWorkingCopyFolder durable), workingCopyName, deleteFirst)
                    return None
            }
            match result with
            | Error error ->
                sender <! (requestId, Error error)
            | Ok (Some pdb) ->
                sender <! (requestId, Ok pdb)
            | Ok None -> ()
            return! loop ()
        
        | DeleteWorkingCopy (requestId, workingCopy) ->
            ctx.Log.Value.Info("Deleting edition working copy {pdb} on instance {instance} requested", workingCopy.Name, instance.Name)
            workingCopyFactory <<! WorkingCopyFactoryActor.DeleteWorkingCopy(Some requestId, workingCopy.Name, false)
            return! loop ()

        }

    loop ()

let spawn 
        parameters instance 
        (shortTaskExecutor:IActorRef<Application.OracleShortTaskExecutor.Command>)
        (workingCopyFactory:IActorRef<Application.WorkingCopyFactoryActor.Command>)
        (editionPDBName:string) (actorFactory:IActorRefFactory) =

    (Akkling.Spawn.spawn actorFactory "edition"
        <| props (
            masterPDBEditionActorBody
                parameters
                instance
                shortTaskExecutor
                workingCopyFactory
                editionPDBName
        )).Retype<Command>()

