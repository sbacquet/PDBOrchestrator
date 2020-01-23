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
    (oracleLongTaskExecutor:IActorRef<OracleLongTaskExecutor.Command>) 
    (oracleDiskIntensiveTaskExecutor:IActorRef<OracleDiskIntensiveActor.Command>) 
    (editionPDBName:string)
    (ctx : Actor<Command>) =

    let pdbExists pdb : Exceptional<bool> = 
        oracleShortTaskExecutor <? OracleShortTaskExecutor.PDBExists pdb
        |> runWithin parameters.ShortTimeout id (fun () -> sprintf "cannot check if PDB %s exists: timeout exceeded" pdb |> exn |> Error)
    let deletePDB pdb : OraclePDBResult = 
        oracleLongTaskExecutor <? OracleLongTaskExecutor.DeletePDB (None, pdb)
        |> runWithin parameters.LongTimeout id (fun () -> sprintf "PDB %s cannot be deleted : timeout exceeded" pdb |> exn |> Error)
    let cloneEditionPDB durable name : OraclePDBResult =
        oracleDiskIntensiveTaskExecutor <? OracleDiskIntensiveActor.ClonePDB (None, editionPDBName, instance |> getWorkingCopyFolder durable, name)
        |> runWithin parameters.VeryLongTimeout id (fun () -> sprintf "cannot clone PDB %s to %s : timeout exceeded" editionPDBName name |> exn |> Error)
    let isTempWorkingCopy (pdb:string) : Exceptional<bool> = result {
        let! (folder:string option) = 
            oracleShortTaskExecutor <? OracleShortTaskExecutor.GetPDBFilesFolder pdb
            |> runWithin parameters.ShortTimeout id (fun () -> "cannot get files folder : timeout exceeded" |> exn |> Error)
        return folder |> Option.map (fun folder -> folder |> isTemporaryWorkingCopyFolder instance) |> Option.defaultValue false
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
                    let! isTemp = isTempWorkingCopy workingCopyName
                    if (not isTemp) <> durable then
                        return! Error <| (sprintf "working copy %s already exists but for a different durability (%s)" workingCopyName (lifetimeText isTemp) |> exn)
                    else
                        return workingCopyName
                else
                    let! _ = result {
                        if wcExists then // force destruction
                            let! canDelete = 
                                if durable then Ok true
                                else isTempWorkingCopy workingCopyName
                            return!
                                if canDelete then
                                    deletePDB workingCopyName // force creation
                                else
                                    Error <| (sprintf "PDB %s exists and is not a temporary working copy, hence cannot be overwritten" workingCopyName |> exn)
                        else return ""
                    }
                    return! workingCopyName |> cloneEditionPDB durable
            }
            sender <! (requestId, result)
            return! loop ()
        
        | DeleteWorkingCopy (requestId, workingCopy) ->
            ctx.Log.Value.Info("Deleting edition working copy {pdb} on instance {instance} requested", workingCopy.Name, instance.Name)
            let sender = ctx.Sender().Retype<Application.Oracle.OraclePDBResultWithReqId>()
            let result = deletePDB workingCopy.Name
            sender <! (requestId, result)
            return! loop ()

        }

    loop ()

let spawn parameters instance shortTaskExecutor longTaskExecutor oracleDiskIntensiveTaskExecutor (editionPDBName:string) (actorFactory:IActorRefFactory) =

    (Akkling.Spawn.spawn actorFactory "edition"
        <| props (
            masterPDBEditionActorBody 
                parameters
                instance
                shortTaskExecutor 
                longTaskExecutor 
                oracleDiskIntensiveTaskExecutor 
                editionPDBName
        )).Retype<Command>()

