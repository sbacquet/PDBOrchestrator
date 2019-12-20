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

type Command =
| CreateWorkingCopy of WithRequestId<string, bool, bool> // responds with OraclePDBResultWithReqId
| DeleteWorkingCopy of WithRequestId<string> // responds with OraclePDBResultWithReqId

let private masterPDBEditionActorBody 
    (parameters:Parameters)
    (instance:OracleInstance) 
    (oracleShortTaskExecutor:IActorRef<OracleShortTaskExecutor.Command>) 
    (oracleLongTaskExecutor:IActorRef<OracleLongTaskExecutor.Command>) 
    (oracleDiskIntensiveTaskExecutor:IActorRef<OracleDiskIntensiveActor.Command>) 
    (editionPDBName:string)
    (ctx : Actor<Command>) =

    let pdbExists pdb : Async<Exceptional<bool>> = oracleShortTaskExecutor <? OracleShortTaskExecutor.PDBExists pdb
    let deletePDB pdb : Async<Exceptional<string>> = oracleLongTaskExecutor <? OracleLongTaskExecutor.DeletePDB (None, pdb)
    let getPDBFilesFolder pdb : Async<Exceptional.Exceptional<string option>> = oracleShortTaskExecutor <? OracleShortTaskExecutor.GetPDBFilesFolder pdb

    let rec loop () = actor {

        let! command = ctx.Receive()
        let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()

        match command with
        | CreateWorkingCopy (requestId, workingCopyName, durable, force) -> 
            let! result = asyncResult {
                let! wcExists = pdbExists workingCopyName
                if wcExists && (not force) then 
                    ctx.Log.Value.Info("Working copy {pdb} already exists, not enforcing creation.", workingCopyName)
                    return! AsyncResult.retn workingCopyName
                else
                    let! _ = 
                        if wcExists then deletePDB workingCopyName // force creation
                        else AsyncResult.retn ""
                    let destPath = sprintf "%s/%s" instance.WorkingCopyDestPath (if durable then "durable" else "temporary")
                    return! oracleDiskIntensiveTaskExecutor <? OracleDiskIntensiveActor.ClonePDB (None, editionPDBName, destPath, workingCopyName)
            }
            sender <! (requestId, result)
            return! loop ()

        | DeleteWorkingCopy (requestId, pdb) ->
            let! pdbFilesFolder = getPDBFilesFolder pdb
            let result:OraclePDBResult =
                match pdbFilesFolder with
                | Ok (Some folder) ->
                    if folder.StartsWith(instance.WorkingCopyDestPath) then
                        deletePDB pdb
                        |> runWithin parameters.LongTimeout id (fun () -> sprintf "PDB %s cannot be deleted : timeout exceeded" pdb |> exn |> Error)
                    else
                        sprintf "PDB %s is not a working copy" pdb |> exn |> Error
                | Ok None ->
                    sprintf "cannot find any file folder for PDB %s" pdb |> exn |> Error
                | Error error ->
                    Error error
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

