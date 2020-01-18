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
| CreateWorkingCopy of WithRequestId<string> // responds with OraclePDBResultWithReqId

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
    let cloneEditionPDB name : OraclePDBResult =
        oracleDiskIntensiveTaskExecutor <? OracleDiskIntensiveActor.ClonePDB (None, editionPDBName, instance.WorkingCopyDestPath, name)
        |> runWithin parameters.VeryLongTimeout id (fun () -> sprintf "cannot clone PDB %s to %s : timeout exceeded" editionPDBName name |> exn |> Error)
    let isWorkingCopy (pdb:string) : Exceptional<bool> = result {
        let! (folder:string option) = 
            oracleShortTaskExecutor <? OracleShortTaskExecutor.GetPDBFilesFolder pdb
            |> runWithin parameters.ShortTimeout id (fun () -> "cannot get files folder : timeout exceeded" |> exn |> Error)
        return folder |> Option.map (fun folder -> folder |> isWorkingCopyFolder instance) |> Option.defaultValue false
    }

    let rec loop () = actor {

        let! command = ctx.Receive()
        let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()

        match command with
        | CreateWorkingCopy (requestId, workingCopyName) -> 
            let result = result {
                let! wcExists = pdbExists workingCopyName
                let! _ = 
                    if wcExists then
                        result {
                            let! isWorkingCopy = isWorkingCopy workingCopyName
                            if isWorkingCopy then
                                return! deletePDB workingCopyName // force creation
                            else
                                return! Error <| (sprintf "PDB %s exists and is not a working copy, hence cannot be overwritten" workingCopyName |> exn)
                        }
                    else Ok ""
                return! cloneEditionPDB workingCopyName
            }
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

