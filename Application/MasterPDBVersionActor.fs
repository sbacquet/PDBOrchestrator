module Application.MasterPDBVersionActor

open Akkling
open Akka.Actor
open Domain.MasterPDBVersion
open Application.PendingRequest
open Application.Oracle
open Application.Parameters
open Domain.Common
open Domain.Common.Exceptional
open Domain.OracleInstance
open Application.Common
open Application

type Command =
| CreateWorkingCopy of WithRequestId<string, bool> // responds with OraclePDBResultWithReqId
| CollectGarbage // no response
| HaraKiri // no response

type CommandToParent =
| KillVersion of int

let getSnapshotSourceName (pdb:string) (masterPDBVersion:MasterPDBVersion) (suffix:string) = sprintf "%s_V%03d_%s" (pdb.ToUpper()) masterPDBVersion.Number (suffix.ToUpper())

let private masterPDBVersionActorBody 
    (parameters:Parameters)
    (instance:OracleInstance) 
    (oracleShortTaskExecutor:IActorRef<OracleShortTaskExecutor.Command>) 
    (oracleLongTaskExecutor:IActorRef<OracleLongTaskExecutor.Command>) 
    (oracleDiskIntensiveTaskExecutor:IActorRef<OracleDiskIntensiveActor.Command>) 
    (masterPDBName:string)
    (masterPDBVersion:MasterPDBVersion) 
    (ctx : Actor<Command>) =

    let snapshotSourceName = getSnapshotSourceName masterPDBName masterPDBVersion parameters.ServerInstanceName
    let pdbExists pdb : Exceptional<bool> = 
        oracleShortTaskExecutor <? OracleShortTaskExecutor.PDBExists pdb
        |> runWithin parameters.ShortTimeout id (fun () -> sprintf "cannot check if PDB %s exists: timeout exceeded" pdb |> exn |> Error)
    let deletePDB pdb : Exceptional<string> = 
        oracleLongTaskExecutor <? OracleLongTaskExecutor.DeletePDB (None, pdb)
        |> runWithin parameters.LongTimeout id (fun () -> sprintf "PDB %s cannot be deleted : timeout exceeded" pdb |> exn |> Error)

    let rec loop () = actor {

        let! command = ctx.Receive()
        let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()

        match command with
        | CreateWorkingCopy (requestId, workingCopyName, snapshot) -> 
            let result:OraclePDBResult = result {
                let! wcExists = pdbExists workingCopyName
                let! _ = 
                    if wcExists then deletePDB workingCopyName // force creation
                    else Ok ""
                let sourceManifest = Domain.MasterPDBVersion.manifestFile masterPDBName masterPDBVersion.Number
                let destPath = instance.WorkingCopyDestPath
                if (instance.SnapshotCapable && snapshot) then
                    let! snapshotSourceExists = pdbExists snapshotSourceName
                    let! _ = 
                        if (not snapshotSourceExists) then
                            oracleDiskIntensiveTaskExecutor <? OracleDiskIntensiveActor.ImportPDB (None, sourceManifest, instance.SnapshotSourcePDBDestPath, snapshotSourceName)
                            |> runWithin parameters.VeryLongTimeout id (fun () -> sprintf "cannot create snapshot source PDB %s : timeout exceeded" snapshotSourceName |> exn |> Error)
                        else
                            ctx.Log.Value.Debug("Snapshot source PDB {pdb} already exists", snapshotSourceName)
                            Ok ""
                    let result:OraclePDBResult =
                        oracleLongTaskExecutor <? OracleLongTaskExecutor.SnapshotPDB (None, snapshotSourceName, destPath, workingCopyName)
                        |> runWithin parameters.LongTimeout id (fun () -> sprintf "cannot snapshot PDB %s to %s : timeout exceeded" snapshotSourceName workingCopyName |> exn |> Error)
                    return! result
                else
                    let result:OraclePDBResult =
                        oracleDiskIntensiveTaskExecutor <? OracleDiskIntensiveActor.ImportPDB (None, sourceManifest, destPath, workingCopyName)
                        |> runWithin parameters.VeryLongTimeout id (fun () -> sprintf "cannot create PDB clone %s : timeout exceeded" workingCopyName |> exn |> Error)
                    return! result
            }
            sender <! (requestId, result)
            return! loop ()

        | CollectGarbage ->
            if instance.SnapshotCapable then
                let _ = result {
                    let! _ = deletePDB snapshotSourceName
                    retype (ctx.Parent()) <! KillVersion masterPDBVersion.Number
                    return ()
                }
                return! loop ()
            else
                return! loop ()

        | HaraKiri ->
            ctx.Log.Value.Info("Stop of actor for version {pdbversion} of {pdb} requested", masterPDBVersion.Number, masterPDBName)
            retype ctx.Self <! Akka.Actor.PoisonPill.Instance
            return! loop ()
    }

    loop ()

let spawn parameters instance shortTaskExecutor longTaskExecutor oracleDiskIntensiveTaskExecutor (masterPDBName:string) (masterPDBVersion:MasterPDBVersion) (actorFactory:IActorRefFactory) =

    (Akkling.Spawn.spawnAnonymous actorFactory
        <| props (
            masterPDBVersionActorBody 
                parameters
                instance
                shortTaskExecutor 
                longTaskExecutor 
                oracleDiskIntensiveTaskExecutor 
                masterPDBName
                masterPDBVersion
        )).Retype<Command>()

