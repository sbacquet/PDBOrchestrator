module Application.MasterPDBVersionActor

open Akkling
open Akka.Actor
open Domain.MasterPDBVersion
open Application.PendingRequest
open Application.Oracle
open Application.Parameters
open Domain.Common
open Domain.Common.Exceptional
open Domain.Common.Validation
open Domain.OracleInstance
open Application.Common

type Command =
| CreateWorkingCopy of WithRequestId<string, string, bool> // responds with OraclePDBResultWithReqId
| DeleteWorkingCopy of WithRequestId<string> // responds with OraclePDBResultWithReqId
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
    let pdbExists pdb : Async<Exceptional<bool>> = oracleShortTaskExecutor <? OracleShortTaskExecutor.PDBExists pdb
    let deletePDB pdb : Async<Exceptional<string>> = oracleLongTaskExecutor <? OracleLongTaskExecutor.DeletePDB (None, pdb)
    let deletePDBOlderThan delay pdb : Async<Validation<bool, exn>> = oracleLongTaskExecutor <? OracleLongTaskExecutor.DeletePDBOlderThan (None, pdb, delay)
    let getPDBNamesLike like : Async<Exceptional<string list>> = oracleShortTaskExecutor <? OracleShortTaskExecutor.GetPDBNamesLike like
    let getPDBFilesFolder pdb : Async<Exceptional.Exceptional<string option>> = oracleShortTaskExecutor <? OracleShortTaskExecutor.GetPDBFilesFolder pdb

    let rec loop () = actor {

        let! command = ctx.Receive()
        let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()

        match command with
        | CreateWorkingCopy (requestId, sourceManifest, workingCopyName, force) -> 
            let! result = asyncResult {
                let! wcExists = pdbExists workingCopyName
                if wcExists && (not force) then 
                    ctx.Log.Value.Info("Working copy {pdb} already exists, not enforcing creation.", workingCopyName)
                    return! AsyncResult.retn workingCopyName
                else
                    let! _ = 
                        if wcExists then deletePDB workingCopyName // force creation
                        else AsyncResult.retn ""
                    if (instance.SnapshotCapable) then
                        let! snapshotSourceExists = pdbExists snapshotSourceName
                        let! _ = 
                            if (not snapshotSourceExists) then
                                oracleDiskIntensiveTaskExecutor <? OracleDiskIntensiveActor.ImportPDB (None, sourceManifest, instance.SnapshotSourcePDBDestPath, snapshotSourceName)
                            else
                                ctx.Log.Value.Debug("Snapshot source PDB {pdb} already exists", snapshotSourceName)
                                AsyncResult.retn ""
                        return! oracleLongTaskExecutor <? OracleLongTaskExecutor.SnapshotPDB (None, snapshotSourceName, workingCopyName)
                    else
                        return! oracleDiskIntensiveTaskExecutor <? OracleDiskIntensiveActor.ImportPDB (None, sourceManifest, instance.WorkingCopyDestPath, workingCopyName)
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

        | CollectGarbage ->
            ctx.Log.Value.Info("Collecting garbage of PDB {pdb} version {pdbversion}", masterPDBName, masterPDBVersion.Number)
            if instance.SnapshotCapable then
                let like = getSnapshotSourceName masterPDBName masterPDBVersion "%"
                let! _ = asyncValidation {
                    let! thisVersionSourcePDBs = getPDBNamesLike like
                    let! isSourceDeletedList = thisVersionSourcePDBs |> AsyncValidation.traverseS (deletePDBOlderThan parameters.GarbageCollectionDelay)
                    let sourceAndIsDeleted = List.zip thisVersionSourcePDBs isSourceDeletedList
                    let isDeleted = sourceAndIsDeleted |> List.exists (fun (pdb, deleted) -> deleted && pdb.ToUpper() = snapshotSourceName.ToUpper())
                    if isDeleted then retype (ctx.Parent()) <! KillVersion masterPDBVersion.Number
                }
                ctx.Log.Value.Info("Collected garbage of PDB {pdb} version {pdbversion}", masterPDBName, masterPDBVersion.Number)
                return! loop ()
            else
                return! loop ()

        | HaraKiri ->
            ctx.Log.Value.Info("Actor for version {pdbversion} of {pdb} is stopped", masterPDBVersion.Number, masterPDBName)
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

