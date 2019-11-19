module Application.MasterPDBVersionActor

open Akkling
open Akka.Actor
open Domain.MasterPDBVersion
open Application.PendingRequest
open Application.OracleLongTaskExecutor
open Application.OracleDiskIntensiveActor
open Application.Oracle
open Application.Parameters
open Domain.Common
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
    (oracleAPI:#Application.Oracle.IOracleAPI) 
    (instance:OracleInstance) 
    (oracleLongTaskExecutor:IActorRef<OracleLongTaskExecutor.Command>) 
    (oracleDiskIntensiveTaskExecutor:IActorRef<OracleDiskIntensiveActor.Command>) 
    (masterPDBName:string)
    (masterPDBVersion:MasterPDBVersion) 
    (ctx : Actor<Command>) =

    let snapshotSourceName = getSnapshotSourceName masterPDBName masterPDBVersion parameters.ServerInstanceName

    let rec loop () = actor {

        let! command = ctx.Receive()
        let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()

        match command with
        | CreateWorkingCopy (requestId, sourceManifest, workingCopyName, force) -> 
            let! result = asyncResult {
                let! wcExists = oracleAPI.PDBExists workingCopyName
                if wcExists && (not force) then 
                    ctx.Log.Value.Info("Working copy {pdb} already exists, not enforcing creation.", workingCopyName)
                    return! AsyncResult.retn workingCopyName
                else
                    let! _ = 
                        if wcExists then // force creation
                            oracleLongTaskExecutor <? DeletePDB (None, workingCopyName)
                        else AsyncResult.retn ""
                    if (instance.SnapshotCapable) then
                        let! snapshotSourceExists = oracleAPI.PDBExists snapshotSourceName
                        let! _ = 
                            if (not snapshotSourceExists) then
                                oracleDiskIntensiveTaskExecutor <? ImportPDB (None, sourceManifest, instance.SnapshotSourcePDBDestPath, snapshotSourceName)
                            else
                                ctx.Log.Value.Debug("Snapshot source PDB {pdb} already exists", snapshotSourceName)
                                AsyncResult.retn ""
                        return! oracleLongTaskExecutor <? SnapshotPDB (None, snapshotSourceName, workingCopyName)
                    else
                        return! oracleDiskIntensiveTaskExecutor <? ImportPDB (None, sourceManifest, instance.WorkingCopyDestPath, workingCopyName)
            }
            sender <! (requestId, result)
            return! loop ()

        | DeleteWorkingCopy (requestId, pdb) ->
            let! pdbFilesFolder = oracleAPI.GetPDBFilesFolder pdb
            let result:OraclePDBResult =
                match pdbFilesFolder with
                | Ok (Some folder) ->
                    if folder.StartsWith(instance.WorkingCopyDestPath) then
                        oracleLongTaskExecutor <? DeletePDB (None, pdb) 
                        |> runWithin parameters.LongTimeout id (fun () -> sprintf "PDB %s cannot be deleted : timeout exceeded" pdb |> exn |> Error)
                    else
                        sprintf "PDB %s is not a working copy" pdb |> exn |> Error
                | Ok None ->
                    sprintf "cannot file any file folder for PDB %s" pdb |> exn |> Error
                | Error error ->
                    Error error
            sender <! (requestId, result)
            return! loop ()

        | CollectGarbage ->
            ctx.Log.Value.Info("Collecting garbage of PDB {pdb} version {pdbversion}", masterPDBName, masterPDBVersion.Number)
            if instance.SnapshotCapable then
                let like = getSnapshotSourceName masterPDBName masterPDBVersion "%"
                let! _ = asyncValidation {
                    let! thisVersionSourcePDBs = oracleAPI.GetPDBNamesLike like
                    let! isSourceDeletedList = thisVersionSourcePDBs |> AsyncValidation.traverseS (oracleAPI.DeletePDBWithSnapshots (Some parameters.GarbageCollectionDelay))
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

let spawn parameters (oracleAPI:#Application.Oracle.IOracleAPI) instance longTaskExecutor oracleDiskIntensiveTaskExecutor (masterPDBName:string) (masterPDBVersion:MasterPDBVersion) (actorFactory:IActorRefFactory) =

    (Akkling.Spawn.spawnAnonymous actorFactory
        <| props (
            masterPDBVersionActorBody 
                parameters
                oracleAPI
                instance
                longTaskExecutor 
                oracleDiskIntensiveTaskExecutor 
                masterPDBName
                masterPDBVersion
        )).Retype<Command>()

