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
open Domain.MasterPDBWorkingCopy

type Command =
| CreateWorkingCopy of WithRequestId<string, bool, bool> // responds with OraclePDBResultWithReqId
| DeleteWorkingCopy of WithRequestId<MasterPDBWorkingCopy>
| CollectGarbage // no response
| HaraKiri // no response
| Delete

type CommandToParent =
| KillVersion of int

let getSnapshotSourceName (pdb:string) (masterPDBVersion:MasterPDBVersion) (suffix:string) = sprintf "%s_V%03d_%s" (pdb.ToUpper()) masterPDBVersion.VersionNumber (suffix.ToUpper())

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
        | CreateWorkingCopy (requestId, workingCopyName, snapshot, durable) -> 
            let result:OraclePDBResult = result {
                let! wcExists = pdbExists workingCopyName
                let! _ = 
                    if wcExists then
                        result {
                            let! canDelete = 
                                if durable then Ok true
                                else isTempWorkingCopy workingCopyName
                            if canDelete then
                                return! deletePDB workingCopyName // force creation
                            else
                                return! Error <| (sprintf "PDB %s exists and is not a temporary working copy, hence cannot be overwritten" workingCopyName |> exn)
                        }
                    else Ok ""
                let sourceManifest = Domain.MasterPDBVersion.manifestFile masterPDBName masterPDBVersion.VersionNumber
                let destPath = instance |> getWorkingCopyFolder durable
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

        | DeleteWorkingCopy (requestId, workingCopy) ->
            ctx.Log.Value.Info("Deleting working copy {pdb} on instance {instance} requested", workingCopy.Name, instance.Name)
            let sender = ctx.Sender().Retype<Application.Oracle.OraclePDBResultWithReqId>()
            let result = deletePDB workingCopy.Name
            sender <! (requestId, result)
            return! loop ()

        | CollectGarbage ->
            if instance.SnapshotCapable then
                let _ = result {
                    let! _ = deletePDB snapshotSourceName
                    retype (ctx.Parent()) <! KillVersion masterPDBVersion.VersionNumber
                    return ()
                }
                return! loop ()
            else
                retype (ctx.Parent()) <! KillVersion masterPDBVersion.VersionNumber
                return! loop ()

        | HaraKiri ->
            ctx.Log.Value.Info("Stop of actor for version {pdbversion} of {pdb} requested", masterPDBVersion.VersionNumber, masterPDBName)
            retype ctx.Self <! Akka.Actor.PoisonPill.Instance
            return! loop ()

        | Delete ->
            ctx.Log.Value.Info("Deleting version {pdbversion} of {pdb}", masterPDBVersion.VersionNumber, masterPDBName)
            let sourceManifest = Domain.MasterPDBVersion.manifestFile masterPDBName masterPDBVersion.VersionNumber
            // TODO : read manifest file and get Oracle files location
            // TODO : delete Oracle files
            // TODO : delete manifest file
            if instance.SnapshotCapable then deletePDB snapshotSourceName |> ignore else ()
            retype (ctx.Parent()) <! KillVersion masterPDBVersion.VersionNumber
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

