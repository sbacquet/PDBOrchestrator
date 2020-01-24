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
| CreateWorkingCopy of WithRequestId<string, bool, bool, bool> // responds with OraclePDBResultWithReqId
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
    (workingCopyFactory:IActorRef<Application.WorkingCopyFactoryActor.Command>)
    (masterPDBName:string)
    (masterPDBVersion:MasterPDBVersion) 
    (ctx : Actor<Command>) =

    let snapshotSourceName = getSnapshotSourceName masterPDBName masterPDBVersion parameters.ServerInstanceName
    let pdbExists pdb : Exceptional<bool> = 
        oracleShortTaskExecutor <? OracleShortTaskExecutor.PDBExists pdb
        |> runWithin parameters.ShortTimeout id (fun () -> sprintf "cannot check if PDB %s exists: timeout exceeded" pdb |> exn |> Error)
    let deleteSnaphotSourcePDB pdb : OraclePDBResult = 
        oracleLongTaskExecutor <? OracleLongTaskExecutor.DeletePDB (None, pdb)
        |> runWithin parameters.LongTimeout id (fun () -> sprintf "PDB %s cannot be deleted : timeout exceeded" pdb |> exn |> Error)
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
    let importPDB manifest path name : OraclePDBResult =
        oracleDiskIntensiveTaskExecutor <? OracleDiskIntensiveActor.ImportPDB (None, manifest, path, name)
        |> runWithin parameters.VeryLongTimeout id (fun () -> sprintf "cannot import PDB %s : timeout exceeded" name |> exn |> Error)

    let rec loop () =
        
        actor {

        let! command = ctx.Receive()
        let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()

        match command with
        | CreateWorkingCopy (requestId, workingCopyName, snapshot, durable, force) -> 
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
                        if wcExists then // force destruction
                            let! canDelete = 
                                if durable then Ok true
                                else isTempWorkingCopy workingCopyName
                            return!
                                if canDelete then
                                    Ok true // force creation
                                else
                                    Error <| (sprintf "PDB %s exists and is not a temporary working copy, so cannot be overwritten" workingCopyName |> exn)
                        else
                            return false
                    }
                    let sourceManifest = Domain.MasterPDBVersion.manifestFile masterPDBName masterPDBVersion.VersionNumber
                    let destPath = instance |> getWorkingCopyFolder durable
                    if instance.SnapshotCapable && snapshot then
                        let! snapshotSourceExists = pdbExists snapshotSourceName
                        let! _ = 
                            if (not snapshotSourceExists) then
                                importPDB sourceManifest instance.SnapshotSourcePDBDestPath snapshotSourceName
                            else
                                ctx.Log.Value.Debug("Snapshot source PDB {pdb} already exists", snapshotSourceName)
                                Ok ""
                        workingCopyFactory <<! WorkingCopyFactoryActor.CreateWorkingCopyBySnapshot(Some requestId, snapshotSourceName, destPath, workingCopyName, deleteFirst)
                        return None
                    else
                        workingCopyFactory <<! WorkingCopyFactoryActor.CreateWorkingCopyByClone(Some requestId, sourceManifest, destPath, workingCopyName, deleteFirst)
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
            ctx.Log.Value.Info("Deleting working copy {pdb} on instance {instance} requested", workingCopy.Name, instance.Name)
            workingCopyFactory <<! WorkingCopyFactoryActor.DeleteWorkingCopy(Some requestId, workingCopy.Name, false)
            return! loop ()

        | CollectGarbage ->
            if instance.SnapshotCapable then
                let _ = result {
                    let! _ = deleteSnaphotSourcePDB snapshotSourceName
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
            if instance.SnapshotCapable then deleteSnaphotSourcePDB snapshotSourceName |> ignore else ()
            retype (ctx.Parent()) <! KillVersion masterPDBVersion.VersionNumber
            return! loop ()
        
        }

    loop ()

let spawn 
        parameters instance 
        (shortTaskExecutor:IActorRef<Application.OracleShortTaskExecutor.Command>)
        (longTaskExecutor:IActorRef<Application.OracleLongTaskExecutor.Command>) 
        (oracleDiskIntensiveTaskExecutor:IActorRef<Application.OracleDiskIntensiveActor.Command>)
        (workingCopyFactory:IActorRef<Application.WorkingCopyFactoryActor.Command>)
        (masterPDBName:string) (masterPDBVersion:MasterPDBVersion) (actorFactory:IActorRefFactory) =

    (Akkling.Spawn.spawnAnonymous actorFactory
        <| props (
            masterPDBVersionActorBody 
                parameters
                instance
                shortTaskExecutor 
                longTaskExecutor 
                oracleDiskIntensiveTaskExecutor
                workingCopyFactory
                masterPDBName
                masterPDBVersion
        )).Retype<Command>()

