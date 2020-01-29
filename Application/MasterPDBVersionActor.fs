﻿module Application.MasterPDBVersionActor

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
    let importPDB manifest path name : OraclePDBResult =
        oracleDiskIntensiveTaskExecutor <? OracleDiskIntensiveActor.ImportPDB (None, manifest, path, name)
        |> runWithin parameters.VeryLongTimeout id (fun () -> sprintf "cannot import PDB %s : timeout exceeded" name |> exn |> Error)

    let rec loop () =
        
        actor {

        let! command = ctx.Receive()
        let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()

        match command with
        | CreateWorkingCopy (requestId, workingCopyName, snapshot, durable, force) -> 
            let sourceManifest = Domain.MasterPDBVersion.manifestFile masterPDBName masterPDBVersion.VersionNumber
            let destPath = instance |> getWorkingCopyFolder durable
            let result = result {
                if instance.SnapshotCapable && snapshot then
                    let! snapshotSourceExists = pdbExists snapshotSourceName
                    let! _ = 
                        if (not snapshotSourceExists) then
                            importPDB sourceManifest instance.SnapshotSourcePDBDestPath snapshotSourceName
                        else
                            ctx.Log.Value.Debug("Snapshot source PDB {pdb} already exists", snapshotSourceName)
                            Ok ""
                    workingCopyFactory <<! WorkingCopyFactoryActor.CreateWorkingCopyBySnapshot(Some requestId, snapshotSourceName, destPath, workingCopyName, durable, force)
                    return ()
                else
                    workingCopyFactory <<! WorkingCopyFactoryActor.CreateWorkingCopyByClone(Some requestId, sourceManifest, destPath, workingCopyName, durable, force)
                    return ()
            }
            match result with
            | Error error ->
                sender <! (requestId, Error error)
            | _ -> ()
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

