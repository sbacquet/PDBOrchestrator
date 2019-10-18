module Application.MasterPDBVersionActor

open Akkling
open Akka.Actor
open Domain.MasterPDB
open Domain.MasterPDBVersion
open Application.PendingRequest
open Application.OracleLongTaskExecutor
open Application.OracleDiskIntensiveActor
open Application.Oracle
open Application.Common
open System
open Application.GlobalParameters
open Domain.Common

type Command =
| Snapshot of WithRequestId<string, string, string, string> // responds with OraclePDBResultWithReqId
| CollectGarbage // no response
| HaraKiri // no response

type CommandToParent =
| KillVersion of int

let getSnapshotSourceName (pdb:string) (masterPDBVersion:MasterPDBVersion) (suffix:string) = sprintf "%s_V%03d_%s" (pdb.ToUpper()) masterPDBVersion.Number (suffix.ToUpper())

let private masterPDBVersionActorBody 
    (parameters:GlobalParameters)
    (oracleAPI:#Application.Oracle.IOracleAPI) 
    (oracleLongTaskExecutor:IActorRef<OracleLongTaskExecutor.Command>) 
    (oracleDiskIntensiveTaskExecutor:IActorRef<OracleDiskIntensiveActor.Command>) 
    (masterPDBName:string)
    (masterPDBVersion:MasterPDBVersion) 
    (ctx : Actor<_>) =

    let snapshotSourceName = getSnapshotSourceName masterPDBName masterPDBVersion parameters.ServerInstanceName

    let rec loop () = actor {

        let! (msg:obj) = ctx.Receive()
        let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()

        match msg with
        | :? Command as command ->
            match command with
            | Snapshot (requestId, snapshotSourceManifest, snapshotSourceDest, snapshotName, snapshotDest) -> 
                let! result = asyncResult {
                    let! snapshotSourceExists = oracleAPI.PDBExists snapshotSourceName
                    let! _ = 
                        if (not snapshotSourceExists) then
                            oracleDiskIntensiveTaskExecutor <? ImportPDB (None, snapshotSourceManifest, snapshotSourceDest, snapshotSourceName)
                        else
                            ctx.Log.Value.Debug("Snapshot source PDB {pdb} already exists", snapshotSourceName)
                            AsyncResult.retn snapshotSourceName
                    return! oracleLongTaskExecutor <? SnapshotPDB (None, snapshotSourceName, snapshotDest, snapshotName)
                }
                sender <! (requestId, result)
                return! loop ()

            | CollectGarbage ->
                ctx.Log.Value.Info("Collecting garbage of PDB {pdb} version {pdbversion}", masterPDBName, masterPDBVersion.Number)
                let like = getSnapshotSourceName masterPDBName masterPDBVersion "%"
                let! _ = asyncValidation {
                    let! thisVersionSourcePDBs = oracleAPI.GetPDBNamesLike like
                    let! isSourceDeletedList = thisVersionSourcePDBs |> AsyncValidation.traverseS (oracleAPI.DeletePDBWithSnapshots parameters.GarbageCollectionDelay)
                    let sourceAndIsDeleted = List.zip thisVersionSourcePDBs isSourceDeletedList
                    let isDeleted = sourceAndIsDeleted |> List.exists (fun (pdb, deleted) -> deleted && pdb.ToUpper() = snapshotSourceName.ToUpper())
                    if isDeleted then retype (ctx.Parent()) <! KillVersion masterPDBVersion.Number
                }
                ctx.Log.Value.Info("Collected garbage of PDB {pdb} version {pdbversion}", masterPDBName, masterPDBVersion.Number)
                return! loop ()

            | HaraKiri ->
                ctx.Log.Value.Info("Actor for version {pdbversion} of {pdb} is stopped", masterPDBVersion.Number, masterPDBName)
                retype ctx.Self <! Akka.Actor.PoisonPill.Instance
                return! loop ()

        | _ -> return! loop ()
    }

    loop ()

let spawn parameters (oracleAPI:#Application.Oracle.IOracleAPI) longTaskExecutor oracleDiskIntensiveTaskExecutor (masterPDBName:string) (masterPDBVersion:MasterPDBVersion) (actorFactory:IActorRefFactory) =

    (Akkling.Spawn.spawnAnonymous actorFactory
        <| props (
            masterPDBVersionActorBody 
                parameters
                oracleAPI
                longTaskExecutor 
                oracleDiskIntensiveTaskExecutor 
                masterPDBName
                masterPDBVersion
        )).Retype<Command>()

