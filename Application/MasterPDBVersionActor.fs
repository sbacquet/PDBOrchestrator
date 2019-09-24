module Application.MasterPDBVersionActor

open Akkling
open Akka.Actor
open Domain.MasterPDB
open Domain.MasterPDBVersion
open Application.PendingRequest
open Application.OracleLongTaskExecutor
open Application.OracleDiskIntensiveActor
open Domain.OracleInstance
open Application.Oracle

type Command =
| Snapshot of WithRequestId<string, string, string, string> // responds with WithRequestId<OraclePDBResult>
| DeleteSnapshot of WithRequestId<string>

let getSnapshotSourceName (masterPDBVersion:MasterPDBVersion) = sprintf "%s_v%03d" (masterPDBVersion.MasterPDBName.ToUpper()) masterPDBVersion.Number

let masterPDBVersionActorBody 
    (oracleAPI:#Application.Oracle.IOracleAPI) 
    (oracleLongTaskExecutor:IActorRef<OracleLongTaskExecutor.Command>) 
    (oracleDiskIntensiveTaskExecutor:IActorRef<OracleDiskIntensiveActor.Command>) 
    (masterPDBVersion:MasterPDBVersion) 
    (ctx : Actor<_>) =

    let rec loop () = actor {
        let! msg = ctx.Receive();

        match msg with
        | Snapshot (requestId, snapshotSourceManifest, snapshotSourceDest, snapshotName, snapshotDest) -> 
            let snapshotSourceName = getSnapshotSourceName masterPDBVersion
            let snapshotSourceExists = oracleAPI.PDBExists snapshotSourceName |> Async.RunSynchronously
            if (not snapshotSourceExists) then
                let importResult:WithRequestId<OraclePDBResult> = 
                    oracleDiskIntensiveTaskExecutor <? ImportPDB (requestId, snapshotSourceManifest, snapshotSourceDest, snapshotSourceName)
                    |> Async.RunSynchronously
                match snd importResult with
                | Error _ -> 
                    ctx.Sender() <! importResult
                    return! loop ()
                | Ok _ -> ()
            else
                logDebugf ctx "Snapshot source PDB %s already exists" snapshotSourceName
            oracleLongTaskExecutor <<! SnapshotPDB (requestId, snapshotSourceName, snapshotDest, snapshotName)
            return! loop ()

        | DeleteSnapshot (requestId, snapshotName) ->
            return! loop ()
    }

    loop ()

let masterPDBVersionActorName (versionNumber:int) = Common.ActorName (sprintf "Version=%d" versionNumber)

let spawn (oracleAPI:#Application.Oracle.IOracleAPI) longTaskExecutor oracleDiskIntensiveTaskExecutor (masterPDBVersion:MasterPDBVersion) (actorFactory:IActorRefFactory) =

    let (Common.ActorName actorName) = masterPDBVersionActorName masterPDBVersion.Number
    
    Akkling.Spawn.spawn actorFactory actorName 
        <| props (
            masterPDBVersionActorBody 
                oracleAPI
                longTaskExecutor 
                oracleDiskIntensiveTaskExecutor 
                masterPDBVersion
        )

