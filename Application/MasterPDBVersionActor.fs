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
| Snapshot of WithRequestId<string, string>
| DeleteSnapshot of WithRequestId<string>

let getSnapshotSourceName (masterPDBVersion:MasterPDBVersion) = sprintf "%s_v%03d" (masterPDBVersion.MasterPDBName.ToUpper()) masterPDBVersion.Number

let masterPDBVersionActorBody 
    (oracleAPI:#Application.Oracle.IOracleAPI) 
    (oracleLongTaskExecutor:IActorRef<OracleLongTaskExecutor.Command>) 
    (oracleDiskIntensiveTaskExecutor:IActorRef<OracleDiskIntensiveActor.Command>) 
    (oracleInstance:OracleInstance)
    (masterPDB:MasterPDB) 
    (masterPDBVersion:MasterPDBVersion) 
    (ctx : Actor<_>) =

    let rec loop () = actor {
        let! msg = ctx.Receive();

        match msg with
        | Snapshot (requestId, snapshotName, dest) -> 
            let snapshotSourceName = getSnapshotSourceName masterPDBVersion
            let! snapshotSourceExists = oracleAPI.PDBExists snapshotSourceName
            if (not snapshotSourceExists) then
                let (importResult:OraclePDBResult) = 
                    oracleDiskIntensiveTaskExecutor <? ImportPDB (requestId, masterPDB.Manifest, "" (* TODO *), snapshotSourceName)
                    |> Async.RunSynchronously
                match importResult with
                | Error error -> 
                    ctx.Sender() <! importResult
                    return loop ()
                | Ok result -> ()
            oracleLongTaskExecutor <<! SnapshotPDB (requestId, snapshotSourceName, dest, snapshotName)

        | DeleteSnapshot (requestId, snapshotName) -> () // TODO
            
        return loop ()
    }

    loop ()

let masterPDBVersionActorName (versionNumber:int) = Common.ActorName (sprintf "Version=%d" versionNumber)

let spawn (oracleAPI:#Application.Oracle.IOracleAPI) (oracleInstance:OracleInstance) longTaskExecutor oracleDiskIntensiveTaskExecutor masterPDB (masterPDBVersion:MasterPDBVersion) (actorFactory:IActorRefFactory) =

    let (Common.ActorName actorName) = masterPDBVersionActorName masterPDBVersion.Number
    
    Akkling.Spawn.spawn actorFactory actorName 
        <| props (
            masterPDBVersionActorBody 
                oracleAPI
                longTaskExecutor 
                oracleDiskIntensiveTaskExecutor 
                oracleInstance
                masterPDB
                masterPDBVersion
        )

