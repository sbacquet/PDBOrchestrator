module Application.MasterPDBVersionActor

open Akkling
open Akka.Actor
open Domain.MasterPDBVersion
open Application.PendingRequest

type Command =
| Snapshot of WithRequestId<string>
| DeleteSnapshot of WithRequestId<string>

let masterPDBVersionActorBody (oracleAPI:#Application.Oracle.IOracleAPI) longTaskExecutor oracleDiskIntensiveTaskExecutor (masterPDBVersion : Domain.MasterPDBVersion.MasterPDBVersion) (ctx : Actor<_>) =

    let rec loop () = actor {
        let! msg = ctx.Receive();

        match msg with
        | Snapshot (requestId, snapshotName) -> ()
        | DeleteSnapshot (requestId, snapshotName) -> () // TODO

        return loop ()
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

