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
open Application.Common
open System

type Command =
| Snapshot of WithRequestId<string, string, string, string> // responds with WithRequestId<OraclePDBResult>

let getSnapshotSourceName (pdb:string) (masterPDBVersion:MasterPDBVersion) = sprintf "%s_v%03d" (pdb.ToUpper()) masterPDBVersion.Number

let cDefaultTimeout = 5000
let cImportTimeout = int(Math.Round(TimeSpan.FromMinutes(20.).TotalMilliseconds)) // TODO

let masterPDBVersionActorBody 
    (oracleAPI:#Application.Oracle.IOracleAPI) 
    (oracleLongTaskExecutor:IActorRef<OracleLongTaskExecutor.Command>) 
    (oracleDiskIntensiveTaskExecutor:IActorRef<OracleDiskIntensiveActor.Command>) 
    (masterPDBName:string)
    (masterPDBVersion:MasterPDBVersion) 
    (ctx : Actor<_>) =

    let snapshotSourceName = getSnapshotSourceName masterPDBName masterPDBVersion

    let rec loop () = actor {

        let! msg = ctx.Receive()
        let sender = ctx.Sender().Retype<WithRequestId<OraclePDBResult>>()

        match msg with
        | Snapshot (requestId, snapshotSourceManifest, snapshotSourceDest, snapshotName, snapshotDest) -> 
            let snapshotSourceExistsMaybe = oracleAPI.PDBExists snapshotSourceName |> runWithinElseDefault cDefaultTimeout (Error (exn "timeout reached"))
            match snapshotSourceExistsMaybe with
            | Ok snapshotSourceExists ->
                if (not snapshotSourceExists) then
                    let importResult:WithRequestId<OraclePDBResult> = 
                        oracleDiskIntensiveTaskExecutor <? ImportPDB (requestId, snapshotSourceManifest, snapshotSourceDest, snapshotSourceName)
                        |> runWithin cImportTimeout id (fun () -> (requestId, Error (exn (sprintf "timeout reached while importing %s" snapshotSourceName))))
                    match snd importResult with
                    | Error _ -> 
                        sender <! importResult
                        return! loop ()
                    | Ok _ -> ()
                else
                    logDebugf ctx "Snapshot source PDB %s already exists" snapshotSourceName
                oracleLongTaskExecutor <<! SnapshotPDB (requestId, snapshotSourceName, snapshotDest, snapshotName)
            | Error ex -> sender <! (requestId, Error ex)
            return! loop ()
    }

    loop ()

let masterPDBVersionActorName (versionNumber:int) = Common.ActorName (sprintf "Version=%d" versionNumber)

let spawn (oracleAPI:#Application.Oracle.IOracleAPI) longTaskExecutor oracleDiskIntensiveTaskExecutor (masterPDBName:string) (masterPDBVersion:MasterPDBVersion) (actorFactory:IActorRefFactory) =

    let (Common.ActorName actorName) = masterPDBVersionActorName masterPDBVersion.Number
    
    Akkling.Spawn.spawn actorFactory actorName 
        <| props (
            masterPDBVersionActorBody 
                oracleAPI
                longTaskExecutor 
                oracleDiskIntensiveTaskExecutor 
                masterPDBName
                masterPDBVersion
        )

