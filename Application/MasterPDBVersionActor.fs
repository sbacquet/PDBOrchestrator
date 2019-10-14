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

type Command =
| Snapshot of WithRequestId<string, string, string, string> // responds with WithRequestId<OraclePDBResult>
| HaraKiri

type CommandToParent =
| KillVersion of int

let getSnapshotSourceName (pdb:string) (masterPDBVersion:MasterPDBVersion) = sprintf "%s_V%03d" (pdb.ToUpper()) masterPDBVersion.Number

#if DEBUG
let cDefaultTimeout = -1
let cImportTimeout = -1
let cInactivityTimeout = TimeSpan.FromSeconds(15.)
#else
let cDefaultTimeout = 5000
let cImportTimeout = int(Math.Round(TimeSpan.FromMinutes(20.).TotalMilliseconds)) // TODO config
let cInactivityTimeout = TimeSpan.FromHours(12.) // TODO config
#endif

let masterPDBVersionActorBody 
    (oracleAPI:#Application.Oracle.IOracleAPI) 
    (oracleLongTaskExecutor:IActorRef<OracleLongTaskExecutor.Command>) 
    (oracleDiskIntensiveTaskExecutor:IActorRef<OracleDiskIntensiveActor.Command>) 
    (masterPDBName:string)
    (masterPDBVersion:MasterPDBVersion) 
    (ctx : Actor<_>) =

    let snapshotSourceName = getSnapshotSourceName masterPDBName masterPDBVersion

    let rec loop () = actor {

        let! (msg:obj) = ctx.Receive()
        let sender = ctx.Sender().Retype<WithRequestId<OraclePDBResult>>()

        match msg with
        | :? Command as command ->
            match command with
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

            | HaraKiri ->
                retype ctx.Self <! Akka.Actor.PoisonPill.Instance
                return! loop ()

        | :? Akka.Actor.ReceiveTimeout ->
            ctx.SetReceiveTimeout None
            ctx.Log.Value.Warning("Version {pdbversion} of {pdb} is not used for {0} => kill the actor", masterPDBVersion.Number, masterPDBName, cInactivityTimeout)
            retype (ctx.Parent()) <! KillVersion masterPDBVersion.Number
            return! loop ()

        | _ -> return! loop ()
    }

    ctx.SetReceiveTimeout (Some cInactivityTimeout)
    loop ()

let spawn (oracleAPI:#Application.Oracle.IOracleAPI) longTaskExecutor oracleDiskIntensiveTaskExecutor (masterPDBName:string) (masterPDBVersion:MasterPDBVersion) (actorFactory:IActorRefFactory) =

    (Akkling.Spawn.spawnAnonymous actorFactory
        <| props (
            masterPDBVersionActorBody 
                oracleAPI
                longTaskExecutor 
                oracleDiskIntensiveTaskExecutor 
                masterPDBName
                masterPDBVersion
        )).Retype<Command>()

