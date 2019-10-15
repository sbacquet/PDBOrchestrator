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

type Command =
| Snapshot of WithRequestId<string, string, string, string> // responds with WithRequestId<OraclePDBResult>
| CollectGarbage // no response
| HaraKiri // no response

type CommandToParent =
| KillVersion of int

let getSnapshotSourceName (pdb:string) (masterPDBVersion:MasterPDBVersion) (suffix:string) = sprintf "%s_V%03d_%s" (pdb.ToUpper()) masterPDBVersion.Number (suffix.ToUpper())

let masterPDBVersionActorBody 
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
        let sender = ctx.Sender().Retype<WithRequestId<OraclePDBResult>>()

        match msg with
        | :? Command as command ->
            match command with
            | Snapshot (requestId, snapshotSourceManifest, snapshotSourceDest, snapshotName, snapshotDest) -> 
                let snapshotSourceExistsMaybe = oracleAPI.PDBExists snapshotSourceName |> runWithinElseDefault parameters.ShortTimeout (Error (exn "timeout reached"))
                match snapshotSourceExistsMaybe with
                | Ok snapshotSourceExists ->
                    if (not snapshotSourceExists) then
                        let importResult:WithRequestId<OraclePDBResult> = 
                            oracleDiskIntensiveTaskExecutor <? ImportPDB (requestId, snapshotSourceManifest, snapshotSourceDest, snapshotSourceName)
                            |> runWithin parameters.VeryLongTimeout id (fun () -> (requestId, Error (exn (sprintf "timeout reached while importing %s" snapshotSourceName))))
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

            | CollectGarbage ->
                ctx.Log.Value.Info("Collecting garbage of PDB {pdb} version {pdbversion}", masterPDBName, masterPDBVersion.Number)
                let like = getSnapshotSourceName masterPDBName masterPDBVersion "%"
                let! thisVersionSourcePDBs = oracleAPI.GetPDBNamesLike like
                match thisVersionSourcePDBs with
                | Ok pdbs -> 
                    let results = 
                        pdbs |> List.map (fun pdb -> 
                            let r = 
                                oracleAPI.DeletePDBWithSnapshots parameters.GarbageCollectionDelay pdb 
                                |> runWithinElseDefault 
                                    parameters.LongTimeout 
                                    (sprintf "timeout reached while deleting %s and its snapshots" masterPDBName |> exn |> Error)
                            (pdb, r) 
                        )
                    results |> List.iter (fun (_, result) -> result |> Result.mapError (fun error -> ctx.Log.Value.Error(error.ToString())) |> ignore)
                    let isDeleted = results |> List.exists (fun (pdb, result) -> pdb.ToUpper() = snapshotSourceName.ToUpper() && match result with | Ok r -> r | Error _ -> false)
                    if isDeleted then retype (ctx.Parent()) <! KillVersion masterPDBVersion.Number
                | Error error -> 
                    ctx.Log.Value.Error(error.ToString())
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

