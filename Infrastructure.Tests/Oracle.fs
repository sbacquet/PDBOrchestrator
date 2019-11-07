module Infrastructure.Tests.Oracle

open System
open Xunit

open Infrastructure.Oracle
open Microsoft.Extensions.Logging.Abstractions
open Application.Oracle
open Domain.Common
open Serilog
open Microsoft.Extensions.Logging
open Oracle.ManagedDataAccess.Client

Serilog.Log.Logger <- 
    (new LoggerConfiguration()).
        WriteTo.Trace().
        MinimumLevel.Debug().
        CreateLogger()

let loggerFactory = (new Serilog.Extensions.Logging.SerilogLoggerFactory(dispose=true) :> ILoggerFactory)

let instance = 
    Domain.OracleInstance.consOracleInstance
        []
        "intcdb2" "fr1psl010716.misys.global.ad" None
        "sys" "syspwd8"
        "" ""
        "" "" ""
        "/u01/app/oracle/oradata/SB_PDBs"
        "/u01/app/oracle/oradata/SB_PDBs"
        "/u01/app/oracle/oradata/SB_PDBs"
        "/u01/app/oracle/oradata/SB_PDBs"
        ""
        true
let oracleAPI : IOracleAPI = new OracleAPI (loggerFactory, instance) :> IOracleAPI
let conn = connAsDBAFromInstance instance
let connIn = connAsDBAInFromInstance instance

let mapAsyncError (x:Async<Result<'a,OracleException>>) : Async<Result<'a,exn>> = AsyncResult.mapError (fun ex -> ex :> exn) x

[<Fact>]
let ``Fail to get inexisting PDB from server`` () =
    let pdb = getPDBOnServer conn "xxxxxxxxxx" |> Async.RunSynchronously
    pdb |> Result.mapError raise |> ignore
    pdb |> Result.map (fun p -> Assert.True(p.IsNone)) |> ignore

[<Fact>]
let ``Import and delete PDB`` () =
    let commands = asyncResult {
        let! r = getPDBOnServer conn "toto" |> mapAsyncError
        let! _ = if r.IsSome then Error (exn "PDB toto already exists") else Ok "good"
        let! r = getPDBOnServerLike conn "toto%" |> mapAsyncError
        let! _ = if List.length r <> 0 then Error (exn "PDB toto% already exists") else Ok "good"
        let! _ = oracleAPI.ImportPDB "test1.xml" "/u01/app/oracle/oradata/SB_PDBs" "toto"
        let! r = getPDBOnServer conn "toto" |> mapAsyncError
        let! _ = if r.IsNone then Error (exn "No PDB toto ??") else Ok "good"
        let! r = getPDBOnServerLike conn "toto%" |> mapAsyncError
        let! _ = if List.length r <> 1 then Error (exn "No PDB toto% ??") else Ok "good"
        let! _ = if (List.head r).Name.ToUpper() <> "TOTO" then Error (sprintf "PDB is not TOTO (%s) ??" ((List.head r).Name.ToUpper()) |> exn) else Ok "good"
        return "ok"
    }

    [ commands; oracleAPI.DeletePDB "toto" ] 
    |> Async.sequenceS
    |> Async.RunSynchronously 
    |> List.iter (Result.mapError raise >> ignore)

[<Fact>]
let ``Create a real working copy`` () =
    let commands1 = asyncResult {
        let stopWatch = System.Diagnostics.Stopwatch.StartNew()
        let! _ = oracleAPI.ImportPDB "test1.xml" "/u01/app/oracle/oradata/SB_PDBs" "source"
        stopWatch.Stop()
        Log.Logger.Debug("Time to import : {time}", stopWatch.Elapsed.TotalSeconds)
        stopWatch.Restart()
        let! _ = oracleAPI.SnapshotPDB "source" "snapshot"
        stopWatch.Stop()
        Log.Logger.Debug("Time to snapshot : {time}", stopWatch.Elapsed.TotalSeconds)
        let! hasSnapshots = oracleAPI.PDBHasSnapshots "source"
        return! if hasSnapshots then Ok "Snapshot" else Error (exn "No snapshot ??!!")
    }

    let commands2 = asyncResult {
        let! _ = oracleAPI.DeletePDB "snapshot"
        let! stillExists = oracleAPI.PDBExists "snapshot"
        let! _ = if stillExists then Error (exn "Snapshot not deleted ??!!") else Ok "Snapshot deleted"
        let! hasSnapshots = oracleAPI.PDBHasSnapshots "source"
        let! _ = if hasSnapshots then Error (exn "Yet some snapshot ??!!") else Ok "No snapshot"
        let! _ = oracleAPI.DeletePDB "source"
        let! stillExists = oracleAPI.PDBExists "source"
        let! _ = if stillExists then Error (exn "Source not deleted ??!!") else Ok "Source deleted"
        return "Everything fine!"
    }

    [ commands1; commands2 ] 
    |> Async.sequenceS 
    |> Async.RunSynchronously 
    |> List.iter (Result.mapError raise >> ignore)

[<Fact>]
let ``Get snapshots older than 15 seconds`` () =
    let commands1 = asyncResult {
        let! _ = oracleAPI.ImportPDB "test1.xml" "/u01/app/oracle/oradata/SB_PDBs" "source"
        let! _ = oracleAPI.SnapshotPDB "source" "snapshot"
        let! snapshots = oracleAPI.PDBSnapshots "source"
        let! _ = if snapshots.Length <> 1 then Error (exn "Got no snapshot!") else Ok "# snapshots is 1, good"
        let seconds = 15
        let createdBefore = TimeSpan.FromSeconds((float)seconds)
        let! snapshots = pdbSnapshotsOlderThan conn createdBefore "source" |> mapAsyncError
        let! _ = if snapshots.Length <> 0 then Error (exn "Got a snapshot before 15 sec!") else Ok "# snapshots is 0, good"
        Async.Sleep(1000*(seconds+5)) |> Async.RunSynchronously
        let! snapshots = pdbSnapshotsOlderThan conn createdBefore "source" |> mapAsyncError
        let! _ = if snapshots.Length <> 1 then Error (exn "Got no snapshot after 20 sec!") else Ok "# snapshots is 1, good"
        return "Everything fine!"
    }    

    let commands2 = asyncResult {
        let! _ = oracleAPI.DeletePDB "snapshot"
        let! stillExists = oracleAPI.PDBExists "snapshot"
        let! _ = if stillExists then Error (exn "Snapshot not deleted ??!!") else Ok "Snapshot deleted"
        let! hasSnapshots = oracleAPI.PDBHasSnapshots "source"
        let! _ = if hasSnapshots then Error (exn "Yet some snapshot ??!!") else Ok "No snapshot"
        let! _ = oracleAPI.DeletePDB "source"
        let! stillExists = oracleAPI.PDBExists "source"
        let! _ = if stillExists then Error (exn "Source not deleted ??!!") else Ok "Source deleted"
        return "Everything fine!"
    }

    [ commands1; commands2 ] 
    |> Async.sequenceS 
    |> Async.RunSynchronously 
    |> List.iter (Result.mapError raise >> ignore)

[<Fact>]
let ``Delete snapshots older than 15 seconds`` () =
    let seconds = 15
    let res = asyncValidation {
        let! _ = oracleAPI.ImportPDB "test1.xml" "/u01/app/oracle/oradata/SB_PDBs" "source"
        let! _ = oracleAPI.SnapshotPDB "source" "snapshot"
        let! snapshots = oracleAPI.PDBSnapshots "source"
        let! _ = if snapshots.Length <> 1 then Error (exn "Got no snapshot!") else Ok "# snapshots is 1, good"
        Async.Sleep(1000*(seconds+5)) |> Async.RunSynchronously
        let! deleted = oracleAPI.DeletePDBWithSnapshots (TimeSpan.FromSeconds((float)seconds)) "source"
        let! _ = if deleted then Ok "deleted properly" else Error (exn "Source not deleted ??!!")
        let! stillExists = oracleAPI.PDBExists "snapshot"
        let! _ = if stillExists then Error (exn "Snapshot not deleted ??!!") else Ok "Snapshot deleted"
        let! hasSnapshots = oracleAPI.PDBHasSnapshots "source"
        let! _ = if hasSnapshots then Error (exn "Yet some snapshot ??!!") else Ok "No snapshot"
        let! stillExists = oracleAPI.PDBExists "source"
        let! _ = if stillExists then Error (exn "Source not deleted ??!!") else Ok "Source deleted"
        return "Everything fine!"
    }
    let tasks = [
        res
        oracleAPI.DeletePDB "source" |> Async.map (fun _ -> Validation.Valid "source") // delete source PDB in case the test failed
    ]

    tasks 
    |> AsyncValidation.sequenceS
    |> Async.RunSynchronously 
    |> Validation.mapErrors (fun errors -> errors |> List.map (fun ex -> ex.Message) |> String.concat "\n" |> failwith) 
    |> ignore

[<Fact>]
let ``Get PDB files folder`` () =
    let folderMaybe = Infrastructure.Oracle.getPDBFilesFolder conn "ORCLPDB" |> Async.RunSynchronously 
    match folderMaybe with
    | Ok (Some folder) -> Assert.Equal("/u01/app/oracle/oradata/INTCDB2/ORCLPDB", folder)
    | Ok None -> failwith "no PDB file found with name=ORCLPDB"
    | Error e -> failwithf "Oracle error : %s" e.Message

[<Fact>]
let ``Get Oracle directory`` () =
    let path = Infrastructure.Oracle.getOracleDirectoryPath conn "DATA_PUMP_DIR" |> Async.RunSynchronously
    match path with
    | Ok path -> Assert.Equal("/u01/app/oracle/admin/INTCDB2/dpdump/", path)
    | Error error -> raise error

[<Fact>]
let ``Create and delete PDB`` () =
    let logger = loggerFactory.CreateLogger()
    let tasks = [
        Infrastructure.Oracle.createAndGrantPDB logger conn connIn false "dbadmin" "pass" "/u01/app/oracle/oradata/SB_PDBs" "testsb" |> AsyncValidation.ofAsyncResult
        Infrastructure.Oracle.deletePDB logger conn false "testsb" |> AsyncValidation.ofAsyncResult
    ]
    let res = tasks |> AsyncValidation.sequenceS |> Async.RunSynchronously
    res |> Validation.mapErrors (fun errors -> errors |> List.map (fun ex -> ex.Message) |> String.concat "\n" |> failwith) |> ignore

