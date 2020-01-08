module Infrastructure.Tests.Oracle

open System
open Xunit

open Infrastructure
open Infrastructure.Oracle
open Infrastructure.OracleInstanceAPI
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
let logger = loggerFactory.CreateLogger("tests")

let [<Literal>]cPDBFolder = "/u01/app/oracle/oradata/SB_PDBs"
let [<Literal>]cWCFolder = "/u01/app/oracle/oradata/SB_PDBs/WorkingCopies"
let [<Literal>]cTempWCFolder = "/u01/app/oracle/oradata/SB_PDBs/WorkingCopies/temporary"

let instance = 
    Domain.OracleInstance.consOracleInstance
        List.empty
        List.empty
        "intcdb2" "fr1psl010716.misys.global.ad" None
        "sys" "syspwd8"
        "system" "syspwd8"
        "oracle" "m15y5db" "ssh-ed25519 256 CcNFefba5mM1EW9RGjJrbxBmyyeVGIMHOCamkpgQJa8=" "not used"
        cPDBFolder
        cPDBFolder
        cWCFolder
        cPDBFolder
        "DP_DIR" "/u01/app/intcdb_dumps"
        true
let oracleAPI : IOracleAPI = new OracleInstanceAPI (loggerFactory, instance) :> IOracleAPI
let conn = connAsDBAFromInstance logger instance
let connIn = connAsDBAInFromInstance logger instance

let mapAsyncError (x:Async<Result<'a,OracleException>>) : Async<Result<'a,exn>> = AsyncResult.mapError (fun ex -> ex :> exn) x

[<Fact>]
let ``Fail to get inexisting PDB from server`` () =
    let pdb = getPDBOnServer conn "xxxxxxxxxx" |> Async.RunSynchronously
    match pdb with
    | Ok None -> ()
    | Ok (Some _) -> failwith "should not exist"
    | Error error -> raise error

[<Fact>]
let ``Import and delete PDB`` () =
    let commands = asyncResult {
        let! r = getPDBOnServer conn "toto" |> mapAsyncError
        let! _ = if r.IsSome then Error (exn "PDB toto already exists") else Ok "good"
        let! r = getPDBOnServerLike conn "toto%" |> mapAsyncError
        let! _ = if List.length r <> 0 then Error (exn "PDB toto% already exists") else Ok "good"
        let! _ = oracleAPI.ImportPDB "test1.xml" cPDBFolder "toto"
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
        let! _ = oracleAPI.ImportPDB "test1.xml" cPDBFolder "source"
        stopWatch.Stop()
        Log.Logger.Debug("Time to import : {time}", stopWatch.Elapsed.TotalSeconds)
        stopWatch.Restart()
        let! _ = oracleAPI.SnapshotPDB "source" cPDBFolder "snapshot"
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
        let! _ = oracleAPI.ImportPDB "test1.xml" cPDBFolder "source"
        let! _ = oracleAPI.SnapshotPDB "source" cTempWCFolder "snapshot"
        let! snapshots = oracleAPI.PDBSnapshots "source"
        let! _ = if snapshots.Length <> 1 then Error (exn "Got no snapshot!") else Ok "# snapshots is 1, good"
        let seconds = 15
        let createdBefore = TimeSpan.FromSeconds((float)seconds)
        let! snapshots = "source" |> pdbSnapshots conn (Some cTempWCFolder) (Some createdBefore) |> mapAsyncError
        let! _ = if snapshots.Length <> 0 then Error (exn "Got a snapshot before 15 sec!") else Ok "# snapshots is 0, good"
        Async.Sleep(1000*(seconds+5)) |> Async.RunSynchronously
        let! snapshots = "source" |> pdbSnapshots conn (Some cTempWCFolder) (Some createdBefore) |> mapAsyncError
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
let ``Get PDB files folder`` () =
    let folderMaybe = Infrastructure.Oracle.getPDBFilesFolder conn "ORCLPDB" |> Async.RunSynchronously 
    match folderMaybe with
    | Ok (Some folder) -> Assert.Equal("/u01/app/oracle/oradata/INTCDB2/ORCLPDB", folder)
    | Ok None -> failwith "no PDB file found with name=ORCLPDB"
    | Error e -> failwithf "Oracle error : %s" e.Message

[<Fact>]
let ``Get Oracle directory`` () =
    let path = Infrastructure.Oracle.getOracleDirectoryPath connIn "orclpdb" "DATA_PUMP_DIR" |> Async.RunSynchronously
    match path with
    | Ok path -> Assert.StartsWith("/u01/app/oracle/admin/INTCDB2/dpdump/", path)
    | Error error -> raise error

[<Fact>]
let ``Get wrong Oracle directory`` () =
    let path = Infrastructure.Oracle.getOracleDirectoryPath connIn "orclpdb" "notexists" |> Async.RunSynchronously
    match path with
    | Ok _ -> failwith "should not exist"
    | Error _ -> ()

[<Fact>]
let ``Create and delete PDB`` () =
    let logger = loggerFactory.CreateLogger()
    let tasks = [
        Infrastructure.Oracle.createAndGrantPDB logger conn connIn false "dbadmin" "pass" cPDBFolder "testsb" |> AsyncValidation.ofAsyncResult
        Infrastructure.Oracle.deletePDB logger conn false "testsb" |> AsyncValidation.ofAsyncResult
    ]
    let res = tasks |> AsyncValidation.sequenceS |> Async.RunSynchronously
    res |> Validation.mapErrors (fun errors -> errors |> List.map (fun ex -> ex.Message) |> String.concat "\n" |> failwith) |> ignore

[<Fact>]
let ``Create PDB and import schema`` () =
    let res = oracleAPI.NewPDBFromDump (TimeSpan.FromMinutes(3.) |> Some) "testsb" @"\\sophis\dumps\NEW_USER.DMP" [ "NEW_USER" ] [ "NEW_USER", "pass" ] |> Async.RunSynchronously
    let deletionResult = result {
        use! session = FileTransfer.newSessionFromInstance (System.TimeSpan.FromMinutes(1.) |> Some) instance
        return! sprintf "%s/TESTSB_V001.XML" instance.MasterPDBManifestsPath |> FileTransfer.deleteRemoteFile session
    }
    res |> Result.mapError raise |> ignore
    deletionResult |> Result.mapError raise |> ignore
