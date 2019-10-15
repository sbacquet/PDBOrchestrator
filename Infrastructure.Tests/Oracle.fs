module Infrastructure.Tests.Oracle

open System
open Xunit

open Infrastructure.Oracle
open Microsoft.Extensions.Logging.Abstractions
open Application.Oracle
open Domain.Common.Result
open Serilog
open Microsoft.Extensions.Logging

Serilog.Log.Logger <- 
    (new LoggerConfiguration()).
        WriteTo.Trace().
        MinimumLevel.Debug().
        CreateLogger()

let loggerFactory = (new Serilog.Extensions.Logging.SerilogLoggerFactory(dispose=true) :> ILoggerFactory)

let conn = Sql.withNewConnection (openConn "fr1psl010716.misys.global.ad" 1521 "intcdb2" "sys" "syspwd8" true)
let connIn pdb = Sql.withNewConnection (openConn "fr1psl010716.misys.global.ad" 1521 pdb "sys" "syspwd8" true)
let oracleAPI : IOracleAPI = new OracleAPI (loggerFactory, conn, connIn) :> IOracleAPI


[<Fact>]
let ``Fail to get inexisting PDB from server`` () =
    let pdb = getPDBOnServer conn "xxxxxxxxxx" |> Async.RunSynchronously
    pdb |> Result.mapError raise |> ignore
    pdb |> Result.map (fun p -> Assert.True(p.IsNone)) |> ignore

[<Fact>]
let ``Import and delete PDB`` () =
    let result = result {
        let! r = getPDBOnServer conn "toto" |> Async.RunSynchronously |> toOraclePDBResult
        let! _ = if r.IsSome then Error (exn "PDB toto already exists") else Ok "good"
        let! r = getPDBOnServerLike conn "toto%" |> Async.RunSynchronously |> toOraclePDBResult
        let! _ = if List.length r <> 0 then Error (exn "PDB toto% already exists") else Ok "good"
        let! _ = oracleAPI.ImportPDB "/u01/app/oracle/oradata/SB_PDBs/test1.xml" "/u01/app/oracle/oradata/SB_PDBs" "toto" |> Async.RunSynchronously
        let! r = getPDBOnServer conn "toto" |> Async.RunSynchronously |> toOraclePDBResult
        let! _ = if r.IsNone then Error (exn "No PDB toto ??") else Ok "good"
        let! r = getPDBOnServerLike conn "toto%" |> Async.RunSynchronously |> toOraclePDBResult
        let! _ = if List.length r <> 1 then Error (exn "No PDB toto% ??") else Ok "good"
        let! _ = if (List.head r).Name.ToUpper() <> "TOTO" then Error (sprintf "PDB is not TOTO (%s) ??" ((List.head r).Name.ToUpper()) |> exn) else Ok "good"
        return "ok"
    }
    oracleAPI.DeletePDB "toto" |> Async.RunSynchronously |> ignore
    result |> Result.mapError raise |> ignore

[<Fact>]
let ``Snapshot PDB`` () =
    let res = result {
        let stopWatch = System.Diagnostics.Stopwatch.StartNew()
        let! _ = oracleAPI.ImportPDB "/u01/app/oracle/oradata/SB_PDBs/test1.xml" "/u01/app/oracle/oradata/SB_PDBs" "source" |> Async.RunSynchronously
        stopWatch.Stop()
        Log.Logger.Debug("Time to import : {time}", stopWatch.Elapsed.TotalSeconds)
        stopWatch.Restart()
        let! _ = oracleAPI.SnapshotPDB "source" "/u01/app/oracle/oradata/SB_PDBs" "snapshot" |> Async.RunSynchronously
        stopWatch.Stop()
        Log.Logger.Debug("Time to snapshot : {time}", stopWatch.Elapsed.TotalSeconds)
        let! hasSnapshots = oracleAPI.PDBHasSnapshots "source" |> Async.RunSynchronously
        return! if hasSnapshots then Ok "Snapshot" else Error (exn "No snapshot ??!!")
    }

    let res2 = result {
        let! _ = oracleAPI.DeletePDB "snapshot" |> Async.RunSynchronously
        let! stillExists = oracleAPI.PDBExists "snapshot" |> Async.RunSynchronously
        let! _ = if stillExists then Error (exn "Snapshot not deleted ??!!") else Ok "Snapshot deleted"
        let! hasSnapshots = oracleAPI.PDBHasSnapshots "source" |> Async.RunSynchronously
        let! _ = if hasSnapshots then Error (exn "Yet some snapshot ??!!") else Ok "No snapshot"
        let! _ = oracleAPI.DeletePDB "source" |> Async.RunSynchronously
        let! stillExists = oracleAPI.PDBExists "source" |> Async.RunSynchronously
        let! _ = if stillExists then Error (exn "Source not deleted ??!!") else Ok "Source deleted"
        return "Everything fine!"
    }

    res |> Result.mapError raise |> ignore
    res2 |> Result.mapError raise |> ignore

[<Fact>]
let ``Snapshots older than 15 seconds`` () =
    let logger = loggerFactory.CreateLogger()
    let res = result {
        let! _ = oracleAPI.ImportPDB "/u01/app/oracle/oradata/SB_PDBs/test1.xml" "/u01/app/oracle/oradata/SB_PDBs" "source" |> Async.RunSynchronously
        let! _ = oracleAPI.SnapshotPDB "source" "/u01/app/oracle/oradata/SB_PDBs" "snapshot" |> Async.RunSynchronously
        let! snapshots = oracleAPI.PDBSnapshots "source" |> Async.RunSynchronously
        let! _ = if snapshots.Length <> 1 then Error (exn "Got no snapshot!") else Ok "# snapshots is 1, good"
        let seconds = 15
        let createdBefore = TimeSpan.FromSeconds((float)seconds)
        let! snapshots = pdbSnapshotsOlderThan conn createdBefore "source" |> Async.RunSynchronously |> toOraclePDBResult
        let! _ = if snapshots.Length <> 0 then Error (exn "Got a snapshot before 15 sec!") else Ok "# snapshots is 0, good"
        Async.Sleep(1000*(seconds+5)) |> Async.RunSynchronously
        let! snapshots = pdbSnapshotsOlderThan conn createdBefore "source" |> Async.RunSynchronously |> toOraclePDBResult
        let! _ = if snapshots.Length <> 1 then Error (exn "Got no snapshot after 20 sec!") else Ok "# snapshots is 1, good"
        return "Everything fine!"
    }    

    let res2 = result {
        let! _ = oracleAPI.DeletePDB "snapshot" |> Async.RunSynchronously
        let! stillExists = oracleAPI.PDBExists "snapshot" |> Async.RunSynchronously
        let! _ = if stillExists then Error (exn "Snapshot not deleted ??!!") else Ok "Snapshot deleted"
        let! hasSnapshots = oracleAPI.PDBHasSnapshots "source" |> Async.RunSynchronously
        let! _ = if hasSnapshots then Error (exn "Yet some snapshot ??!!") else Ok "No snapshot"
        let! _ = oracleAPI.DeletePDB "source" |> Async.RunSynchronously
        let! stillExists = oracleAPI.PDBExists "source" |> Async.RunSynchronously
        let! _ = if stillExists then Error (exn "Source not deleted ??!!") else Ok "Source deleted"
        return "Everything fine!"
    }

    res |> Result.mapError raise |> ignore
    res2 |> Result.mapError raise |> ignore

[<Fact>]
let ``Delete snapshots older than 15 seconds`` () =
    let logger = loggerFactory.CreateLogger()
    let seconds = 15
    let res = result {
        let! _ = oracleAPI.ImportPDB "/u01/app/oracle/oradata/SB_PDBs/test1.xml" "/u01/app/oracle/oradata/SB_PDBs" "source" |> Async.RunSynchronously
        let! _ = oracleAPI.SnapshotPDB "source" "/u01/app/oracle/oradata/SB_PDBs" "snapshot" |> Async.RunSynchronously
        let! snapshots = oracleAPI.PDBSnapshots "source" |> Async.RunSynchronously
        let! _ = if snapshots.Length <> 1 then Error (exn "Got no snapshot!") else Ok "# snapshots is 1, good"
        Async.Sleep(1000*(seconds+5)) |> Async.RunSynchronously
        let! deleted = oracleAPI.DeletePDBWithSnapshots (TimeSpan.FromSeconds((float)seconds)) "source" |> Async.RunSynchronously
        let! _ = if deleted then Ok "deleted properly" else Error (exn "Source not deleted ??!!")
        let! stillExists = oracleAPI.PDBExists "snapshot" |> Async.RunSynchronously
        let! _ = if stillExists then Error (exn "Snapshot not deleted ??!!") else Ok "Snapshot deleted"
        let! hasSnapshots = oracleAPI.PDBHasSnapshots "source" |> Async.RunSynchronously
        let! _ = if hasSnapshots then Error (exn "Yet some snapshot ??!!") else Ok "No snapshot"
        let! stillExists = oracleAPI.PDBExists "source" |> Async.RunSynchronously
        let! _ = if stillExists then Error (exn "Source not deleted ??!!") else Ok "Source deleted"
        return "Everything fine!"
    }

    res |> Result.mapError raise |> ignore
