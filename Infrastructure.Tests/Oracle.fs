module Infrastructure.Tests.Oracle

open System
open Xunit

open Infrastructure.Oracle
open Microsoft.Extensions.Logging.Abstractions
open Application.Oracle
open Domain.Common.Result

let conn = Sql.withNewConnection (openConn "fr1psl010716.misys.global.ad" 1521 "intcdb2" "sys" "syspwd8" true)
let connIn pdb = Sql.withNewConnection (openConn "fr1psl010716.misys.global.ad" 1521 pdb "sys" "syspwd8" true)
let oracleAPI : IOracleAPI = new OracleAPI (NullLoggerFactory.Instance, conn, connIn) :> IOracleAPI

[<Fact>]
let ``Fail to get inexisting PDB from server`` () =
    let pdb = getPDBOnServer conn "xxxxxxxxxx" |> Async.RunSynchronously
    pdb |> Result.mapError raise |> ignore
    pdb |> Result.map (fun p -> Assert.True(p.IsNone)) |> ignore

[<Fact>]
let ``Import PDB`` () =
    let result = oracleAPI.ImportPDB "/u01/app/oracle/oradata/SB_PDBs/test1.xml" "/u01/app/oracle/oradata/SB_PDBs" "toto" |> Async.RunSynchronously
    oracleAPI.DeletePDB "toto" |> Async.RunSynchronously |> ignore
    result |> Result.mapError raise |> ignore

[<Fact>]
let ``Snapshot PDB`` () =
    let res = result {
        let! _ = oracleAPI.ImportPDB "/u01/app/oracle/oradata/SB_PDBs/test1.xml" "/u01/app/oracle/oradata/SB_PDBs" "source" |> Async.RunSynchronously
        let! _ = oracleAPI.SnapshotPDB "source" "/u01/app/oracle/oradata/SB_PDBs" "snapshot" |> Async.RunSynchronously
        let! hasSnapshots = oracleAPI.PDBHasSnapshots "source" |> Async.RunSynchronously
        return! if hasSnapshots then Ok "Snapshot" else Error (exn "No snapshot ??!!")
    }
    res |> Result.mapError raise |> ignore

    let res = result {
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
