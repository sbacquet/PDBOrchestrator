module Infrastructure.Tests.Oracle

open System
open Xunit

open Infrastructure.Oracle
open Microsoft.Extensions.Logging.Abstractions
open Microsoft.Extensions.Logging
open Application.Oracle
open Domain.Common.Result

let conn = Sql.withNewConnection (openConn "fr1psl010716.misys.global.ad" 1521 "intcdb2" "sys" "syspwd8" true)
let connIn pdb = Sql.withNewConnection (openConn "fr1psl010716.misys.global.ad" 1521 pdb "sys" "syspwd8" true)
let oracleAPI : IOracleAPI = new OracleAPI (NullLoggerFactory.Instance, conn, connIn) :> IOracleAPI

[<Fact>]
let ``Fail to get inexisting PDB from server`` () =
    let pdb = getPDBOnServer conn "xxxxxxxxxx" |> Async.RunSynchronously
    Assert.True(pdb.IsNone)

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
        return if (oracleAPI.PDBHasSnapshots "source" |> Async.RunSynchronously) then Ok "snapshot" else Error (exn "No snapshot ??!!")
    }
    res |> Result.mapError raise |> ignore
    let res = result {
        let! _ = oracleAPI.DeletePDB "snapshot" |> Async.RunSynchronously
        let! _ = oracleAPI.DeletePDB "source" |> Async.RunSynchronously
        return if (oracleAPI.PDBHasSnapshots "source" |> Async.RunSynchronously) then Error "Yet some snapshots ??!!" else Ok "snapshot"
    }
    res |> Result.mapError raise |> ignore
