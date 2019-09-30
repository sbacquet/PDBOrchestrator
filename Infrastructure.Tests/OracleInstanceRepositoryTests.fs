module Infrastructure.Tests.OracleInstanceRepository

open Xunit
open Infrastructure.OracleInstanceRepository
open Domain.OracleInstance
open Application.OrchestratorActor

let [<Literal>]testFolder = __SOURCE_DIRECTORY__ + @"\tests\instances"

[<Fact>]
let ``Save and load Oracle instance`` () =
    let instance1Name = "test1"
    let repo = OracleInstanceRepository(testFolder) :> OracleInstanceRepo
    let instance1 : OracleInstance = {
        Name = instance1Name
        Server = "fr1psl010716.misys.global.ad"
        Port = None
        DBAUser = "sys"
        DBAPassword = "pass"
        MasterPDBManifestsPath = "x"
        MasterPDBDestPath = "xx"
        SnapshotPDBDestPath = "xxx"
        SnapshotSourcePDBDestPath = "xxxx"
        OracleDirectoryForDumps = "xxxxx"
        MasterPDBs = [ "test1"; "test2" ]
    }
    let repo' = repo.Put instance1Name instance1
    let i1 = repo'.Get instance1Name
    Assert.Equal(instance1, i1)
