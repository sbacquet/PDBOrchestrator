module Infrastructure.Tests.OracleInstanceRepository

open Xunit
open Infrastructure.OracleInstanceRepository
open Domain.OracleInstance
open Application.Common

let [<Literal>]testFolder = __SOURCE_DIRECTORY__ + @"\tests\instances"

[<Fact>]
let ``Save and load Oracle instance`` () =
    let instance1Name = "test1"
    let repo = OracleInstanceRepository(testFolder, instance1Name) :> IOracleInstanceRepository
    let instance1 =
        consOracleInstance
            [ "test1"; "test2" ]
            instance1Name "fr1psl010716.misys.global.ad" None
            "sys" "pass"
            "userForImport" ""
            "userForFileTransfer" "pass" "azerty"
            "x"
            "xx"
            "xxx"
            "xxxx"
            "xxxxx" ""
            true

    let repo' = repo.Put instance1
    let i1 = repo'.Get ()
    Assert.Equal(instance1, i1)
