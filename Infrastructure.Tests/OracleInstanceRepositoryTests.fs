module Infrastructure.Tests.OracleInstanceRepository

open Xunit
open Infrastructure.OracleInstanceRepository
open Domain.OracleInstance
open Application.Common
open Domain.MasterPDBWorkingCopy

let [<Literal>]testFolder = __SOURCE_DIRECTORY__ + @"\tests\instances"

[<Fact>]
let ``Save and load Oracle instance`` () =
    let instance1Name = "test1"
    let repo = OracleInstanceRepository((fun _ _ -> raise), testFolder, instance1Name, "A") :> IOracleInstanceRepository
    let wc = [ newTempWorkingCopy (System.TimeSpan.FromHours(12.)) "me" (SpecificVersion 13) "test1" "wc" ]
    let instance1 =
        consOracleInstance
            [ "test1"; "test2" ]
            wc
            instance1Name "fr1psl010716.misys.global.ad" None
            "sys" "pass"
            "userForImport" ""
            "userForFileTransfer" "pass" "azerty" "toto"
            "x"
            "xx"
            "xxx"
            "xxxx"
            "xxxxx" ""
            true

    let repo' = repo.Put instance1
    let i1 = repo'.Get ()
    Assert.Equal(instance1, i1)

[<Fact>]
let ``Log error when saving Oracle instance`` () =
    let mutable passedHere = false
    let instance1Name = "test1"
    let repo = OracleInstanceRepository((fun _ _ _ -> passedHere <- true), "*", instance1Name, "A") :> IOracleInstanceRepository
    let instance1 =
        consOracleInstance
            [ "test1"; "test2" ]
            List.empty
            instance1Name "fr1psl010716.misys.global.ad" None
            "sys" "pass"
            "userForImport" ""
            "userForFileTransfer" "pass" "azerty" "toto"
            "x"
            "xx"
            "xxx"
            "xxxx"
            "xxxxx" ""
            true
    let repo' = repo.Put instance1
    Assert.True(passedHere)
    Assert.Equal(repo, repo')
