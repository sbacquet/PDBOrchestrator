module Infrastructure.Tests.OracleInstanceRepository

open Xunit
open Infrastructure.OracleInstanceRepository
open Domain.OracleInstance
open Application.Common
open Domain.MasterPDBWorkingCopy
open Akkling.TestKit

let [<Literal>]testFolder = __SOURCE_DIRECTORY__ + @"\tests\instances"
let tempLifespan = System.TimeSpan.FromHours(12.)
let gitParams : GitParams = {
    LogError = failwithf "Error for %s : %s"
    GetModifyComment = sprintf "Commit %s"
    GetAddComment = sprintf "Add %s"
}

[<Fact>]
let ``Save and load Oracle instance`` () = testDefault <| fun tck ->
    let instance1Name = "test1"
    let wc = [ newTempWorkingCopy tempLifespan "me" (SpecificVersion 13) instance1Name true "wc" ]
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

    let gitActor = tck |> Infrastructure.GITActor.spawnForTests testFolder
    let repo = NewOracleInstanceRepository(tempLifespan, (fun _ _ -> raise), gitActor, gitParams, testFolder, instance1, "A") :> IOracleInstanceRepository
    let repo' = repo.Put instance1
    let i1 = repo'.Get ()
    Assert.Equal(instance1, i1)

[<Fact>]
let ``Log error when saving Oracle instance`` () = testDefault <| fun tck ->
    let mutable passedHere = false
    let instance1Name = "test1"
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

    let gitActor = tck |> Infrastructure.GITActor.spawnForTests testFolder
    let repo = NewOracleInstanceRepository(tempLifespan, (fun _ _ _ -> passedHere <- true), gitActor, gitParams, "*", instance1, "A") :> IOracleInstanceRepository
    let repo' = repo.Put instance1
    Assert.True(passedHere)
    Assert.Equal(repo, repo')
