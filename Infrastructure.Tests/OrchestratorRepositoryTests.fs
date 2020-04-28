module Infrastructure.Tests.OrchestratorRepository

open Xunit
open Infrastructure.OrchestratorRepository
open Domain.Orchestrator
open Application.Common
open Akkling.TestKit

let [<Literal>]testFolder = __SOURCE_DIRECTORY__ + @"\tests\orchestrator"
let gitParams : GitParams = {
    LogError = failwithf "Error for %s : %s"
    GetModifyComment = sprintf "Commit %s"
}

[<Fact>]
let ``Save and load orchestrator`` () = testDefault <| fun tck ->
    let orchestrator = consOrchestrator [ "instance1"; "instance2"] "instance2"
    let gitActor = tck |> Infrastructure.GITActor.spawnForTests testFolder
    let repo = NewOrchestratorRepository((fun _ -> raise), gitActor, gitParams, testFolder, orchestrator) :> IOrchestratorRepository
    let repo' = repo.Put orchestrator
    let o' = repo'.Get()
    Assert.Equal(orchestrator, o')

[<Fact>]
let ``Log error when saving orchestrator`` () = testDefault <| fun tck ->
    let mutable passedHere = false
    let orchestrator = consOrchestrator [ "instance1"; "instance2"] "instance2"
    let gitActor = tck |> Infrastructure.GITActor.spawnForTests testFolder
    let repo = NewOrchestratorRepository((fun _ _ -> passedHere <- true), gitActor, gitParams, "*", orchestrator) :> IOrchestratorRepository
    let repo' = repo.Put orchestrator
    Assert.True(passedHere)
    Assert.Equal(repo, repo')
