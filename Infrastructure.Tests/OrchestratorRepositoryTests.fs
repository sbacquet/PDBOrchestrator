module Infrastructure.Tests.OrchestratorRepository

open Xunit
open Infrastructure.OrchestratorRepository
open Domain.Orchestrator
open Application.Common

let [<Literal>]orchestratorName = "orchestrator"
let [<Literal>]testFolder = __SOURCE_DIRECTORY__ + @"\tests\" + orchestratorName

[<Fact>]
let ``Save and load orchestrator`` () =
    let repo = OrchestratorRepository((fun _ -> raise), testFolder, orchestratorName) :> IOrchestratorRepository
    let orchestrator = consOrchestrator [ "instance1"; "instance2"] "instance2"
    let repo' = repo.Put orchestrator
    let o' = repo'.Get()
    Assert.Equal(orchestrator, o')

[<Fact>]
let ``Log error when saving orchestrator`` () =
    let mutable passedHere = false
    let repo = OrchestratorRepository((fun _ _ -> passedHere <- true), "*", orchestratorName) :> IOrchestratorRepository
    let orchestrator = consOrchestrator [ "instance1"; "instance2"] "instance2"
    let repo' = repo.Put orchestrator
    Assert.True(passedHere)
    Assert.Equal(repo, repo')
