module Infrastructure.Tests.OrchestratorRepository

open Xunit
open Infrastructure.OrchestratorRepository
open Domain.Orchestrator
open Application.Common

let [<Literal>]orchestratorName = "orchestrator"
let [<Literal>]testFolder = __SOURCE_DIRECTORY__ + @"\tests\" + orchestratorName

[<Fact>]
let ``Save and load orchestrator`` () =
    let repo = OrchestratorRepository(testFolder, orchestratorName) :> IOrchestratorRepository
    let orchestrator = consOrchestrator [ "instance1"; "instance2"] "instance2"
    let repo' = repo.Put orchestrator
    let o' = repo'.Get()
    Assert.Equal(orchestrator, o')
