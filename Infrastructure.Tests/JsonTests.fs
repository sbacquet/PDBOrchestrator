module Infrastructure.Tests.Json

open System
open Xunit
open Domain.MasterPDB
open Domain.OracleInstance
open Domain.Orchestrator
open Infrastructure
open Chiron

[<Fact>]
let ``Serialize and deserialize master PDB`` () =
    let password = "toto"
    let pdb = newMasterPDB "test1" [ consSchema "toto" password "Invest" ] "me" "comment1"
    let json = pdb |> MasterPDBJson.masterPDBtoJson
    let pdb' = json |> MasterPDBJson.jsonToMasterPDB
    match pdb' with
    | JPass p -> Assert.Equal(pdb, p)
    | JFail e -> failwith (e.ToString())

[<Fact>]
let ``Serialize and deserialize Oracle instance`` () =
    let instance1Name = "test1"
    let instance1 =
        consOracleInstance
            [ "test1"; "test2" ]
            instance1Name "fr1psl010716.misys.global.ad" None
            "sys" "pass"
            "userForImport" "pass2"
            "userForFileTransfer" "pass3" "azerty"
            "x"
            "xx"
            "xxx"
            "xxxx"
            "xxxxx" ""
            true

    let json = instance1 |> OracleInstanceJson.oracleInstanceToJson
    let instance1' = json |> OracleInstanceJson.jsonToOracleInstance
    match instance1' with
    | JPass i -> Assert.Equal(instance1, i)
    | JFail e -> failwith (e.ToString())

[<Fact>]
let ``Serialize and deserialize orchestrator`` () =
    let orchestrator = consOrchestrator [ "instance1"; "instance2" ] "instance1"
    let json = orchestrator |> OrchestratorJson.orchestratorToJson
    let orchestrator' = OrchestratorJson.jsonToOrchestrator json
    match orchestrator' with
    | JPass o -> Assert.Equal(orchestrator, o)
    | JFail e -> failwith (e.ToString())
