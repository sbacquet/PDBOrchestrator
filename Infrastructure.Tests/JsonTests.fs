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
    let date = DateTime.Now
    let pdb = newMasterPDB "test1" [ consSchema "toto" password "Invest" ] "me" date "comment1"
    let json = pdb |> MasterPDBJson.masterPDBtoJson
    let pdb' = json |> MasterPDBJson.jsonToMasterPDB
    match pdb' with
    | JPass p -> Assert.Equal(pdb, p)
    | JFail e -> failwith (e.ToString())

[<Fact>]
let ``Serialize and deserialize Oracle instance`` () =
    let instance1Name = "test1"
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
    let json = instance1 |> OracleInstanceJson.oracleInstancetoJson
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
