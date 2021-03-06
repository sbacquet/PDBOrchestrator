﻿module Infrastructure.Tests.Json

open System
open Xunit
open Domain.MasterPDB
open Domain.MasterPDBVersion
open Domain.MasterPDBWorkingCopy
open Domain.OracleInstance
open Domain.Orchestrator
open Infrastructure
open Chiron
open Application.OracleInstanceActor
open Application

[<Fact>]
let ``Serialize and deserialize master PDB`` () =
    let password = "toto"
    let props = [ "key", "value" ] |> Map.ofList
    let pdb = consMasterPDB "test1" [ consSchema "toto" password "Invest" ] [ newPDBVersion "me" "comment" ] (newEditionInfo "me" |> Some) false (Some "editionRole") props
    let json = pdb |> MasterPDBJson.masterPDBtoJson
    let pdb' = json |> MasterPDBJson.jsonToMasterPDB
    match pdb' with
    | JPass p -> Assert.Equal(pdb, p)
    | JFail e -> e |> JsonFailure.summarize |> failwith

[<Fact>]
let ``Serialize and deserialize Oracle instance`` () =
    let instance1Name = "test1"
    let wc = [ newTempWorkingCopy (System.TimeSpan.FromHours(12.)) "me" (SpecificVersion 13) "test1" true "wc" ]
    let instance1 =
        consOracleInstance
            [ "test1"; "test2" ]
            wc
            instance1Name "fr1psl010716.misys.global.ad" None
            "sys" "pass"
            "userForImport" "pass2"
            "userForFileTransfer" "pass3" "azerty" "toto"
            "x"
            "xx"
            "xxx"
            "xxxx"
            "xxxxx" ""
            true

    let json = instance1 |> OracleInstanceJson.oracleInstanceToJson
    let instance1' = json |> OracleInstanceJson.jsonToOracleInstance
    match instance1' with
    | JPass i -> 
        let wcMap = wc |> List.map (fun w -> w.Name, w) |> Map.ofList
        Assert.Equal(instance1, { i with WorkingCopies = wcMap })
    | JFail e -> e |> JsonFailure.summarize |> failwith

[<Fact>]
let ``Serialize and deserialize orchestrator`` () =
    let orchestrator = consOrchestrator [ "instance1"; "instance2" ] "instance1"
    let json = orchestrator |> OrchestratorJson.orchestratorToJson
    let orchestrator' = OrchestratorJson.jsonToOrchestrator json
    match orchestrator' with
    | JPass o -> Assert.Equal(orchestrator, o)
    | JFail e -> e |> JsonFailure.summarize |> failwith

[<Fact>]
let ``Serialize and deserialize CreateMasterPDBParams`` () =
    let user = "sbacquet" |> UserRights.consUserWithDefaults []
    let pars:CreateMasterPDBParams = newCreateMasterPDBParams "testsb" @"\\sophis\dumps\NEW_USER.DMP" [ "NEW_USER" ] [ "NEW_USER", "pass", "FusionInvest" ] user "this is a comment"
    let json = pars |> Infrastructure.HttpHandlers.createMasterPDBParamsToJson
    let pars' = json |> Infrastructure.HttpHandlers.jsonToCreateMasterPDBParams user
    match pars' with
    | JPass p -> Assert.Equal(pars, p)
    | JFail e -> e |> JsonFailure.summarize |> failwith
