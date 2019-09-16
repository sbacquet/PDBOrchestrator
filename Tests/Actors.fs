﻿module Tests.Actors

open Akkling
open Akkling.TestKit
open Xunit
open Application
open Domain.PDB
open Domain.State
open Domain.OrchestratorState
open Application.OracleServerActor
open Application.OrchestratorActor

let domainState : Domain.State.State = { 
    MasterPDBs = [ Domain.PDB.newMasterPDB "test1" [ Domain.PDB.newSchema "user" "password" "FusionInvest" ] ]
    MasterPDBVersions = [ newPDBVersion "test1" 1 "me" "no comment" ]
    LockedMasterPDBs = Map.empty
    UnusedMasterPDBVersions = Set.empty
}

[<Fact>]
let ``Test state transfer`` () = testDefault <| fun tck ->
    let aref1 = spawn tck "StateAgent1" <| props (oracleServerActorBody domainState)
    let aref2 = spawn tck "StateAgent2" <| props (oracleServerActorBody (newState()))

    (retype aref1) <! TransferState "StateAgent2"

    expectMsg tck (StateSet "StateAgent2")

let orchestratorState = {
    OracleInstances = [ { Name = "server1"; Server = "toto.com"; DBAUser = ""; DBAPassword = "" } ]
    PrimaryServer = "server1"
}

let getInstanceState name =
    match name with
    | "server1" -> domainState
    | _ -> newState()

[<Fact>]
let ``Synchronize state`` () = testDefault <| fun tck ->
    let orchestrator = spawn tck "orchestrator" <| props (orchestratorActorBody orchestratorState getInstanceState)

    orchestrator <! Synchronize "server2"

    expectMsg tck (StateSet "server2")

[<Fact>]
let ``Oracle server actor creates PDB`` () = testDefault <| fun tck ->
    let oracleActor = spawn tck "server1" <| props (oracleServerActorBody domainState)

    let res : Application.Oracle.OraclePDBResult = (retype oracleActor) <? CreateMasterPDB "test1" |> Async.RunSynchronously
    Some res
    //let expected : Application.Oracle.OraclePDBResult = Ok "test1"
    //expectMsg tck expected
