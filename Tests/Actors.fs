module Tests.Actors

open Akkling
open Akkling.TestKit
open Xunit
open Application
open Domain.PDB
open Domain.State
open Domain.OrchestratorState
open Application.OracleServerActor
open Application.OrchestratorActor
open Application.PendingRequest
open Application.Oracle

let domainState : Domain.State.State = { 
    MasterPDBs = [ Domain.PDB.newMasterPDB "test1" [ Domain.PDB.newSchema "user" "password" "FusionInvest" ] ]
    MasterPDBVersions = [ newPDBVersion "test1" 1 "me" "no comment" ]
    LockedMasterPDBs = Map.empty
    UnusedMasterPDBVersions = Set.empty
}

let fakeOracleAPI = {
    NewPDBFromDump = fun _ _ _ _ _ _ _ _ name -> printfn "Creating new PDB %s..." name; Ok name
    ClosePDB = fun name -> Ok name
    DeletePDB = fun name -> Ok name
    ExportPDB = fun _ name -> Ok name
    ImportPDB = fun _ _ name -> Ok name
    SnapshotPDB = fun _ _ name -> Ok name
}

[<Fact>]
let ``Test state transfer`` () = testDefault <| fun tck ->
    let aref1 = spawn tck "StateAgent1" <| props (oracleServerActorBody fakeOracleAPI domainState)
    let aref2 = spawn tck "StateAgent2" <| props (oracleServerActorBody fakeOracleAPI (newState()))

    (retype aref1) <! TransferState "StateAgent2"

    expectMsg tck (StateSet "StateAgent2")

let orchestratorState = {
    OracleInstances = [ 
        { Name = "server1"; Server = "toto.com"; DBAUser = ""; DBAPassword = "" } 
        { Name = "server2"; Server = "toto.com"; DBAUser = ""; DBAPassword = "" } 
    ]
    PrimaryServer = "server1"
}

let getInstanceState name =
    match name with
    | "server1" -> domainState
    | _ -> newState()

[<Fact>]
let ``Synchronize state`` () = testDefault <| fun tck ->
    let orchestrator = spawn tck "orchestrator" <| props (orchestratorActorBody fakeOracleAPI orchestratorState getInstanceState)

    orchestrator <! Synchronize "server2"

    expectMsg tck (StateSet "server2")

[<Fact>]
let ``Oracle server actor creates PDB`` () = testDefault <| fun tck ->
    let oracleActor = spawn tck "server1" <| props (oracleServerActorBody fakeOracleAPI domainState)

    let stateBefore : DTO.State = retype oracleActor <? Application.OracleServerActor.GetState |> Async.RunSynchronously

    let parameters = {
        Name = "test1"
        Dump = ""
        Schemas = []
        TargetSchemas = []
        User = ""
        Comment = ""
    }
    let res : MasterPDBCreationResult = retype oracleActor <? CreateMasterPDB (newRequestId(), parameters) |> Async.RunSynchronously

    let stateAfter : DTO.State = retype oracleActor <? Application.OracleServerActor.GetState |> Async.RunSynchronously

    Some res
    //let expected : Application.Oracle.OraclePDBResult = Ok "test1"
    //expectMsg tck expected

