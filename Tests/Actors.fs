module Tests.Actors

open Akkling
open Akkling.TestKit
open Xunit
open Application
open Domain.PDB
open Domain.OracleInstanceState
open Domain.OrchestratorState
open Application.OracleInstanceActor
open Application.OrchestratorActor
open Application.PendingRequest
open Application.Oracle
open Akka.Configuration
open Serilog

let instance1State : OracleInstanceState = { 
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

#if DEBUG
let test, expectMsg =
    if (System.Diagnostics.Debugger.IsAttached) then
        Serilog.Log.Logger <- (new LoggerConfiguration()).WriteTo.Trace().MinimumLevel.Debug().CreateLogger()
        let config = ConfigurationFactory.ParseString @"
        akka { 
            loglevel=DEBUG,  loggers=[""Akka.Logger.Serilog.SerilogLogger, Akka.Logger.Serilog""] 
            actor {
                debug {
                    receive = on
                    unhandled = on
                    lifecycle = off
                }
            }
            test {
                default-timeout = 36000s
            }
        }"
        Akkling.TestKit.test config, fun tck -> expectMsgWithin tck (System.TimeSpan.FromHours(10.))
    else
        testDefault, expectMsg
#else
let test = testDefault
#endif

[<Fact>]
let ``Test state transfer`` () = test <| fun tck ->
    let aref1 = spawn tck "StateAgent1" <| props (oracleInstanceActorBody fakeOracleAPI instance1State)
    let aref2 = spawn tck "StateAgent2" <| props (oracleInstanceActorBody fakeOracleAPI (newState()))

    (retype aref1) <! TransferState "StateAgent2"

    expectMsg tck (StateSet "StateAgent2") |> ignore

let orchestratorState = {
    OracleInstances = [ 
        { Name = "server1"; Server = "toto.com"; DBAUser = ""; DBAPassword = "" } 
        { Name = "server2"; Server = "toto.com"; DBAUser = ""; DBAPassword = "" } 
    ]
    PrimaryServer = "server1"
}

let getInstanceState name =
    match name with
    | "server1" -> instance1State
    | _ -> newState()

[<Fact>]
let ``Synchronize state`` () = test <| fun tck ->
    let orchestrator = spawn tck "orchestrator" <| props (orchestratorActorBody fakeOracleAPI orchestratorState getInstanceState)

    orchestrator <! Synchronize "server2"

    expectMsg tck (StateSet "server2") |> ignore

[<Fact>]
let ``Oracle server actor creates PDB`` () = test <| fun tck ->
    let oracleActor = spawn tck "server1" <| props (oracleInstanceActorBody fakeOracleAPI instance1State)

    let stateBefore : DTO.State = retype oracleActor <? OracleInstanceActor.GetState |> Async.RunSynchronously

    let parameters = {
        Name = "test2"
        Dump = ""
        Schemas = []
        TargetSchemas = []
        User = ""
        Comment = ""
    }
    let res : MasterPDBCreationResult = retype oracleActor <? CreateMasterPDB (newRequestId(), parameters) |> Async.RunSynchronously

    let stateAfter : DTO.State = retype oracleActor <? OracleInstanceActor.GetState |> Async.RunSynchronously
    ()