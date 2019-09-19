module Tests.Actors

open Akkling
open Akkling.TestKit
open Xunit
open Application
open Domain.PDB
open Domain.OracleInstance
open Domain.Orchestrator
open Application.PendingRequest
open Application.Oracle
open Akka.Configuration
open Serilog
open Microsoft.Extensions.Logging

#if DEBUG
let expectMsg tck =
    if (System.Diagnostics.Debugger.IsAttached) then
        expectMsgWithin tck (System.TimeSpan.FromHours(10.))
    else
        Akkling.TestKit.expectMsg tck

let test, (loggerFactory : ILoggerFactory) =
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
        Akkling.TestKit.test config, 
        (new Serilog.Extensions.Logging.SerilogLoggerFactory(dispose=true) :> ILoggerFactory)
    else
        testDefault, 
        (new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory() :> ILoggerFactory)
#else
let test = testDefault
let (loggerFactory : ILoggerFactory) = new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory() :> ILoggerFactory
#endif

let instance1 : OracleInstance = {
    Name = "server1"
    DBAUser = "sys"
    DBAPassword = "syspwd8"
    Server = "fr1psl010716.misys.global.ad"
    Port = None
    MasterPDBManifestsPath = ""
    TestPDBManifestsPath = ""
    OracleDirectoryForDumps = ""
}

let instance1State : OracleInstanceState = { 
    MasterPDBs = [ Domain.PDB.newMasterPDB "test1" [ Domain.PDB.newSchema "user" "password" "FusionInvest" ] ]
    MasterPDBVersions = [ newPDBVersion "test1" 1 "me" "no comment" ]
    LockedMasterPDBs = Map.empty
    UnusedMasterPDBVersions = Set.empty
}

let instance2 : OracleInstance = {
    Name = "server2"
    DBAUser = "sys"
    DBAPassword = "syspwd8"
    Server = "xxx"
    Port = None
    MasterPDBManifestsPath = ""
    TestPDBManifestsPath = ""
    OracleDirectoryForDumps = ""
}

type FakeOracleAPI() = 
    member this.Logger = loggerFactory.CreateLogger("Fake Oracle API")
    interface IOracleAPI with
        member this.NewPDBFromDump _ _ _ _ _ _ _ _ name = 
            this.Logger.LogDebug("Creating new PDB {PDB}...", name)
            Ok name
        member this.ClosePDB name = Ok name
        member this.DeletePDB name = Ok name
        member this.ExportPDB _ name = Ok name
        member this.ImportPDB _ _ name = Ok name
        member this.SnapshotPDB _ _ name = Ok name

let fakeOracleAPI = FakeOracleAPI()

let getInstance name =
    match name with
    | "server1" -> Some instance1
    | "server2" -> Some instance2
    | _ -> None

let getInstanceState name =
    match name with
    | "server1" -> Some instance1State
    | "server2" -> Some (newState())
    | _ -> None

[<Fact>]
let ``Test state transfer`` () = test <| fun tck ->
    let aref1 = tck |> OracleInstanceActor.spawn getInstance getInstanceState (fun _ -> fakeOracleAPI) "server1"
    let aref2 = tck |> OracleInstanceActor.spawn getInstance getInstanceState (fun _ -> fakeOracleAPI) "server2"

    let state : OracleInstanceActor.StateSet = retype aref1 <? OracleInstanceActor.TransferState "server2" |> Async.RunSynchronously

    ()

let orchestratorState = {
    OracleInstanceNames = [ "server1"; "server2" ]
    PrimaryServer = "server1"
}

[<Fact>]
let ``Synchronize state`` () = test <| fun tck ->
    let orchestrator = tck |> OrchestratorActor.spawn getInstance getInstanceState (fun _ -> fakeOracleAPI) orchestratorState

    let state : OracleInstanceActor.StateSet = retype orchestrator <? OrchestratorActor.Synchronize "server2" |> Async.RunSynchronously

    ()

[<Fact>]
let ``Oracle server actor creates PDB`` () = test <| fun tck ->
    let oracleActor = tck |> OracleInstanceActor.spawn getInstance getInstanceState (fun _ -> fakeOracleAPI) "server1"

    let stateBefore = retype oracleActor <? OracleInstanceActor.GetState |> Async.RunSynchronously |> Application.DTO.stateToDTO
    Assert.Equal(1, stateBefore.MasterPDBs.Length)

    let parameters : OracleInstanceActor.CreateMasterPDBParams = {
        Name = "test2"
        Dump = @"c:\windows\system.ini" // always exists
        Schemas = [ "schema1" ]
        TargetSchemas = [ "targetschema1", "pass1", "FusionInvest" ]
        User = "me"
        Comment = "yeah"
    }
    let res : OracleInstanceActor.MasterPDBCreationResult = retype oracleActor <? OracleInstanceActor.CreateMasterPDB parameters |> Async.RunSynchronously
    match res with
    | OracleInstanceActor.MasterPDBCreated creationResponse -> 
        match creationResponse with
        | Ok _ -> ()
        | Error ex -> failwithf "the creation of %s failed : %A" parameters.Name ex
    | OracleInstanceActor.InvalidRequest errors -> failwithf "the request is invalid : %A" errors

    let stateAfter = retype oracleActor <? OracleInstanceActor.GetState |> Async.RunSynchronously |> Application.DTO.stateToDTO
    Assert.Equal(2, stateAfter.MasterPDBs.Length)
    ()

[<Fact>]
let ``Orchestrator actor creates PDB`` () = test <| fun tck ->
    let orchestratorActor = tck |> OrchestratorActor.spawn getInstance getInstanceState (fun _ -> fakeOracleAPI) orchestratorState

    let parameters : OracleInstanceActor.CreateMasterPDBParams = {
        Name = "test2"
        Dump = @"c:\windows\system.ini" // always exists
        Schemas = [ "schema1" ]
        TargetSchemas = [ "targetschema1", "pass1", "FusionInvest" ]
        User = "me"
        Comment = "yeah"
    }
    let res : OracleInstanceActor.MasterPDBCreationResult = retype orchestratorActor <? OrchestratorActor.CreateMasterPDB parameters |> Async.RunSynchronously
    match res with
    | OracleInstanceActor.MasterPDBCreated creationResponse -> 
        match creationResponse with
        | Ok _ -> ()
        | Error ex -> failwithf "the creation of %s failed : %A" parameters.Name ex
    | OracleInstanceActor.InvalidRequest errors -> failwithf "the request is invalid : %A" errors
