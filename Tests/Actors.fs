module Tests.Actors

open Akkling
open Akkling.TestKit
open Xunit
open Application
open Domain.MasterPDB
open Domain.OracleInstance
open Domain.Orchestrator
open Application.PendingRequest
open Application.Oracle
open Akka.Configuration
open Serilog
open Microsoft.Extensions.Logging
open System

#if DEBUG
let expectMsg tck =
    if (System.Diagnostics.Debugger.IsAttached) then
        expectMsgWithin tck (System.TimeSpan.FromHours(10.))
    else
        Akkling.TestKit.expectMsg tck

let test, (loggerFactory : ILoggerFactory) =
    if (System.Diagnostics.Debugger.IsAttached) then
        Serilog.Log.Logger <- 
            (new LoggerConfiguration()).
                WriteTo.Trace(outputTemplate="[{SourceContext}] {Message}{NewLine}").
                MinimumLevel.Debug().
                CreateLogger()
        let config = ConfigurationFactory.ParseString @"
        akka { 
            loglevel=DEBUG,  loggers=[""Akka.Logger.Serilog.SerilogLogger, Akka.Logger.Serilog""] 
            actor {
                debug {
                    receive = on
                    unhandled = on
                    lifecycle = on
                }
            }
            test {
                default-timeout = 36000s
            }
        }"
        Akkling.TestKit.test config, 
        (new Serilog.Extensions.Logging.SerilogLoggerFactory(dispose=true) :> ILoggerFactory)
    else
        Akkling.TestKit.test (ConfigurationFactory.ParseString @"akka { actor { ask-timeout = 1s } }"), 
        (new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory() :> ILoggerFactory)
#else
let test = Akkling.TestKit.test (ConfigurationFactory.ParseString @"akka { actor { ask-timeout = 1s } }")
let (loggerFactory : ILoggerFactory) = new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory() :> ILoggerFactory
#endif

let instance1 : OracleInstance = {
    Name = "server1"
    Server = "fr1psl010716.misys.global.ad"
    Port = None
    DBAUser = "sys"
    DBAPassword = "syspwd8"
    MasterPDBManifestsPath = ""
    TestPDBManifestsPath = ""
    OracleDirectoryForDumps = ""
    MasterPDBs = [ "test1" ]
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
    MasterPDBs = [ "test2" ]
}

type FakeOracleAPI() = 
    member this.Logger = loggerFactory.CreateLogger("Fake Oracle API")
    interface IOracleAPI with
        member this.NewPDBFromDump _ _ _ _ _ _ _ _ name = async {
            this.Logger.LogDebug("Creating new PDB {PDB}...", name)
            return Ok name
        }
        member this.ClosePDB name = async { return Ok name }
        member this.DeletePDB name = async { return Ok name }
        member this.ExportPDB _ name = async { return Ok name }
        member this.ImportPDB _ _ name = async { return Ok name }
        member this.SnapshotPDB _ _ name = async { return Ok name }

let fakeOracleAPI = FakeOracleAPI()

let getInstance name =
    match name with
    | "server1" -> instance1
    | "server2" -> instance2
    | _ -> failwithf "Oracle instance %s does not exist" name

let getMasterPDB name =
    match name with
    | "test1" -> newMasterPDB name [ consSchema "toto" "toto" "Invest" ] "me" DateTime.Now "comment1"
    | "test2" -> newMasterPDB name [ consSchema "toto" "toto" "Invest" ] "me" DateTime.Now "comment2"
    | _ -> failwithf "master PDB %s does not exist" name

[<Fact>]
let ``Test state transfer`` () = test <| fun tck ->
    let aref1 = tck |> OracleInstanceActor.spawn (fun _ -> fakeOracleAPI) getMasterPDB instance1
    let aref2 = tck |> OracleInstanceActor.spawn (fun _ -> fakeOracleAPI) getMasterPDB instance2

    let state : OracleInstanceActor.StateSet = retype aref1 <? OracleInstanceActor.TransferState aref2 |> Async.RunSynchronously
    state |> Result.mapError (fun error -> failwith error) |> ignore
    ()

let orchestratorState = {
    OracleInstanceNames = [ "server1"; "server2" ]
    PrimaryServer = "server1"
}

[<Fact>]
let ``Synchronize state`` () = test <| fun tck ->
    let orchestrator = tck |> OrchestratorActor.spawn (fun _ -> fakeOracleAPI) getInstance getMasterPDB orchestratorState
    //monitor tck orchestrator
    let state : OracleInstanceActor.StateSet = retype orchestrator <? OrchestratorActor.Synchronize "server2" |> Async.RunSynchronously
    state |> Result.mapError (fun error -> failwith error) |> ignore
    ()

[<Fact>]
let ``Oracle server actor creates PDB`` () = test <| fun tck ->
    let oracleActor = tck |> OracleInstanceActor.spawn (fun _ -> fakeOracleAPI) getMasterPDB instance1

    let stateBefore = retype oracleActor <? OracleInstanceActor.GetState |> Async.RunSynchronously
    Assert.Equal(1, stateBefore.MasterPDBs.Length)

    let parameters : OracleInstanceActor.CreateMasterPDBParams = {
        Name = "test2"
        Dump = @"c:\windows\system.ini" // always exists
        Schemas = [ "schema1" ]
        TargetSchemas = [ "targetschema1", "pass1", "Invest" ]
        User = "me"
        Date = DateTime.Now
        Comment = "yeah"
    }
    let res : OracleInstanceActor.MasterPDBCreationResult = retype oracleActor <? OracleInstanceActor.CreateMasterPDB parameters |> Async.RunSynchronously
    match res with
    | OracleInstanceActor.MasterPDBCreated creationResponse -> 
        match creationResponse with
        | Ok _ -> ()
        | Error ex -> failwithf "the creation of %s failed : %A" parameters.Name ex
    | OracleInstanceActor.InvalidRequest errors -> failwithf "the request is invalid : %A" errors

    let stateAfter = retype oracleActor <? OracleInstanceActor.GetState |> Async.RunSynchronously
    Assert.Equal(2, stateAfter.MasterPDBs.Length)
    ()

[<Fact>]
let ``Orchestrator actor creates PDB`` () = test <| fun tck ->
    let orchestrator = tck |> OrchestratorActor.spawn (fun _ -> fakeOracleAPI) getInstance getMasterPDB orchestratorState

    let parameters : OracleInstanceActor.CreateMasterPDBParams = {
        Name = "test2"
        Dump = @"c:\windows\system.ini" // always exists
        Schemas = [ "schema1" ]
        TargetSchemas = [ "targetschema1", "pass1", "FusionInvest" ]
        User = "me"
        Date = DateTime.Now
        Comment = "yeah"
    }
    let res : OracleInstanceActor.MasterPDBCreationResult = retype orchestrator <? OrchestratorActor.CreateMasterPDB parameters |> Async.RunSynchronously
    match res with
    | OracleInstanceActor.MasterPDBCreated creationResponse -> 
        match creationResponse with
        | Ok _ -> ()
        | Error ex -> failwithf "the creation of %s failed : %A" parameters.Name ex
    | OracleInstanceActor.InvalidRequest errors -> failwithf "the request is invalid : %A" errors
