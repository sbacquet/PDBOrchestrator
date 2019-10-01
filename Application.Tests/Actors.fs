module Application.Tests.Actors

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
open Application.DTO.OracleInstance
open Application.DTO.Orchestrator
open Application.MasterPDBActor
open Application.OrchestratorActor
open Domain.Common.Validation
open Application.Common

#if DEBUG
let run cont = runWithinElseTimeoutException -1 cont

let expectMsg tck =
    if (System.Diagnostics.Debugger.IsAttached) then
        expectMsgWithin tck (System.TimeSpan.FromHours(10.))
    else
        Akkling.TestKit.expectMsg tck

let expectMsgFilter tck =
    if (System.Diagnostics.Debugger.IsAttached) then
        expectMsgFilterWithin tck (System.TimeSpan.FromHours(10.))
    else
        Akkling.TestKit.expectMsgFilter tck

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
        }"
        Akkling.TestKit.test config, 
        (new Serilog.Extensions.Logging.SerilogLoggerFactory(dispose=true) :> ILoggerFactory)
    else
        Akkling.TestKit.testDefault,
        (new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory() :> ILoggerFactory)
#else
let run cont = runWithinElseTimeoutException 5000 cont
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
    MasterPDBDestPath = ""
    SnapshotPDBDestPath = ""
    SnapshotSourcePDBDestPath = ""
    OracleDirectoryForDumps = ""
    MasterPDBs = [ "test1"; "test2" ]
}

let instance2 : OracleInstance = {
    Name = "server2"
    DBAUser = "sys"
    DBAPassword = "syspwd8"
    Server = "xxx"
    Port = None
    MasterPDBManifestsPath = ""
    MasterPDBDestPath = ""
    SnapshotPDBDestPath = ""
    SnapshotSourcePDBDestPath = ""
    OracleDirectoryForDumps = ""
    MasterPDBs = [ "test2" ]
}

type FakeOracleAPI() = 
    member this.Logger = loggerFactory.CreateLogger("Fake Oracle API")
    interface IOracleAPI with
        member this.NewPDBFromDump _ _ _ _ _ _ _ _ name = async {
            this.Logger.LogDebug("Creating new PDB {PDB}...", name)
#if DEBUG
            do! Async.Sleep 3000
#endif
            return Ok name
        }
        member this.ClosePDB name = async { 
            this.Logger.LogDebug("Closing PDB {PDB}...", name)
            return Ok name 
        }
        member this.DeletePDB name = async { 
            this.Logger.LogDebug("Deleting PDB {PDB}...", name)
            return Ok name 
        }
        member this.ExportPDB _ name = async { 
            this.Logger.LogDebug("Exporting PDB {PDB}...", name)
            return Ok name 
        }
        member this.ImportPDB _ _ name = async { 
            this.Logger.LogDebug("Importing PDB {PDB}...", name)
            return Ok name 
        }
        member this.SnapshotPDB _ _ name = async { 
            this.Logger.LogDebug("Snapshoting PDB {PDB}...", name)
            return Ok name 
        }
        member this.PDBHasSnapshots _ = async { 
            return false
        }
        member this.PDBExists _ = async { 
            return true
        }

let fakeOracleAPI = FakeOracleAPI()

type FakeOracleInstanceRepo(map : Map<string, OracleInstance>) =
    interface IOracleInstanceRepository with
        member this.Get pdb = map |> Map.find pdb
        member this.Put name pdb = upcast FakeOracleInstanceRepo (map.Add(name, pdb))

let allInstances = 
    [
        "server1", instance1
        "server2", instance2
    ] |> Map.ofList

type FakeMasterPDBRepo(map : Map<string, MasterPDB>) =
    interface IMasterPDBRepository with
        member this.Get pdb = map |> Map.find pdb
        member this.Put name pdb = upcast FakeMasterPDBRepo (map.Add(name, pdb))

let masterPDBMap1 =
    [ 
        "test1", (newMasterPDB "test1" [ consSchema "toto" "toto" "Invest" ] "me" DateTime.Now "comment1")
        "test2", (newMasterPDB "test2" [ consSchema "toto" "toto" "Invest" ] "me" DateTime.Now "new comment2")
    ] |> Map.ofList

let masterPDBMap2 =
    [ 
        "test2", (newMasterPDB "test2" [ consSchema "toto" "toto" "Invest" ] "me" DateTime.Now "comment2")
    ] |> Map.ofList

[<Fact>]
let ``Test state transfer`` () = test <| fun tck ->
    let aref1 = tck |> OracleInstanceActor.spawn (fun _ -> fakeOracleAPI) (FakeMasterPDBRepo masterPDBMap1) instance1
    let aref2 = tck |> OracleInstanceActor.spawn (fun _ -> fakeOracleAPI) (FakeMasterPDBRepo masterPDBMap2) instance2

    let state : OracleInstanceActor.StateSet = retype aref1 <? OracleInstanceActor.TransferInternalState aref2 |> run
    state |> Result.mapError (fun error -> failwith error) |> ignore
    ()

let orchestratorState = {
    OracleInstanceNames = [ "server1"; "server2" ]
    PrimaryInstance = "server1"
}

let getMasterPDBRepo (instance:OracleInstance) = 
    match instance.Name with
    | "server1" -> FakeMasterPDBRepo masterPDBMap1
    | "server2" -> FakeMasterPDBRepo masterPDBMap2
    | name -> failwithf "Oracle instance %s does not exist" name

[<Fact>]
let ``Synchronize state`` () = test <| fun tck ->
    let instanceRepo = FakeOracleInstanceRepo allInstances
    let orchestrator = tck |> OrchestratorActor.spawn (fun _ -> fakeOracleAPI) instanceRepo getMasterPDBRepo orchestratorState
    //monitor tck orchestrator
    let state : OracleInstanceActor.StateSet = retype orchestrator <? OrchestratorActor.Synchronize "server2" |> run
    state |> Result.mapError (fun error -> failwith error) |> ignore
    ()

[<Fact>]
let ``Oracle server actor creates PDB`` () = test <| fun tck ->
    let oracleActor = tck |> OracleInstanceActor.spawn (fun _ -> fakeOracleAPI) (FakeMasterPDBRepo masterPDBMap2) instance2

    let stateBefore : OracleInstanceState = retype oracleActor <? OracleInstanceActor.GetState |> run
    Assert.Equal(1, stateBefore.MasterPDBs.Length)

    let parameters : OracleInstanceActor.CreateMasterPDBParams = {
        Name = "test3"
        Dump = @"c:\windows\system.ini" // always exists
        Schemas = [ "schema1" ]
        TargetSchemas = [ "targetschema1", "pass1", "Invest" ]
        User = "me"
        Date = DateTime.Now
        Comment = "yeah"
    }
    let _, res : WithRequestId<OracleInstanceActor.MasterPDBCreationResult> = 
        retype oracleActor <? OracleInstanceActor.CreateMasterPDB (newRequestId(), parameters) |> run
    match res with
    | OracleInstanceActor.MasterPDBCreated creationResponse -> ()
    | OracleInstanceActor.MasterPDBCreationFailure error -> failwithf "the creation of %s failed : %s" parameters.Name error
    | OracleInstanceActor.InvalidRequest errors -> failwithf "the request is invalid : %A" errors

    let stateAfter = retype oracleActor <? OracleInstanceActor.GetState |> run
    Assert.Equal(2, stateAfter.MasterPDBs.Length)
    ()

let pollRequestStatus (tck:Tck) orchestrator requestId =
    let rec requestStatus () = async {
        let status : WithRequestId<RequestStatus> = orchestrator <? OrchestratorActor.GetRequest requestId |> run
        match snd status with
        | Pending -> 
            tck.Log.Log(Akka.Event.LogLevel.InfoLevel, "The request {RequestId} is pending, waiting...", requestId)
#if DEBUG
            do! Async.Sleep 1000
#else
            do! Async.Sleep 10
#endif
            return! requestStatus ()
        | s -> return s
    }
    requestStatus() |> run

let throwIfRequestNotCompletedOk tck orchestrator request =
    match request with
    | Invalid errors -> failwith (System.String.Join("; ", errors))
    | Valid requestId ->
        let status = requestId |> pollRequestStatus tck orchestrator
        match status with
        | CompletedOk _ -> ()
        | _ -> failwith "operation not completed successfully"

[<Fact>]
let ``Orchestrator actor creates PDB`` () = test <| fun tck ->
    let instanceRepo = FakeOracleInstanceRepo allInstances
    let orchestrator = tck |> OrchestratorActor.spawn (fun _ -> fakeOracleAPI) instanceRepo getMasterPDBRepo orchestratorState

    let stateBefore : OrchestratorState = orchestrator <? OrchestratorActor.GetState |> run
    Assert.Equal(2, stateBefore.OracleInstances.[0].MasterPDBs.Length)

    let parameters : OracleInstanceActor.CreateMasterPDBParams = {
        Name = "test3"
        Dump = @"c:\windows\system.ini" // always exists
        Schemas = [ "schema1" ]
        TargetSchemas = [ "targetschema1", "pass1", "FusionInvest" ]
        User = "me"
        Date = DateTime.Now
        Comment = "yeah"
    }
    let request : RequestValidation = 
        orchestrator <? OrchestratorActor.CreateMasterPDB ("me", parameters) |> run

    request |> throwIfRequestNotCompletedOk tck orchestrator

    let stateAfter : OrchestratorState = orchestrator <? OrchestratorActor.GetState |> run
    Assert.Equal(3, stateAfter.OracleInstances.[0].MasterPDBs.Length)

[<Fact>]
let ``Lock master PDB`` () = test <| fun tck ->
    let pdb1 = newMasterPDB "test1" [ consSchema "toto" "toto" "Invest" ] "me" DateTime.Now "comment1"
    let longTaskExecutor = tck |> OracleLongTaskExecutor.spawn fakeOracleAPI
    let oracleDiskIntensiveTaskExecutor = tck |> OracleDiskIntensiveActor.spawn fakeOracleAPI
    let masterPDBActor = tck |> MasterPDBActor.spawn fakeOracleAPI instance1 longTaskExecutor oracleDiskIntensiveTaskExecutor pdb1
    
    retype masterPDBActor <! MasterPDBActor.PrepareForModification (newRequestId(), 1, "me")

    let lockedMess = expectMsgFilter tck (fun (mess:obj) -> 
        match mess with
        | :? WithRequestId<PrepareForModificationResult> as result ->
            match snd result with
            | Locked _ -> true
            | _ -> false
        | _ -> false
    )

    let preparedMess = expectMsgFilter tck (fun (mess:obj) -> 
        match mess with
        | :? WithRequestId<MasterPDBActor.PrepareForModificationResult> as result -> 
            match snd result with
            | Prepared _ -> true
            | _ -> false
        | _ -> false
    )

    let (_, result) : WithRequestId<MasterPDBActor.RollbackResult> = 
        retype masterPDBActor <? MasterPDBActor.Rollback (newRequestId()) |> run

    result |> Result.mapError (fun error -> failwith error) |> ignore

[<Fact>]
let ``OracleInstance locks master PDB`` () = test <| fun tck ->
    let oracleActor = tck |> OracleInstanceActor.spawn (fun _ -> fakeOracleAPI) (FakeMasterPDBRepo masterPDBMap1) instance1

    let (_, result) : WithRequestId<MasterPDBActor.PrepareForModificationResult> = 
        retype oracleActor <? OracleInstanceActor.PrepareMasterPDBForModification (newRequestId(), "test1", 1, "me") |> run

    match result with
    | Prepared _ -> ()
    | Locked _ -> failwith "this Locked event shoud not be recevied by orchestrator"
    | PreparationFailure error -> failwith error

[<Fact>]
let ``Orchestrator locks master PDB`` () = test <| fun tck ->
    let instanceRepo = FakeOracleInstanceRepo allInstances
    let orchestrator = tck |> OrchestratorActor.spawn (fun _ -> fakeOracleAPI) instanceRepo getMasterPDBRepo orchestratorState

    let request : RequestValidation = 
        orchestrator <? OrchestratorActor.PrepareMasterPDBForModification ("me", "test1", 1) |> run

    request |> throwIfRequestNotCompletedOk tck orchestrator

    let request : RequestValidation = 
        orchestrator <? OrchestratorActor.RollbackMasterPDB ("me", "test1") |> run
    
    request |> throwIfRequestNotCompletedOk tck orchestrator

[<Fact>]
let ``Snapshot PDB`` () = test <| fun tck ->
    let pdb1 = newMasterPDB "test1" [ consSchema "toto" "toto" "Invest" ] "me" DateTime.Now "comment1"
    let longTaskExecutor = tck |> OracleLongTaskExecutor.spawn fakeOracleAPI
    let oracleDiskIntensiveTaskExecutor = tck |> OracleDiskIntensiveActor.spawn fakeOracleAPI
    let masterPDBActor = tck |> MasterPDBActor.spawn fakeOracleAPI instance1 longTaskExecutor oracleDiskIntensiveTaskExecutor pdb1
    
    let (_, result):WithRequestId<SnapshotResult> = retype masterPDBActor <? MasterPDBActor.SnapshotVersion (newRequestId(), 1, "snapshot") |> run
    result |> Result.mapError (fun error -> failwith error) |> ignore

[<Fact>]
let ``OracleInstance snapshots PDB`` () = test <| fun tck ->
    let oracleActor = tck |> OracleInstanceActor.spawn (fun _ -> fakeOracleAPI) (FakeMasterPDBRepo masterPDBMap1) instance1

    let (_, result):WithRequestId<SnapshotResult> = retype oracleActor <? OracleInstanceActor.SnapshotMasterPDBVersion (newRequestId(), "test1", 1, "snapshot") |> run
    result |> Result.mapError (fun error -> failwith error) |> ignore

[<Fact>]
let ``Orchestrator snapshots PDB`` () = test <| fun tck ->
    let instanceRepo = FakeOracleInstanceRepo allInstances
    let orchestrator = tck |> OrchestratorActor.spawn (fun _ -> fakeOracleAPI) instanceRepo getMasterPDBRepo orchestratorState

    let request : RequestValidation = 
        orchestrator <? OrchestratorActor.SnapshotMasterPDBVersion ("me", "server1", "test1", 1, "snapshot") |> run

    request |> throwIfRequestNotCompletedOk tck orchestrator
