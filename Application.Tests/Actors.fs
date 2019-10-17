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
open Application.GlobalParameters

let parameters : GlobalParameters = {
    ServerInstanceName = "A"
#if DEBUG
    ShortTimeout = None
    LongTimeout = None
    VeryLongTimeout = None
#else
    ShortTimeout = TimeSpan.FromSeconds(5.) |> Some
    LongTimeout = TimeSpan.FromMinutes(2.) |> Some
    VeryLongTimeout = TimeSpan.FromMinutes(20.) |> Some
#endif
    NumberOfOracleLongTaskExecutors = 3
    NumberOfOracleDiskIntensiveTaskExecutors = 1
    GarbageCollectionDelay = TimeSpan.FromMinutes(1.)
}

#if DEBUG
let quickTimeout = None

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
let quickTimeout = TimeSpan.FromMilliseconds(100.) |> Some
let test = Akkling.TestKit.test (ConfigurationFactory.ParseString @"akka { actor { ask-timeout = 1s } }")
let (loggerFactory : ILoggerFactory) = new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory() :> ILoggerFactory
#endif

let run cont = runWithinElseTimeoutException parameters.ShortTimeout cont
let runQuick cont = runWithinElseTimeoutException quickTimeout cont

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
            do! Async.Sleep 100
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
            do! Async.Sleep 100
            return Ok name 
        }
        member this.PDBHasSnapshots _ = async { 
            return Ok false
        }
        member this.PDBExists _ = async { 
            return Ok true
        }
        member this.DeletePDBWithSnapshots _ name = async { 
            return Valid false
        }
        member this.PDBSnapshots name = async {
            return Ok []
        }
        member this.GetPDBNamesLike (like:string) = 
            raise (System.NotImplementedException())

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
        "test1", (newMasterPDB "test1" [ consSchema "toto" "toto" "Invest" ] "me" "comment1")
        "test2", (newMasterPDB "test2" [ consSchema "toto" "toto" "Invest" ] "me" "new comment2")
    ] |> Map.ofList

let masterPDBMap2 =
    [ 
        "test2", (newMasterPDB "test2" [ consSchema "toto" "toto" "Invest" ] "me" "comment2")
    ] |> Map.ofList

let spawnOrchestratorActor = OrchestratorActor.spawn parameters (fun _ -> fakeOracleAPI)
let spawnOracleInstanceActor = OracleInstanceActor.spawn parameters (fun _ -> fakeOracleAPI)
let spawnMasterPDBActor = MasterPDBActor.spawn parameters fakeOracleAPI

[<Fact>]
let ``Test state transfer`` () = test <| fun tck ->
    let aref1 = tck |> spawnOracleInstanceActor (FakeMasterPDBRepo masterPDBMap1) instance1
    let aref2 = tck |> spawnOracleInstanceActor (FakeMasterPDBRepo masterPDBMap2) instance2

    let state : OracleInstanceActor.StateResult = retype aref1 <? OracleInstanceActor.TransferInternalState aref2 |> run
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
let ``API synchronizes state`` () = test <| fun tck ->
    let instanceRepo = FakeOracleInstanceRepo allInstances
    let orchestrator = tck |> spawnOrchestratorActor instanceRepo getMasterPDBRepo orchestratorState
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""
    let state = API.synchronizePrimaryInstanceWith ctx "server2" |> run
    state |> Result.mapError (fun error -> failwith error) |> ignore
    ()

[<Fact>]
let ``Oracle instance actor creates PDB`` () = test <| fun tck ->
    let oracleActor = tck |> spawnOracleInstanceActor (FakeMasterPDBRepo masterPDBMap2) instance2

    let stateBefore : OracleInstanceActor.StateResult = retype oracleActor <? OracleInstanceActor.GetState |> runQuick
    match stateBefore with
    | Ok state -> Assert.Equal(1, state.MasterPDBs.Length)
    | Error error -> failwith error

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

    let stateAfter : OracleInstanceActor.StateResult = retype oracleActor <? OracleInstanceActor.GetState |> runQuick
    match stateAfter with
    | Ok state -> Assert.Equal(2, state.MasterPDBs.Length)
    | Error error -> failwith error
    ()

let pollRequestStatus (ctx:API.APIContext) requestId =
    let rec requestStatus () = async {
        let status : WithRequestId<RequestStatus> = ctx.Orchestrator <? OrchestratorActor.GetRequest requestId |> runQuick
        match snd status with
        | Pending -> 
#if DEBUG
            ctx.Logger.LogInformation("The request {RequestId} is pending, waiting...", requestId)
            do! Async.Sleep 1000
#else
            do! Async.Sleep 10
#endif
            return! requestStatus ()
        | s -> return s
    }
    requestStatus() |> run

let throwIfRequestNotCompletedOk (ctx:API.APIContext) request =
    match request with
    | Invalid errors -> failwith (System.String.Join("; ", errors))
    | Valid requestId ->
        let status = requestId |> pollRequestStatus ctx
        match status with
        | CompletedOk _ -> ()
        | _ -> failwith "operation not completed successfully"

[<Fact>]
let ``API creates PDB`` () = test <| fun tck ->
    let instanceRepo = FakeOracleInstanceRepo allInstances
    let orchestrator = tck |> spawnOrchestratorActor instanceRepo getMasterPDBRepo orchestratorState
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let stateBefore = API.getState ctx |> runQuick
    Assert.Equal(2, stateBefore.OracleInstances.[0].MasterPDBs.Length)

    let request = 
        API.createMasterPDB ctx 
            "me" 
            "test3" 
            @"c:\windows\system.ini" 
            [ "schema1" ] 
            [ "targetschema1", "pass1", "FusionInvest" ] 
            "yeah" 
        |> runQuick
    request |> throwIfRequestNotCompletedOk ctx

    let stateAfter = API.getState ctx |> runQuick
    Assert.Equal(3, stateAfter.OracleInstances.[0].MasterPDBs.Length)

[<Fact>]
let ``Lock master PDB`` () = test <| fun tck ->
    let pdb1 = newMasterPDB "test1" [ consSchema "toto" "toto" "Invest" ] "me" "comment1"
    let longTaskExecutor = tck |> OracleLongTaskExecutor.spawn parameters fakeOracleAPI
    let oracleDiskIntensiveTaskExecutor = tck |> OracleDiskIntensiveActor.spawn parameters fakeOracleAPI
    let masterPDBActor = tck |> spawnMasterPDBActor instance1 longTaskExecutor oracleDiskIntensiveTaskExecutor pdb1
    
    retype masterPDBActor <! MasterPDBActor.PrepareForModification (newRequestId(), 1, "me")

    let preparedMess = expectMsgFilter tck (fun (mess:obj) -> 
        match mess with
        | :? WithRequestId<MasterPDBActor.PrepareForModificationResult> as result -> 
            match snd result with
            | Prepared _ -> true
            | _ -> false
        | _ -> false
    )

    let (_, result) : WithRequestId<MasterPDBActor.EditionDone> = 
        retype masterPDBActor <? MasterPDBActor.Rollback (newRequestId(), "me") |> run

    result |> Result.mapError (fun error -> failwith error) |> ignore

[<Fact>]
let ``OracleInstance locks master PDB`` () = test <| fun tck ->
    let oracleActor = tck |> spawnOracleInstanceActor (FakeMasterPDBRepo masterPDBMap1) instance1

    let (_, result) : WithRequestId<MasterPDBActor.PrepareForModificationResult> = 
        retype oracleActor <? OracleInstanceActor.PrepareMasterPDBForModification (newRequestId(), "test1", 1, "me") |> run

    match result with
    | Prepared _ -> ()
    | PreparationFailure error -> failwith error

[<Fact>]
let ``API edits and rolls back master PDB`` () = test <| fun tck ->
    let instanceRepo = FakeOracleInstanceRepo allInstances
    let orchestrator = tck |> spawnOrchestratorActor instanceRepo getMasterPDBRepo orchestratorState
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.prepareMasterPDBForModification ctx "me" "test1" 1 |> runQuick
    request |> throwIfRequestNotCompletedOk ctx

    let request = API.rollbackMasterPDB ctx "me" "test1" |> runQuick
    request |> throwIfRequestNotCompletedOk ctx

[<Fact>]
let ``API edits and commits master PDB`` () = test <| fun tck ->
    let instanceRepo = FakeOracleInstanceRepo allInstances
    let orchestrator = tck |> spawnOrchestratorActor instanceRepo getMasterPDBRepo orchestratorState
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.prepareMasterPDBForModification ctx "me" "test1" 1 |> runQuick
    request |> throwIfRequestNotCompletedOk ctx

    let request = API.commitMasterPDB ctx "me" "test1" "version 2" |> runQuick
    request |> throwIfRequestNotCompletedOk ctx

    let state = API.getMasterPDBState ctx orchestratorState.PrimaryInstance "test1" |> run
    match state with
    | Ok pdb -> Assert.Equal("version 2", pdb.Versions.[1].Comment)
    | Error error -> failwith error

[<Fact>]
let ``MasterPDB snapshots a version`` () = test <| fun tck ->
    let pdb1 = newMasterPDB "test1" [ consSchema "toto" "toto" "Invest" ] "me" "comment1"
    let longTaskExecutor = tck |> OracleLongTaskExecutor.spawn parameters fakeOracleAPI
    let oracleDiskIntensiveTaskExecutor = tck |> OracleDiskIntensiveActor.spawn parameters fakeOracleAPI
    let masterPDBActor = tck |> spawnMasterPDBActor instance1 longTaskExecutor oracleDiskIntensiveTaskExecutor pdb1
    
    let (_, result):WithRequestId<SnapshotResult> = retype masterPDBActor <? MasterPDBActor.SnapshotVersion (newRequestId(), 1, "snapshot") |> run
    result |> Result.mapError (fun error -> failwith error) |> ignore

[<Fact>]
let ``OracleInstance snapshots PDB`` () = test <| fun tck ->
    let oracleActor = tck |> spawnOracleInstanceActor (FakeMasterPDBRepo masterPDBMap1) instance1

    let (_, result):WithRequestId<SnapshotResult> = retype oracleActor <? OracleInstanceActor.SnapshotMasterPDBVersion (newRequestId(), "test1", 1, "snapshot") |> run
    result |> Result.mapError (fun error -> failwith error) |> ignore

[<Fact>]
let ``API snapshots PDB`` () = test <| fun tck ->
    let instanceRepo = FakeOracleInstanceRepo allInstances
    let orchestrator = tck |> spawnOrchestratorActor instanceRepo getMasterPDBRepo orchestratorState
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.snapshotMasterPDBVersion ctx "me" "server1" "test1" 1 "snapshot" |> runQuick
    request |> throwIfRequestNotCompletedOk ctx

[<Fact>]
let ``API gets no pending changes`` () = test <| fun tck ->
    let instanceRepo = FakeOracleInstanceRepo allInstances
    let orchestrator = tck |> spawnOrchestratorActor instanceRepo getMasterPDBRepo orchestratorState
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""
    API.snapshotMasterPDBVersion ctx "me" "server1" "test1" 1 "snap1" |> runQuick |> ignore
    let pendingChangesMaybe = API.getPendingChanges ctx |> runQuick
    match pendingChangesMaybe with
    | Ok pendingChanges -> Assert.True(pendingChanges.IsNone)
    | Error error -> failwith error

[<Fact>]
let ``API gets pending changes`` () = test <| fun tck ->
    let instanceRepo = FakeOracleInstanceRepo allInstances
    let getMasterPDBRepo (instance:OracleInstance) = 
        match instance.Name with
        | "server1" -> 
            let lockedMasterPDB = consMasterPDB "locked" [] [ Domain.MasterPDBVersion.newPDBVersion "me" "comment" ] (newLockInfo "lockman" |> Some)
            FakeMasterPDBRepo (masterPDBMap1 |> Map.add "test3" lockedMasterPDB)
        | "server2" -> FakeMasterPDBRepo masterPDBMap2
        | name -> failwithf "Oracle instance %s does not exist" name
    let instanceRepo = FakeOracleInstanceRepo ([ "server1", { instance1 with MasterPDBs = "test3" :: instance1.MasterPDBs }] |> Map.ofList)
    let orchestratorState = { OracleInstanceNames = [ "server1" ]; PrimaryInstance = "server1" }
    let orchestrator = tck |> spawnOrchestratorActor instanceRepo getMasterPDBRepo orchestratorState
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""
    // Enqueue a read-only request
    API.snapshotMasterPDBVersion ctx "me" "server1" "test1" 1 "snap1" |> runQuick |> ignore
    // Enqueue a change request
    API.prepareMasterPDBForModification ctx "me" "test2" 1 |> runQuick |> ignore
    // At that point, the requests above should still be pending (100 ms long)
    let pendingChangesMaybe = API.getPendingChanges ctx |> runQuick
    match pendingChangesMaybe with
    | Ok pendingChanges -> 
        let lockedPDBName, lockInfo = pendingChanges.Value.OpenMasterPDBs.Head
        Assert.Equal("locked", lockedPDBName)
        Assert.Equal("lockman", lockInfo.Locker)
        Assert.Equal(1, pendingChanges.Value.Commands.Length)
    | Error error -> failwith error
