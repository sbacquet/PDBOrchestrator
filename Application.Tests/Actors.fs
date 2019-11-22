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

let parameters : Application.Parameters.Parameters = {
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
                WriteTo.Trace(outputTemplate="{LogSource} {Message}{NewLine}").
                MinimumLevel.Debug().
                CreateLogger()
        let config = ConfigurationFactory.ParseString @"
        akka { 
            loglevel=DEBUG,  loggers=[""Akka.Logger.Serilog.SerilogLogger, Akka.Logger.Serilog""] 
            actor {
                debug {
                    receive = off
                    unhandled = on
                    lifecycle = off
                }
            }
        }"
        Akkling.TestKit.test config, 
        (new Serilog.Extensions.Logging.SerilogLoggerFactory(dispose=true) :> ILoggerFactory)
    else
        Akkling.TestKit.testDefault,
        (new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory() :> ILoggerFactory)
#else
let quickTimeout = TimeSpan.FromSeconds(1.) |> Some
let test = Akkling.TestKit.test (ConfigurationFactory.ParseString @"akka { actor { ask-timeout = 1s } }")
let (loggerFactory : ILoggerFactory) = new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory() :> ILoggerFactory
#endif

let run cont = runWithinElseTimeoutException parameters.ShortTimeout cont
let runQuick cont = runWithinElseTimeoutException quickTimeout cont

let instance1 = 
    consOracleInstance
        [ "test1"; "test2" ]
        "server1" "xxx" None
        "xxx" "xxx"
        "xxx" ""
        "xxx" "" "" ""
        ""
        ""
        ""
        ""
        "" ""
        true

let instance2 = 
    consOracleInstance
        [ "test2" ]
        "server2" "xxx" None
        "xxx" "xxx"
        "xxx" ""
        "xxx" "" "" ""
        ""
        ""
        ""
        ""
        "" ""
        false

type FakeOracleAPI(existingPDBs : Set<string>) = 
    member this.Logger = loggerFactory.CreateLogger("Fake Oracle API")
    interface IOracleAPI with
        member this.NewPDBFromDump _ name _ _ _ = async {
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
        member this.SnapshotPDB _ name = async { 
            this.Logger.LogDebug("Snapshoting PDB {PDB}...", name)
            do! Async.Sleep 100
            return Ok name 
        }
        member this.PDBHasSnapshots _ = async { 
            return Ok false
        }
        member this.PDBExists name = async { 
            return Ok (existingPDBs |> Set.contains name)
        }
        member this.DeletePDBWithSnapshots _ name = async { 
            return Valid false
        }
        member this.PDBSnapshots name = async {
            return Ok []
        }
        member this.GetPDBNamesLike (like:string) = raise (System.NotImplementedException())
        member this.GetPDBFilesFolder name = async { return Ok (Some "fake") }
        member this.GetOldPDBsFromFolder olderThan workingCopyFolder = async { return Ok [] }
 
let fakeOracleAPI = FakeOracleAPI(Set.empty)

type FakeOracleInstanceRepo(instance) =
    interface IOracleInstanceRepository with
        member __.Get () = instance
        member __.Put newInstance = upcast FakeOracleInstanceRepo newInstance

let allInstances = 
    [
        "server1", instance1
        "server2", instance2
    ] |> Map.ofList

let getInstanceRepo name = 
    FakeOracleInstanceRepo allInstances.[name] :> IOracleInstanceRepository

type FakeMasterPDBRepo(pdb: MasterPDB) =
    interface IMasterPDBRepository with
        member __.Get () = pdb
        member __.Put newPDB = upcast FakeMasterPDBRepo newPDB

let masterPDBMap1 =
    [ 
        "test1", (newMasterPDB "test1" [ consSchema "toto" "toto" "Invest" ] "me" "comment1")
        "test2", (newMasterPDB "test2" [ consSchema "toto" "toto" "Invest" ] "me" "new comment2")
    ] |> Map.ofList

let masterPDBMap2 =
    [ 
        "test2", (newMasterPDB "test2" [ consSchema "toto" "toto" "Invest" ] "me" "comment2")
    ] |> Map.ofList

let getMasterPDBRepo (instance:OracleInstance) name = 
    match instance.Name with
    | "server1" -> FakeMasterPDBRepo masterPDBMap1.[name] :> IMasterPDBRepository
    | "server2" ->  FakeMasterPDBRepo masterPDBMap2.[name] :> IMasterPDBRepository
    | name -> failwithf "Oracle instance %s does not exist" name

let newMasterPDBRepo _ pdb = FakeMasterPDBRepo pdb :> IMasterPDBRepository

let orchestratorState = {
    OracleInstanceNames = [ "server1"; "server2" ]
    PrimaryInstance = "server1"
}

type FakeOrchestratorRepo(orchestrator) =
    interface IOrchestratorRepository with
        member __.Get () = orchestrator
        member __.Put newOrchestrator = upcast FakeOrchestratorRepo newOrchestrator

let orchestratorRepo = FakeOrchestratorRepo(orchestratorState)

let spawnOrchestratorActor = OrchestratorActor.spawn parameters (fun _ -> fakeOracleAPI) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
let spawnOracleInstanceActor = OracleInstanceActor.spawn parameters (fun _ -> fakeOracleAPI) getInstanceRepo getMasterPDBRepo newMasterPDBRepo
let spawnMasterPDBActor = MasterPDBActor.spawn parameters fakeOracleAPI

[<Fact>]
let ``State transfer`` () = test <| fun tck ->
    let aref1 = spawnOracleInstanceActor tck "server1"
    let aref2 = spawnOracleInstanceActor tck "server2"

    let state : OracleInstanceActor.StateResult = retype aref1 <? OracleInstanceActor.TransferInternalState aref2 |> run
    state |> Result.mapError (fun error -> failwith error) |> ignore
    ()

[<Fact>]
let ``API synchronizes state`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""
    let ok = API.enterReadOnlyMode ctx |> runQuick
    Assert.True(ok)
    let state = API.synchronizePrimaryInstanceWith ctx "server2" |> run
    state |> Result.mapError (fun error -> failwith error) |> ignore
    ()

[<Fact>]
let ``Oracle instance actor creates PDB`` () = test <| fun tck ->
    let oracleActor = spawnOracleInstanceActor tck "server2"

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
    | OracleInstanceActor.MasterPDBCreationFailure (_, error) -> failwithf "the creation of %s failed : %s" parameters.Name error
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
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let stateBefore = API.getState ctx |> runQuick
    Assert.Equal(2, stateBefore.OracleInstances.[0].MasterPDBs.Length)

    let request = 
        let pars = 
            Application.OracleInstanceActor.newCreateMasterPDBParams
                "test3" 
                @"c:\windows\system.ini" 
                [ "schema1" ] 
                [ "targetschema1", "pass1", "FusionInvest" ] 
                "me" 
                "yeah" 
        API.createMasterPDB ctx pars
        |> runQuick
    request |> throwIfRequestNotCompletedOk ctx

    let stateAfter = API.getState ctx |> runQuick
    Assert.Equal(3, stateAfter.OracleInstances.[0].MasterPDBs.Length)

[<Fact>]
let ``Lock master PDB`` () = test <| fun tck ->
    let longTaskExecutor = tck |> OracleLongTaskExecutor.spawn parameters fakeOracleAPI
    let oracleDiskIntensiveTaskExecutor = tck |> OracleDiskIntensiveActor.spawn parameters fakeOracleAPI
    let masterPDBActor = tck |> spawnMasterPDBActor instance1 longTaskExecutor oracleDiskIntensiveTaskExecutor getMasterPDBRepo "test1"
    
    retype masterPDBActor <! MasterPDBActor.PrepareForModification (newRequestId(), 1, "me")

    expectMsgFilter tck (fun (mess:obj) -> 
        match mess with
        | :? WithRequestId<MasterPDBActor.PrepareForModificationResult> as result -> 
            match snd result with
            | Prepared _ -> true
            | _ -> false
        | _ -> false
    ) |> ignore

    let (_, result) : WithRequestId<MasterPDBActor.EditionRolledBack> = 
        retype masterPDBActor <? MasterPDBActor.Rollback (newRequestId(), "me") |> run

    result |> Result.mapError (fun error -> failwith error) |> ignore

[<Fact>]
let ``OracleInstance locks master PDB`` () = test <| fun tck ->
    let oracleActor = spawnOracleInstanceActor tck "server1"

    let (_, result) : WithRequestId<MasterPDBActor.PrepareForModificationResult> = 
        retype oracleActor <? OracleInstanceActor.PrepareMasterPDBForModification (newRequestId(), "test1", 1, "me") |> run

    match result with
    | Prepared _ -> ()
    | PreparationFailure (_, error) -> failwith error

[<Fact>]
let ``API edits and rolls back master PDB`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.prepareMasterPDBForModification ctx "me" "test1" 1 |> runQuick
    request |> throwIfRequestNotCompletedOk ctx

    let request = API.rollbackMasterPDB ctx "me" "test1" |> runQuick
    request |> throwIfRequestNotCompletedOk ctx

[<Fact>]
let ``API edits and commits master PDB`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
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
let ``MasterPDB creates a working copy`` () = test <| fun tck ->
    let longTaskExecutor = tck |> OracleLongTaskExecutor.spawn parameters fakeOracleAPI
    let oracleDiskIntensiveTaskExecutor = tck |> OracleDiskIntensiveActor.spawn parameters fakeOracleAPI
    let masterPDBActor = tck |> spawnMasterPDBActor instance1 longTaskExecutor oracleDiskIntensiveTaskExecutor getMasterPDBRepo "test1"
    
    let (_, result):WithRequestId<CreateWorkingCopyResult> = retype masterPDBActor <? MasterPDBActor.CreateWorkingCopy (newRequestId(), 1, "workingcopy", false) |> run
    result |> Result.mapError (fun error -> failwith error) |> ignore

[<Fact>]
let ``OracleInstance creates a working copy`` () = test <| fun tck ->
    let oracleActor = spawnOracleInstanceActor tck "server1"

    let (_, result):WithRequestId<CreateWorkingCopyResult> = retype oracleActor <? OracleInstanceActor.CreateWorkingCopy (newRequestId(), "test1", 1, "workingcopy", false) |> run
    result |> Result.mapError (fun error -> failwith error) |> ignore

[<Fact>]
let ``OracleInstance (non snapshot capable) creates a working copy`` () = test <| fun tck ->
    let oracleActor = spawnOracleInstanceActor tck "server2"

    let (_, result):WithRequestId<CreateWorkingCopyResult> = retype oracleActor <? OracleInstanceActor.CreateWorkingCopy (newRequestId(), "test2", 1, "workingcopy", false) |> run
    result |> Result.mapError (fun error -> failwith error) |> ignore

[<Fact>]
let ``API creates a working copy`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.createWorkingCopy ctx "me" "server1" "test1" 1 "workingcopy" false |> runQuick
    request |> throwIfRequestNotCompletedOk ctx

[<Fact>]
let ``API gets no pending changes`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""
    API.createWorkingCopy ctx "me" "server1" "test1" 1 "snap1" false |> runQuick |> ignore
    let pendingChangesMaybe = API.getPendingChanges ctx |> runQuick
    match pendingChangesMaybe with
    | Ok pendingChanges -> Assert.True(pendingChanges.IsNone)
    | Error error -> failwith error

[<Fact>]
let ``API gets pending changes`` () = test <| fun tck ->
    let getMasterPDBRepo (instance:OracleInstance) name = 
        match instance.Name with
        | "server1" -> 
            let lockedMasterPDB = consMasterPDB "locked" [] [ Domain.MasterPDBVersion.newPDBVersion "me" "comment" ] (newEditionInfo "lockman" |> Some) false Map.empty
            match name with
            | "test1" | "test2" -> FakeMasterPDBRepo masterPDBMap1.[name] :> IMasterPDBRepository
            | "locked" -> FakeMasterPDBRepo lockedMasterPDB :> IMasterPDBRepository
            | name -> failwithf "Master PDB %s does not exist on instance %s" name instance.Name
        | name -> failwithf "Oracle instance %s does not exist" name
    let getInstanceRepo _ = FakeOracleInstanceRepo ({ instance1 with MasterPDBs = "locked" :: instance1.MasterPDBs }) :> IOracleInstanceRepository
    let orchestratorRepo = FakeOrchestratorRepo { OracleInstanceNames = [ "server1" ]; PrimaryInstance = "server1" }
    let orchestrator = tck |> OrchestratorActor.spawn parameters (fun _ -> FakeOracleAPI([ "locked"; "locked_EDITION" ] |> Set.ofList)) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""
    // Enqueue a read-only request
    API.createWorkingCopy ctx "me" "server1" "test1" 1 "snap1" false |> runQuick |> ignore
    // Enqueue a change request
    API.prepareMasterPDBForModification ctx "me" "test2" 1 |> runQuick |> ignore
    // At that point, the requests above should still be pending (100 ms long)
    let pendingChangesMaybe = API.getPendingChanges ctx |> runQuick
    match pendingChangesMaybe with
    | Ok pendingChanges -> 
        let lockedPDBName, lockInfo = pendingChanges.Value.OpenMasterPDBs.Head
        Assert.Equal("locked", lockedPDBName)
        Assert.Equal("lockman", lockInfo.Editor)
        Assert.Equal(1, pendingChanges.Value.Commands.Length)
    | Error error -> failwith error
