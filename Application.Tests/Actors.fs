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
open Application.OrchestratorActor
open Domain.Common.Validation
open Application.Common
open Domain.MasterPDBWorkingCopy
open Domain.MasterPDBVersion
open Application

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
    NumberOfOracleShortTaskExecutors = 10
    NumberOfOracleLongTaskExecutors = 3
    NumberOfOracleDiskIntensiveTaskExecutors = 1
    NumberOfWorkingCopyWorkers = 10
    TemporaryWorkingCopyLifetime = TimeSpan.FromMinutes(1.)
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
        }
        OracleDiskIntensiveTaskExecutorMailbox {
            mailbox-type : ""Application.OracleDiskIntensiveTaskExecutorMailbox, Application""
        }
        "
        Akkling.TestKit.test config, 
        (new Serilog.Extensions.Logging.SerilogLoggerFactory(dispose=true) :> ILoggerFactory)
    else
        let config = ConfigurationFactory.ParseString @"
        OracleDiskIntensiveTaskExecutorMailbox {
            mailbox-type : ""Application.OracleDiskIntensiveTaskExecutorMailbox, Application""
        }
        "
        Akkling.TestKit.test config, 
        (new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory() :> ILoggerFactory)
#else
let quickTimeout = TimeSpan.FromSeconds(1.) |> Some
let test = Akkling.TestKit.test (ConfigurationFactory.ParseString @"akka { actor { ask-timeout = 1s } }")
let (loggerFactory : ILoggerFactory) = new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory() :> ILoggerFactory
#endif

let run cont = runWithinElseTimeoutException parameters.ShortTimeout cont
let runQuick cont = runWithinElseTimeoutException quickTimeout cont

let [<Literal>]cManifestsPath = "/manifests"
let [<Literal>]cMasterDataPath = "/master-data"
let [<Literal>]cWorkingCopiesPath = "/working-copies"
let [<Literal>]cSnapshotSourcesPath = "/snapshot-sources"

let instance1 = 
    consOracleInstance
        [ "test1"; "test2"; "test3"; "GOLDEN" ]
        List.empty
        "server1" "server1.com" None
        "xxx" "xxx"
        "xxx" ""
        "xxx" "" "" ""
        cManifestsPath
        cMasterDataPath
        cWorkingCopiesPath
        cSnapshotSourcesPath
        "" ""
        true

let instance2 = 
    consOracleInstance
        [ "test2"; "test3" ]
        List.empty
        "server2" "server2.com" None
        "xxx" "xxx"
        "xxx" ""
        "xxx" "" "" ""
        cManifestsPath
        cMasterDataPath
        cWorkingCopiesPath
        cSnapshotSourcesPath
        "" ""
        false

type FakeOracleAPI(existingPDBs : Set<string>) = 
    let mutable _existingPDBs = existingPDBs |> Set.map (fun pdb -> pdb.ToUpper())
    let _existingPDBsLock = new Object()
    let mutable _pdbFolderMap : Map<string, string> = Map.empty
    let _pdbFolderMapLock = new Object()

    let addExistingPDB (pdb:string) = lock _existingPDBsLock (fun () -> _existingPDBs <- _existingPDBs |> Set.add (pdb.ToUpper()))
    let removeExistingPDB (pdb:string) = lock _existingPDBsLock (fun () -> _existingPDBs <- _existingPDBs |> Set.remove (pdb.ToUpper()))
    let pdbExists (pdb:string) = lock _existingPDBsLock (fun () -> _existingPDBs |> Set.contains (pdb.ToUpper()))

    let addPDBFolder (pdb:string) folder = lock _pdbFolderMapLock (fun () -> _pdbFolderMap <- _pdbFolderMap |> Map.add (pdb.ToUpper()) folder)
    let removePDBFolder (pdb:string) = lock _pdbFolderMapLock (fun () -> _pdbFolderMap <- _pdbFolderMap |> Map.remove (pdb.ToUpper()))
    let getPDBFolder (pdb:string) = lock _pdbFolderMapLock (fun () -> _pdbFolderMap |> Map.tryFind (pdb.ToUpper()))

    member __.Logger = loggerFactory.CreateLogger("Fake Oracle API")
    member __.AddPDBFolder (pdb:string) folder = addPDBFolder pdb folder

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
            if not (pdbExists name) then
                return sprintf "%s does not exist" name |> exn |> Error
            else
                this.Logger.LogDebug("Deleting PDB {PDB}...", name)
                removeExistingPDB name
                removePDBFolder name
                return Ok name 
        }
        member this.ExportPDB _ name = async { 
            this.Logger.LogDebug("Exporting PDB {PDB}...", name)
            return Ok name 
        }
        member this.ImportPDB _ folder name = async { 
            this.Logger.LogDebug("Importing PDB {PDB}...", name)
            addExistingPDB name
            addPDBFolder name folder
            return Ok name 
        }
        member this.SnapshotPDB sourcePDB folder name = async { 
            this.Logger.LogDebug("Snapshoting PDB {sourcePDB} to {snapshotCopy}...", sourcePDB, name)
            addExistingPDB name
            addPDBFolder name folder
            return Ok name 
        }
        member this.ClonePDB sourcePDB folder name = async { 
            this.Logger.LogDebug("Cloning PDB {sourcePDB} to {destPDB}...", sourcePDB, name)
            addExistingPDB name
            addPDBFolder name folder
            return Ok name 
        }
        member this.PDBHasSnapshots _ = async { 
            return Ok false
        }
        member this.PDBExists name = async { 
            return Ok <| pdbExists name
        }
        member this.PDBSnapshots name = async {
            return Ok []
        }
        member this.GetPDBNamesLike (like:string) = raise (System.NotImplementedException())
        member this.GetPDBFilesFolder name = async { 
            return (getPDBFolder name |> Ok)
        }
 
type FakeOracleInstanceRepo(instance) =
    interface IOracleInstanceRepository with
        member __.Get () = instance
        member __.Put newInstance = upcast FakeOracleInstanceRepo newInstance
        member __.PutWorkingCopiesOnly newInstance = upcast FakeOracleInstanceRepo newInstance

let allInstances = 
    [
        "server1", instance1
        "server2", instance2
    ] |> Map.ofList

let getInstanceRepo (name:string) = 
    FakeOracleInstanceRepo allInstances.[name.ToLower()] :> IOracleInstanceRepository

type FakeMasterPDBRepo(pdb: MasterPDB) =
    interface IMasterPDBRepository with
        member __.Get () = pdb
        member __.Put newPDB = upcast FakeMasterPDBRepo newPDB

let test3 = newMasterPDB "TEST3" [ consSchema "toto" "toto" "Invest" ] "me" "comment" |> addNewVersionToMasterPDB "me" "next comment" |> fst
let masterPDBMap1 =
    let test2 = newMasterPDB "TEST2" [ consSchema "toto" "toto" "Invest" ] "me" "new comment2"
    [ 
        "TEST1", newMasterPDB "TEST1" [ consSchema "toto" "toto" "Invest" ] "me" "comment1"
        "TEST2", test2 |> addNewVersionToMasterPDB "me" "tata" |> fst
        "TEST3", test3
        "GOLDEN", consMasterPDB "GOLDEN" [ consSchema "GOLDEN" "pass" "Invest" ] [ newPDBVersion "me" "comment" ] None false (Some "qaRole") Map.empty
    ] |> Map.ofList

let masterPDBMap2 =
    [ 
        "TEST2", newMasterPDB "TEST2" [ consSchema "toto" "toto" "Invest" ] "me" "comment2"
        "TEST3", test3
    ] |> Map.ofList

let getMasterPDBRepo (instance:OracleInstance) (name:string) = 
    match instance.Name with
    | "server1" -> FakeMasterPDBRepo masterPDBMap1.[name.ToUpper()] :> IMasterPDBRepository
    | "server2" ->  FakeMasterPDBRepo masterPDBMap2.[name.ToUpper()] :> IMasterPDBRepository
    | name -> failwithf "Oracle instance %s does not exist" name

let newMasterPDBRepo _ pdb = FakeMasterPDBRepo pdb :> IMasterPDBRepository

let orchestratorState = {
    OracleInstanceNames = [ "server2"; "server1" ]
    PrimaryInstance = "server1"
}

type FakeOrchestratorRepo(orchestrator) =
    interface IOrchestratorRepository with
        member __.Get () = orchestrator
        member __.Put newOrchestrator = upcast FakeOrchestratorRepo newOrchestrator

let orchestratorRepo = FakeOrchestratorRepo(orchestratorState)

let spawnOrchestratorActor = OrchestratorActor.spawn parameters (fun _ -> FakeOracleAPI(Set.empty)) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
let spawnOracleInstanceActor = OracleInstanceActor.spawn parameters (fun _ -> FakeOracleAPI(Set.empty)) getInstanceRepo getMasterPDBRepo newMasterPDBRepo
let spawnMasterPDBActor = MasterPDBActor.spawn parameters

let me = "me" |> UserRights.consUser []
let notMe = "not_me" |> UserRights.consUser []

[<Fact>]
let ``State transfer`` () = test <| fun tck ->
    let aref1 = spawnOracleInstanceActor tck "server1"
    let aref2 = spawnOracleInstanceActor tck "server2"

    let state : OracleInstanceActor.StateResult = retype aref1 <? OracleInstanceActor.TransferInternalState aref2 |> run
    state |> Result.mapError failwith |> ignore
    ()

[<Fact>]
let ``API synchronizes state`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""
    let ok = API.enterMaintenanceMode ctx |> runQuick
    Assert.True(ok)
    let state = API.synchronizePrimaryInstanceWith ctx "server2" |> run
    state |> Result.mapError failwith |> ignore
    ()

[<Fact>]
let ``API synchronizes version`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""
    let res = API.declareMasterPDBVersionSynchronizedWithPrimary ctx "server2" "TEST2" 2 |> runQuick
    res |> Result.mapError failwith |> ignore
    let state = API.getMasterPDBState ctx "server2" "TEST2" |> runQuick
    state |> Result.map (fun s -> 
        let version = s.Versions |> List.tryFind (fun v -> v.VersionNumber = 2)
        Assert.True(version.IsSome)
        Assert.True(version.Value.Comment = "tata")
    ) |> ignore

[<Fact>]
let ``Oracle instance actor creates PDB`` () = test <| fun tck ->
    let oracleActor = spawnOracleInstanceActor tck "server2"

    let stateBefore : OracleInstanceActor.StateResult = retype oracleActor <? OracleInstanceActor.GetState |> runQuick
    match stateBefore with
    | Ok state -> Assert.Equal(2, state.MasterPDBs.Length)
    | Error error -> failwith error

    let parameters = 
        Application.OracleInstanceActor.newCreateMasterPDBParams
            "newpdb"
            @"c:\windows\system.ini" // always exists
            [ "schema1" ]
            [ "targetschema1", "pass1", "Invest" ]
            me
            "yeah"
    let _, res : WithRequestId<OracleInstanceActor.MasterPDBCreationResult> = 
        retype oracleActor <? OracleInstanceActor.CreateMasterPDB (newRequestId(), parameters) |> run
    match res with
    | OracleInstanceActor.MasterPDBCreated _ -> ()
    | OracleInstanceActor.MasterPDBCreationFailure (_, _, error) -> failwithf "the creation of %s failed : %s" parameters.Name error
    | OracleInstanceActor.InvalidRequest errors -> failwithf "the request is invalid : %A" errors

    let stateAfter : OracleInstanceActor.StateResult = retype oracleActor <? OracleInstanceActor.GetState |> runQuick
    match stateAfter with
    | Ok state -> Assert.Equal(3, state.MasterPDBs.Length)
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

let throwIfRequestInvalid request =
    match request with
    | Invalid errors -> failwith (System.String.Join("; ", errors))
    | Valid _ -> ()

let throwIfRequestValid request =
    match request with
    | Invalid _ -> ()
    | Valid _ -> failwith "This request should be invalid"

let throwIfRequestNotCompletedOk (ctx:API.APIContext) request =
    match request with
    | Invalid errors -> failwith (System.String.Join("; ", errors))
    | Valid requestId ->
        let status = requestId |> pollRequestStatus ctx
        match status with
        | Done (CompletedOk (_, data), _) -> data
        | Done (CompletedWithError error, _) -> failwithf "operation not completed successfully : %s" error
        | _ -> failwith "operation not completed successfully (unknown reason)"

let throwIfRequestNotCompletedWithError (ctx:API.APIContext) request =
    match request with
    | Invalid errors -> failwith (System.String.Join("; ", errors))
    | Valid requestId ->
        let status = requestId |> pollRequestStatus ctx
        match status with
        | Done (CompletedWithError _, _) -> ()
        | _ -> failwith "operation should not complete successfully"

[<Fact>]
let ``API creates PDB`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let stateBefore = API.getInstanceState ctx "primary" |> runQuick
    stateBefore |> Result.mapError failwith |> ignore
    stateBefore |> Result.map (fun instance -> Assert.Equal(4, instance.MasterPDBs.Length)) |> ignore

    let request = 
        let pars = 
            Application.OracleInstanceActor.newCreateMasterPDBParams
                "newpdb" 
                @"c:\windows\system.ini" 
                [ "schema1" ] 
                [ "targetschema1", "pass1", "FusionInvest" ] 
                me 
                "yeah" 
        API.createMasterPDB ctx pars
        |> runQuick
    let _ = request |> throwIfRequestNotCompletedOk ctx

    let stateAfter = API.getInstanceState ctx "primary" |> runQuick
    stateAfter |> Result.mapError failwith |> ignore
    stateAfter |> Result.map (fun instance -> Assert.Equal(5, instance.MasterPDBs.Length)) |> ignore

[<Fact>]
let ``API fails to create a PDB`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = 
        let pars = 
            Application.OracleInstanceActor.newCreateMasterPDBParams
                "test1" 
                @"c:\windows\system.ini" 
                [ "schema1" ] 
                [ "targetschema1", "pass1", "FusionInvest" ] 
                me 
                "yeah" 
        API.createMasterPDB ctx pars |> runQuick
    request |> throwIfRequestNotCompletedWithError ctx

[<Fact>]
let ``Lock master PDB`` () = test <| fun tck ->
    let fakeOracleAPI = FakeOracleAPI(Set.empty)
    let shortTaskExecutor = tck |> OracleShortTaskExecutor.spawn parameters fakeOracleAPI
    let longTaskExecutor = tck |> OracleLongTaskExecutor.spawn parameters fakeOracleAPI
    let oracleDiskIntensiveTaskExecutor = tck |> OracleDiskIntensiveActor.spawn parameters fakeOracleAPI
    let workingCopyFactory = tck |> WorkingCopyFactoryActor.spawn parameters instance1 shortTaskExecutor longTaskExecutor oracleDiskIntensiveTaskExecutor
    let masterPDBActor = tck |> spawnMasterPDBActor instance1 shortTaskExecutor longTaskExecutor oracleDiskIntensiveTaskExecutor workingCopyFactory getMasterPDBRepo "test1"
    
    retype masterPDBActor <! MasterPDBActor.PrepareForModification (newRequestId(), 1, me)

    expectMsgFilter tck (fun (mess:obj) -> 
        match mess with
        | :? WithRequestId<MasterPDBActor.PrepareForModificationResult> as result -> 
            match snd result with
            | MasterPDBActor.Prepared _ -> true
            | _ -> false
        | _ -> false
    ) |> ignore

    let (_, result) : WithRequestId<MasterPDBActor.EditionRolledBack> = 
        retype masterPDBActor <? MasterPDBActor.Rollback (newRequestId(), me) |> run

    result |> Result.mapError failwith |> ignore

[<Fact>]
let ``OracleInstance locks master PDB`` () = test <| fun tck ->
    let oracleActor = spawnOracleInstanceActor tck "server1"

    let (_, result) : WithRequestId<MasterPDBActor.PrepareForModificationResult> = 
        retype oracleActor <? OracleInstanceActor.PrepareMasterPDBForModification (newRequestId(), "TEST1", 1, me) |> run

    match result with
    | MasterPDBActor.Prepared _ -> ()
    | MasterPDBActor.PreparationFailure (_, error) -> failwith error

[<Fact>]
let ``API edits and rolls back master PDB`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.prepareMasterPDBForModification ctx me "test1" 1 |> runQuick
    let _ = request |> throwIfRequestNotCompletedOk ctx

    let editionInfo = API.getMasterPDBEditionInfo ctx "test1" |> runQuick
    editionInfo |> Result.mapError failwith |> ignore
    editionInfo |> Result.map (fun editionInfo ->
        Assert.Equal("TEST1", editionInfo.MasterPDBName)
        Assert.Equal("me", editionInfo.EditionInfo.Editor)
        Assert.NotEmpty(editionInfo.Schemas)
        editionInfo.Schemas |> List.iter (fun schema -> Assert.True(schema.ConnectionString |> Option.isSome))
    ) |> ignore

    let request = API.rollbackMasterPDB ctx me "test1" |> runQuick
    request |> throwIfRequestNotCompletedOk ctx |> ignore

    let editionInfo = API.getMasterPDBEditionInfo ctx "test1" |> runQuick
    editionInfo |> Result.map (fun _ -> failwith "edition info should not be available") |> ignore

[<Fact>]
let ``API edits and commits master PDB`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.prepareMasterPDBForModification ctx me "test1" 1 |> runQuick
    let _ = request |> throwIfRequestNotCompletedOk ctx

    let request = API.commitMasterPDB ctx me "test1" "version 2" |> runQuick
    let _ = request |> throwIfRequestNotCompletedOk ctx

    let editionInfo = API.getMasterPDBEditionInfo ctx "test1" |> runQuick
    editionInfo |> Result.map (fun _ -> failwith "edition info should not be available") |> ignore

    let state = API.getMasterPDBState ctx orchestratorState.PrimaryInstance "test1" |> run
    match state with
    | Ok pdb -> Assert.Equal("version 2", pdb.Versions.[1].Comment)
    | Error error -> failwith error

[<Fact>]
let ``API cannot edit a master PDB if user not granted`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let notQAguy = UserRights.consUser [ "anyRole" ] "notQA"
    let request = API.prepareMasterPDBForModification ctx notQAguy "golden" 1 |> runQuick
    request |> throwIfRequestNotCompletedWithError ctx

[<Fact>]
let ``API can edit a master PDB if user granted`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let QAguy = UserRights.consUser [ UserRights.rolePrefix+"qaRole"; "otherRole" ] "QA"
    let request = API.prepareMasterPDBForModification ctx QAguy "golden" 1 |> runQuick
    request |> throwIfRequestNotCompletedOk ctx |> ignore

[<Fact>]
let ``API deletes a version of master PDB`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let state = API.getMasterPDBState ctx orchestratorState.PrimaryInstance "test2" |> run
    match state with
    | Ok pdb -> Assert.Equal(false, (pdb.Versions |> List.find (fun v -> v.VersionNumber = 2)).Deleted)
    | Error error -> failwith error

    let result = API.deleteMasterPDBVersion ctx "test2" 2 false |> runQuick
    result |> Result.mapError failwith |> ignore
    result |> Result.map (fun invalidInstances -> Assert.False(invalidInstances |> List.isEmpty)) |> ignore

    let state = API.getMasterPDBState ctx orchestratorState.PrimaryInstance "test2" |> run
    match state with
    | Ok pdb -> Assert.Equal(true, (pdb.Versions |> List.find (fun v -> v.VersionNumber = 2)).Deleted)
    | Error error -> failwith error

    let result = API.deleteMasterPDBVersion ctx "test3" 2 false |> runQuick
    result |> Result.mapError failwith |> ignore
    result |> Result.map (fun invalidInstances -> Assert.True(invalidInstances |> List.isEmpty)) |> ignore

    let state = API.getMasterPDBState ctx orchestratorState.PrimaryInstance "test3" |> run
    match state with
    | Ok pdb -> Assert.Equal(true, (pdb.Versions |> List.find (fun v -> v.VersionNumber = 2)).Deleted)
    | Error error -> failwith error

    let state = API.getMasterPDBState ctx "server2" "test3" |> run
    match state with
    | Ok pdb -> Assert.Equal(true, (pdb.Versions |> List.find (fun v -> v.VersionNumber = 2)).Deleted)
    | Error error -> failwith error


[<Fact>]
let ``API creates a snapshot working copy`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.createWorkingCopy ctx me "any" "test2" 2 "workingcopy" true false false |> runQuick
    let data = request |> throwIfRequestNotCompletedOk ctx
    Assert.True(data |> List.contains (PDBName "WORKINGCOPY"))
    Assert.True(data |> List.contains (PDBService "server1.com/WORKINGCOPY"))
    Assert.True(data |> List.contains (OracleInstance "server1"))

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "WORKINGCOPY" && wc.CreatedBy = "me") |> Option.isSome)
    | Error error -> failwith error 

[<Fact>]
let ``API must create the PDB if not exists even if working copy registered`` () = test <| fun tck ->
    let getInstanceRepo _ = FakeOracleInstanceRepo ({ instance1 with WorkingCopies = [ "WORKINGCOPY", newTempWorkingCopy (System.TimeSpan.FromDays 1.) "me" (SpecificVersion 1) "TEST1" true "WORKINGCOPY" ] |> Map.ofList }) :> IOracleInstanceRepository
    let oracleAPI = FakeOracleAPI(Set.empty) :> IOracleAPI
    let orchestrator = tck |> OrchestratorActor.spawn parameters (fun _ -> oracleAPI) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "WORKINGCOPY" true false false |> runQuick
    let data = request |> throwIfRequestNotCompletedOk ctx
    Assert.True(data |> List.contains (PDBName "WORKINGCOPY"))
    Assert.True(data |> List.contains (PDBService "server1.com/WORKINGCOPY"))
    Assert.True(data |> List.contains (OracleInstance "server1"))

    let pdbExists = oracleAPI.PDBExists "WORKINGCOPY" |> Async.RunSynchronously
    pdbExists |> Result.map (fun exists -> Assert.True(exists, "the PDB was not created")) |> ignore
    pdbExists |> Result.mapError raise |> ignore

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "WORKINGCOPY" && wc.CreatedBy = "me") |> Option.isSome)
    | Error error -> failwith error 

[<Fact>]
let ``API cannot delete a version with working copy if not forcing`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.createWorkingCopy ctx me "server1" "test2" 2 "workingcopy" true false false |> runQuick
    request |> throwIfRequestNotCompletedOk ctx |> ignore

    let result = API.deleteMasterPDBVersion ctx "test2" 2 false |> runQuick
    result |> Result.map (fun _ -> failwith "version should not be deletable") |> ignore

[<Fact>]
let ``API can delete a version with working copy if forcing`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.createWorkingCopy ctx me "server1" "test2" 2 "workingcopy" true false false |> runQuick
    request |> throwIfRequestNotCompletedOk ctx |> ignore

    let result = API.deleteMasterPDBVersion ctx "test2" 2 true |> runQuick
    result |> Result.mapError failwith |> ignore

[<Fact>]
let ``API skips creation of a snapshot working copy`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "workingcopy" true false false |> runQuick
    request |> throwIfRequestNotCompletedOk ctx |> ignore
    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    let firstWC = 
        match instanceState with
        | Ok instance -> instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "WORKINGCOPY" && wc.CreatedBy = "me")
        | Error error -> failwith error
    Assert.True(firstWC.IsSome)

    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "workingcopy" true false false |> runQuick
    request |> throwIfRequestNotCompletedOk ctx |> ignore

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    let firstWC' = 
        match instanceState with
        | Ok instance -> instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "WORKINGCOPY" && wc.CreatedBy = "me")
        | Error error -> failwith error
    Assert.Equal(firstWC.Value.MasterPDBName, firstWC'.Value.MasterPDBName)

[<Fact>]
let ``API overwrites an existing working copy`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.createWorkingCopy ctx me "server1" "test2" 1 "workingcopy" true false false |> runQuick
    request |> throwIfRequestNotCompletedOk ctx |> ignore
    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    let firstWC = 
        match instanceState with
        | Ok instance -> instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "WORKINGCOPY" && wc.CreatedBy = "me")
        | Error error -> failwith error
    Assert.True(firstWC.IsSome)

    // overwrite with same version
    let request = API.createWorkingCopy ctx me "server1" "test2" 1 "workingcopy" true false true |> runQuick
    request |> throwIfRequestNotCompletedOk ctx |> ignore

    // overwrite with other version
    let request = API.createWorkingCopy ctx me "server1" "test2" 2 "workingcopy" true false true |> runQuick
    request |> throwIfRequestNotCompletedOk ctx |> ignore

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    let firstWC' = 
        match instanceState with
        | Ok instance -> instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "WORKINGCOPY" && wc.CreatedBy = "me")
        | Error error -> failwith error
    Assert.NotEqual(firstWC.Value, firstWC'.Value)

[<Fact>]
let ``API fails to overwrite an existing temporary working copy`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    // create temp working copy
    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "workingcopy" true false false |> runQuick
    request |> throwIfRequestNotCompletedOk ctx |> ignore

    // returns the existing temp working copy
    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "workingcopy" true false false |> runQuick
    request |> throwIfRequestNotCompletedOk ctx |> ignore

    // cannot overwrite with a durable copy if not forcing
    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "workingcopy" true true false |> runQuick
    request |> throwIfRequestValid

    // cannot overwrite with a temp copy of another version if not forcing
    let request = API.createWorkingCopy ctx me "server1" "test1" 2 "workingcopy" true false false |> runQuick
    request |> throwIfRequestValid

    // cannot overwrite with a temp copy of another master PDB if not forcing
    let request = API.createWorkingCopy ctx me "server1" "test2" 1 "workingcopy" true false false |> runQuick
    request |> throwIfRequestValid

    // cannot overwrite if not same user
    let request = API.createWorkingCopy ctx notMe "server1" "test1" 1 "workingcopy" true false false |> runQuick
    request |> throwIfRequestValid

    // cannot overwrite if not same user, even if forcing
    let request = API.createWorkingCopy ctx notMe "server1" "test1" 1 "workingcopy" true false true |> runQuick
    request |> throwIfRequestValid

    // cannot overwrite with an edition temp copy
    let request = API.createWorkingCopyOfEdition ctx me "test1" "workingcopy" true true |> runQuick
    request |> throwIfRequestNotCompletedWithError ctx

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True((instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "WORKINGCOPY" && wc.CreatedBy = "me")).Value.Lifetime |> Lifetime.isDurable |> not)
    | Error error -> failwith error 

[<Fact>]
let ``API fails to overwrite an existing durable working copy`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    // create durable working copy
    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "workingcopy" true true false |> runQuick
    request |> throwIfRequestNotCompletedOk ctx |> ignore

    // return the existing durable working copy
    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "workingcopy" true true false |> runQuick
    request |> throwIfRequestNotCompletedOk ctx |> ignore

    // cannot overwrite with a temp working copy
    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "workingcopy" true false false |> runQuick
    request |> throwIfRequestValid

    // cannot overwrite with a temp working copy, even if forcing
    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "workingcopy" true false true |> runQuick
    request |> throwIfRequestNotCompletedWithError ctx

    // cannot overwrite with a durable copy of another version if not forcing
    let request = API.createWorkingCopy ctx me "server1" "test1" 2 "workingcopy" true true false |> runQuick
    request |> throwIfRequestValid

    // cannot overwrite with a durable copy of another master PDB if not forcing
    let request = API.createWorkingCopy ctx me "server1" "test2" 1 "workingcopy" true true false |> runQuick
    request |> throwIfRequestValid

    // cannot overwrite if not same user
    let request = API.createWorkingCopy ctx notMe "server1" "test1" 1 "workingcopy" true true false |> runQuick
    request |> throwIfRequestValid

    // cannot overwrite if not same user, even if forcing
    let request = API.createWorkingCopy ctx notMe "server1" "test1" 1 "workingcopy" true true true |> runQuick
    request |> throwIfRequestValid

    // cannot overwrite with an edition durable copy
    let request = API.createWorkingCopyOfEdition ctx me "test1" "workingcopy" true true |> runQuick
    request |> throwIfRequestNotCompletedWithError ctx

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True((instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "WORKINGCOPY" && wc.CreatedBy = "me")).Value.Lifetime |> Lifetime.isDurable)
    | Error error -> failwith error 

[<Fact>]
let ``API creates a clone working copy`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "workingcopy" false false false |> runQuick
    let data = request |> throwIfRequestNotCompletedOk ctx
    Assert.True(data |> List.contains (PDBName "WORKINGCOPY"))
    Assert.True(data |> List.contains (PDBService "server1.com/WORKINGCOPY"))
    Assert.True(data |> List.contains (OracleInstance "server1"))

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "WORKINGCOPY" && wc.CreatedBy = "me") |> Option.isSome)
    | Error error -> failwith error 

[<Fact>]
let ``API fails to create a snapshot working copy`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.createWorkingCopy ctx me "server1" "test1" 10 "workingcopy" true false false |> runQuick
    request |> throwIfRequestValid

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "WORKINGCOPY") |> Option.isNone)
    | Error error -> failwith error 

[<Fact>]
let ``API fails to create a clone working copy`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.createWorkingCopy ctx me "server1" "test1" 10 "workingcopy" false false false |> runQuick
    request |> throwIfRequestValid

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "WORKINGCOPY") |> Option.isNone)
    | Error error -> failwith error 

[<Fact>]
let ``API creates a working copy of edition`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.prepareMasterPDBForModification ctx me "test1" 1 |> runQuick
    let _ = request |> throwIfRequestNotCompletedOk ctx

    let request = API.createWorkingCopyOfEdition ctx me "test1" "workingcopy" false false |> runQuick
    let data = request |> throwIfRequestNotCompletedOk ctx
    Assert.True(data |> List.contains (PDBName "WORKINGCOPY"))
    Assert.True(data |> List.contains (PDBService "server1.com/WORKINGCOPY"))
    Assert.True(data |> List.contains (OracleInstance "server1"))

[<Fact>]
let ``API fails to create a working copy of edition`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    // Test1 not being edited
    let request = API.createWorkingCopyOfEdition ctx me "test1" "workingcopy" false false |> runQuick
    request |> throwIfRequestNotCompletedWithError ctx

    // PDB notexists does not exist
    let request = API.createWorkingCopyOfEdition ctx me "notexists" "workingcopy" false false |> runQuick
    request |> throwIfRequestNotCompletedWithError ctx

[<Fact>]
let ``API gets no pending changes`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""
    API.createWorkingCopy ctx me "server1" "test1" 1 "snap1" true false false |> runQuick |> ignore
    let pendingChangesMaybe = API.getPendingChanges ctx |> runQuick
    match pendingChangesMaybe with
    | Ok pendingChanges -> Assert.True(pendingChanges.IsNone)
    | Error error -> failwith error

[<Fact>]
let ``API gets pending changes`` () = test <| fun tck ->
    let getMasterPDBRepo (instance:OracleInstance) (name:string) = 
        match instance.Name with
        | "server1" -> 
            let lockedMasterPDB = consMasterPDB "locked" [] [ Domain.MasterPDBVersion.newPDBVersion "me" "comment" ] (newEditionInfo "lockman" |> Some) false None Map.empty
            match name with
            | "TEST1" | "TEST2" | "TEST3" | "GOLDEN" -> FakeMasterPDBRepo masterPDBMap1.[name] :> IMasterPDBRepository
            | "LOCKED" -> FakeMasterPDBRepo lockedMasterPDB :> IMasterPDBRepository
            | name -> failwithf "Master PDB %s does not exist on instance %s" name instance.Name
        | name -> failwithf "Oracle instance %s does not exist" name
    let getInstanceRepo _ = FakeOracleInstanceRepo ({ instance1 with MasterPDBs = "LOCKED" :: instance1.MasterPDBs }) :> IOracleInstanceRepository
    let orchestratorRepo = FakeOrchestratorRepo { OracleInstanceNames = [ "server1" ]; PrimaryInstance = "server1" }
    let orchestrator = tck |> OrchestratorActor.spawn parameters (fun _ -> FakeOracleAPI([ "locked"; "locked_EDITION" ] |> Set.ofList)) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""
    // Enqueue a read-only request
    API.createWorkingCopy ctx me "server1" "test1" 1 "snap1" true false false |> runQuick |> ignore
    // Enqueue a change request
    API.prepareMasterPDBForModification ctx me "test2" 2 |> runQuick |> ignore
    // At that point, the requests above should still be pending (100 ms long)
    let pendingChangesMaybe = API.getPendingChanges ctx |> runQuick
    match pendingChangesMaybe with
    | Ok pendingChanges -> 
        let lockedPDBName, lockInfo = pendingChanges.Value.OpenMasterPDBs.Head
        Assert.Equal("LOCKED", lockedPDBName)
        Assert.Equal("lockman", lockInfo.Editor)
        Assert.Equal(1, pendingChanges.Value.Commands.Length)
    | Error error -> failwith error

[<Fact>]
let ``API can delete an existing temp working copy`` () = test <| fun tck ->
    let getInstanceRepo _ = FakeOracleInstanceRepo ({ instance1 with WorkingCopies = [ "TEST1WC", newTempWorkingCopy (System.TimeSpan.FromDays 1.) "me" (SpecificVersion 1) "TEST1" true "TEST1WC" ] |> Map.ofList }) :> IOracleInstanceRepository
    let orchestrator = tck |> OrchestratorActor.spawn parameters (fun _ -> FakeOracleAPI([ "test1wc" ] |> Set.ofList)) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "TEST1WC" && wc.CreatedBy = "me") |> Option.isSome)
    | Error error -> failwith error 

    let request = API.deleteWorkingCopy ctx me "server1" "test1wc" false |> runQuick
    let data = request |> throwIfRequestNotCompletedOk ctx
    Assert.True(data |> List.contains (PDBName "TEST1WC"))

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "TEST1WC") |> Option.isNone)
    | Error error -> failwith error 

[<Fact>]
let ``API cannot delete a working copy of different durability than requested`` () = test <| fun tck ->
    let getInstanceRepo _ = 
        FakeOracleInstanceRepo (
            { instance1 with 
                WorkingCopies = [ 
                    "TEST1WC", newTempWorkingCopy (System.TimeSpan.FromDays 1.) "me" (SpecificVersion 1) "TEST1" true "TEST1WC" 
                    "TEST1WC_durable", newDurableWorkingCopy "me" (SpecificVersion 1) "TEST1" true "TEST1WC_durable"
                ] |> Map.ofList }
        ) :> IOracleInstanceRepository
    let orchestrator = tck |> OrchestratorActor.spawn parameters (fun _ -> FakeOracleAPI([ "test1wc"; "TEST1WC_durable" ] |> Set.ofList)) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.deleteWorkingCopy ctx me "server1" "TEST1WC_durable" false |> runQuick
    request |> throwIfRequestNotCompletedWithError ctx

    let request = API.deleteWorkingCopy ctx me "server1" "TEST1WC" true |> runQuick
    request |> throwIfRequestNotCompletedWithError ctx

[<Fact>]
let ``API can delete an existing durable working copy`` () = test <| fun tck ->
    let getInstanceRepo _ = FakeOracleInstanceRepo ({ instance1 with WorkingCopies = [ "TEST1WC", newDurableWorkingCopy "me" (SpecificVersion 1) "TEST1" true "TEST1WC" ] |> Map.ofList }) :> IOracleInstanceRepository
    let orchestrator = tck |> OrchestratorActor.spawn parameters (fun _ -> FakeOracleAPI([ "test1wc" ] |> Set.ofList)) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "TEST1WC" && wc.CreatedBy = "me") |> Option.isSome)
    | Error error -> failwith error 

    let request = API.deleteWorkingCopy ctx me "server1" "test1wc" true |> runQuick
    let data = request |> throwIfRequestNotCompletedOk ctx
    Assert.True(data |> List.contains (PDBName "TEST1WC"))

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "TEST1WC") |> Option.isNone)
    | Error error -> failwith error 

[<Fact>]
let ``API can delete a temp working copy even if not registered`` () = test <| fun tck ->
    let oracleAPI = FakeOracleAPI([ "test1wc" ] |> Set.ofList)
    oracleAPI.AddPDBFolder "test1wc" (cWorkingCopiesPath |> buildWorkingCopyFolder false)
    let orchestrator = tck |> OrchestratorActor.spawn parameters (fun _ -> oracleAPI) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.deleteWorkingCopy ctx me "server1" "test1wc" false |> runQuick
    let data = request |> throwIfRequestNotCompletedOk ctx
    Assert.True(data |> List.contains (PDBName "TEST1WC"))

[<Fact>]
let ``API cannot delete a durable working copy if not registered`` () = test <| fun tck ->
    let oracleAPI = FakeOracleAPI([ "test1wc" ] |> Set.ofList)
    oracleAPI.AddPDBFolder "test1wc" (cWorkingCopiesPath |> buildWorkingCopyFolder true)
    let orchestrator = tck |> OrchestratorActor.spawn parameters (fun _ -> oracleAPI) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.deleteWorkingCopy ctx me "server1" "test1wc" true |> runQuick
    request |> throwIfRequestNotCompletedWithError ctx

[<Fact>]
let ``API can overwrite a temp working copy that exists but is not registered`` () = test <| fun tck ->
    let oracleAPI = FakeOracleAPI([ "test1wc" ] |> Set.ofList)
    oracleAPI.AddPDBFolder "test1wc" (cWorkingCopiesPath |> buildWorkingCopyFolder false)
    let orchestrator = tck |> OrchestratorActor.spawn parameters (fun _ -> oracleAPI) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "TEST1WC" && wc.CreatedBy = "me") |> Option.isNone)
    | Error error -> failwith error 

    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "TEST1WC" true false false |> runQuick
    let data = request |> throwIfRequestNotCompletedOk ctx
    Assert.True(data |> List.contains (PDBName "TEST1WC"))

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "TEST1WC") |> Option.isSome)
    | Error error -> failwith error 

    // Test also with force
    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "TEST1WC" true false true |> runQuick
    let data = request |> throwIfRequestNotCompletedOk ctx
    Assert.True(data |> List.contains (PDBName "TEST1WC"))

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "TEST1WC") |> Option.isSome)
    | Error error -> failwith error 

[<Fact>]
let ``API cannot create a temp working copy if durable copy exists with same name and is not registered`` () = test <| fun tck ->
    let oracleAPI = FakeOracleAPI([ "test1wc" ] |> Set.ofList)
    oracleAPI.AddPDBFolder "test1wc" (cWorkingCopiesPath |> buildWorkingCopyFolder true)
    let orchestrator = tck |> OrchestratorActor.spawn parameters (fun _ -> oracleAPI) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "TEST1WC" && wc.CreatedBy = "me") |> Option.isNone)
    | Error error -> failwith error 

    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "TEST1WC" true false false |> runQuick
    request |> throwIfRequestNotCompletedWithError ctx

    // Not possible even if forcing
    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "TEST1WC" true false true |> runQuick
    request |> throwIfRequestNotCompletedWithError ctx

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "TEST1WC" && wc.CreatedBy = "me") |> Option.isNone)
    | Error error -> failwith error 

[<Fact>]
let ``API can force creating a durable working copy if temp copy exists with same name and is not registered`` () = test <| fun tck ->
    let oracleAPI = FakeOracleAPI([ "test1wc" ] |> Set.ofList)
    oracleAPI.AddPDBFolder "test1wc" (cWorkingCopiesPath |> buildWorkingCopyFolder false)
    let orchestrator = tck |> OrchestratorActor.spawn parameters (fun _ -> oracleAPI) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True(instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "TEST1WC" && wc.CreatedBy = "me") |> Option.isNone)
    | Error error -> failwith error 

    let request = API.createWorkingCopy ctx me "server1" "test1" 1 "TEST1WC" true true true |> runQuick
    let data = request |> throwIfRequestNotCompletedOk ctx
    Assert.True(data |> List.contains (PDBName "TEST1WC"))

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> Assert.True((instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "TEST1WC" && wc.CreatedBy = "me")).Value.Lifetime |> Lifetime.isDurable)
    | Error error -> failwith error 

[<Fact>]
let ``API extends a temporary working copy`` () = test <| fun tck ->
    let wc = consWorkingCopy (System.DateTime.Parse "01/01/2020") (Temporary (System.DateTime.Parse "02/01/2020")) "me" (SpecificVersion 1) "TEST1" true "TEST1WC"
    let getInstanceRepo _ = FakeOracleInstanceRepo ({ instance1 with WorkingCopies = [ wc.Name, wc ] |> Map.ofList }) :> IOracleInstanceRepository
    let orchestrator = tck |> OrchestratorActor.spawn parameters (fun _ -> FakeOracleAPI([ "test1wc" ] |> Set.ofList)) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let result = API.extendWorkingCopy ctx "server1" "test1wc" |> runQuick
    result |> Result.mapError failwith |> ignore

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> 
        let wc1 = instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "TEST1WC")
        Assert.True(wc1.IsSome)
        match wc1.Value.Lifetime with
        | Temporary lifetime -> Assert.True(lifetime > System.DateTime.UtcNow)
        | _ -> failwith "??"
    | Error error -> 
        failwith error 

[<Fact>]
let ``API extends a durable working copy`` () = test <| fun tck ->
    let wc = consWorkingCopy (System.DateTime.Parse "01/01/2020") Durable "me" (SpecificVersion 1) "TEST1" true "TEST1WC"
    let getInstanceRepo _ = FakeOracleInstanceRepo ({ instance1 with WorkingCopies = [ wc.Name, wc ] |> Map.ofList }) :> IOracleInstanceRepository
    let orchestrator = tck |> OrchestratorActor.spawn parameters (fun _ -> FakeOracleAPI([ "test1wc" ] |> Set.ofList)) getInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let result = API.extendWorkingCopy ctx "server1" "test1wc" |> runQuick
    result |> Result.mapError failwith |> ignore

    let instanceState = "server1" |> API.getInstanceState ctx |> runQuick
    match instanceState with
    | Ok instance -> 
        let wc1 = instance.WorkingCopies |> List.tryFind (fun wc -> wc.Name = "TEST1WC")
        Assert.True(wc1.IsSome)
        match wc1.Value.Lifetime with
        | Durable -> Assert.Equal(System.DateTime.UtcNow.Date, wc1.Value.CreationDate.Date)
        | _ -> failwith "??"
    | Error error -> 
        failwith error 

[<Fact>]
let ``API fails to delete a working copy`` () = test <| fun tck ->
    let orchestrator = tck |> spawnOrchestratorActor
    let ctx = API.consAPIContext tck orchestrator loggerFactory ""

    let request = API.deleteWorkingCopy ctx me "server1" "doesnotexist" false |> runQuick
    request |> throwIfRequestNotCompletedWithError ctx
