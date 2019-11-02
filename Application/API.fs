module Application.API

open Akka.Actor
open Microsoft.Extensions.Logging
open Akkling
open Application.OrchestratorActor
open Application.PendingRequest

type APIContext = {
    Orchestrator : IActorRef<Command>
    Logger : ILogger
    System : IActorRefFactory
    Endpoint : string
}

let consAPIContext system orchestratorActor (loggerFactory:ILoggerFactory) endpoint =
    { System = system; Orchestrator = orchestratorActor; Logger = loggerFactory.CreateLogger("API"); Endpoint = endpoint }

let getState (ctx:APIContext) : Async<Application.DTO.Orchestrator.OrchestratorState> =
    ctx.Orchestrator <? GetState

let getInstanceState (ctx:APIContext) instance : Async<Application.OracleInstanceActor.StateResult> =
    ctx.Orchestrator <? GetInstanceState instance

let getMasterPDBState (ctx:APIContext) instance pdb : Async<Application.MasterPDBActor.StateResult> =
    ctx.Orchestrator <? GetMasterPDBState (instance, pdb)

let synchronizePrimaryInstanceWith (ctx:APIContext) instance : Async<Application.OracleInstanceActor.StateResult> =
    retype ctx.Orchestrator <? Synchronize instance

let getRequestStatus (ctx:APIContext) requestId : Async<WithRequestId<RequestStatus>> =
    ctx.Orchestrator <? GetRequest requestId

let createWorkingCopy (ctx:APIContext) user instance masterPDBName versionNumber snapshotName force : Async<RequestValidation> =
    ctx.Orchestrator <? CreateWorkingCopy (user, instance, masterPDBName, versionNumber, snapshotName, force)

let deleteWorkingCopy (ctx:APIContext) user instance masterPDBName versionNumber snapshotName : Async<RequestValidation> =
    ctx.Orchestrator <? DeleteWorkingCopy (user, instance, masterPDBName, versionNumber, snapshotName)

let createMasterPDB (ctx:APIContext) user name dump schemas targetSchemas comment : Async<RequestValidation> =
    let pars : OracleInstanceActor.CreateMasterPDBParams = {
        Name = name
        Dump = dump
        Schemas = schemas
        TargetSchemas = targetSchemas
        User = user
        Date = System.DateTime.Now
        Comment = comment
    }
    ctx.Orchestrator <? OrchestratorActor.CreateMasterPDB (user, pars)

let prepareMasterPDBForModification (ctx:APIContext) user pdb version : Async<RequestValidation> =
    ctx.Orchestrator <? OrchestratorActor.PrepareMasterPDBForModification (user, pdb, version)

let rollbackMasterPDB (ctx:APIContext) user pdb : Async<RequestValidation> =
    ctx.Orchestrator <? OrchestratorActor.RollbackMasterPDB (user, pdb)

let commitMasterPDB (ctx:APIContext) user pdb comment : Async<RequestValidation> =
    ctx.Orchestrator <? OrchestratorActor.CommitMasterPDB (user, pdb, comment)

let getPendingChanges (ctx:APIContext) : Async<Result<Option<PendingChanges>,string>> =
    retype ctx.Orchestrator <? OrchestratorActor.GetPendingChanges

let enterReadOnlyMode (ctx:APIContext) : Async<bool> =
    retype ctx.Orchestrator <? OrchestratorActor.EnterReadOnlyMode

let enterNormalMode (ctx:APIContext) : Async<bool> =
    retype ctx.Orchestrator <? OrchestratorActor.EnterNormalMode

let isReadOnlyMode (ctx:APIContext) : Async<bool> =
    retype ctx.Orchestrator <? OrchestratorActor.IsReadOnlyMode

let collectGarbage (ctx:APIContext) =
    retype ctx.Orchestrator <! OrchestratorActor.CollectGarbage

let switchPrimaryOracleInstanceWith (ctx:APIContext) instance : Async<Result<string,string*string>> =
    retype ctx.Orchestrator <? SetPrimaryOracleInstance instance
