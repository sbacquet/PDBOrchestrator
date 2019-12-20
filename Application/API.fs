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

let createWorkingCopy (ctx:APIContext) user instance masterPDBName versionNumber wcName snapshot durable force : Async<RequestValidation> =
    ctx.Orchestrator <? CreateWorkingCopy (user, instance, masterPDBName, versionNumber, wcName, snapshot, durable, force)

let createWorkingCopyOfEdition (ctx:APIContext) user masterPDBName wcName durable force : Async<RequestValidation> =
    ctx.Orchestrator <? CreateWorkingCopyOfEdition (user, masterPDBName, wcName, durable, force)

let deleteWorkingCopy (ctx:APIContext) user instance masterPDBName versionNumber snapshotName : Async<RequestValidation> =
    ctx.Orchestrator <? DeleteWorkingCopy (user, instance, masterPDBName, versionNumber, snapshotName)

let deleteWorkingCopyOfEdition (ctx:APIContext) user masterPDBName wcName : Async<RequestValidation> =
    ctx.Orchestrator <? DeleteWorkingCopyOfEdition (user, masterPDBName, wcName)

let createMasterPDB (ctx:APIContext) (pars:OracleInstanceActor.CreateMasterPDBParams) : Async<RequestValidation> =
    ctx.Orchestrator <? OrchestratorActor.CreateMasterPDB (pars.User, pars)

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

let getDumpTransferInfo (ctx:APIContext) instance : Async<Result<Application.OracleInstanceActor.DumpTransferInfo, string>> =
    retype ctx.Orchestrator <? GetDumpTransferInfo instance
