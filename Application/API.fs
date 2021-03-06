﻿module Application.API

open Akka.Actor
open Microsoft.Extensions.Logging
open Akkling
open Application.OrchestratorActor
open Application.PendingRequest

type APIContext = {
    Orchestrator : IActorRef<Command>
    Logger : ILogger
    System : IActorRefFactory
    Hostname : string
    Port : int
    Endpoint : string
}

let consAPIContext system orchestratorActor (loggerFactory:ILoggerFactory) hostname port = {
    System = system
    Orchestrator = orchestratorActor
    Logger = loggerFactory.CreateLogger("API")
    Hostname = hostname
    Port = port
    Endpoint = sprintf "https://%s:%d" hostname port
}

let getState (ctx:APIContext) : Async<Application.DTO.Orchestrator.OrchestratorDTO> =
    ctx.Orchestrator <? GetState

let getInstanceState (ctx:APIContext) (instance:string) : Async<Application.OracleInstanceActor.StateResult> =
    ctx.Orchestrator <? GetInstanceState (instance.ToLower())

let getInstanceBasicState (ctx:APIContext) (instance:string) : Async<Application.OracleInstanceActor.BasicStateResult> =
    ctx.Orchestrator <? GetInstanceBasicState (instance.ToLower())

let getMasterPDBState (ctx:APIContext) (instance:string) (pdb:string) : Async<Application.MasterPDBActor.StateResult> =
    ctx.Orchestrator <? GetMasterPDBState (instance.ToLower(), pdb.ToUpper())

let getMasterPDBEditionInfo (ctx:APIContext) (pdb:string) : Async<Application.MasterPDBActor.EditionInfoResult> =
    ctx.Orchestrator <? GetMasterPDBEditionInfo (pdb.ToUpper())

let synchronizePrimaryInstanceWith (ctx:APIContext) (instance:string) : Async<Application.OracleInstanceActor.StateResult> =
    retype ctx.Orchestrator <? Synchronize (instance.ToLower())

let getRequestStatus (ctx:APIContext) requestId : Async<WithRequestId<RequestStatus>> =
    ctx.Orchestrator <? GetRequest requestId

let deleteRequest (ctx:APIContext) requestId : unit =
    ctx.Orchestrator <! DeleteRequest requestId

let createWorkingCopy (ctx:APIContext) user (instance:string) (masterPDBName:string) versionNumber (wcName:string) snapshot durable force : Async<RequestValidation> =
    ctx.Orchestrator <? CreateWorkingCopy (user, instance.ToLower(), masterPDBName.ToUpper(), versionNumber, wcName.ToUpper(), snapshot, durable, force)

let createWorkingCopyOfEdition (ctx:APIContext) user (masterPDBName:string) (wcName:string) durable force : Async<RequestValidation> =
    ctx.Orchestrator <? CreateWorkingCopyOfEdition (user, masterPDBName.ToUpper(), wcName.ToUpper(), durable, force)

let deleteWorkingCopy (ctx:APIContext) user (instance:string) (wcName:string) (durable:bool) : Async<RequestValidation> =
    ctx.Orchestrator <? DeleteWorkingCopy (user, instance.ToLower(), wcName.ToUpper(), durable)

let extendWorkingCopy (ctx:APIContext) (instance:string) (wcName:string) : Async<Result<Domain.MasterPDBWorkingCopy.MasterPDBWorkingCopy, string>> =
    ctx.Orchestrator <? ExtendWorkingCopy (instance.ToLower(), wcName.ToUpper())

let createMasterPDB (ctx:APIContext) (pars:OracleInstanceActor.CreateMasterPDBParams) : Async<RequestValidation> =
    ctx.Orchestrator <? OrchestratorActor.CreateMasterPDB (pars.User, pars)

let prepareMasterPDBForModification (ctx:APIContext) user (pdb:string) version : Async<RequestValidation> =
    ctx.Orchestrator <? OrchestratorActor.PrepareMasterPDBForModification (user, pdb.ToUpper(), version)

let rollbackMasterPDB (ctx:APIContext) user (pdb:string) : Async<RequestValidation> =
    ctx.Orchestrator <? OrchestratorActor.RollbackMasterPDB (user, pdb.ToUpper())

let commitMasterPDB (ctx:APIContext) user (pdb:string) comment : Async<RequestValidation> =
    ctx.Orchestrator <? OrchestratorActor.CommitMasterPDB (user, pdb.ToUpper(), comment)

let getPendingChanges (ctx:APIContext) : Async<Result<Option<PendingChanges>,string>> =
    retype ctx.Orchestrator <? OrchestratorActor.GetPendingChanges

let enterMaintenanceMode (ctx:APIContext) : Async<bool> =
    retype ctx.Orchestrator <? OrchestratorActor.EnterMaintenanceMode

let enterNormalMode (ctx:APIContext) : Async<bool> =
    retype ctx.Orchestrator <? OrchestratorActor.EnterNormalMode

let isMaintenanceMode (ctx:APIContext) : Async<bool> =
    retype ctx.Orchestrator <? OrchestratorActor.IsMaintenanceMode

let collectGarbage (ctx:APIContext) =
    retype ctx.Orchestrator <! OrchestratorActor.CollectGarbage

let collectInstanceGarbage (ctx:APIContext) (instance:string) =
    retype ctx.Orchestrator <! OrchestratorActor.CollectInstanceGarbage instance

let switchPrimaryOracleInstanceWith (ctx:APIContext) (instance:string) : Async<Result<string,string*string>> =
    retype ctx.Orchestrator <? SetPrimaryOracleInstance (instance.ToLower())

let getDumpTransferInfo (ctx:APIContext) (instance:string) : Async<Result<Application.OracleInstanceActor.DumpTransferInfo, string>> =
    retype ctx.Orchestrator <? GetDumpTransferInfo (instance.ToLower())

let deleteMasterPDBVersion (ctx:APIContext) (pdb:string) (version:int) (force:bool) : Async<Application.OrchestratorActor.DeleteVersionResult> =
    retype ctx.Orchestrator <? DeleteMasterPDBVersion (pdb.ToUpper(), version, force)

let switchLock (ctx:APIContext) (pdb:string) : Async<Result<bool,string>> =
    retype ctx.Orchestrator <? OrchestratorActor.SwitchLock (pdb.ToUpper())

let getPendingCommands (ctx:APIContext) : Async<Command list> =
    retype ctx.Orchestrator <? OrchestratorActor.GetPendingCommands

let declareMasterPDBVersionSynchronizedWithPrimary (ctx:APIContext) (instance:string) (pdb:string) (version:int) : Async<Result<unit,string>> =
    retype ctx.Orchestrator <? OrchestratorActor.MasterPDBVersionSynchronizedWithPrimary (instance, pdb, version)
