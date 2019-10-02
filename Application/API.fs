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
}

let consAPIContext system orchestratorActor (loggerFactory:ILoggerFactory) =
    { System = system; Orchestrator = orchestratorActor; Logger = loggerFactory.CreateLogger("API")}

let getState (ctx:APIContext) : Async<Application.DTO.Orchestrator.OrchestratorState> =
    ctx.Orchestrator <? GetState

let synchronizePrimaryInstanceWith (ctx:APIContext) instance : Async<Application.OracleInstanceActor.StateSet> =
    ctx.Orchestrator <? Synchronize instance

let getRequestStatus (ctx:APIContext) requestId : Async<WithRequestId<RequestStatus>> =
    ctx.Orchestrator <? GetRequest requestId

let snapshotMasterPDBVersion (ctx:APIContext) user instance masterPDBName versionNumber snapshotName : Async<RequestValidation> =
    ctx.Orchestrator <? SnapshotMasterPDBVersion (user, instance, masterPDBName, versionNumber, snapshotName)

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
