module Application.API

open Akka.Actor
open Microsoft.Extensions.Logging
open Akkling
open Application.OrchestratorActor
open Application.Common
open Application.PendingRequest

type APIContext = {
    Orchestrator : IActorRef<Command>
    Logger : ILogger
    System : IActorRefFactory
}

let consAPIContext system orchestratorActor (loggerFactory:ILoggerFactory) =
    { System = system; Orchestrator = orchestratorActor; Logger = loggerFactory.CreateLogger("API")}

let run cont = runWithinElseTimeoutException 1000 cont // TODO

let getState (ctx:APIContext) =
    let state:Application.DTO.Orchestrator.OrchestratorState = ctx.Orchestrator <? GetState |> run
    state

let synchronizePrimaryInstanceWith (ctx:APIContext) instance =
    let result:Application.OracleInstanceActor.StateSet = ctx.Orchestrator <? Synchronize instance |> run
    result

let getRequestStatus (ctx:APIContext) requestId =
    let requestStatus:WithRequestId<RequestStatus> = ctx.Orchestrator <? GetRequest requestId |> run
    requestStatus |> snd

let snapshotMasterPDBVersion (ctx:APIContext) user instance masterPDBName versionNumber snapshotName : RequestValidation =
    let result:RequestValidation = ctx.Orchestrator <? SnapshotMasterPDBVersion (user, instance, masterPDBName, versionNumber, snapshotName) |> run
    result

let createMasterPDB (ctx:APIContext) user name dump schemas targetSchemas comment =
    let pars : OracleInstanceActor.CreateMasterPDBParams = {
        Name = name
        Dump = dump
        Schemas = schemas
        TargetSchemas = targetSchemas
        User = user
        Date = System.DateTime.Now
        Comment = comment
    }
    let result:RequestValidation = ctx.Orchestrator <? OrchestratorActor.CreateMasterPDB (user, pars) |> run
    result

let prepareMasterPDBForModification (ctx:APIContext) user pdb version =
    let result:RequestValidation = ctx.Orchestrator <? OrchestratorActor.PrepareMasterPDBForModification (user, pdb, version) |> run
    result

let rollbackMasterPDB (ctx:APIContext) user pdb =
    let result:RequestValidation = ctx.Orchestrator <? OrchestratorActor.RollbackMasterPDB (user, pdb) |> run
    result
