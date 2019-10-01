open Infrastructure
open Application.Common
open Microsoft.Extensions.Logging
open Serilog.Extensions.Logging
open Domain.OracleInstance
open Application
open Serilog

[<EntryPoint>]
let main args =
    Serilog.Log.Logger <- LoggerConfiguration().WriteTo.Console().CreateLogger()
    let loggerFactory = new SerilogLoggerFactory(dispose=true) :> ILoggerFactory
    let akkaConfig = 
        Akkling.Configuration.parse @"
    akka { 
        loggers=[""Akka.Logger.Serilog.SerilogLogger, Akka.Logger.Serilog""] 
    }" |> Akkling.Configuration.fallback (Akkling.Configuration.load())

    let rootFolder = System.Environment.CurrentDirectory
    let orchestratorName = "orchestrator"
    let orchestratorPath = sprintf "%s\%s" rootFolder orchestratorName
    let orchestratorRepo = OrchestratorRepository.OrchestratorRepository orchestratorPath :> IOrchestratorRepository
    let oracleInstanceRepo = OracleInstanceRepository.OracleInstanceRepository orchestratorPath
    let getMasterPDBRepo (instance:OracleInstance) = 
        MasterPDBRepository.loadMasterPDBRepository (OracleInstanceRepository.instanceFolder orchestratorPath instance.Name) instance.MasterPDBs
    let getOracleAPI (instance:OracleInstance) = Oracle.OracleAPI(loggerFactory, Oracle.connAsDBAFromInstance instance, Oracle.connAsDBAInFromInstance instance)
    let orchestrator = orchestratorRepo.Get orchestratorName

    let system = Akkling.System.create "pdb-orchestrator-system" akkaConfig
    let orchestratorActor = system |> OrchestratorActor.spawn getOracleAPI oracleInstanceRepo getMasterPDBRepo orchestrator

    System.Console.WriteLine "OK!"
    0
