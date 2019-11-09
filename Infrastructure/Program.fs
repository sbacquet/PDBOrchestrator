module PDBOrchestrator

open Infrastructure
open Application.Common
open Microsoft.Extensions.Logging
open Domain.OracleInstance
open Application
open Serilog
open Akkling
open Domain.Common.Validation
open System
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.DependencyInjection

[<RequireQualifiedAccess>]
module Config =
    let private invalidConfig (errors:string list) =
        Serilog.Log.Logger.Error("The configuration is invalid : {0}", String.Join("; ", errors))
    #if DEBUG
        Console.WriteLine("Press a key to exit...")
        Console.ReadKey() |> ignore
    #endif
        exit 1

    let validateConfig config =
        let infrastuctureParameters = config |> Configuration.configToInfrastuctureParameters
        let validInfrastuctureParameters = 
            match infrastuctureParameters with
            | Invalid errors -> invalidConfig errors
            | Valid parameters -> parameters
        let applicationParameters = config |> Configuration.configToApplicationParameters
        let validApplicationParameters = 
            match applicationParameters with
            | Invalid errors -> invalidConfig errors
            | Valid parameters -> parameters
        validInfrastuctureParameters, validApplicationParameters

    let akkaConfig =
        let config =
    #if DEBUG
            Akkling.Configuration.parse @"
            akka { 
                loglevel=DEBUG
                loggers=[""Akka.Logger.Serilog.SerilogLogger, Akka.Logger.Serilog""] l
                actor {
                    debug {
                        receive = off
                        unhandled = on
                        lifecycle = off
                    }
                }
            }"
    #else
            Akkling.Configuration.parse @"akka { loggers=[""Akka.Logger.Serilog.SerilogLogger, Akka.Logger.Serilog""] }"
    #endif
        config

[<EntryPoint>]
let main args =
    let config = RestAPI.buildConfiguration args
    Serilog.Log.Logger <- 
        LoggerConfiguration().
            ReadFrom.Configuration(config).
#if DEBUG
            MinimumLevel.Is(Events.LogEventLevel.Debug).
#else
            MinimumLevel.Override("Microsoft.AspNetCore", Events.LogEventLevel.Warning).
#endif
            Enrich.FromLogContext().
            CreateLogger()

    let infrastuctureParameters, validApplicationParameters = 
        Config.validateConfig config

    let rootFolder = infrastuctureParameters.Root
    let orchestratorName = "orchestrator"
    let orchestratorPath = sprintf "%s\%s" rootFolder orchestratorName
    let orchestratorRepo = OrchestratorRepository.OrchestratorRepository(orchestratorPath, orchestratorName) :> IOrchestratorRepository
    let getOracleInstanceRepo name = OracleInstanceRepository.OracleInstanceRepository(orchestratorPath, name) :> IOracleInstanceRepository
    let getInstanceFolder = OracleInstanceRepository.instanceFolder orchestratorPath
    let getMasterPDBRepo (instance:OracleInstance) name = MasterPDBRepository.MasterPDBRepository(getInstanceFolder instance.Name, name) :> IMasterPDBRepository
    let newMasterPDBRepo (instance:OracleInstance) pdb = MasterPDBRepository.NewMasterPDBRepository(getInstanceFolder instance.Name, pdb) :> IMasterPDBRepository
    let loggerFactory = new Serilog.Extensions.Logging.SerilogLoggerFactory(dispose=true) :> ILoggerFactory
    let getOracleAPI (instance:OracleInstance) = OracleInstanceAPI.OracleInstanceAPI(loggerFactory, instance)

    use system = Akkling.System.create "sys" Config.akkaConfig
    let orchestratorActor = system |> OrchestratorActor.spawn validApplicationParameters getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let port = infrastuctureParameters.Port
    let endPoint = sprintf "http://%s:%d" infrastuctureParameters.DNSName port // TODO (https)
    let apiContext = 
        API.consAPIContext 
            system 
            orchestratorActor 
            loggerFactory 
            endPoint

    WebHostBuilder()
        .UseWebRoot("wwwroot")
        .UseConfiguration(config)
        .UseKestrel(fun options -> options.Listen(System.Net.IPAddress.IPv6Any, port))
        .UseIISIntegration()
        .Configure(Action<IApplicationBuilder> (RestAPI.configureApp apiContext))
        .ConfigureServices(Action<IServiceCollection> (RestAPI.configureServices loggerFactory))
        .UseSerilog()
        .Build()
        .Run()

    System.Console.WriteLine "Exiting..."
    system.Stop(untyped orchestratorActor)
    system.Terminate().Wait()
    0
