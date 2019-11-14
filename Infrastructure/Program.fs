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
open FSharp.Control.Tasks.V2.ContextInsensitive
open System.Threading.Tasks

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
                        lifecycle = on
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
    let orchestratorPath = System.IO.Path.Combine(rootFolder, orchestratorName)
    let orchestratorRepo = OrchestratorRepository.OrchestratorRepository(orchestratorPath, orchestratorName) :> IOrchestratorRepository
    let getOracleInstanceRepo name = OracleInstanceRepository.OracleInstanceRepository(orchestratorPath, name) :> IOracleInstanceRepository
    let getInstanceFolder = OracleInstanceRepository.instanceFolder orchestratorPath
    let getMasterPDBRepo (instance:OracleInstance) name = MasterPDBRepository.MasterPDBRepository(getInstanceFolder instance.Name, name) :> IMasterPDBRepository
    let newMasterPDBRepo (instance:OracleInstance) pdb = MasterPDBRepository.NewMasterPDBRepository(getInstanceFolder instance.Name, pdb) :> IMasterPDBRepository
    let loggerFactory = new Serilog.Extensions.Logging.SerilogLoggerFactory(dispose=true) :> ILoggerFactory
    let getOracleAPI (instance:OracleInstance) = OracleInstanceAPI.OracleInstanceAPI(loggerFactory, instance)

    use system = Akkling.System.create "sys" Config.akkaConfig
    try
        let orchestratorActor = system |> OrchestratorActor.spawn validApplicationParameters getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
        system |> OrchestratorWatcher.spawn orchestratorActor |> ignore
        let port = infrastuctureParameters.Port
        let endPoint = sprintf "http://%s:%d" infrastuctureParameters.DNSName port // TODO (https)
        let apiContext = 
            API.consAPIContext 
                system 
                orchestratorActor 
                loggerFactory 
                endPoint

        let host =
            WebHostBuilder()
                .UseWebRoot("wwwroot")
                .UseConfiguration(config)
                .UseKestrel(fun options -> options.Listen(System.Net.IPAddress.IPv6Any, port))
                .UseIISIntegration()
                .Configure(Action<IApplicationBuilder> (RestAPI.configureApp apiContext))
                .ConfigureServices(Action<IServiceCollection> (RestAPI.configureServices loggerFactory))
                .UseSerilog()
                .Build()
        // Set up termination of web server on Akka system failure
        let appLifetime = host.Services.GetRequiredService<IApplicationLifetime>()
        Async.Start (async {
            do! system.WhenTerminated |> Async.AwaitTask
            appLifetime.StopApplication()
        })

        host.Run()

        System.Console.WriteLine "Exiting..."
        if not system.WhenTerminated.IsCompleted then
            system.Stop(untyped orchestratorActor)
            system.Terminate().Wait()
            0
        else
            #if DEBUG
            Console.WriteLine("Press a key to exit...")
            Console.ReadKey() |> ignore
            #endif
            1
    with ex ->
        Serilog.Log.Logger.Fatal("A fatal error occurred : {error}", ex.Message)
        System.Console.WriteLine "Aborting..."
        system.Terminate().Wait()
        #if DEBUG
        Console.WriteLine("Press a key to exit...")
        Console.ReadKey() |> ignore
        #endif
        1
        