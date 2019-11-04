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
open Microsoft.Extensions.Configuration
open Giraffe

let loggerFactory = new Serilog.Extensions.Logging.SerilogLoggerFactory(dispose=true) :> ILoggerFactory

[<RequireQualifiedAccess>]
module Rest =

    let buildEndpoint dnsName port = sprintf "http://%s:%d" dnsName port

    let webApp (apiCtx:API.APIContext) : HttpFunc -> HttpFunc = 
        choose [
            GET >=> choose [
                routef "/requests/%O" (HttpHandlers.getRequestStatus apiCtx)
                routef "/instances/%s/master-pdbs/%s" (HttpHandlers.getMasterPDB apiCtx)
                routef "/instances/%s" (HttpHandlers.getInstance apiCtx) // works with /instance/primary as well
                route "/instances" >=> HttpHandlers.getAllInstances apiCtx

                // Routes for admins
                route "/pending-changes" >=> HttpHandlers.getPendingChanges apiCtx
                route "/mode" >=> HttpHandlers.getMode apiCtx
            ]
            POST >=> choose [
                // Commit edition
                routef "/instances/primary/master-pdbs/%s/edition" (HttpHandlers.commitMasterPDB apiCtx)

                // Routes for admins
                route "/garbage-collection" >=> HttpHandlers.collectGarbage apiCtx
            ]
            PUT >=> choose [
                // Prepare for edition
                routef "/instances/primary/master-pdbs/%s/edition" (HttpHandlers.prepareMasterPDBForModification apiCtx)
                // Create working copy
                routef "/instances/%s/master-pdbs/%s/%i/working-copies/%s" (HttpHandlers.createWorkingCopy apiCtx)

                // Routes for admins
                route "/mode/maintenance" >=> HttpHandlers.enterReadOnlyMode apiCtx
                route "/mode/normal" >=> HttpHandlers.enterNormalMode apiCtx
                route "/instances/primary" >=> HttpHandlers.switchPrimaryOracleInstanceWith apiCtx
            ]
            DELETE >=> choose [
                // Rollback edition
                routef "/instances/primary/master-pdbs/%s/edition" (HttpHandlers.rollbackMasterPDB apiCtx)
                // Delete working copy
                routef "/instances/%s/master-pdbs/%s/%i/working-copies/%s" (HttpHandlers.deleteWorkingCopy apiCtx)
            ]
            PATCH >=> choose [
                // Declare the given instance synchronized with primary
                routef "/instances/%s" (HttpHandlers.synchronizePrimaryInstanceWith apiCtx)
            ]
            RequestErrors.BAD_REQUEST "Unknown HTTP request"
        ]

    let errorHandler (ex : Exception) (logger : Microsoft.Extensions.Logging.ILogger) =
        logger.LogError(ex, "An unhandled exception has occurred while executing the request.")
        clearResponse >=> setStatusCode 500 >=> text ex.Message

    let configureApp (apiCtx:API.APIContext) (app : IApplicationBuilder) =
        let env = app.ApplicationServices.GetService<IHostingEnvironment>()
        (match env.IsDevelopment() with
        | true  -> app.UseDeveloperExceptionPage()
        | false -> app.UseGiraffeErrorHandler errorHandler)
            //.UseHttpsRedirection()
            //.UseCors(configureCors)
            //.UseAuthentication()
            .UseStaticFiles()
            .UseGiraffe(webApp apiCtx) |> ignore

    let configureServices (services : IServiceCollection) =
        services
            .AddSingleton(typeof<ILoggerFactory>, loggerFactory)
            .AddGiraffe() |> ignore

    let buildConfiguration (args:string[]) =
        let builder = ConfigurationBuilder().AddJsonFile("appsettings.json", optional=true)
        let aspnetcoreEnv = System.Environment.GetEnvironmentVariable "ASPNETCORE_ENVIRONMENT"
        let builder = 
            if (not (System.String.IsNullOrEmpty aspnetcoreEnv)) then
                builder.AddJsonFile(sprintf "appsettings.%s.json" aspnetcoreEnv, optional=true)
            else builder
        builder.
            AddCommandLine(args).
            Build()

[<RequireQualifiedAccess>]
module Config =
    let invalidConfig (errors:string list) =
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
                loggers=[""Akka.Logger.Serilog.SerilogLogger, Akka.Logger.Serilog""] 
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
    let config = Rest.buildConfiguration args
    Serilog.Log.Logger <- 
        LoggerConfiguration().
            ReadFrom.Configuration(config).
#if DEBUG
            MinimumLevel.Is(Events.LogEventLevel.Debug).
#endif
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
    let getOracleAPI (instance:OracleInstance) = Oracle.OracleAPI(loggerFactory, Oracle.connAsDBAFromInstance instance, Oracle.connAsDBAInFromInstance instance)

    use system = Akkling.System.create "sys" Config.akkaConfig
    let orchestratorActor = system |> OrchestratorActor.spawn validApplicationParameters getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
    let port = infrastuctureParameters.Port
    let apiContext = 
        API.consAPIContext 
            system 
            orchestratorActor 
            loggerFactory 
            (Rest.buildEndpoint infrastuctureParameters.DNSName port)

    WebHostBuilder()
        .UseWebRoot("wwwroot")
        .UseConfiguration(config)
        .UseKestrel(fun options -> options.Listen(System.Net.IPAddress.IPv6Any, port))
        .UseIISIntegration()
        .Configure(Action<IApplicationBuilder> (Rest.configureApp apiContext))
        .ConfigureServices(Rest.configureServices)
        .Build()
        .Run()

    System.Console.WriteLine "Exiting..."
    system.Stop(untyped orchestratorActor)
    system.Terminate().Wait()
    0
