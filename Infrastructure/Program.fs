module PDBOrchestrator

open Infrastructure
open Application.Common
open Microsoft.Extensions.Logging
open Serilog.Extensions.Logging
open Domain.OracleInstance
open Application
open Application.OrchestratorActor
open Serilog
open Akka.Actor
open Akkling
open Domain.Common.Validation
open System
open Microsoft.AspNetCore
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Http
open Microsoft.Extensions.Options
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Configuration
open Giraffe

let loggerFactory = new SerilogLoggerFactory(dispose=true) :> ILoggerFactory

[<RequireQualifiedAccess>]
module Rest =
    let buildEndpoint dnsname port =
        let dnsName = 
            if (dnsname = null) then 
                (sprintf "%s.%s" (System.Environment.GetEnvironmentVariable("COMPUTERNAME")) (System.Environment.GetEnvironmentVariable("USERDNSDOMAIN")))
            else
                dnsname
        sprintf "http://%s:%d" dnsName port

    let webApp (apiCtx:API.APIContext) : HttpFunc -> HttpFunc = 
        choose [
            GET >=> choose [
                routef "/request/%O" (HttpHandlers.getRequestStatus apiCtx)
                routef "/instance/%s/masterpdb/%s" (HttpHandlers.getMasterPDB apiCtx)
                routef "/instance/%s" (HttpHandlers.getInstance apiCtx)
                route "/instance" >=> HttpHandlers.getAllInstances apiCtx

                // Routes for admins
                route "/pendingchanges" >=> HttpHandlers.getPendingChanges apiCtx
                route "/read-only-mode" >=> HttpHandlers.isReadOnlyMode apiCtx
            ]
            POST >=> choose [
                // Commit edition
                routef "/instance/primary/masterpdb/%s/edition" (HttpHandlers.commitMasterPDB apiCtx)
                // Snapshot
                routef "/instance/%s/masterpdb/%s/%i/snapshot/%s" (HttpHandlers.snapshot apiCtx)

                // Routes for admins
                route "/garbagecollection" >=> HttpHandlers.collectGarbage apiCtx
            ]
            PUT >=> choose [
                // Prepare for edition
                routef "/instance/primary/masterpdb/%s/edition" (HttpHandlers.prepareMasterPDBForModification apiCtx)
                // Routes for admins
                route "/read-only-mode" >=> HttpHandlers.enterReadOnlyMode apiCtx
            ]
            DELETE >=> choose [
                // Rollback edition
                routef "/instance/primary/masterpdb/%s/edition" (HttpHandlers.rollbackMasterPDB apiCtx)
                // Routes for admins
                route "/read-only-mode" >=> HttpHandlers.exitReadOnlyMode apiCtx
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
            .UseGiraffe(webApp apiCtx) |> ignore

    let configureServices (config : IConfiguration) (services : IServiceCollection) =
        services
            .AddSingleton(typeof<ILoggerFactory>, loggerFactory)
            .AddGiraffe() |> ignore

    let buildConfiguration (args:string[]) =
        ConfigurationBuilder()
            .AddCommandLine(args)
            .Build()

[<EntryPoint>]
let main args =
#if DEBUG
    let logLevel = Events.LogEventLevel.Debug
#else
    let logLevel = Events.LogEventLevel.Information
#endif
    Serilog.Log.Logger <- LoggerConfiguration().WriteTo.Console().MinimumLevel.Is(logLevel).CreateLogger()
    let akkaConfig = 
        Akkling.Configuration.parse @"
    akka { 
        loggers=[""Akka.Logger.Serilog.SerilogLogger, Akka.Logger.Serilog""] 
    }"

    let config = Rest.buildConfiguration args
    let parameters = config |> Configuration.configToGlobalParameters
    let validParameters = 
        match parameters with
        | Invalid errors -> 
            Serilog.Log.Logger.Error("The configuration is invalid : {0}", String.Join("; ", errors))
#if DEBUG
            Console.WriteLine("Press a key to exit...")
            Console.ReadKey() |> ignore
#endif
            exit 1
        | Valid parameters -> parameters

    let rootFolder = config.GetValue("root", System.Environment.CurrentDirectory)
    let orchestratorName = "orchestrator"
    let orchestratorPath = sprintf "%s\%s" rootFolder orchestratorName
    let orchestratorRepo = OrchestratorRepository.OrchestratorRepository orchestratorPath :> IOrchestratorRepository
    let oracleInstanceRepo = OracleInstanceRepository.OracleInstanceRepository orchestratorPath
    let getMasterPDBRepo (instance:OracleInstance) = 
        MasterPDBRepository.loadMasterPDBRepository (OracleInstanceRepository.instanceFolder orchestratorPath instance.Name) instance.MasterPDBs
    let getOracleAPI (instance:OracleInstance) = Oracle.OracleAPI(loggerFactory, Oracle.connAsDBAFromInstance instance, Oracle.connAsDBAInFromInstance instance)
    let orchestrator = orchestratorRepo.Get orchestratorName

    use system = Akkling.System.create "pdb-orchestrator-system" akkaConfig
    let orchestratorActor = system |> OrchestratorActor.spawn validParameters getOracleAPI oracleInstanceRepo getMasterPDBRepo orchestrator
    let port = if config.["port"] = null then 59275 else (Int32.Parse(config.["port"]))
    let apiContext = API.consAPIContext system orchestratorActor loggerFactory (Rest.buildEndpoint config.["dnsname"] port)

    WebHostBuilder()
        .UseConfiguration(config)
        .UseKestrel(fun options -> options.Listen(System.Net.IPAddress.IPv6Any, port))
        .UseIISIntegration()
        .Configure(Action<IApplicationBuilder> (Rest.configureApp apiContext))
        .ConfigureServices(Rest.configureServices config)
        .Build()
        .Run()

    System.Console.WriteLine "Exiting..."
    system.Stop(untyped orchestratorActor)
    system.Terminate().Wait()
    0
