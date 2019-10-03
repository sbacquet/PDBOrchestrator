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
    let webApp : HttpFunc -> HttpFunc = 
        choose [
            subRoute "/api"
                (choose [
                    GET >=> choose [
                        route "/hello" >=> HttpHandlers.handleGetHello
                    ]
                ])
            setStatusCode 404 >=> text "Not found" 
        ]

    let errorHandler (ex : Exception) (logger : Microsoft.Extensions.Logging.ILogger) =
        logger.LogError(ex, "An unhandled exception has occurred while executing the request.")
        clearResponse >=> setStatusCode 500 >=> text ex.Message

    let configureApp (app : IApplicationBuilder) =
        let env = app.ApplicationServices.GetService<IHostingEnvironment>()
        (match env.IsDevelopment() with
        | true  -> app.UseDeveloperExceptionPage()
        | false -> app.UseGiraffeErrorHandler errorHandler)
            //.UseHttpsRedirection()
            //.UseCors(configureCors)
            //.UseAuthentication()
            .UseGiraffe(webApp) |> ignore

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
    Serilog.Log.Logger <- LoggerConfiguration().WriteTo.Console().CreateLogger()
    let akkaConfig = 
        Akkling.Configuration.parse @"
    akka { 
        loggers=[""Akka.Logger.Serilog.SerilogLogger, Akka.Logger.Serilog""] 
    }"

    //let rootFolder = System.Environment.CurrentDirectory
    //let orchestratorName = "orchestrator"
    //let orchestratorPath = sprintf "%s\%s" rootFolder orchestratorName
    //let orchestratorRepo = OrchestratorRepository.OrchestratorRepository orchestratorPath :> IOrchestratorRepository
    //let oracleInstanceRepo = OracleInstanceRepository.OracleInstanceRepository orchestratorPath
    //let getMasterPDBRepo (instance:OracleInstance) = 
    //    MasterPDBRepository.loadMasterPDBRepository (OracleInstanceRepository.instanceFolder orchestratorPath instance.Name) instance.MasterPDBs
    //let getOracleAPI (instance:OracleInstance) = Oracle.OracleAPI(loggerFactory, Oracle.connAsDBAFromInstance instance, Oracle.connAsDBAInFromInstance instance)
    //let orchestrator = orchestratorRepo.Get orchestratorName

    //use system = Akkling.System.create "pdb-orchestrator-system" akkaConfig
    //let orchestratorActor = system |> OrchestratorActor.spawn getOracleAPI oracleInstanceRepo getMasterPDBRepo orchestrator
    //let ctx = API.consAPIContext system orchestratorActor loggerFactory

#if false
    // TEST --------------------------------------------------------
    let state = API.getState ctx
    printfn "State:\n%A" state

    let run cont = runWithinElseTimeoutException 100 cont
    
    let res : RequestValidation = API.snapshotMasterPDBVersion ctx "me" "instance1" "test1" 1 "toto" |> run
    match res with
    | Valid req -> 
        printfn "Request id = %s" (req.ToString())
        System.Threading.Thread.Sleep 5000
        printfn "Request for %s = %A" (req.ToString()) (API.getRequestStatus ctx req |> run)
    | Invalid errors -> printfn "Cannot snapshot : %A" errors

    System.Console.ReadKey() |> ignore
    // TEST --------------------------------------------------------
#endif

    let config = Rest.buildConfiguration args
    WebHostBuilder()
        .UseConfiguration(config)
        .UseKestrel()
        .UseIISIntegration()
        .Configure(Action<IApplicationBuilder> Rest.configureApp)
        .ConfigureServices(Rest.configureServices config)
        .Build()
        .Run()

    System.Console.WriteLine "Exiting..."
    //system.Stop(untyped orchestratorActor)
    //system.Terminate().Wait()
    0
