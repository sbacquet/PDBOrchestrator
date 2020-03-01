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
open System.Net
open System.Security.Authentication
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Configuration
open Microsoft.AspNetCore.Authentication
open Microsoft.AspNetCore.Authentication.JwtBearer
open Microsoft.IdentityModel.Protocols
open Microsoft.IdentityModel.Protocols.OpenIdConnect
open Microsoft.IdentityModel.Tokens
open Giraffe

[<RequireQualifiedAccess>]
module Config =
    let private invalidConfig (errors:string list) =
        Serilog.Log.Logger.Error("The configuration is invalid : {0}", String.Join("; ", errors))
        Serilog.Log.Logger.Information("Server stopped.")
        Serilog.Log.CloseAndFlush()
        Console.WriteLine("Server stopped.")
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

    let akkaConfig level =
        Akkling.Configuration.parse (sprintf @"
            akka { 
                loglevel=%s
                loggers=[""Akka.Logger.Serilog.SerilogLogger, Akka.Logger.Serilog""] l
                actor {
                    debug {
                        receive = off
                        unhandled = on
                        lifecycle = off
                    }
                }
            }" level)

let configureApp (apiCtx:API.APIContext) (app : IApplicationBuilder) =
    let env = app.ApplicationServices.GetService<IHostingEnvironment>()
    let builder = 
        if env.IsDevelopment() then app.UseDeveloperExceptionPage()
        else app.UseGiraffeErrorHandler RestAPI.errorHandler

    let localizationOptions = RequestLocalizationOptions()
    localizationOptions.DefaultRequestCulture <- Microsoft.AspNetCore.Localization.RequestCulture("en-US")
    let allCultures = System.Globalization.CultureInfo.GetCultures(Globalization.CultureTypes.AllCultures)
    localizationOptions.SupportedCultures <- allCultures
    localizationOptions.SupportedUICultures <- allCultures
    builder
        .UseRequestLocalization(localizationOptions)        
        .UseStaticFiles()
        .UseSerilogRequestLogging()
        .UseAuthentication()
        .UseGiraffe(RestAPI.webApp apiCtx) |> ignore

let configureServices (loggerFactory : ILoggerFactory) openIdConnectUrl domain (services : IServiceCollection) =
    let authenticationOptions (o : AuthenticationOptions) =
        o.DefaultAuthenticateScheme <- JwtBearerDefaults.AuthenticationScheme
        o.DefaultChallengeScheme <- JwtBearerDefaults.AuthenticationScheme
    let jwtBearer = 
        let jwtBearerOptions (options : JwtBearerOptions) =
            let configurationManager = new ConfigurationManager<OpenIdConnectConfiguration>(openIdConnectUrl, new OpenIdConnectConfigurationRetriever())
            let openIdConfig = configurationManager.GetConfigurationAsync(Threading.CancellationToken.None).Result;
            options.IncludeErrorDetails <- true
            options.ConfigurationManager <- configurationManager
            options.RefreshOnIssuerKeyNotFound <- true
            options.SaveToken <- true
            options.TokenValidationParameters <- TokenValidationParameters (
                NameClaimType = "preferred_username",

                ValidateIssuer = true,
                ValidIssuer = openIdConfig.Issuer,
                ValidateIssuerSigningKey = true,

                ValidateAudience = true,
                ValidAudiences = [ "pdb-orchestrator"; "pdb-orchestrator_"+domain ], // TODO: remove 1st

                ValidateLifetime = true,
                RequireSignedTokens = true
            )
        Action<JwtBearerOptions> jwtBearerOptions

    services
        .AddSingleton(typeof<ILoggerFactory>, loggerFactory)
        .AddGiraffe() 
        .AddAuthentication(authenticationOptions).AddJwtBearer(jwtBearer) |> ignore
    |> ignore

let buildConfiguration (args:string[]) =
    let aspnetcoreEnv = System.Environment.GetEnvironmentVariable "ASPNETCORE_ENVIRONMENT"
    let env = if (System.String.IsNullOrEmpty aspnetcoreEnv) then "Production" else aspnetcoreEnv
    let config = 
        ConfigurationBuilder().
            AddJsonFile("appsettings.json", optional=true).
            AddJsonFile(sprintf "appsettings.%s.json" env, optional=true).
            AddJsonFile("customappsettings.json", optional=true).
            AddEnvironmentVariables().
            AddCommandLine(args).
            Build()
    ConfigurationBuilder().
        AddInMemoryCollection(config |> Configuration.mapConfigValuesToKeys [ "{{SERVER_INSTANCE}}", "UniqueName" ]).
        Build()

[<EntryPoint>]
let main args =
    let config = buildConfiguration args
    Log.Logger <- 
        LoggerConfiguration().
            ReadFrom.Configuration(config).
            CreateLogger()

    let infrastuctureParameters, validApplicationParameters = Config.validateConfig config

    let rootFolder = infrastuctureParameters.Root
    let orchestratorName = "orchestrator"
    let orchestratorPath = System.IO.Path.Combine(rootFolder, orchestratorName)
    let ifGit gitParams = if infrastuctureParameters.UseGit then Some gitParams else None

    let gitParamsForOrchestrator : Infrastructure.OrchestratorRepository.GitParams = {
        LogError = fun _ error -> Log.Error("Cannot commit modifications done in orchestrator : Git returned : {ex}", error)
        GetModifyComment = fun _ -> "Modified orchestrator"
    }
    let logOrchestratorSaveFailure (path:string) (ex:exn) = 
        Log.Error("Cannot write modifications done in orchestrator to file {path} : {ex}", path, ex.Message)
    let orchestratorRepo = 
        OrchestratorRepository.OrchestratorRepository(
            logOrchestratorSaveFailure, 
            ifGit gitParamsForOrchestrator, 
            orchestratorPath, 
            orchestratorName) :> IOrchestratorRepository

    let logOracleInstanceSaveFailure (instance:string) (path:string) (ex:exn) = 
        Log.Error("Error while writing modifications done in Oracle instance {instance} to file {path} : {ex}", instance, path, ex.Message)
    let gitParamsForOracleInstance : Infrastructure.OracleInstanceRepository.GitParams = {
        LogError = fun instance error -> Log.Error("Cannot commit modifications done in Oracle instance {instance} : Git returned : {ex}", instance, error)
        GetModifyComment = sprintf "Modified Oracle instance %s"
        GetAddComment = sprintf "Added Oracle instance %s"
    }
    let getOracleInstanceRepo name = 
        OracleInstanceRepository.OracleInstanceRepository(
            validApplicationParameters.TemporaryWorkingCopyLifetime,
            logOracleInstanceSaveFailure, 
            ifGit gitParamsForOracleInstance, 
            orchestratorPath, 
            name, 
            validApplicationParameters.ServerInstanceName) :> IOracleInstanceRepository
    let getInstanceFolder = OracleInstanceRepository.instanceFolder orchestratorPath

    let logMasterPDBSaveFailure (pdb:string) (path:string) (ex:exn) = 
        Log.Error("Cannot write modifications done in master PDB {pdb} to file {path} : {ex}", pdb, path, ex.Message)
    let gitParamsForMasterPDB instance : Infrastructure.MasterPDBRepository.GitParams = {
        LogError = fun pdb error -> Log.Error("Cannot commit modifications done in master PDB {pdb} : Git returned : {ex}", pdb, error)
        GetModifyComment = fun name -> sprintf "Modified master PDB %s on Oracle instance %s" name instance.Name
        GetAddComment = fun name -> sprintf "Added master PDB %s to Oracle instance %s" name instance.Name
    }
    let getMasterPDBRepo (instance:OracleInstance) name = 
        MasterPDBRepository.MasterPDBRepository(
            logMasterPDBSaveFailure,
            ifGit (gitParamsForMasterPDB instance),
            getInstanceFolder instance.Name, 
            name) :> IMasterPDBRepository
    let newMasterPDBRepo (instance:OracleInstance) pdb = 
        MasterPDBRepository.NewMasterPDBRepository(
            logMasterPDBSaveFailure, 
            ifGit (gitParamsForMasterPDB instance),
            getInstanceFolder instance.Name, 
            pdb) :> IMasterPDBRepository

    let loggerFactory = new Serilog.Extensions.Logging.SerilogLoggerFactory(dispose=true) :> ILoggerFactory
    let getOracleAPI (instance:OracleInstance) = OracleInstanceAPI.OracleInstanceAPI(loggerFactory, instance)

    use system = Akkling.System.create "sys" (Config.akkaConfig (config.GetValue("Akka:LogLevel", "INFO")))
    try
        let orchestratorActor = system |> OrchestratorActor.spawn validApplicationParameters getOracleAPI getOracleInstanceRepo getMasterPDBRepo newMasterPDBRepo orchestratorRepo
        system |> OrchestratorWatcher.spawn orchestratorActor |> ignore
        let port = infrastuctureParameters.Port
        let endPoint = sprintf "https://%s:%d" infrastuctureParameters.DNSName port
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
                .UseUrls() // Prevent warning at startup
                .UseKestrel(fun options ->
                    options.Listen(
                        IPAddress.IPv6Any, 
                        port,
                        fun listenOptions -> 
                            listenOptions.UseHttps(
                                infrastuctureParameters.CertificatePath, 
                                "pas1234!",
                                // By default, Tls (1.0) is not used => not compatible enough with some clients' Windows machine
                                fun options -> options.SslProtocols <- SslProtocols.Tls ||| SslProtocols.Tls11 ||| SslProtocols.Tls12
                            ) |> ignore
                    )
                    if (not infrastuctureParameters.EnforceHTTPS) then options.Listen(IPAddress.IPv6Any, port+1) |> ignore
                )
                .Configure(Action<IApplicationBuilder> (configureApp apiContext))
                .ConfigureServices(Action<IServiceCollection> (configureServices loggerFactory infrastuctureParameters.OpenIdConnectUrl infrastuctureParameters.Domain))
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
            Serilog.Log.CloseAndFlush()
            Console.WriteLine("Server stopped.")
            0
        else
            Serilog.Log.CloseAndFlush()
            Console.WriteLine("Server stopped.")
            #if DEBUG
            Console.WriteLine("Press a key to exit...")
            Console.ReadKey() |> ignore
            #endif
            1
    with ex ->
        Serilog.Log.Logger.Fatal("A fatal error occurred : {error}", ex.Message)
        System.Console.WriteLine "Aborting..."
        system.Terminate().Wait()
        Serilog.Log.CloseAndFlush()
        Console.WriteLine("Server stopped.")
        #if DEBUG
        Console.WriteLine("Press a key to exit...")
        Console.ReadKey() |> ignore
        #endif
        1
        