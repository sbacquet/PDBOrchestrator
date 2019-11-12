module Infrastructure.RestAPI

open Infrastructure
open Microsoft.Extensions.Logging
open Application
open Giraffe
open System
open Microsoft.AspNetCore.Builder
open Microsoft.Extensions.Hosting
open Serilog
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Configuration

let private webApp (apiCtx:API.APIContext) : HttpFunc -> HttpFunc = 
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
            // New PDB
            route "/instances/primary/master-pdbs" >=> HttpHandlers.createNewPDB apiCtx

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

let private errorHandler (ex : Exception) (logger : Microsoft.Extensions.Logging.ILogger) =
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
        .UseSerilogRequestLogging()
        .UseGiraffe(webApp apiCtx) |> ignore

let configureServices (loggerFactory : ILoggerFactory) (services : IServiceCollection) =
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

