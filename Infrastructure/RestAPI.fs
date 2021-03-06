﻿module Infrastructure.RestAPI

open Infrastructure
open Microsoft.Extensions.Logging
open Application
open Giraffe
open System

let webApp (apiCtx:API.APIContext) : HttpHandler = 
    HttpHandlers.authenticationNeeded >=> 
    choose [
        GET >=> choose [
            routef "/requests/%O" (HttpHandlers.getRequestStatus apiCtx)
            routef "/instances/%s/master-pdbs/%s/versions/%i/working-copies/%s" (HttpHandlers.getWorkingCopyOfMasterPDBVersion apiCtx)
            routef "/instances/%s/master-pdbs/%s/versions/%i/working-copies" (HttpHandlers.getWorkingCopiesOfMasterPDBVersion apiCtx)
            routef "/instances/%s/master-pdbs/%s/versions/%i" (HttpHandlers.getMasterPDBVersion apiCtx)
            routef "/instances/%s/master-pdbs/%s/versions" (HttpHandlers.getMasterPDBVersions apiCtx)
            routef "/instances/primary/master-pdbs/%s/edition/working-copies/%s" (HttpHandlers.getWorkingCopyOfMasterPDBEdition apiCtx)
            routef "/instances/primary/master-pdbs/%s/edition/working-copies" (HttpHandlers.getWorkingCopiesOfMasterPDBEdition apiCtx)
            routef "/instances/primary/master-pdbs/%s/edition" (HttpHandlers.getMasterPDBEditionInfo apiCtx)
            routef "/instances/%s/master-pdbs/%s/working-copies/%s" (HttpHandlers.getWorkingCopyOfMasterPDB apiCtx)
            routef "/instances/%s/master-pdbs/%s/working-copies" (HttpHandlers.getWorkingCopiesOfMasterPDB apiCtx)
            routef "/instances/%s/master-pdbs/%s" (HttpHandlers.getMasterPDB apiCtx)
            routef "/instances/%s/master-pdbs" (HttpHandlers.getMasterPDBs apiCtx)
            routef "/instances/%s/dump-import-info" (HttpHandlers.getDumpTransferInfo apiCtx)
            routef "/instances/%s/working-copies/%s" (HttpHandlers.getWorkingCopy apiCtx)
            routef "/instances/%s/working-copies" (HttpHandlers.getWorkingCopies apiCtx)
            routef "/instances/%s" (HttpHandlers.getBasicInstance apiCtx) // works with /instances/primary as well
            route "/instances" >=> HttpHandlers.getAllInstances apiCtx

            // Routes for admins
            route "/pending-changes" >=> HttpHandlers.getPendingChanges apiCtx
            route "/pending-commands/count" >=> HttpHandlers.getPendingCommandsCount apiCtx
            route "/pending-commands" >=> HttpHandlers.getPendingCommands apiCtx
            route "/mode" >=> HttpHandlers.getMode apiCtx
        ]
        POST >=> choose [
            // Commit edition
            routef "/instances/primary/master-pdbs/%s/edition" (HttpHandlers.commitMasterPDB apiCtx)
            // New PDB
            route "/instances/primary/master-pdbs" >=> HttpHandlers.createNewPDB apiCtx
        ]
        PUT >=> choose [
            // Create working copy of edition
            routef "/instances/primary/master-pdbs/%s/edition/working-copies/%s" (HttpHandlers.createWorkingCopyOfEdition apiCtx)
            // Prepare for edition
            routef "/instances/primary/master-pdbs/%s/edition" (HttpHandlers.prepareMasterPDBForModification apiCtx)
            // Create working copy on Oracle instance selected automatically
            routef "/instances/auto/master-pdbs/%s/versions/%i/working-copies/%s" (HttpHandlers.createWorkingCopyAuto apiCtx)
            // Create working copy, selecting the Oracle instance (needs a role)
            routef "/instances/%s/master-pdbs/%s/versions/%i/working-copies/%s" (HttpHandlers.createWorkingCopyForcing apiCtx)

            // Routes for admins
            route "/mode/maintenance" >=> HttpHandlers.enterMaintenanceMode apiCtx
            route "/mode/normal" >=> HttpHandlers.enterNormalMode apiCtx
            route "/instances/primary" >=> HttpHandlers.switchPrimaryOracleInstanceWith apiCtx
        ]
        DELETE >=> choose [
            // Delete request
            routef "/requests/%O" (HttpHandlers.deleteRequest apiCtx)
            // Rollback edition
            routef "/instances/primary/master-pdbs/%s/edition" (HttpHandlers.rollbackMasterPDB apiCtx)
            // Delete master PDB version
            routef "/instances/primary/master-pdbs/%s/versions/%i" (HttpHandlers.deleteMasterPDBVersion apiCtx)
            // Delete working copy
            routef "/instances/%s/working-copies/%s" (HttpHandlers.deleteWorkingCopy apiCtx)
            // Collect global garbage
            route "/instances/all/garbage" >=> HttpHandlers.collectGarbage apiCtx
            // Collect garbage of an instance
            routef "/instances/%s/garbage" (HttpHandlers.collectInstanceGarbage apiCtx)
        ]
        PATCH >=> choose [
            // Extend lifetime of working copy
            routef "/instances/%s/working-copies/%s" (HttpHandlers.extendWorkingCopy apiCtx)
            // Declare version as synchronized
            routef "/instances/%s/master-pdbs/%s/versions/%i" (HttpHandlers.declareMasterPDBVersionSynchronizedWithPrimary apiCtx)
            // Switch edition lock
            routef "/instances/primary/master-pdbs/%s" (HttpHandlers.switchLock apiCtx)
            // Declare the given instance synchronized with primary
            routef "/instances/%s" (HttpHandlers.synchronizePrimaryInstanceWith apiCtx)
        ]
        RequestErrors.badRequest (text "Sorry, this URL is not handled by the PDB server.")
    ]

let errorHandler (ex : Exception) (logger : Microsoft.Extensions.Logging.ILogger) =
    logger.LogError(ex, "An unhandled exception has occurred while executing the request.")
    clearResponse >=> setStatusCode 500 >=> text ex.Message
