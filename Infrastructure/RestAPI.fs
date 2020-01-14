module Infrastructure.RestAPI

open Infrastructure
open Microsoft.Extensions.Logging
open Application
open Giraffe
open System

let webApp (apiCtx:API.APIContext) : HttpFunc -> HttpFunc = 
    choose [
        GET >=> choose [
            routef "/requests/%O" (HttpHandlers.getRequestStatus apiCtx)
            routef "/instances/%s/master-pdbs/%s/versions/%i/working-copies/%s" (HttpHandlers.getWorkingCopyForMasterPDBVersion apiCtx)
            routef "/instances/%s/master-pdbs/%s/versions/%i/working-copies" (HttpHandlers.getWorkingCopiesForMasterPDBVersion apiCtx)
            routef "/instances/%s/master-pdbs/%s/versions/%i" (HttpHandlers.getMasterPDBVersion apiCtx)
            routef "/instances/%s/master-pdbs/%s/versions" (HttpHandlers.getMasterPDBVersions apiCtx)
            routef "/instances/primary/master-pdbs/%s/edition/working-copies/%s" (HttpHandlers.getWorkingCopyForMasterPDBEdition apiCtx)
            routef "/instances/primary/master-pdbs/%s/edition/working-copies" (HttpHandlers.getWorkingCopiesForMasterPDBEdition apiCtx)
            routef "/instances/primary/master-pdbs/%s/edition" (HttpHandlers.getMasterPDBEditionInfo apiCtx)
            routef "/instances/%s/master-pdbs/%s/working-copies/%s" (HttpHandlers.getWorkingCopyForMasterPDB apiCtx)
            routef "/instances/%s/master-pdbs/%s/working-copies" (HttpHandlers.getWorkingCopiesForMasterPDB apiCtx)
            routef "/instances/%s/master-pdbs/%s" (HttpHandlers.getMasterPDB apiCtx)
            routef "/instances/%s/master-pdbs" (HttpHandlers.getMasterPDBs apiCtx)
            routef "/instances/%s/dump-import-info" (HttpHandlers.getDumpTransferInfo apiCtx)
            routef "/instances/%s/working-copies/%s" (HttpHandlers.getWorkingCopy apiCtx)
            routef "/instances/%s/working-copies" (HttpHandlers.getWorkingCopies apiCtx)
            routef "/instances/%s" (HttpHandlers.getBasicInstance apiCtx) // works with /instances/primary as well
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
        ]
        PUT >=> choose [
            // Create working copy of edition
            routef "/instances/primary/master-pdbs/%s/edition/working-copies/%s" (HttpHandlers.createWorkingCopyOfEdition apiCtx)
            // Prepare for edition
            routef "/instances/primary/master-pdbs/%s/edition" (HttpHandlers.prepareMasterPDBForModification apiCtx)
            // Create working copy
            routef "/instances/%s/master-pdbs/%s/versions/%i/working-copies/%s" (HttpHandlers.createWorkingCopy apiCtx)

            // Routes for admins
            route "/mode/maintenance" >=> HttpHandlers.enterMaintenanceMode apiCtx
            route "/mode/normal" >=> HttpHandlers.enterNormalMode apiCtx
            route "/instances/primary" >=> HttpHandlers.switchPrimaryOracleInstanceWith apiCtx
        ]
        DELETE >=> choose [
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
            // Declare the given instance synchronized with primary
            routef "/instances/%s" (HttpHandlers.synchronizePrimaryInstanceWith apiCtx)
        ]
        RequestErrors.BAD_REQUEST "Unknown HTTP request"
    ]

let errorHandler (ex : Exception) (logger : Microsoft.Extensions.Logging.ILogger) =
    logger.LogError(ex, "An unhandled exception has occurred while executing the request.")
    clearResponse >=> setStatusCode 500 >=> text ex.Message
