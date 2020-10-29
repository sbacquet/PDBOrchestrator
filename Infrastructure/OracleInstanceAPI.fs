module Infrastructure.OracleInstanceAPI

open Infrastructure.Oracle
open Microsoft.Extensions.Logging
open Application.Oracle
open Domain.Common

let portOrDefault (instance:Domain.OracleInstance.OracleInstance) = instance.Port |> Option.defaultValue 1521

let connAsDBAInFromInstance (logger:ILogger) (instance:Domain.OracleInstance.OracleInstance) service =
    let port = instance |> portOrDefault
    Sql.withNewConnection (openConn instance.Server port service instance.DBAUser instance.DBAPassword true)

let connAsDBAFromInstance (logger:ILogger) instance = connAsDBAInFromInstance logger instance instance.Name

type OracleInstanceAPI(loggerFactory : ILoggerFactory, instance : Domain.OracleInstance.OracleInstance) = 

    let logger = loggerFactory.CreateLogger(sprintf "Oracle API for instance %s" instance.Name)
    let connAsDBA = connAsDBAFromInstance logger instance
    let connAsDBAIn = connAsDBAInFromInstance logger instance

    let getManifestPath = sprintf "%s/%s" instance.MasterPDBManifestsPath

    interface IOracleAPI with
        member __.NewPDBFromDump timeout name dumpPath schemas targetSchemas =
            let manifest = Domain.MasterPDBVersion.manifestFile name 1 |> getManifestPath
            createManifestFromDump 
                logger 
                connAsDBA connAsDBAIn 
                timeout 
                instance.UserForImport instance.UserForImportPassword 
                instance.Server instance.UserForFileTransfer instance.UserForFileTransferPassword instance.ServerHostkeySHA256
                "dbadmin" "pass"
                instance.MasterPDBManifestsPath dumpPath 
                schemas targetSchemas 
                instance.OracleDirectoryForDumps 
                instance.OracleDirectoryPathForDumps 
                manifest
                true // tolerant to import errors
                name

        member __.OpenPDB readWrite name = asyncResult {
            let modeString = if readWrite then "READ WRITE" else "READ ONLY"
            let! pdb = getPDBOnServer connAsDBA name
            match pdb with
            | Some pdb ->
                if pdb.OpenMode <> modeString then
                    return!
                        openPDB logger connAsDBA readWrite name
                else
                    return name
            | None ->
                return! Error <| (sprintf "cannot find PDB %s on Oracle server %s" name instance.Name |> exn)
        }

        member __.ClosePDB name = closePDB logger connAsDBA name

        member __.DeletePDB name = deleteSourcePDB logger connAsDBA name

        member __.ExportPDB manifest name = closeAndExportPDB logger connAsDBA (getManifestPath manifest) name

        member __.ImportPDB manifest destFolder readWrite schemas name = 
            let script pdb = asyncResult {
                let! _ =
                    if readWrite then recreateTempTablespaceForUsers connAsDBAIn (schemas |> List.map fst) pdb
                    else AsyncResult.retn pdb
                let! _ = 
                    schemas 
                    |> List.map (fun (user, pass) -> disableUserScheduledJobs instance.Server (instance |> portOrDefault) user pass pdb)
                    |> AsyncResult.sequenceS
                return pdb
            }
            importAndOpen logger connAsDBA connAsDBAIn (getManifestPath manifest) destFolder readWrite (Some script) name

        member __.SnapshotPDB sourcePDB destFolder schemas name =
            let script = recreateTempTablespaceForUsers connAsDBAIn schemas
            snapshotAndOpenPDB logger connAsDBA sourcePDB destFolder (Some script) name

        member __.ClonePDB sourcePDB destFolder schemas name =
            let script = recreateTempTablespaceForUsers connAsDBAIn schemas
            cloneAndOpenPDB logger connAsDBA sourcePDB destFolder (Some script) name

        member __.PDBHasSnapshots name = pdbHasSnapshots connAsDBA name

        member __.PDBExists name = async {
            let! result = PDBExistsOnServer connAsDBA name
            match result with
            | Ok _ -> return result
            | Error error ->
                // Retry 1 time, because seems to fail sometimes for unknown reason
                logger.LogError("PDBExists failed ({0}), retrying 1 more time...", error.Message)
                do! Async.Sleep 5000
                return! PDBExistsOnServer connAsDBA name
        }

        member __.PDBSnapshots name = pdbSnapshots connAsDBA None None name

        member __.GetPDBNamesLike like = getPDBNamesLike connAsDBA like

        member __.GetPDBFilesFolder name = getPDBFilesFolder connAsDBA name
