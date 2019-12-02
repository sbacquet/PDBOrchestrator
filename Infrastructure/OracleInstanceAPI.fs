module Infrastructure.OracleInstanceAPI

open Infrastructure.Oracle
open Microsoft.Extensions.Logging
open Application.Oracle

let connAsDBAInFromInstance (logger:ILogger) (instance:Domain.OracleInstance.OracleInstance) service =
    let port = instance.Port |> Option.defaultValue 1521
    Sql.withNewConnection (openConn instance.Server port service instance.DBAUser instance.DBAPassword true)

let connAsDBAFromInstance (logger:ILogger) instance = connAsDBAInFromInstance logger instance instance.Name

type OracleInstanceAPI(loggerFactory : ILoggerFactory, instance : Domain.OracleInstance.OracleInstance) = 

    let logger = loggerFactory.CreateLogger(sprintf "Oracle API for instance %s" instance.Name)
    let connAsDBA = connAsDBAFromInstance logger instance
    let connAsDBAIn = connAsDBAInFromInstance logger instance

    let getManifestPath = sprintf "%s/%s" instance.MasterPDBManifestsPath

    interface IOracleAPI with
        member __.NewPDBFromDump timeout name dumpPath schemas targetSchemas =
            let manifest = Domain.MasterPDB.manifestFile name 1 |> getManifestPath
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

        member __.ClosePDB name =
            closePDB logger connAsDBA name
            |> toOraclePDBResultAsync

        member __.DeletePDB name =
            deleteSourcePDB logger connAsDBA name

        member __.ExportPDB manifest name = 
            closeAndExportPDB logger connAsDBA (getManifestPath manifest) name
            |> toOraclePDBResultAsync

        member __.ImportPDB manifest dest name = 
            importAndOpen logger connAsDBA (getManifestPath manifest) dest name
            |> toOraclePDBResultAsync

        member __.SnapshotPDB from name = 
            snapshotAndOpenPDB logger connAsDBA from instance.WorkingCopyDestPath name
            |> toOraclePDBResultAsync

        member __.PDBHasSnapshots name = 
            pdbHasSnapshots connAsDBA name
            |> toOraclePDBResultAsync

        member __.PDBExists name = 
            PDBExistsOnServer connAsDBA name
            |> toOraclePDBResultAsync

        member __.PDBSnapshots name =
            pdbSnapshots connAsDBA name
            |> toOraclePDBResultAsync

        member __.DeletePDBWithSnapshots (olderThan:System.TimeSpan option) name =
            deletePDBWithSnapshots logger connAsDBA olderThan name
            |> toOraclePDBValidationAsync

        member __.GetPDBNamesLike like = 
            getPDBNamesLike connAsDBA like
            |> toOraclePDBResultAsync

        member __.GetPDBFilesFolder name =
            getPDBFilesFolder connAsDBA name
            |> toOraclePDBResultAsync

        member __.GetOldPDBsFromFolder olderThan folder =
            getOldPDBsHavingFilesFolderStartWith connAsDBA olderThan folder
            |> toOraclePDBResultAsync
