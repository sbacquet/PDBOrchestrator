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

    let recreateTempTablespaceForUsers users pdb =
        if users |> List.isEmpty then
            AsyncResult.retn pdb
        else
            let userTempFileName = "users_temp.dbf"
            let createEmptyTablespaceSQL = 
                sprintf @"
FOR r IN (select regexp_replace(pathtmp, '/[^/]+$', '', 1, 1) as pathtmp from (select FILE_NAME as pathtmp from dba_temp_files where TABLESPACE_NAME = 'TEMP' and rownum <=1))
LOOP
   EXECUTE IMMEDIATE 'create tablespace EMPTY datafile ''' || r.pathtmp || '/%s'' size 128M AUTOEXTEND ON NEXT 1M';
   EXECUTE IMMEDIATE 'drop tablespace EMPTY';
   EXECUTE IMMEDIATE 'create temporary tablespace USERSTEMP tempfile ''' || r.pathtmp || '/%s'' REUSE AUTOEXTEND ON NEXT 10M';
END LOOP;
"                   userTempFileName userTempFileName
            let alterUsersTempTablespaceSQL =
                users
                |> List.map (sprintf "EXECUTE IMMEDIATE 'alter user %s temporary tablespace USERSTEMP';")
                |> String.concat ""
            let script = 
                sprintf @"
BEGIN
%s
%s
END;
"                   createEmptyTablespaceSQL alterUsersTempTablespaceSQL
            execAsync pdb (connAsDBAIn pdb) script

    let resetTempTablespaceForUsers users pdb =
        if users |> List.isEmpty then
            AsyncResult.retn pdb
        else
            let alterUsersTempTablespaceSQL =
                users
                |> List.map (sprintf "EXECUTE IMMEDIATE 'alter user %s temporary tablespace TEMP';")
                |> String.concat ""
            let script = 
                sprintf @"
BEGIN
%s
EXECUTE IMMEDIATE 'DROP TABLESPACE USERSTEMP INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS';
END;
"                   alterUsersTempTablespaceSQL
            execAsync pdb (connAsDBAIn pdb) script

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

        member __.ExportPDB manifest schemas name = asyncResult {
            let! _ = resetTempTablespaceForUsers schemas name
            let! _ = closeAndExportPDB logger connAsDBA (getManifestPath manifest) name
            return name
        }

        member __.ImportPDB manifest destFolder readWrite schemas name = 
            let script pdb = asyncResult {
                let! _ =
                    if readWrite then recreateTempTablespaceForUsers (schemas |> List.map fst) pdb
                    else AsyncResult.retn pdb
                let! _ = 
                    schemas 
                    |> List.map (fun (user, pass) -> disableUserScheduledJobs instance.Server (instance |> portOrDefault) user pass pdb)
                    |> AsyncResult.sequenceS
                return pdb
            }
            importAndOpen logger connAsDBA connAsDBAIn (getManifestPath manifest) destFolder readWrite (Some script) name

        member __.SnapshotPDB sourcePDB destFolder schemas name =
            let script = recreateTempTablespaceForUsers schemas
            snapshotAndOpenPDB logger connAsDBA sourcePDB destFolder (Some script) name

        member __.ClonePDB sourcePDB destFolder schemas name =
            cloneAndOpenPDB logger connAsDBA sourcePDB destFolder None name

        member __.PDBHasSnapshots name = pdbHasSnapshots connAsDBA name

        member __.PDBExists name = PDBExistsOnServer connAsDBA name

        member __.PDBSnapshots name = pdbSnapshots connAsDBA None None name

        member __.GetPDBNamesLike like = getPDBNamesLike connAsDBA like

        member __.GetPDBFilesFolder name = getPDBFilesFolder connAsDBA name
