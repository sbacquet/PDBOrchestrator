﻿module Infrastructure.Oracle

open System
open System.Data
open Oracle.ManagedDataAccess.Client
open Compensable
open Application.Oracle
open Microsoft.Extensions.Logging
open Domain.Common
open System.Globalization
open System.Diagnostics
open Infrastructure.RunProcessAsync
open Domain.Common.Validation

type PDBCompensableAction = CompensableAction<string, Oracle.ManagedDataAccess.Client.OracleException>
type PDBCompensableAsyncAction = CompensableAsyncAction<string, Oracle.ManagedDataAccess.Client.OracleException>

let toOraclePDBResult result =
    result |> Result.mapError (fun error -> error :> exn)

let toOraclePDBResultAsync result = async {
    let! r = result
    return toOraclePDBResult r
}

let convertComp (comp:PDBCompensableAsyncAction) : CompensableAsyncAction<string, exn> =
    let (action, compensation) = comp
    let newAction t = async {
        let! x = action t
        return toOraclePDBResult x
    }
    (newAction, compensation)

let openConn host port service user password sysdba = fun () ->
    let connectionString = 
        let sysdbaString = if (sysdba) then "DBA Privilege=SYSDBA" else ""
        sprintf 
            @"Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%d)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=%s)));User Id=%s;Password=%s;%s"
            host port service user password sysdbaString

    let conn = new OracleConnection(connectionString)
    conn.Open()
    conn :> IDbConnection

let connAsDBAInFromInstance (instance:Domain.OracleInstance.OracleInstance) service =
    let port = instance.Port |> Option.defaultValue 1521
    Sql.withNewConnection (openConn instance.Server port service instance.DBAUser instance.DBAPassword true)

let connAsDBAFromInstance instance = connAsDBAInFromInstance instance instance.Name

let exec result conn a = 
    try 
        Sql.execNonQuery conn a [] |> ignore
        Ok result 
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        Error ex

let execAsync result conn a = async {
    try 
        // Buggy : throws ObjectDisposedException
        //let! _ = Sql.asyncExecNonQuery conn a []
        use! reader = Sql.asyncExecReader conn a []
        reader |> List.ofDataReader |> ignore
        return Ok result 
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let P = Sql.Parameter.make
let (=>) a b = Sql.Parameter.make(a, b)

let createPDB (logger:ILogger) connAsDBA adminUserName adminUserPassword dest keepOpen (name:string) = asyncResult {
    logger.LogDebug("Creating PDB {pdb}", name)
    let closeSql = if (keepOpen) then "" else sprintf @"execute immediate 'alter pluggable database %s close immediate';" name
    let! result = 
        sprintf 
            @"
    BEGIN
	    DECLARE
		    shared_mem_alloc_failed EXCEPTION;
		    PRAGMA EXCEPTION_INIT (shared_mem_alloc_failed, -4031);
	    BEGIN
            execute immediate 'CREATE PLUGGABLE DATABASE %s ADMIN USER %s IDENTIFIED BY %s ROLES = (DBA) DEFAULT TABLESPACE USERS NOLOGGING CREATE_FILE_DEST=''%s''';
            execute immediate 'ALTER PLUGGABLE DATABASE %s OPEN READ WRITE';
            %s
	    EXCEPTION
		    WHEN shared_mem_alloc_failed THEN
			    BEGIN
				    execute immediate 'DROP PLUGGABLE DATABASE %s INCLUDING DATAFILES';
				    RAISE;
			    END;
	    END;
    END;
    "
            name adminUserName adminUserPassword dest name closeSql name
        |> execAsync name connAsDBA
    logger.LogDebug("Created PDB {pdb}", name)
    return result
}

let grantPDB (logger:ILogger) connAsDBAIn (name:string) =
    logger.LogDebug("Granting PDB {pdb}", name)
    @"
BEGIN
    execute immediate 'GRANT execute ON sys.dbms_lock TO public';
    execute immediate 'GRANT SCHEDULER_ADMIN TO public';
    execute immediate 'grant execute on CTX_DDL to public';
    execute immediate 'alter profile DEFAULT limit password_life_time UNLIMITED';
END;
"
    |> execAsync name (connAsDBAIn name)

let createDirectory (logger:ILogger) connAsDBAIn dirName dirPath grantUser (name:string) =
    logger.LogDebug("Creating directory {dir} in PDB {pdb}", dirName, name)
    sprintf 
        @"
BEGIN
execute immediate 'CREATE DIRECTORY %s AS ''%s''';
execute immediate 'GRANT READ, WRITE ON DIRECTORY %s TO %s';
END;
"
        dirName dirPath dirName grantUser
    |> execAsync name (connAsDBAIn name)

let closePDB (logger:ILogger) connAsDBA (name:string) = async {
    logger.LogDebug("Closing PDB {pdb}", name)
    let! result = 
        sprintf @"ALTER PLUGGABLE DATABASE %s CLOSE IMMEDIATE" name
        |> execAsync name connAsDBA
    return
        match result with
        | Ok _ -> 
            logger.LogDebug("Closed PDB {pdb}", name)
            result
        | Error ex -> 
            match ex.Number with
            | 65020 -> // already closed -> ignore it
                logger.LogDebug("Closed PDB {pdb}", name)
                Ok name
            | _ -> 
                result
}

// Warning! Does not check existence of snapshots
let deletePDB (logger:ILogger) connAsDBA closeIfOpen (name:string) = asyncResult {
    logger.LogDebug("Deleting PDB {pdb}", name)
    let! _ = if (closeIfOpen) then closePDB logger connAsDBA name else AsyncResult.retn name
    let! result = sprintf @"DROP PLUGGABLE DATABASE %s INCLUDING DATAFILES" name |> execAsync name connAsDBA
    logger.LogDebug("Deleted PDB {pdb}", name)
    return result
}

let createPDBCompensable (logger:ILogger) connAsDBA adminUserName adminUserPassword dest keepOpen = 
    compensableAsync
        (createPDB logger connAsDBA adminUserName adminUserPassword dest keepOpen)
        (deletePDB logger connAsDBA true)

let openPDB (logger:ILogger) connAsDBA readWrite (name:string) = asyncResult {
    logger.LogDebug("Opening PDB {pdb}", name)
    let readMode = if readWrite then "READ WRITE" else "READ ONLY"
    let! result = 
        sprintf @"ALTER PLUGGABLE DATABASE %s OPEN %s FORCE" name readMode
        |> execAsync name connAsDBA
    logger.LogDebug("Opened PDB {pdb}", name)
    return result
}

let openPDBCompensable (logger:ILogger) connAsDBA readWrite = 
    compensableAsync 
        (openPDB logger connAsDBA readWrite) 
        (closePDB logger connAsDBA)

let closePDBCompensable (logger:ILogger) connAsDBA readWrite = 
    compensableAsync 
        (closePDB logger connAsDBA)
        (openPDB logger connAsDBA readWrite) 

let getOracleDirectoryPath connAsDBAIn pdb (dir:string) = async {
    try
        let! path = 
            Sql.asyncExecReader 
                (connAsDBAIn pdb) 
                (sprintf "select directory_path from dba_directories where upper(directory_name)='%s'" (dir.ToUpper()))
                [] 
        let path:string option = path |> Sql.mapFirst (Sql.asScalar)
        return path |> Option.map Ok |> Option.defaultValue (sprintf "Oracle directory %s does not exist in PDB %s" dir pdb |> exn |> Error)
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return sprintf "cannot get Oracle directory %s in PDB %s" dir pdb |> exn |> Error
}

let schemaExists connAsDBAIn (schema:string) (name:string) = async {
    try
        let! result = 
            Sql.asyncExecScalar 
                (connAsDBAIn name)
                (sprintf "select count(*) from dba_users where upper(username)='%s'" (schema.ToUpper()))
                [] 
        return result |> Option.get <> 0M |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let deleteSchema (logger:ILogger) connAsDBAIn (schema:string) name =
    logger.LogDebug("Creating schema {schema}", schema)
    sprintf "drop user %s cascade" schema |> execAsync name (connAsDBAIn name)
    
let createSchema (logger:ILogger) connAsDBAIn (schema:string) (pass:string) deleteExisting (name:string) = asyncResult {
    logger.LogDebug("Creating schema {schema}", schema)
    let! _ = 
        if deleteExisting then
            asyncResult {
                let! exists = schemaExists connAsDBAIn schema name
                return! 
                    if exists then
                        deleteSchema logger connAsDBAIn schema name
                    else
                        AsyncResult.retn name
            }
        else
            AsyncResult.retn name
    return!
        sprintf 
            @"
    BEGIN
    execute immediate 'create user %s identified by %s default tablespace USERS temporary tablespace TEMP';
    execute immediate 'grant CONNECT, CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE DATABASE LINK, ALTER SESSION, CREATE PROCEDURE, CREATE TRIGGER, CREATE TYPE, CREATE SYNONYM, RESOURCE, CREATE INDEXTYPE, CREATE MATERIALIZED VIEW, MANAGE SCHEDULER to %s';
    execute immediate 'alter user %s quota unlimited on USERS';
    END;
    "
            schema pass schema schema
        |> execAsync name (connAsDBAIn name)
}

let createSchemas (logger:ILogger) connAsDBAIn (schemas:(string*string) list) deleteExisting name = async {
    let! result = schemas |> AsyncValidation.traverseS (fun (schema, pass) -> createSchema logger connAsDBAIn schema pass deleteExisting name |> AsyncValidation.ofAsyncResult)
    match result with
    | Valid _ -> return Ok name
    | Invalid errors -> return errors |> List.map (fun ex -> ex.Message) |> String.concat "; " |> exn |> Error
}

let deleteSchemas (logger:ILogger) connAsDBAIn (schemas:string list) name = async {
    let! result = schemas |> AsyncValidation.traverseS (fun schema -> deleteSchema logger connAsDBAIn schema name |> AsyncValidation.ofAsyncResult)
    match result with
    | Valid _ -> return Ok name
    | Invalid errors -> return errors |> List.map (fun ex -> ex.Message) |> String.concat "; " |> exn |> Error
}

let createSchemaCompensable (logger:ILogger) connAsDBAIn (schema:string) (pass:string) deleteExisting =
    compensableAsync
        (createSchema logger connAsDBAIn schema pass deleteExisting)
        (deleteSchema logger connAsDBAIn schema)

let createSchemasCompensable (logger:ILogger) connAsDBAIn (schemas:(string*string) list) deleteExisting =
    compensableAsync
        (createSchemas logger connAsDBAIn schemas deleteExisting)
        (deleteSchemas logger connAsDBAIn (schemas |> List.map fst))

let transferFile fromPath toPath user pass =
    let exe = "pscp.exe"
    let args = [
        "-batch"
        "-hostkey \"d8:6e:e3:4b:bd:f4:c8:f6:ee:76:29:1e:b5:f9:e8:6b\""
        sprintf "-l %s" user
        sprintf "-pw %s" pass
        sprintf "\"%s\"" fromPath
        sprintf "\"%s\"" toPath
    ]
    ()

let importSchemas 
    (logger:ILogger) 
    connAsDBAIn 
    (timeout:TimeSpan option) 
    userForImport userForImportPassword 
    host userForFileTransfer userForFileTransferPassword serverFingerPrint 
    (dumpPath:string) 
    (schemas:string list) (targetSchemas:string list) 
    (directory:string) 
    name 
    = asyncResult {

    let! dumpDest = directory |> getOracleDirectoryPath connAsDBAIn name
    let! _ = FileTransfer.uploadFileAsync timeout dumpPath host (sprintf "%s/" dumpDest) userForFileTransfer userForFileTransferPassword serverFingerPrint
    let dumpFile = System.IO.Path.GetFileNameWithoutExtension(dumpPath).ToUpper()
    let logFile = sprintf "%s_impdp.log" dumpFile
    let args = [ 
        sprintf "'%s/%s@%s/%s'" userForImport userForImportPassword host name
        sprintf "schemas=%s" (schemas |> String.concat ",")
        sprintf "directory=%s" directory
        sprintf "dumpfile=%s.DMP" dumpFile
        sprintf "logfile=%s" logFile
        "transform=\"OID:N\""
        "version=COMPATIBLE"
        "exclude=user" // to ignore ORA-31684 because schema already exists
        targetSchemas 
        |> List.zip schemas
        |> List.map (fun (schema, targetSchema) -> sprintf "%s:%s" schema targetSchema)
        |> String.concat ","
        |> sprintf "remap_schema=%s"
    ]
    logger.LogDebug("Running the following command : impdp {0:l}", args |> String.concat " ")
    let! exitCode = 
        runProcessAsync 
            timeout
            "impdp" 
            args
    return!
        match exitCode with
        | 0 -> Ok name
        | exitCode -> sprintf "error while importing dump %s in PDB %s : exit code %d" dumpPath name exitCode |> exn |> Error
}

let createAndImportSchemas 
    (logger:ILogger) 
    connAsDBAIn 
    (timeout:TimeSpan option)
    userForImport userForImportPassword
    host userForFileTransfer userForFileTransferPassword serverFingerPrint
    (dumpPath:string) 
    (schemas:string list) (targetSchemas:(string * string) list) 
    deleteExisting 
    (directory:string) =
    [
        createSchemasCompensable logger connAsDBAIn targetSchemas deleteExisting
        notCompensableAsync (importSchemas logger connAsDBAIn timeout userForImport userForImportPassword host userForFileTransfer userForFileTransferPassword serverFingerPrint dumpPath schemas (targetSchemas |> List.map fst) directory)
    ] |> composeAsync logger

let createAndGrantPDB (logger:ILogger) connAsDBA connAsDBAIn keepOpen adminUserName adminUserPassword dest = 
    [
        createPDBCompensable logger connAsDBA adminUserName adminUserPassword dest true
        notCompensableAsync (grantPDB logger connAsDBAIn)
        notCompensableAsync (if keepOpen then (fun name -> async { return Ok name }) else closePDB logger connAsDBA)
    ] |> composeAsync logger

let exportPDB (logger:ILogger) connAsDBA manifest (name:string) = 
    logger.LogDebug("Exporting PDB {pdb}", name)
    sprintf 
        @"
BEGIN
    execute immediate 'ALTER PLUGGABLE DATABASE %s UNPLUG INTO ''%s''';
    execute immediate 'DROP PLUGGABLE DATABASE %s KEEP DATAFILES';
END;
    "
        name manifest name
    |> execAsync name connAsDBA

let closeAndExportPDB (logger:ILogger) connAsDBA manifest = 
    [
        closePDBCompensable logger connAsDBA true
        notCompensableAsync (exportPDB logger connAsDBA manifest)
    ] |> composeAsync logger


let createManifestFromDump 
    (logger:ILogger) 
    connAsDBA connAsDBAIn 
    timeout
    userForImport userForImportPassword 
    host userForFileTransfer userForFileTransferPassword serverFingerPrint
    adminUserName adminUserPassword 
    dest (dumpPath:string) 
    (schemas:string list) (targetSchemas:(string * string) list) 
    (directory:string) 
    (directoryPath:string) 
    (manifest:string)
    =
    [
        createPDBCompensable logger connAsDBA adminUserName adminUserPassword dest true |> convertComp
        notCompensableAsync (grantPDB logger connAsDBAIn) |> convertComp
        notCompensableAsync (createDirectory logger connAsDBAIn directory directoryPath userForImport) |> convertComp
        notCompensableAsync 
            (createAndImportSchemas 
                logger 
                connAsDBAIn 
                timeout
                userForImport userForImportPassword 
                host userForFileTransfer userForFileTransferPassword serverFingerPrint 
                dumpPath 
                schemas targetSchemas 
                true 
                directory)
        notCompensableAsync (closePDB logger connAsDBA) |> convertComp
        notCompensableAsync (exportPDB logger connAsDBA manifest) |> convertComp
    ] |> composeAsync logger

let importPDB (logger:ILogger) connAsDBA manifest dest (name:string) =
    logger.LogDebug("Importing PDB {pdb}", name)
    sprintf 
        @"
BEGIN
	DECLARE
		shared_mem_alloc_failed EXCEPTION;
		PRAGMA EXCEPTION_INIT (shared_mem_alloc_failed, -4031);
	BEGIN
		execute immediate 'CREATE PLUGGABLE DATABASE %s AS CLONE USING ''%s'' NOLOGGING CREATE_FILE_DEST=''%s''';
	EXCEPTION
		WHEN shared_mem_alloc_failed THEN
			BEGIN
				execute immediate 'DROP PLUGGABLE DATABASE %s INCLUDING DATAFILES';
				RAISE;
			END;
	END;
END;
"
        name manifest dest name
    |> execAsync name connAsDBA

let importPDBCompensable (logger:ILogger) connAsDBA manifest dest = 
    compensableAsync 
        (importPDB logger connAsDBA manifest dest) 
        (deletePDB logger connAsDBA true)

let importAndOpen (logger:ILogger) connAsDBA manifest dest =
    [
        importPDBCompensable logger connAsDBA manifest dest
        openPDBCompensable logger connAsDBA true
    ] |> composeAsync logger

let snapshotPDB (logger:ILogger) connAsDBA from dest name =
    logger.LogDebug("Snapshoting PDB {pdb} to {snapshot}", from, name)
    sprintf 
        @"
BEGIN
	DECLARE
		shared_mem_alloc_failed EXCEPTION;
		PRAGMA EXCEPTION_INIT (shared_mem_alloc_failed, -4031);
	BEGIN
        execute immediate 'ALTER PLUGGABLE DATABASE %s OPEN READ ONLY FORCE';
		execute immediate 'CREATE PLUGGABLE DATABASE %s FROM %s SNAPSHOT COPY NOLOGGING CREATE_FILE_DEST=''%s''';
	EXCEPTION
		WHEN shared_mem_alloc_failed THEN
			BEGIN
				execute immediate 'DROP PLUGGABLE DATABASE %s INCLUDING DATAFILES';
				RAISE;
			END;
	END;
END;
"
        from name from dest name
    |> execAsync name connAsDBA

let snapshotPDBCompensable (logger:ILogger) connAsDBA manifest dest = 
    compensableAsync 
        (snapshotPDB logger connAsDBA manifest dest) 
        (deletePDB logger connAsDBA true)

let snapshotAndOpenPDB (logger:ILogger) connAsDBA manifest dest =
    [
        snapshotPDBCompensable logger connAsDBA manifest dest
        openPDBCompensable logger connAsDBA true
    ] |> composeAsync logger

type RawOraclePDB = {
    Id: decimal
    Name: string
    OpenMode: string
    Guid: string
    SnapId: decimal option
}

let getPDBsOnServer connAsDBA = async {
    try
        use! result = Sql.asyncExecReader connAsDBA "select con_id as Id, Name, open_mode as OpenMode, rawtohex(guid) as Guid, SNAPSHOT_PARENT_CON_ID as SnapId from v$pdbs" [] 
        return result |> Sql.map (Sql.asRecord<RawOraclePDB> "") |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let getPDBOnServer connAsDBA (name:string) = async {
    try
        use! result = 
            Sql.asyncExecReader 
                connAsDBA 
                (sprintf "select con_id as Id, Name, open_mode as OpenMode, rawtohex(guid) as Guid, SNAPSHOT_PARENT_CON_ID as SnapId from v$pdbs where upper(Name)='%s'" (name.ToUpper()))
                [] 
        return result |> Sql.mapFirst (Sql.asRecord<RawOraclePDB> "") |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let getPDBsWithFilesFolder connAsDBA filter = async {
    let filter = filter |> Option.map (fun f -> sprintf " and (%s)" f) |> Option.defaultValue ""
    try
        use! result = 
            Sql.asyncExecReader 
                connAsDBA 
                (sprintf "select p.name, regexp_replace(f.file_name, '(^.+)/[[:alnum:]_]+\.[[:alnum:]_]+$', '\1') as folder from (select con_id, file_name, row_number() over (partition by con_id order by file_name) as rownumber from cdb_data_files) f, v$pdbs p where f.rownumber=1 and p.con_id=f.con_id%s" filter)
                [] 
        let pdbs:(string*string) list = result |> Sql.map (Sql.asTuple2) |> Seq.toList
        return Ok pdbs
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let getPDBsHavingFilesFolderStartWith connAsDBA folder filter = async {
    let filter = filter |> Option.map (fun f -> sprintf " and (%s)" f) |> Option.defaultValue ""
    try
        use! result = 
            Sql.asyncExecReader 
                connAsDBA 
                (sprintf "select distinct p.name from cdb_data_files f, v$pdbs p where p.con_id=f.con_id and f.file_name like '%s/%%'%s" folder filter)
                [] 
        let pdbs:string list = result |> Sql.map (Sql.asScalar) |> Seq.toList
        return Ok pdbs
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let getPDBFilesFolder connAsDBA (name:string) = async {
    let! pdbs = getPDBsWithFilesFolder connAsDBA (sprintf "upper(p.name)='%s'" (name.ToUpper()) |> Some)
    return pdbs |> Result.map (fun pdbs -> pdbs |> List.tryHead |> Option.map snd)
}

let getPDBNamesLike connAsDBA (like:string) = async {
    try
        let! result =
            Sql.asyncExecReader 
                connAsDBA 
                (sprintf "select name from v$pdbs where upper(name) like '%s'" (like.ToUpper()))
                [] 
        let names = result |> Sql.map (fun dr -> (string)dr?name.Value) |> Seq.toList
        return Ok names
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let getPDBOnServerLike connAsDBA (like:string) = async {
    try
        use! result = 
            Sql.asyncExecReader 
                connAsDBA 
                (sprintf "select con_id as Id, Name, open_mode as OpenMode, rawtohex(guid) as Guid, SNAPSHOT_PARENT_CON_ID as SnapId from v$pdbs where upper(Name) like '%s'" (like.ToUpper()))
                [] 
        return result |> Sql.map (Sql.asRecord<RawOraclePDB> "") |> Seq.toList |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let PDBExistsOnServer connAsDBA (name:string) = async {
    try
        let! result = 
            Sql.asyncExecScalar 
                connAsDBA 
                (sprintf "select count(*) from v$pdbs where upper(Name)='%s'" (name.ToUpper()))
                [] 
        return result |> Option.get > 0M |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let isSnapshotOfClause (pdb:string) = sprintf "SNAPSHOT_PARENT_CON_ID=(select CON_ID from v$pdbs where upper(name)='%s')" (pdb.ToUpper())

let pdbSnapshots connAsDBA (name:string) = async {
    try
        use! result = 
            Sql.asyncExecReader
                connAsDBA
                (sprintf @"select name from v$pdbs where %s" (isSnapshotOfClause name))
                []
        return result |> Sql.map (fun d -> (string)d?name.Value) |> List.ofSeq |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let olderThanClause (olderThan:System.TimeSpan) prefix = 
    let prefix = prefix |> Option.defaultValue "v$pdbs"
    sprintf "%s.creation_time <= SYSDATE - %s" prefix (olderThan.TotalDays.ToString("F15", CultureInfo.InvariantCulture.NumberFormat))

let pdbsFilteredOlderThan connAsDBA filter (olderThan:System.TimeSpan) = async {
    let where = filter |> Option.map (fun f -> sprintf " and (%s)" f) |> Option.defaultValue ""
    try
        use! result = 
            Sql.asyncExecReader
                connAsDBA
                (sprintf 
                    @"select name from v$pdbs where %s%s" 
                    (olderThanClause olderThan None)
                    where
                )
                []
        return result |> Sql.map (fun d -> (string)d?name.Value) |> List.ofSeq |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let pdbsOlderThan connAsDBA = pdbsFilteredOlderThan connAsDBA None

let pdbSnapshotsOlderThan connAsDBA (olderThan:System.TimeSpan) (name:string) = 
    pdbsFilteredOlderThan connAsDBA (isSnapshotOfClause name |> Some) olderThan

let getOldPDBsHavingFilesFolderStartWith connAsDBA olderThan folder = 
    getPDBsHavingFilesFolderStartWith connAsDBA folder (Some (olderThanClause olderThan (Some "p")))

let pdbHasSnapshots connAsDBA (name:string) = async {
    try
        let! result = 
            Sql.asyncExecScalar
                connAsDBA
                (sprintf @"select count(*) from v$pdbs where %s" (isSnapshotOfClause name))
                []
        return result |> Option.get > 0M |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

// Delete a PDB that can possibly be a snapshot source (check if snapshots first)
let deleteSourcePDB (logger:ILogger) connAsDBA (name:string) = async {
    let! hasSnapshotsMaybe = pdbHasSnapshots connAsDBA name
    match hasSnapshotsMaybe with
    | Ok hasSnapshots ->
        match hasSnapshots with
        | false -> return! deletePDB logger connAsDBA true name |> toOraclePDBResultAsync
        | true -> return Error (sprintf "PDB %s cannot be deleted because open snapshots have been created from it" name |> exn)
    | Error error -> return Error (upcast error)
}

let deletePDBWithSnapshots (logger:ILogger) connAsDBA (olderThan:System.TimeSpan) (name:string) = asyncValidation {
    logger.LogDebug("Deleting PDB {pdb} and dependant snapshots older than {delay}...", name, olderThan)
    let mapError (x:Async<Result<'a,OracleException>>) : Async<Result<'a,exn>> = AsyncResult.mapError (fun ex -> ex :> exn) x
    let! snapshots = pdbSnapshotsOlderThan connAsDBA olderThan name |> mapError
    let deleteSnapshot (snapshot:string) = asyncValidation {
        do! () // mandatory for the next line (log) to be in the same async block
        logger.LogDebug("Deleting PDB snapshot {pdb}...", snapshot)
        let! result = 
            snapshot
            |> deletePDB logger connAsDBA true
            |> AsyncResult.mapError (fun oracleExn -> exn (sprintf "cannot delete snapshot PDB %s : %s" snapshot oracleExn.Message))
            |> AsyncValidation.ofAsyncResult
        logger.LogDebug("Deleted PDB snapshot {pdb}.", snapshot)
        return result
    }
    let! _ = snapshots |> AsyncValidation.traverseS deleteSnapshot
    let! hasSnapshots = pdbHasSnapshots connAsDBA name |> mapError
    if not hasSnapshots then
        let! _ = deletePDB logger connAsDBA true name |> mapError
        logger.LogDebug("Deleted PDB {pdb} and all dependant snapshots.", name)
        return true
    else
        logger.LogDebug("Deleted some snapshots dependant of {pdb}.", name)
        return false
}

let getWorkingCopiesOlderThan connAsDBA (olderThan:System.TimeSpan) workingCopyFolder =
    let filter = sprintf "%s and folder like '%s/%%'" (olderThanClause olderThan (Some "p")) workingCopyFolder |> Some
    getPDBsWithFilesFolder connAsDBA filter |> AsyncResult.map (List.map fst)

let deleteWorkingCopiesOlderThan (logger:ILogger) connAsDBA (olderThan:System.TimeSpan) workingCopyFolder = asyncValidation {
    let! oldWorkingCopies = getWorkingCopiesOlderThan connAsDBA olderThan workingCopyFolder |> toOraclePDBResultAsync |> AsyncValidation.ofAsyncResult
    return! oldWorkingCopies |> AsyncValidation.traverseS (deleteSourcePDB logger connAsDBA >> AsyncValidation.ofAsyncResult)
}

type OracleAPI(loggerFactory : ILoggerFactory, instance) = 

    let connAsDBA = connAsDBAFromInstance instance
    let connAsDBAIn = connAsDBAInFromInstance instance
    let logger = loggerFactory.CreateLogger(sprintf "Oracle API for instance %s" instance.Name)

    let getManifestPath = sprintf "%s/%s" instance.MasterPDBManifestsPath

    interface IOracleAPI with
        member __.NewPDBFromDump timeout name dumpPath schemas targetSchemas =
            let manifest = Domain.MasterPDB.manifestFile name 1 |> getManifestPath
            createManifestFromDump 
                logger 
                connAsDBA connAsDBAIn 
                timeout 
                instance.UserForImport instance.UserForImportPassword 
                instance.Server instance.UserForFileTransfer instance.UserForFileTransferPassword instance.ServerFingerPrint
                "dbadmin" "pass"
                instance.MasterPDBManifestsPath dumpPath 
                schemas targetSchemas 
                instance.OracleDirectoryForDumps 
                instance.OracleDirectoryPathForDumps 
                manifest
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

        member __.DeletePDBWithSnapshots (olderThan:System.TimeSpan) name =
            deletePDBWithSnapshots logger connAsDBA olderThan name

        member __.GetPDBNamesLike like = 
            getPDBNamesLike connAsDBA like
            |> toOraclePDBResultAsync

        member __.GetPDBFilesFolder name =
            getPDBFilesFolder connAsDBA name
            |> toOraclePDBResultAsync

        member __.GetOldPDBsFromFolder olderThan folder =
            getOldPDBsHavingFilesFolderStartWith connAsDBA olderThan folder
            |> toOraclePDBResultAsync
