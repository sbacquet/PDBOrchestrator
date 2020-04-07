module Infrastructure.Oracle

open System
open System.Data
open Oracle.ManagedDataAccess.Client
open Compensable
open Microsoft.Extensions.Logging
open Domain.Common
open System.Globalization
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

let toOraclePDBValidation validation =
    validation |> Validation.mapError (fun error -> error :> exn)

let toOraclePDBValidationAsync validation = async {
    let! r = validation
    return toOraclePDBValidation r
}

let convertComp (comp:PDBCompensableAsyncAction) : CompensableAsyncAction<string, exn> =
    let (action, compensation) = comp
    let newAction t = async {
        let! x = action t
        return toOraclePDBResult x
    }
    (newAction, compensation)

let openConnF (f:string -> IDbConnection) host port service user password sysdba () =
    let connectionString = 
        let sysdbaString = if (sysdba) then "DBA Privilege=SYSDBA" else ""
        sprintf 
            @"Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%d)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=%s)));User Id=%s;Password=%s;Connection Timeout=60;%s"
            host port service user password sysdbaString

    let conn = f connectionString
    conn.Open()
    conn

let openConn = 
    openConnF (fun connectionString -> new OracleConnection(connectionString) :> IDbConnection)

let openConnWithLogging (logger:ILogger) = 

    let loggingConnection conn =
        { new DbConnectionWrapper(conn) with
            override __.Open() =
                logger.LogTrace("Opening connection")
                conn.Open()
            override __.Close() =
                logger.LogTrace("Closing connection")
                conn.Close()
            override __.Dispose() =
                logger.LogTrace("Disposing connection")
                conn.Dispose()
        } :> IDbConnection

    openConnF (fun connectionString -> loggingConnection (new OracleConnection(connectionString) :> IDbConnection))

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
        use! path = 
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

let getFileNameWithoutExtension (path:string) =
    try
         System.IO.Path.GetFileNameWithoutExtension(path) |> Ok
    with
    | ex -> Error ex

let importSchemas 
    (logger:ILogger) 
    connAsDBAIn 
    (timeout:TimeSpan option) 
    userForImport userForImportPassword 
    host userForFileTransfer userForFileTransferPassword serverHostkeySHA256 
    (dumpPath:string) 
    (schemas:string list) (targetSchemas:string list) 
    (directory:string) 
    (tolerantToErrors:bool)
    name 
    = asyncResult {

    try
        let! dumpDest = directory |> getOracleDirectoryPath connAsDBAIn name
        use! session = FileTransfer.newSession timeout host userForFileTransfer userForFileTransferPassword serverHostkeySHA256
        let! _ = FileTransfer.uploadFile session dumpPath (sprintf "%s/" dumpDest)
        let! dumpFile = getFileNameWithoutExtension(dumpPath) |> Result.map (fun fileName -> fileName.ToUpper())
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
            | 5 -> if tolerantToErrors then Ok name else sprintf "dump %s imported with errors" dumpPath |> Exceptional.ofString
            | exitCode -> sprintf "error while importing dump %s in PDB %s : exit code %d" dumpPath name exitCode |> Exceptional.ofString
    with
    | ex -> return! Error ex
}

let createAndImportSchemas 
    (logger:ILogger) 
    connAsDBAIn 
    (timeout:TimeSpan option)
    userForImport userForImportPassword
    host userForFileTransfer userForFileTransferPassword serverHostkeySHA256
    (dumpPath:string) 
    (schemas:string list) (targetSchemas:(string * string) list) 
    deleteExisting 
    (directory:string)
    (tolerantToImportErrors:bool)
    =
    [
        createSchemasCompensable logger connAsDBAIn targetSchemas deleteExisting
        notCompensableAsync (importSchemas logger connAsDBAIn timeout userForImport userForImportPassword host userForFileTransfer userForFileTransferPassword serverHostkeySHA256 dumpPath schemas (targetSchemas |> List.map fst) directory tolerantToImportErrors)
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
    host userForFileTransfer userForFileTransferPassword serverHostkeySHA256
    adminUserName adminUserPassword 
    dest (dumpPath:string) 
    (schemas:string list) (targetSchemas:(string * string) list) 
    (directory:string) 
    (directoryPath:string) 
    (manifest:string)
    (tolerantToImportErrors:bool)
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
                host userForFileTransfer userForFileTransferPassword serverHostkeySHA256 
                dumpPath 
                schemas targetSchemas 
                true 
                directory
                tolerantToImportErrors)
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

let snapshotPDB (logger:ILogger) connAsDBA sourcePDB destFolder name =
    logger.LogDebug("Snapshoting PDB {sourcePDB} to {snapshotCopy}", sourcePDB, name)
    sprintf 
        @"
BEGIN
	DECLARE
		shared_mem_alloc_failed EXCEPTION;
		PRAGMA EXCEPTION_INIT (shared_mem_alloc_failed, -4031);
	BEGIN
        execute immediate 'ALTER PLUGGABLE DATABASE %s OPEN READ ONLY FORCE';
		execute immediate 'CREATE PLUGGABLE DATABASE %s FROM %s PARALLEL 1 SNAPSHOT COPY NOLOGGING CREATE_FILE_DEST=''%s''';
	EXCEPTION
		WHEN shared_mem_alloc_failed THEN
			BEGIN
				execute immediate 'DROP PLUGGABLE DATABASE %s INCLUDING DATAFILES';
				RAISE;
			END;
	END;
END;
"
        sourcePDB name sourcePDB destFolder name
    |> execAsync name connAsDBA

let snapshotPDBCompensable (logger:ILogger) connAsDBA sourcePDB destFolder = 
    compensableAsync 
        (snapshotPDB logger connAsDBA sourcePDB destFolder) 
        (deletePDB logger connAsDBA true)

let snapshotAndOpenPDB (logger:ILogger) connAsDBA sourcePDB destFolder =
    [
        snapshotPDBCompensable logger connAsDBA sourcePDB destFolder
        openPDBCompensable logger connAsDBA true
    ] |> composeAsync logger

let clonePDB (logger:ILogger) connAsDBA sourcePDB destFolder name =
    logger.LogDebug("Cloning PDB {sourcePDB} to {destPDB}", sourcePDB, name)
    sprintf 
        @"
BEGIN
    execute immediate 'CREATE PLUGGABLE DATABASE %s FROM %s PARALLEL 1 NOLOGGING CREATE_FILE_DEST=''%s''';
END;
"
        name sourcePDB destFolder
    |> execAsync name connAsDBA

let clonePDBCompensable (logger:ILogger) connAsDBA manifest dest = 
    compensableAsync 
        (clonePDB logger connAsDBA manifest dest) 
        (deletePDB logger connAsDBA true)

let cloneAndOpenPDB (logger:ILogger) connAsDBA manifest dest =
    [
        clonePDBCompensable logger connAsDBA manifest dest
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
                (sprintf "select p.name, regexp_replace(f.name, '(^.+)/[[:alnum:]_]+\.[[:alnum:]_]+$', '\1') as folder from (select con_id, name, row_number() over (partition by con_id order by name) as rownumber from v$datafile) f, v$pdbs p where f.rownumber=1 and p.con_id=f.con_id%s" filter)
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
                (sprintf "select distinct p.name from v$datafile f, v$pdbs p where p.con_id=f.con_id and f.name like '%s/%%'%s" folder filter)
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
        use! result =
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

let pdbFiltered filter connAsDBA (name:string) = async {
    let filterClause = filter |> Option.map (fun filter -> sprintf " where (%s)" filter) |> Option.defaultValue ""
    try
        use! result = 
            Sql.asyncExecReader
                connAsDBA
                (sprintf @"select name from v$pdbs%s" filterClause)
                []
        return result |> Sql.map (fun d -> (string)d?name.Value) |> List.ofSeq |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let isSnapshotOfClause (pdb:string) = sprintf "SNAPSHOT_PARENT_CON_ID=(select CON_ID from v$pdbs where upper(name)='%s')" (pdb.ToUpper())

let isInFolderClause (folder:string) = sprintf "con_id in (select con_id from v$datafile where name like '%s/%%')" folder

let olderThanClause prefix (olderThan:System.TimeSpan) = 
    let prefix = prefix |> Option.defaultValue "v$pdbs"
    sprintf "%s.creation_time <= SYSDATE - %s" prefix (olderThan.TotalDays.ToString("F15", CultureInfo.InvariantCulture.NumberFormat))

let joinClauses clauses = 
    let s = clauses |> Seq.choose id |> Seq.map (sprintf "(%s)") |> String.concat " and "
    if (String.IsNullOrEmpty s) then None else Some s

let pdbSnapshots connAsDBA (folder:string option) (olderThan:System.TimeSpan option) (name:string) = 
    let filter =
        [ 
            isSnapshotOfClause name |> Some
            folder |> Option.map isInFolderClause
            olderThan |> Option.map (olderThanClause None)
        ] |> joinClauses
    pdbFiltered filter connAsDBA name

let getOldPDBsHavingFilesFolderStartWith connAsDBA olderThan folder = 
    getPDBsHavingFilesFolderStartWith connAsDBA folder (Some (olderThanClause (Some "p") olderThan))

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

let deletePDBSnapshots (logger:ILogger) connAsDBA (folder:string option) (olderThan:System.TimeSpan option) (deleteSource:bool) (sourceName:string) = asyncValidation {
    logger.LogDebug("Deleting PDB {pdb} and snapshot copies...", sourceName, olderThan)
    let! snapshots = pdbSnapshots connAsDBA folder olderThan sourceName
    let deleteSnapshot (snapshot:string) = asyncValidation {
        do! () // mandatory for the next line (log) to be in the same async block
        logger.LogDebug("Deleting PDB snapshot copy {pdb}...", snapshot)
        let! result = 
            snapshot
            |> deletePDB logger connAsDBA true
            |> AsyncValidation.ofAsyncResult
        logger.LogDebug("Deleted PDB snapshot copy {pdb}.", snapshot)
        return result
    }
    let! _ = snapshots |> AsyncValidation.traverseS deleteSnapshot
    let! hasSnapshots = pdbHasSnapshots connAsDBA sourceName
    if not hasSnapshots then
        if deleteSource then
            let! (_:string) = deletePDB logger connAsDBA true sourceName
            logger.LogDebug("Deleted PDB {pdb} and all snapshot copies.", sourceName)
            return true
        else
            logger.LogDebug("Deleted all snapshot copies of PDB {pdb}.", sourceName)
            return false
    else
        logger.LogDebug("Deleted some snapshot copies of {pdb}.", sourceName)
        return false
}
