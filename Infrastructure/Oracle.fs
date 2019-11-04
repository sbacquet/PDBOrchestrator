module Infrastructure.Oracle

open System
open System.Data
open Oracle.ManagedDataAccess.Client
open Compensable
open Application.Oracle
open Microsoft.Extensions.Logging
open Domain.Common
open System.Globalization

type PDBCompensableAction = CompensableAction<string, Oracle.ManagedDataAccess.Client.OracleException>
type PDBCompensableAsyncAction = CompensableAsyncAction<string, Oracle.ManagedDataAccess.Client.OracleException>

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

let importSchemasInPDB (logger:ILogger) connAsDBA (dumpPath:string) (schemas:string list) (targetSchemas:(string * string) list) (directory:string) name = async {
    return Ok name // TODO
}

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


let createManifestFromDump (logger:ILogger) connAsDBA connAsDBAIn adminUserName adminUserPassword dest (dumpPath:string) (schemas:string list) (targetSchemas:(string * string) list) (directory:string) (manifest:string) = 
    [
        createPDBCompensable logger connAsDBA adminUserName adminUserPassword dest true
        notCompensableAsync (grantPDB logger connAsDBAIn)
        notCompensableAsync (importSchemasInPDB logger connAsDBA dumpPath schemas targetSchemas directory)
        notCompensableAsync (closePDB logger connAsDBA)
        notCompensableAsync (exportPDB logger connAsDBA manifest)
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

let getPDBFilesFolder connAsDBA (name:string) = async {
    try
        use! result = 
            Sql.asyncExecReader 
                connAsDBA 
                (sprintf "select regexp_replace(f.file_name, '(^.+)/[[:alnum:]_]+\.[[:alnum:]_]+$', '\1') from (select con_id, file_name, row_number() over (partition by con_id order by file_name) as rownumber from cdb_data_files) f, v$pdbs p where f.rownumber=1 and p.con_id=f.con_id and upper(p.name)='%s'" (name.ToUpper()))
                [] 
        let folder:string option = result |> Sql.mapFirst (Sql.asScalar)
        return Ok folder
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
    
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

let pdbSnapshots connAsDBA (name:string) = async {
    try
        use! result = 
            Sql.asyncExecReader
                connAsDBA
                (sprintf @"select name from v$pdbs where SNAPSHOT_PARENT_CON_ID=(select CON_ID from v$pdbs where upper(name)='%s')" (name.ToUpper()))
                []
        return result |> Sql.map (fun d -> (string)d?name.Value) |> List.ofSeq |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let pdbSnapshotsOlderThan connAsDBA (olderThan:System.TimeSpan) (name:string) = async {
    try
        use! result = 
            Sql.asyncExecReader
                connAsDBA
                (sprintf 
                    @"select name from v$pdbs where creation_time <= SYSDATE - %s and SNAPSHOT_PARENT_CON_ID=(select CON_ID from v$pdbs where upper(name)='%s')" 
                    (olderThan.TotalDays.ToString("F15", CultureInfo.InvariantCulture.NumberFormat))
                    (name.ToUpper()))
                []
        return result |> Sql.map (fun d -> (string)d?name.Value) |> List.ofSeq |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let pdbHasSnapshots connAsDBA (name:string) = async {
    try
        let! result = 
            Sql.asyncExecScalar
                connAsDBA
                (sprintf @"select count(*) from v$pdbs where SNAPSHOT_PARENT_CON_ID=(select CON_ID from v$pdbs where upper(name)='%s')" (name.ToUpper()))
                []
        return result |> Option.get > 0M |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let toOraclePDBResult result =
    result |> Result.mapError (fun error -> error :> exn)

let toOraclePDBResultAsync result = async {
    let! r = result
    return toOraclePDBResult r
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
    logger.LogDebug("Deleting PDB {pdb} and dependant snapshots", name)
    let mapError (x:Async<Result<'a,OracleException>>) : Async<Result<'a,exn>> = AsyncResult.mapError (fun ex -> ex :> exn) x
    let! snapshots = pdbSnapshotsOlderThan connAsDBA olderThan name |> mapError
    let deleteSnapshot (snapshot:string) = asyncValidation {
        do! () // mandatory for the next line (log) to be in the same async block
        logger.LogDebug("Deleting PDB snapshot {pdb}", snapshot)
        let! result = 
            snapshot
            |> deletePDB logger connAsDBA true
            |> AsyncResult.mapError (fun oracleExn -> exn (sprintf "cannot delete snapshot PDB %s : %s" snapshot oracleExn.Message))
            |> AsyncValidation.ofAsyncResult
        logger.LogDebug("Deleted PDB snapshot {pdb}", snapshot)
        return result
    }
    let! _ = snapshots |> AsyncValidation.traverseS deleteSnapshot
    let! hasSnapshots = pdbHasSnapshots connAsDBA name |> mapError
    if not hasSnapshots then
        let! _ = deletePDB logger connAsDBA true name |> mapError
        logger.LogDebug("Deleted PDB {pdb} and all dependant snapshots", name)
        return true
    else
        logger.LogDebug("Deleted some snapshots dependant of {pdb}", name)
        return false
}

type OracleAPI(loggerFactory : ILoggerFactory, connAsDBA : Sql.ConnectionManager, connAsDBAIn : string -> Sql.ConnectionManager) = 

    member __.Logger = loggerFactory.CreateLogger("Oracle API")

    interface IOracleAPI with
        member __.NewPDBFromDump adminUserName adminUserPassword dest dumpPath schemas targetSchemas directory manifest name =
            createManifestFromDump __.Logger connAsDBA connAsDBAIn adminUserName adminUserPassword dest dumpPath schemas targetSchemas directory manifest name
            |> toOraclePDBResultAsync

        member __.ClosePDB name =
            closePDB __.Logger connAsDBA name
            |> toOraclePDBResultAsync

        member __.DeletePDB name =
            deleteSourcePDB __.Logger connAsDBA name

        member __.ExportPDB manifest name = 
            closeAndExportPDB __.Logger connAsDBA manifest name
            |> toOraclePDBResultAsync

        member __.ImportPDB manifest dest name = 
            importAndOpen __.Logger connAsDBA manifest dest name
            |> toOraclePDBResultAsync

        member __.SnapshotPDB from dest name = 
            snapshotAndOpenPDB __.Logger connAsDBA from dest name
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
            deletePDBWithSnapshots __.Logger connAsDBA olderThan name

        member __.GetPDBNamesLike like = 
            getPDBNamesLike connAsDBA like
            |> toOraclePDBResultAsync

        member __.GetPDBFilesFolder name =
            getPDBFilesFolder connAsDBA name
            |> toOraclePDBResultAsync

