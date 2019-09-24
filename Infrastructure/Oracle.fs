module Infrastructure.Oracle

open System
open System.Data
open Oracle.ManagedDataAccess.Client
open Compensable
open Application.Oracle
open Microsoft.Extensions.Logging

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

let exec result conn a = try Sql.execNonQuery conn a [] |> ignore; Ok result with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> Error ex
let execAsync result conn a = async {
    try 
        let! _ = Sql.asyncExecNonQuery conn a []
        return Ok result 
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let P = Sql.Parameter.make
let (=>) a b = Sql.Parameter.make(a, b)

let (>>=) r f = Result.bind f r
let (>=>) f1 f2 = fun x -> f1 x >>= f2

let createPDB (logger:ILogger) connAsDBA adminUserName adminUserPassword dest keepOpen (name:string) = 
    logger.LogDebug("Creating PDB {PDB}", name)
    let closeSql = if (keepOpen) then "" else sprintf @"execute immediate 'alter pluggable database %s close immediate';" name
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

let grantPDB (logger:ILogger) connAsDBAIn (name:string) =
    logger.LogDebug("Granting PDB {PDB}", name)
    @"
BEGIN
    execute immediate 'GRANT execute ON sys.dbms_lock TO public';
    execute immediate 'GRANT SCHEDULER_ADMIN TO public';
    execute immediate 'grant execute on CTX_DDL to public';
    execute immediate 'alter profile DEFAULT limit password_life_time UNLIMITED';
END;
"
    |> execAsync name (connAsDBAIn name)


let deletePDB (logger:ILogger) connAsDBA (name:string) = 
    logger.LogDebug("Deleting PDB {PDB}", name)
    sprintf @"DROP PLUGGABLE DATABASE %s INCLUDING DATAFILES" name
    |> execAsync name connAsDBA

let createPDBCompensable (logger:ILogger) connAsDBA adminUserName adminUserPassword dest keepOpen = 
    compensableAsync
        (createPDB logger connAsDBA adminUserName adminUserPassword dest keepOpen)
        (deletePDB logger connAsDBA)

let openPDB (logger:ILogger) connAsDBA readWrite (name:string) =
    logger.LogDebug("Opening PDB {PDB}", name)
    let readMode = if readWrite then "READ WRITE" else "READ ONLY"
    sprintf @"ALTER PLUGGABLE DATABASE %s OPEN %s FORCE" name readMode
    |> execAsync name connAsDBA

let closePDB (logger:ILogger) connAsDBA (name:string) = async {
    logger.LogDebug("Closing PDB {PDB}", name)
    let! result = 
        sprintf @"ALTER PLUGGABLE DATABASE %s CLOSE IMMEDIATE" name
        |> execAsync name connAsDBA
    match result with
    | Ok result -> return Ok result
    | Error ex -> 
        match ex.Number with
        | 65020 -> return Ok name // already closed -> ignore it
        | _ -> return Error ex
}

let openPDBCompensable (logger:ILogger) connAsDBA readWrite = 
    compensableAsync 
        (openPDB logger connAsDBA readWrite) 
        (closePDB logger connAsDBA)

let importSchemasInPDB (logger:ILogger) connAsDBA (dumpPath:string) (schemas:string list) (targetSchemas:(string * string) list) (directory:string) name = async {
    return Ok name // TODO
}

let createAndGrantPDB (logger:ILogger) connAsDBA connAsDBAIn keepOpen adminUserName adminUserPassword dest = 
    [
        createPDBCompensable logger connAsDBA adminUserName adminUserPassword dest true
        notCompensableAsync (grantPDB logger connAsDBAIn)
        notCompensableAsync (if keepOpen then (fun name -> async { return Ok name }) else closePDB logger connAsDBA)
    ] |> composeAsync logger

let exportPDB (logger:ILogger) connAsDBA manifest name = async {
    let export (pdb:string) =
        logger.LogDebug("Exporting PDB {PDB}", pdb)
        sprintf 
            @"
    BEGIN
        execute immediate 'ALTER PLUGGABLE DATABASE %s UNPLUG INTO ''%s''';
        execute immediate 'DROP PLUGGABLE DATABASE %s KEEP DATAFILES';
    END;
     "
            pdb manifest pdb
        |> execAsync pdb connAsDBA
    
    let! closeResult = closePDB logger connAsDBA name
    match closeResult with
    | Ok r -> return! export name
    | error -> return error
}

let createManifestFromDump (logger:ILogger) connAsDBA connAsDBAIn adminUserName adminUserPassword dest (dumpPath:string) (schemas:string list) (targetSchemas:(string * string) list) (directory:string) (manifest:string) = 
    [
        createPDBCompensable logger connAsDBA adminUserName adminUserPassword dest true
        notCompensableAsync (grantPDB logger connAsDBAIn)
        notCompensableAsync (importSchemasInPDB logger connAsDBA dumpPath schemas targetSchemas directory)
        notCompensableAsync (closePDB logger connAsDBA)
        notCompensableAsync (exportPDB logger connAsDBA manifest)
    ] |> composeAsync logger

let importPDB (logger:ILogger) connAsDBA manifest dest (name:string) =
    logger.LogDebug("Importing PDB {PDB}", name)
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

let snapshotPDB (logger:ILogger) connAsDBA from dest name =
    logger.LogDebug("Snapshoting PDB {PDB} to {snapshot}", from, name)
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

type RawOraclePDB = {
    Id: decimal
    Name: string
    OpenMode: string
    Guid: string
    SnapId: decimal option
}

let getPDBsOnServer connAsDBA = async {
    let! result = Sql.asyncExecReader connAsDBA "select con_id as Id, Name, open_mode as OpenMode, rawtohex(guid) as Guid, SNAPSHOT_PARENT_CON_ID as SnapId from v$pdbs" [] 
    return result |> Sql.map (Sql.asRecord<RawOraclePDB> "")
}

let getPDBOnServer connAsDBA (name:string) = async {
    let! result = 
        Sql.asyncExecReader 
            connAsDBA 
            (sprintf "select con_id as Id, Name, open_mode as OpenMode, rawtohex(guid) as Guid, SNAPSHOT_PARENT_CON_ID as SnapId from v$pdbs where upper(Name)='%s'" (name.ToUpper()))
            [] 
    return result |> Sql.mapFirst (Sql.asRecord<RawOraclePDB> "")
}

let PDBExistsOnServer connAsDBA (name:string) = async {
    let! result = 
        Sql.asyncExecScalar 
            connAsDBA 
            (sprintf "select count(*) from v$pdbs where upper(Name)='%s'" (name.ToUpper()))
            [] 
    return result |> Option.get > 0M
}

let pdbHasSnapshots connAsDBA (name:string) = async {
    let! result = 
        Sql.asyncExecScalar
            connAsDBA
            (sprintf @"select count(*) from v$pdbs where SNAPSHOT_PARENT_CON_ID=(select CON_ID from v$pdbs where upper(name)='%s')" (name.ToUpper()))
            []
    return result |> Option.get > 0M
}

type OracleAPI(loggerFactory : ILoggerFactory, connAsDBA : Sql.ConnectionManager, connAsDBAIn : string -> Sql.ConnectionManager) = 
    member this.Logger = loggerFactory.CreateLogger("Oracle API")
    interface IOracleAPI with
        member this.NewPDBFromDump adminUserName adminUserPassword dest dumpPath schemas targetSchemas directory manifest name = createManifestFromDump this.Logger connAsDBA connAsDBAIn adminUserName adminUserPassword dest dumpPath schemas targetSchemas directory manifest name
        member this.ClosePDB name = closePDB this.Logger connAsDBA name
        member this.DeletePDB name = deletePDB this.Logger connAsDBA name
        member this.ExportPDB manifest name = exportPDB this.Logger connAsDBA manifest name
        member this.ImportPDB manifest dest name = importPDB this.Logger connAsDBA manifest dest name
        member this.SnapshotPDB from dest name = snapshotPDB this.Logger connAsDBA from dest name
        member this.PDBHasSnapshots name = pdbHasSnapshots connAsDBA name
        member this.PDBExists name = PDBExistsOnServer connAsDBA name
