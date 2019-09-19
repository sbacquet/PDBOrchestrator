module Infrastructure.Oracle

open System
open System.Data
open Oracle.ManagedDataAccess.Client
open Compensable
open Application.Oracle
open Microsoft.Extensions.Logging

type PDBCompensableAction = CompensableAction<string, Oracle.ManagedDataAccess.Client.OracleException>

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

// partial application of various common functions, around the connection manager
let exec result conn a = try Sql.execNonQuery conn a [] |> ignore; Ok result with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> Error ex
let P = Sql.Parameter.make
let (=>) a b = Sql.Parameter.make(a, b)

let (>>=) r f = Result.bind f r
let (>=>) f1 f2 = fun x -> f1 x >>= f2

let createPDB (logger:ILogger) connAsDBA adminUserName adminUserPassword dest keepOpen (name:string) : OraclePDBResult = 
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
    |> exec name connAsDBA

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
    |> exec name (connAsDBAIn name)


let deletePDB (logger:ILogger) connAsDBA (name:string) : OraclePDBResult = 
    logger.LogDebug("Deleting PDB {PDB}", name)
    sprintf @"DROP PLUGGABLE DATABASE %s INCLUDING DATAFILES" name
    |> exec name connAsDBA

let createPDBCompensable (logger:ILogger) connAsDBA adminUserName adminUserPassword dest keepOpen = 
    compensable 
        (createPDB logger connAsDBA adminUserName adminUserPassword dest keepOpen)
        (deletePDB logger connAsDBA)

let openPDB (logger:ILogger) connAsDBA readWrite (name:string) : OraclePDBResult =
    logger.LogDebug("Opening PDB {PDB}", name)
    let readMode = if readWrite then "READ WRITE" else "READ ONLY"
    sprintf @"ALTER PLUGGABLE DATABASE %s OPEN %s FORCE" name readMode
    |> exec name connAsDBA

let closePDB (logger:ILogger) connAsDBA (name:string) : OraclePDBResult = 
    logger.LogDebug("Closing PDB {PDB}", name)
    let result = 
        sprintf @"ALTER PLUGGABLE DATABASE %s CLOSE IMMEDIATE" name
        |> exec name connAsDBA
    match result with
    | Ok result -> Ok result
    | Error ex -> 
        match ex.Number with
        | 65020 -> Ok name // already closed -> ignore it
        | _ -> Error ex

let openPDBCompensable (logger:ILogger) connAsDBA readWrite = 
    compensable 
        (openPDB logger connAsDBA readWrite) 
        (closePDB logger connAsDBA)

let importSchemasInPDB (logger:ILogger) connAsDBA (dumpPath:string) (schemas:string list) (targetSchemas:(string * string) list) (directory:string) name : OraclePDBResult =
    Ok name // TODO

let createAndGrantPDB (logger:ILogger) connAsDBA connAsDBAIn keepOpen adminUserName adminUserPassword dest = 
    [
        createPDBCompensable logger connAsDBA adminUserName adminUserPassword dest true
        notCompensable (grantPDB logger connAsDBAIn)
        notCompensable (if keepOpen then Ok else closePDB logger connAsDBA)
    ] |> compose logger

let exportPDB (logger:ILogger) connAsDBA manifest =
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
        |> exec pdb connAsDBA

    closePDB logger connAsDBA >=> export

let createManifestFromDump (logger:ILogger) connAsDBA connAsDBAIn adminUserName adminUserPassword dest (dumpPath:string) (schemas:string list) (targetSchemas:(string * string) list) (directory:string) (manifest:string) = 
    [
        createPDBCompensable logger connAsDBA adminUserName adminUserPassword dest true
        notCompensable (grantPDB logger connAsDBAIn)
        notCompensable (importSchemasInPDB logger connAsDBA dumpPath schemas targetSchemas directory)
        notCompensable (closePDB logger connAsDBA)
        notCompensable (exportPDB logger connAsDBA manifest)
    ] |> compose logger

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
    |> exec name connAsDBA

let snapshotPDB (logger:ILogger) connAsDBA from dest name =
    sprintf 
        @"
BEGIN
	DECLARE
		shared_mem_alloc_failed EXCEPTION;
		PRAGMA EXCEPTION_INIT (shared_mem_alloc_failed, -4031);
	BEGIN
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
        name from dest name
    |> exec name connAsDBA

type RawOraclePDB = {
    Id: decimal
    Name: string
    OpenMode: string
    Guid: string
    SnapId: decimal option
}

let getPDBsOnServer connAsDBA =
    Sql.execReader connAsDBA "select con_id as Id, Name, open_mode as OpenMode, rawtohex(guid) as Guid, SNAPSHOT_PARENT_CON_ID as SnapId from v$pdbs" [] 
    |> Sql.map (Sql.asRecord<RawOraclePDB> "")

let getPDBOnServer connAsDBA (name:string) =
    Sql.execReader 
        connAsDBA 
        (sprintf "select con_id as Id, Name, open_mode as OpenMode, rawtohex(guid) as Guid, SNAPSHOT_PARENT_CON_ID as SnapId from v$pdbs where upper(Name)='%s'" (name.ToUpper()))
        [] 
    |> Sql.mapFirst (Sql.asRecord<RawOraclePDB> "")

let pdbHasSnapshots connAsDBA name =
    Sql.execScalar
        connAsDBA
        (sprintf @"select count(*) from v$pdbs where SNAPSHOT_PARENT_CON_ID=(select CON_ID from v$pdbs where name='%s')" name)
        []
    |> Option.get > 0M

type OracleAPI(loggerFactory : ILoggerFactory, connAsDBA : Sql.ConnectionManager, connAsDBAIn : string -> Sql.ConnectionManager) = 
    member this.Logger = loggerFactory.CreateLogger("Oracle API")
    member this.ConnAsDBA = connAsDBA
    member this.ConnAsDBAIn = connAsDBAIn
    interface IOracleAPI with
        member this.NewPDBFromDump a b c d e f g h i = createManifestFromDump this.Logger this.ConnAsDBA this.ConnAsDBAIn a b c d e f g h i
        member this.ClosePDB name = closePDB this.Logger this.ConnAsDBA name
        member this.DeletePDB name = deletePDB this.Logger this.ConnAsDBA name
        member this.ExportPDB manifest name = exportPDB this.Logger this.ConnAsDBA manifest name
        member this.ImportPDB manifest dest name = importPDB this.Logger this.ConnAsDBA manifest dest name
        member this.SnapshotPDB from dest name = snapshotPDB this.Logger this.ConnAsDBA from dest name
