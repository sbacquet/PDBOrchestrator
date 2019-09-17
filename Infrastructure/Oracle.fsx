module Infrastructure.Oracle

#if INTERACTIVE
#load "CompensableAction.fsx"
#r @"..\packages\FsSql.Core\lib\netstandard2.0\FsSql.dll"
#r @"..\packages\Oracle.ManagedDataAccess.Core\lib\netstandard2.0\Oracle.ManagedDataAccess.dll"
#endif

open System
open System.Data
open Oracle.ManagedDataAccess.Client
open Compensable
open Application.Oracle

type PDBCompensableAction = CompensableAction<string, Oracle.ManagedDataAccess.Client.OracleException>

let openConn host service user password sysdba = fun () ->
    let connectionString = 
        let sysdbaString = if (sysdba) then "DBA Privilege=SYSDBA" else ""
        sprintf 
            @"Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=%s)));User Id=%s;Password=%s;%s"
            host service user password sysdbaString

    let conn = new OracleConnection(connectionString)
    conn.Open()
    conn :> IDbConnection

let connAsDBAIn service = Sql.withNewConnection (openConn "fr1psl010716.misys.global.ad" service "sys" "syspwd8" true)
let connAsDBA = connAsDBAIn "intcdb2"

// partial application of various common functions, around the connection manager
let exec result conn a = try Sql.execNonQuery conn a [] |> ignore; Ok result with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> Error ex
let P = Sql.Parameter.make
let (=>) a b = Sql.Parameter.make(a, b)

let (>>=) r f = Result.bind f r
let (>=>) f1 f2 = fun x -> f1 x >>= f2

let createPDB adminUserName adminUserPassword dest keepOpen name : OraclePDBResult = 
    printfn "Creating PDB %s" name
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

let grantPDB name =
    printfn "Granting PDB %s" name
    @"
BEGIN
    execute immediate 'GRANT execute ON sys.dbms_lock TO public';
    execute immediate 'GRANT SCHEDULER_ADMIN TO public';
    execute immediate 'grant execute on CTX_DDL to public';
    execute immediate 'alter profile DEFAULT limit password_life_time UNLIMITED';
END;
"
    |> exec name (connAsDBAIn name)


let deletePDB name : OraclePDBResult = 
    printfn "Deleting PDB %s" name
    sprintf @"DROP PLUGGABLE DATABASE %s INCLUDING DATAFILES" name
    |> exec name connAsDBA

let createPDBCompensable adminUserName adminUserPassword dest keepOpen = 
    compensable 
        (createPDB adminUserName adminUserPassword dest keepOpen)
        deletePDB

let openPDB readWrite name : OraclePDBResult =
    printfn "Opening PDB %s" name
    let readMode = if readWrite then "READ WRITE" else "READ ONLY"
    sprintf @"ALTER PLUGGABLE DATABASE %s OPEN %s FORCE" name readMode
    |> exec name connAsDBA

let closePDB name : OraclePDBResult = 
    printfn "Closing PDB %s" name
    let result = 
        sprintf @"ALTER PLUGGABLE DATABASE %s CLOSE IMMEDIATE" name
        |> exec name connAsDBA
    match result with
    | Ok result -> Ok result
    | Error ex -> 
        match ex.Number with
        | 65020 -> Ok name // already closed -> ignore it
        | _ -> Error ex

let openPDBCompensable readWrite = 
    compensable 
        (openPDB readWrite) 
        closePDB

let importSchemasInPDB (dumpPath:string) (schemas:string list) (targetSchemas:(string * string) list) (directory:string) name : OraclePDBResult =
    Ok name // TODO

let createAndGrantPDB keepOpen adminUserName adminUserPassword dest = 
    [
        createPDBCompensable adminUserName adminUserPassword dest true
        notCompensable grantPDB
        notCompensable (if keepOpen then Ok else closePDB)
    ] |> compose

let exportPDB manifest =
    let export pdb =
        printfn "Exporting PDB %s" pdb
        sprintf 
            @"
    BEGIN
        execute immediate 'ALTER PLUGGABLE DATABASE %s UNPLUG INTO ''%s''';
        execute immediate 'DROP PLUGGABLE DATABASE %s KEEP DATAFILES';
    END;
     "
            pdb manifest pdb
        |> exec pdb connAsDBA

    closePDB >=> export

let createManifestFromDump adminUserName adminUserPassword dest (dumpPath:string) (schemas:string list) (targetSchemas:(string * string) list) (directory:string) (manifest:string) = 
    [
        createPDBCompensable adminUserName adminUserPassword dest true
        notCompensable grantPDB
        notCompensable (importSchemasInPDB dumpPath schemas targetSchemas directory)
        notCompensable closePDB
        notCompensable (exportPDB manifest)
    ] |> compose

let importPDB manifest dest name =
    printfn "Importing PDB %s" name
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

let snapshotPDB from dest name =
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

let getPDBsOnServer () =
    Sql.execReader connAsDBA "select con_id as Id, Name, open_mode as OpenMode, rawtohex(guid) as Guid, SNAPSHOT_PARENT_CON_ID as SnapId from v$pdbs" [] 
    |> Sql.map (Sql.asRecord<RawOraclePDB> "")

let getPDBOnServer (name:string) =
    Sql.execReader 
        connAsDBA 
        (sprintf "select con_id as Id, Name, open_mode as OpenMode, rawtohex(guid) as Guid, SNAPSHOT_PARENT_CON_ID as SnapId from v$pdbs where upper(Name)='%s'" (name.ToUpper()))
        [] 
    |> Sql.mapFirst (Sql.asRecord<RawOraclePDB> "")

let pdbHasSnapshots name =
    Sql.execScalar
        connAsDBA
        (sprintf @"select count(*) from v$pdbs where SNAPSHOT_PARENT_CON_ID=(select CON_ID from v$pdbs where name='%s')" name)
        []
    |> Option.get > 0M

let oracleAPI : Application.Oracle.OracleAPI = {
    NewPDBFromDump = createManifestFromDump
    ClosePDB = closePDB
    DeletePDB = deletePDB
    ExportPDB = exportPDB
    ImportPDB = importPDB
    SnapshotPDB = snapshotPDB
}