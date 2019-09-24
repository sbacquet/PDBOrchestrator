module Application.Oracle

type OracleResult<'T> = Result<'T, Oracle.ManagedDataAccess.Client.OracleException>
type OraclePDBResult = OracleResult<string>

type IOracleAPI =
    //inherit System.IDisposable
    abstract member NewPDBFromDump : adminUserName:string -> adminUserPassword:string -> dest:string -> dumpPath:string -> schemas:string list -> targetSchemas:(string * string) list -> directory:string -> manifest:string -> name:string -> Async<OraclePDBResult>
    abstract member ClosePDB : name:string -> Async<OraclePDBResult>
    abstract member DeletePDB : name:string -> Async<OraclePDBResult>
    abstract member ExportPDB : manifest:string -> name:string -> Async<OraclePDBResult>
    abstract member ImportPDB : manifest:string -> dest:string -> name:string -> Async<OraclePDBResult>
    abstract member SnapshotPDB : from:string -> dest:string -> name:string -> Async<OraclePDBResult>
    abstract member PDBHasSnapshots : name:string -> Async<bool>
    abstract member PDBExists : name:string -> Async<bool>