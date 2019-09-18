module Application.Oracle

type OracleResult<'T> = Result<'T, Oracle.ManagedDataAccess.Client.OracleException>
type OraclePDBResult = OracleResult<string>

type IOracleAPI =
    abstract member NewPDBFromDump : string -> string -> string -> string -> string list -> (string * string) list -> string -> string -> string -> OraclePDBResult
    abstract member ClosePDB : string -> OraclePDBResult
    abstract member DeletePDB : string -> OraclePDBResult
    abstract member ExportPDB : string -> string -> OraclePDBResult
    abstract member ImportPDB : string -> string -> string -> OraclePDBResult
    abstract member SnapshotPDB : string -> string -> string -> OraclePDBResult
