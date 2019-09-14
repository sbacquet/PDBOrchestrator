module Application.Oracle

type OracleResult<'T> = Result<'T, Oracle.ManagedDataAccess.Client.OracleException>
type OraclePDBResult = OracleResult<string>

type OracleAPI = {
    NewPDB : string -> string -> string -> string -> OraclePDBResult
    ClosePDB : string -> OraclePDBResult
    DeletePDB : string -> OraclePDBResult
    ExportPDB : string -> string -> OraclePDBResult
    ImportPDB : string -> string -> string -> OraclePDBResult
    SnapshotPDB : string -> string -> string -> OraclePDBResult
}