module Application.Oracle

open Domain.Common.Result

type OraclePDBResult = Exceptional<string>

type IOracleAPI =
    //inherit System.IDisposable
    abstract member NewPDBFromDump : adminUserName:string -> adminUserPassword:string -> dest:string -> dumpPath:string -> schemas:string list -> targetSchemas:(string * string) list -> directory:string -> manifest:string -> name:string -> Async<OraclePDBResult>
    abstract member ClosePDB : name:string -> Async<OraclePDBResult>
    abstract member DeletePDB : name:string -> Async<OraclePDBResult>
    abstract member ExportPDB : manifest:string -> name:string -> Async<OraclePDBResult>
    abstract member ImportPDB : manifest:string -> dest:string -> name:string -> Async<OraclePDBResult>
    abstract member SnapshotPDB : from:string -> dest:string -> name:string -> Async<OraclePDBResult>
    abstract member PDBHasSnapshots : name:string -> Async<Exceptional<bool>>
    abstract member PDBSnapshots : name:string -> Async<Exceptional<string list>>
    abstract member PDBExists : name:string -> Async<Exceptional<bool>>
    abstract member DeletePDBWithSnapshots : name:string -> Async<Exceptional<string>>
