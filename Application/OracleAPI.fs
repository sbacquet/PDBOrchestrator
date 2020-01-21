module Application.Oracle

open Domain.Common.Exceptional

type OraclePDBResult = Exceptional<string>

type OraclePDBResultWithReqId = Application.PendingRequest.WithRequestId<OraclePDBResult>

let getOracleServerPort port = port |> Option.defaultValue 1521

type IOracleAPI =
    //inherit System.IDisposable
    abstract member NewPDBFromDump : 
        timeout:System.TimeSpan option ->
        name:string ->
        dumpPath:string -> 
        schemas:string list -> 
        targetSchemas:(string * string) list
        -> Async<OraclePDBResult>

    abstract member ClosePDB : name:string -> Async<OraclePDBResult>

    abstract member DeletePDB : name:string -> Async<OraclePDBResult>

    abstract member ExportPDB : manifest:string -> name:string -> Async<OraclePDBResult>

    abstract member ImportPDB : manifest:string -> destFolder:string -> name:string -> Async<OraclePDBResult>

    abstract member SnapshotPDB : sourcePDB:string -> destFolder:string -> name:string -> Async<OraclePDBResult>

    abstract member ClonePDB : sourcePDB:string -> destFolder:string -> name:string -> Async<OraclePDBResult>

    abstract member PDBHasSnapshots : name:string -> Async<Exceptional<bool>>

    abstract member PDBSnapshots : name:string -> Async<Exceptional<string list>>

    abstract member PDBExists : name:string -> Async<Exceptional<bool>>

    abstract member GetPDBNamesLike : like:string -> Async<Result<string list, exn>>

    abstract member GetPDBFilesFolder : name:string -> Async<Exceptional<string option>>
