module Application.Oracle

open Domain.Common.Exceptional
open Domain.Common.Validation

type OraclePDBResult = Exceptional<string>

type OraclePDBResultWithReqId = Application.PendingRequest.WithRequestId<OraclePDBResult>

let getOracleServerPort port = port |> Option.defaultValue 1521

let oracleInstancePortString port =
    port |> Option.map (fun port -> sprintf ":%d" port) |> Option.defaultValue ""

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

    abstract member ImportPDB : manifest:string -> dest:string -> name:string -> Async<OraclePDBResult>

    abstract member SnapshotPDB : from:string -> dest:string -> name:string -> Async<OraclePDBResult>

    abstract member ClonePDB : from:string -> dest:string -> name:string -> Async<OraclePDBResult>

    abstract member PDBHasSnapshots : name:string -> Async<Exceptional<bool>>

    abstract member PDBSnapshots : name:string -> Async<Exceptional<string list>>

    abstract member PDBExists : name:string -> Async<Exceptional<bool>>

    abstract member DeletePDBSnapshots : folder:string option -> olderThan:System.TimeSpan option -> deleteSource:bool -> sourceName:string -> Async<Validation<bool,exn>>

    abstract member GetPDBNamesLike : like:string -> Async<Result<string list, exn>>

    abstract member GetPDBFilesFolder : name:string -> Async<Exceptional<string option>>

    abstract member GetOldPDBsFromFolder : olderThan:System.TimeSpan -> workingCopyFolder:string -> Async<Result<string list,exn>>
