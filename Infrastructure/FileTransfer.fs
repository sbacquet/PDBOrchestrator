module Infrastructure.FileTransfer

open WinSCP
open Domain.Common

let newSession timeout toHost user pass hostKey =
    try
        let sessionParams = SessionOptions()
        sessionParams.Protocol <- Protocol.Scp
        sessionParams.HostName <- toHost
        sessionParams.UserName <- user
        sessionParams.Password <- pass
        sessionParams.SshHostKeyFingerprint <- hostKey
        if timeout |> Option.isSome then sessionParams.Timeout <- timeout.Value
        let session = new Session()
        session.ExecutablePath <- sprintf "%s\WinSCP.exe" (System.IO.Directory.GetCurrentDirectory())
        session.Open(sessionParams)
        Ok session
    with 
    | ex -> Error ex

let newSessionFromInstance timeout (instance:Domain.OracleInstance.OracleInstance) =
    newSession timeout instance.Server instance.UserForFileTransfer instance.UserForFileTransferPassword instance.ServerHostkeySHA256

let uploadFile (session:Session) fromPath toPath =
    try
        let transferOptions = new TransferOptions();
        transferOptions.TransferMode <- TransferMode.Binary;
        transferOptions.OverwriteMode <- OverwriteMode.Overwrite
 
        let transferResult = session.PutFiles(fromPath, toPath, false, transferOptions);
 
        if transferResult.IsSuccess then
            Ok toPath
        else
            transferResult.Failures |> Seq.map (fun ex -> ex.Message) |> String.concat "\n" |> Exceptional.ofString
    with 
    | ex -> Error ex

let uploadFileAsync (session:Session) fromPath toPath = async {
    return uploadFile session fromPath toPath
}

let downloadFile (session:Session) fromPath toPath =
    try
        let transferOptions = new TransferOptions();
        transferOptions.TransferMode <- TransferMode.Binary;
        transferOptions.OverwriteMode <- OverwriteMode.Overwrite
 
        let transferResult = session.GetFiles(fromPath, toPath, false, transferOptions);
 
        if transferResult.IsSuccess then
            Ok toPath
        else
            transferResult.Failures |> Seq.map (fun ex -> ex.Message) |> String.concat "\n" |> Exceptional.ofString
    with 
    | ex -> Error ex

let downloadFileAsync (session:Session) fromPath toPath = async {
    return downloadFile session fromPath toPath 
}

let deleteRemoteFile (session:Session) filePath =
    try
        let removalResult = session.RemoveFiles(filePath);
 
        if removalResult.IsSuccess then
            Ok filePath
        else
            removalResult.Failures |> Seq.map (fun ex -> ex.Message) |> String.concat "\n" |> Exceptional.ofString
    with 
    | ex -> Error ex

let deleteRemoteFileAsync (session:Session) filePath = async {
    return deleteRemoteFile session filePath
}

let fileExists (session:Session) filePath =
    try
        session.FileExists filePath |> Ok
    with 
    | ex -> Error ex

let fileExistsAsync (session:Session) filePath = async {
    return fileExists session filePath
}

let copyFileBetweenInstances timeout (fromInstance:Domain.OracleInstance.OracleInstance) fromPath (toInstance:Domain.OracleInstance.OracleInstance) toPath = asyncResult {
    let currentLocation = System.Net.Dns.GetHostName()
    let! fromFile = asyncResult {
        if currentLocation = fromInstance.Server then
            return fromPath
        else
            use! session = fromInstance |> newSessionFromInstance timeout
            let toFile =
                if currentLocation = toInstance.Server then toPath
                else System.IO.Path.GetTempFileName()
            return! downloadFileAsync session fromPath toFile
    }
    if currentLocation <> toInstance.Server then
        use! session = toInstance |> newSessionFromInstance timeout
        let toFile =
            if toPath.EndsWith('/') || toPath.EndsWith('\\') then
                toPath + System.IO.Path.GetFileName(fromPath)
            else
                toPath
        return! uploadFileAsync session fromFile toFile
    else
        return toPath
}

let copyFilesBetweenInstances timeout (fromInstance:Domain.OracleInstance.OracleInstance) (fromPath:string seq) (toInstance:Domain.OracleInstance.OracleInstance) (toPath:string seq) = asyncResult {
    if (fromPath |> Seq.isEmpty) || (toPath |> Seq.isEmpty) then
        return! Error ("fromPath and toPath must not be empty" |> exn)
    let toPath =
        if toPath |> Seq.length = 1 then
            let path = toPath |> Seq.head
            if path.EndsWith('/') || path.EndsWith('\\') then
                fromPath |> Seq.map (fun from -> path + System.IO.Path.GetFileName(from))
            else
                toPath
        else
            toPath
    if (fromPath |> Seq.length) <> (toPath |> Seq.length) then
        return! Error ("fromPath and toPath must have the same size" |> exn)
    let currentLocation = System.Net.Dns.GetHostName()
    let! fromFiles = asyncResult {
        if currentLocation = fromInstance.Server then
            return fromPath |> List.ofSeq
        else
            use! session = fromInstance |> newSessionFromInstance timeout
            let toFiles =
                if currentLocation = toInstance.Server then toPath
                else fromPath |> Seq.map (fun _ -> System.IO.Path.GetTempFileName())
            return!
                Seq.zip fromPath toFiles
                |> Seq.map (fun (fromFile, toFile) -> downloadFileAsync session fromFile toFile)
                |> List.ofSeq
                |> AsyncResult.sequenceS
    }
    if currentLocation <> toInstance.Server then
        use! session = toInstance |> newSessionFromInstance timeout
        return! 
            Seq.zip fromFiles toPath
            |> Seq.map (fun (fromFile, toFile) -> uploadFileAsync session fromFile toFile)
            |> List.ofSeq
            |> AsyncResult.sequenceS
    else
        return toPath |> List.ofSeq
}
