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
