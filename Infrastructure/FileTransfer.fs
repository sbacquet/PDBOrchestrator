module Infrastructure.FileTransfer

open WinSCP

let uploadFile timeout fromPath toHost toPath user pass hostKey =
    let sessionParams = SessionOptions()
    sessionParams.Protocol <- Protocol.Scp
    sessionParams.HostName <- toHost
    sessionParams.UserName <- user
    sessionParams.Password <- pass
    sessionParams.SshHostKeyFingerprint <- hostKey
    if timeout |> Option.isSome then sessionParams.Timeout <- timeout.Value

    use session = new Session()
    session.ExecutablePath <- sprintf "%s\WinSCP.exe" (System.IO.Directory.GetCurrentDirectory())
    session.Open(sessionParams)
 
    let transferOptions = new TransferOptions();
    transferOptions.TransferMode <- TransferMode.Binary;
    transferOptions.OverwriteMode <- OverwriteMode.Overwrite
 
    let transferResult = session.PutFiles(fromPath, toPath, false, transferOptions);
 
    if transferResult.IsSuccess then
        Ok toPath
    else
        transferResult.Failures |> Seq.map (fun ex -> ex.Message) |> String.concat "\n" |> exn |> Error

let uploadFileAsync timeout fromPath toHost toPath user pass hostKey = async {
    return uploadFile timeout fromPath toHost toPath user pass hostKey
}
