module Infrastructure.GIT

open System.Diagnostics

let [<Literal>]gitCommand = "git"
let addFileArgument file = sprintf @"add -- ""%s""" file
let commitFileArgument comment file = sprintf @"commit -m ""%s"" -- ""%s""" comment file

let runGitCommand folder args =
    let psi:ProcessStartInfo = ProcessStartInfo()
    psi.WorkingDirectory <- folder
    psi.FileName <- gitCommand
    psi.Arguments <- args
    psi.RedirectStandardOutput <- true
    psi.RedirectStandardError <- true
    try
        use proc = Process.Start(psi)
        proc.WaitForExit()
        match proc.ExitCode with
        | 0 -> Ok <| proc.StandardOutput.ReadToEnd()
        | _ -> Error <| proc.StandardError.ReadToEnd()
    with 
        ex -> Error <| sprintf "cannot run Git : %s" ex.Message

let addFile folder file = runGitCommand folder (addFileArgument file)

let commitFile folder comment file = runGitCommand folder (commitFileArgument comment file)

let initRepo folder = runGitCommand folder "init"
