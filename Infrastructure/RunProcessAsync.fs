module Infrastructure.RunProcessAsync

open System.Diagnostics
open System
open System.Threading.Tasks
open System.Threading

type Microsoft.FSharp.Control.Async with
    static member AwaitTask (t : Task<'T>, timeout : int option) =
        async {
            use cts = new CancellationTokenSource()
            use timer = Task.Delay (timeout |> Option.defaultValue -1, cts.Token)
            try
                let! completed = Async.AwaitTask <| Task.WhenAny(t, timer)
                if completed = (t :> Task) then
                    cts.Cancel ()
                    let! result = Async.AwaitTask t
                    return Ok result
                else return Error ("unknown error" |> exn)
            with ex -> return Error ex
        }

let private runProcessAsyncInternal (timeout:System.TimeSpan option) (proc:Process) = async {
    let tcs = new TaskCompletionSource<int>()
    proc.Exited.Add(fun _ -> tcs.TrySetResult(proc.ExitCode) |> ignore)
    proc.OutputDataReceived.Add(fun args -> Console.WriteLine(args.Data))
    proc.ErrorDataReceived.Add(fun args -> Console.Error.WriteLine(args.Data))

    try
        let started = proc.Start()
        if not started then
            let error = sprintf "Could not start process: %A" proc
            tcs.SetException(exn error)
        else
            proc.BeginOutputReadLine()
            proc.BeginErrorReadLine()
    with ex -> tcs.SetException(ex)
    return! Async.AwaitTask(tcs.Task, timeout |> Option.map (fun t -> (int)t.TotalMilliseconds))
}

let runProcessAsync timeout fileName args = async {
    use proc = new Process()
    proc.StartInfo.FileName <- fileName
    args |> List.iter (fun arg -> proc.StartInfo.ArgumentList.Add(arg))
    proc.StartInfo.UseShellExecute <- false
    proc.StartInfo.CreateNoWindow <- true
    proc.StartInfo.RedirectStandardOutput <- true
    proc.StartInfo.RedirectStandardError <- true
    proc.EnableRaisingEvents <- true
    return! proc |> runProcessAsyncInternal timeout
}    
