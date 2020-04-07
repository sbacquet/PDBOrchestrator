module Infrastructure.Tests.RunProcessAsync

open Infrastructure.RunProcessAsync

open Xunit

[<Fact>]
let ``Run process async`` () =
    let x = runProcessAsync None "ipconfig" [ "/all" ] |> Async.RunSynchronously
    Assert.Equal(Ok 0, x)
    let x = runProcessAsync None "ipconfig" [ "/zzz" ] |> Async.RunSynchronously
    Assert.Equal(Ok 1, x)
    let x = runProcessAsync None "zzz" [] |> Async.RunSynchronously
    Assert.True(match x with | Error _ -> true | Ok _ -> false)
