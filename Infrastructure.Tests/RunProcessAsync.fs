module Infrastructure.Tests.RunProcessAsync

open Infrastructure.RunProcessAsync

open Xunit

[<Fact>]
let ``Run process async`` () =
    let x = runProcessAsync None "impdp" [ "help=y" ] |> Async.RunSynchronously
    Assert.Equal(Ok 0, x)
    let x = runProcessAsync None "impdp" [ "toto/toto@toto" ] |> Async.RunSynchronously
    Assert.Equal(Ok 1, x)
