module Infrastructure.Tests.GITTests

open Akkling.TestKit
open Akkling
open Xunit
open Infrastructure

let test = testDefault

[<Fact>]
let ``Add and commit file to GIT`` () = test <| fun tck ->
    let gitFolder = "../../../tests/git"
    let actor = tck |> GITActor.spawnForTests gitFolder
    let fileName = System.IO.Path.GetRandomFileName()
    let file = gitFolder + "/" + fileName
    System.IO.File.WriteAllText(file, "line 1\r\n")
    let logError name error = System.Diagnostics.Trace.Fail(sprintf "error for %s : %s" name error)
    actor <! GITActor.AddAndCommit (gitFolder, fileName, "test add & commit", (sprintf "Add and commit %s" fileName), logError)
    Async.Sleep 1000 |> Async.RunSynchronously
    System.IO.File.AppendAllText(file, "line 2\r\n")
    actor <! GITActor.Commit (gitFolder, fileName, "test commit", (sprintf "Commit %s" fileName), logError)
    Async.Sleep 1000 |> Async.RunSynchronously
    ()
