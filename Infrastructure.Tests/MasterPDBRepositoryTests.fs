module Infrastructure.Tests.MasterPDBRepository

open Xunit
open Infrastructure.MasterPDBRepository
open Domain.MasterPDB
open Application.Common
open Akkling.TestKit
open Akkling

let [<Literal>]testFolder = __SOURCE_DIRECTORY__ + @"\tests\masterPDBs"
let onRepoFailure _ _ = raise
let gitParams : GitParams = {
    LogError = failwithf "Error for %s : %s"
    GetModifyComment = sprintf "Commit %s"
    GetAddComment = sprintf "Add %s"
}

[<Fact>]
let ``Put and get master PDB`` () = testDefault <| fun tck ->
    let gitActor = tck |> Infrastructure.GITActor.spawnForTests testFolder
    let pdb = newMasterPDB "test1" [ consSchema "toto" "toto" "Invest" ] "me" "comment1"
    let repo = NewMasterPDBRepository(onRepoFailure, gitActor, gitParams, testFolder, pdb) :> IMasterPDBRepository
    let repo' = repo.Put pdb
    let pdb' = repo'.Get()
    Assert.Equal(pdb, pdb')

[<Fact>]
let ``Save and load master PDB`` () = testDefault <| fun tck ->
    let gitActor = tck |> Infrastructure.GITActor.spawnForTests testFolder
    let pdbName = "test2"
    let pdb = newMasterPDB pdbName [ consSchema "toto" "toto" "Invest" ] "me" "comment1"
    let repo = NewMasterPDBRepository(onRepoFailure, gitActor, gitParams, testFolder, pdb) :> IMasterPDBRepository
    repo.Put pdb |> ignore
    let repo2 = MasterPDBRepository(onRepoFailure, gitActor, gitParams, testFolder, pdbName) :> IMasterPDBRepository
    let pdb2 = repo2.Get()
    Assert.Equal(pdb, pdb2)

[<Fact>]
let ``Update master PDB`` () = testDefault <| fun tck ->
    let gitActor = tck |> Infrastructure.GITActor.spawnForTests testFolder
    let pdb = newMasterPDB "test3" [ consSchema "toto" "toto" "Invest" ] "me" "comment1"
    let repo = NewMasterPDBRepository(onRepoFailure, gitActor, gitParams, testFolder, pdb) :> IMasterPDBRepository
    let repo' = repo.Put pdb
    let repo'' = repo'.Put { pdb with EditionState = Some (newEditionInfo "me") }
    let pdb' = repo''.Get()
    Assert.Equal("me", pdb'.EditionState.Value.Editor)

[<Fact>]
let ``Log error when saving master PDB`` () = testDefault <| fun tck ->
    let gitActor = tck |> Infrastructure.GITActor.spawnForTests testFolder
    let mutable passedHere = false
    let pdb = newMasterPDB "test1" [ consSchema "toto" "toto" "Invest" ] "me" "comment1"
    let repo = NewMasterPDBRepository((fun _ _ _ -> passedHere <- true), gitActor, gitParams, "*", pdb) :> IMasterPDBRepository
    let repo' = repo.Put pdb
    Assert.True(passedHere)
    Assert.Equal(repo, repo')

