module Infrastructure.Tests.MasterPDBRepository

open Xunit
open Infrastructure.MasterPDBRepository
open Domain.MasterPDB
open Application.Common

let [<Literal>]testFolder = __SOURCE_DIRECTORY__ + @"\tests\masterPDBs"
let onRepoFailure _ _ = raise

[<Fact>]
let ``Put and get master PDB`` () =
    let pdbName = "test1"
    let repo = MasterPDBRepository(onRepoFailure, None, testFolder, pdbName) :> IMasterPDBRepository
    let pdb = newMasterPDB pdbName [ consSchema "toto" "toto" "Invest" ] "me" "comment1"
    let repo' = repo.Put pdb
    let pdb' = repo'.Get()
    Assert.Equal(pdb, pdb')

[<Fact>]
let ``Save and load master PDB`` () =
    let pdbName = "test2"
    let repo = MasterPDBRepository(onRepoFailure, None, testFolder, pdbName) :> IMasterPDBRepository
    let pdb = newMasterPDB pdbName [ consSchema "toto" "toto" "Invest" ] "me" "comment1"
    repo.Put pdb |> ignore
    let repo2 = MasterPDBRepository(onRepoFailure, None, testFolder, pdbName) :> IMasterPDBRepository
    let pdb2 = repo2.Get()
    Assert.Equal(pdb, pdb2)

[<Fact>]
let ``Update master PDB`` () =
    let pdbName = "test3"
    let repo = MasterPDBRepository(onRepoFailure, None, testFolder, pdbName) :> IMasterPDBRepository
    let pdb = newMasterPDB pdbName [ consSchema "toto" "toto" "Invest" ] "me" "comment1"
    let repo' = repo.Put pdb
    let repo'' = repo'.Put { pdb with EditionState = Some (newEditionInfo "me") }
    let pdb' = repo''.Get()
    Assert.Equal("me", pdb'.EditionState.Value.Editor)

[<Fact>]
let ``Log error when saving master PDB`` () =
    let pdbName = "test1"
    let mutable passedHere = false
    let repo = MasterPDBRepository((fun _ _ _ -> passedHere <- true), None, "*", pdbName) :> IMasterPDBRepository
    let pdb = newMasterPDB pdbName [ consSchema "toto" "toto" "Invest" ] "me" "comment1"
    let repo' = repo.Put pdb
    Assert.True(passedHere)
    Assert.Equal(repo, repo')

