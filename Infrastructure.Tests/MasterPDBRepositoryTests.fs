module Infrastructure.Tests.MasterPDBRepository

open Xunit
open Infrastructure.MasterPDBRepository
open Domain.MasterPDB
open Application.Common

let [<Literal>]testFolder = __SOURCE_DIRECTORY__ + @"\tests\masterPDBs"

[<Fact>]
let ``Put and get master PDB`` () =
    let pdbName = "test1"
    let repo = loadMasterPDBRepository testFolder [] :> IMasterPDBRepository
    let pdb = newMasterPDB pdbName [ consSchema "toto" "toto" "Invest" ] "me" System.DateTime.Now "comment1"
    let repo' = repo.Put pdbName pdb
    let pdb' = repo'.Get pdbName
    Assert.Equal(pdb, pdb')

[<Fact>]
let ``Save and load master PDB`` () =
    let pdbName = "test2"
    let repo = loadMasterPDBRepository testFolder [] :> IMasterPDBRepository
    let pdb = newMasterPDB pdbName [ consSchema "toto" "toto" "Invest" ] "me" System.DateTime.Now "comment1"
    repo.Put pdbName pdb |> ignore
    let repo2 = loadMasterPDBRepository testFolder [ pdbName ] :> IMasterPDBRepository
    let pdb2 = repo2.Get pdbName
    Assert.Equal(pdb, pdb2)

[<Fact>]
let ``Update master PDB`` () =
    let pdbName = "test3"
    let repo = loadMasterPDBRepository testFolder [] :> IMasterPDBRepository
    let pdb = newMasterPDB pdbName [ consSchema "toto" "toto" "Invest" ] "me" System.DateTime.Now "comment1"
    let repo' = repo.Put pdbName pdb
    let repo'' = repo'.Put pdbName { pdb with LockState = Some (consLockInfo "me" System.DateTime.Now) }
    let pdb' = repo''.Get pdbName
    Assert.Equal("me", pdb'.LockState.Value.Locker)
