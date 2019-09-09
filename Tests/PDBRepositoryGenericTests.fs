module Tests.PDBRepositoryGenericTests

open Application.PDBRepository
open Domain.PDB
open Swensen.Unquote


let ``Add and find`` (registerMasterPDB : RegisterMasterPDB) (getMasterPDB : GetMasterPDB) =
    let pdbName = "test1"
    let pdb = newMasterPDB pdbName []
    let result = registerMasterPDB pdb
    match result with
            | Error e -> failwith (errorToMessage e)
            | Ok _ -> ()
    let x = getMasterPDB pdbName
    match x with
    | Error e -> failwith (errorToMessage e)
    | Ok v -> 
        test <@ v.Name = pdbName @>
        test <@ v.LatestVersion = 1 @>

