module Tests.PDBRepositoryGenericTests

open Application.PDBRepository
open Domain.PDB
open Swensen.Unquote


let ``Add and find`` (cleanMasterPDBRepo : CleanMasterPDBRepo) (registerMasterPDB : RegisterMasterPDB) (getMasterPDB : GetMasterPDB) =
    let pdbName = "test1"
    let pdb = newMasterPDB pdbName []
    cleanMasterPDBRepo ()
    let result = registerMasterPDB pdb
    match result with
            | Error e -> failwith (errorToMessage e)
            | Ok _ -> ()
    let x = getMasterPDB pdbName
    match x with
    | Error e -> failwith (errorToMessage e)
    | Ok v -> 
        test <@ v.Name = pdbName @>

