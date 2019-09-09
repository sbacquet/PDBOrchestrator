module Tests.All

open System
open Xunit
open Swensen.Unquote
open Domain.PDB
open Tests.PDBRepositoryGenericTests

[<Fact>]
let ``Fake PDB repo 1`` () =
    let mutable pdbMemoryRepo : MasterPDBs = []
    let registerMasterPDB pdb = 
        pdbMemoryRepo <- pdb :: pdbMemoryRepo
        Ok ()
    let getMasterPDB name = 
        let maybePDB = pdbMemoryRepo |> List.tryFind (fun p -> p.Name = name)
        match maybePDB with
        | None -> Error (Application.PDBRepository.Error.PDBDoesNotExist name)
        | Some pdb -> Ok pdb
    ``Add and find`` registerMasterPDB getMasterPDB

[<Fact>]
let ``Fake PDB repo 2`` () =
    let mutable pdbMemoryRepo : MasterPDBs = []
    let registerMasterPDB pdb = 
        //pdbMemoryRepo <- pdb :: pdbMemoryRepo
        Ok ()
    let getMasterPDB name = 
        let maybePDB = pdbMemoryRepo |> List.tryFind (fun p -> p.Name = name)
        match maybePDB with
        | None -> Application.PDBRepository.PDBResult.Error (Application.PDBRepository.Error.PDBDoesNotExist name)
        | Some pdb -> Ok pdb
    ``Add and find`` registerMasterPDB getMasterPDB