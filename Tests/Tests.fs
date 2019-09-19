module Tests.All

open Xunit
open Domain.PDB
open Tests.PDBRepositoryGenericTests
open Swensen.Unquote
open Application
open Domain.OracleInstance

[<Fact>]
let ``Fake PDB repo in memory`` () =
    let mutable pdbMemoryRepo : MasterPDB list = List.Empty
    let cleanMasterPDBRepo () =
        pdbMemoryRepo <- List.Empty
    let registerMasterPDB pdb = 
        pdbMemoryRepo <- pdb :: pdbMemoryRepo
        Ok ()
    let getMasterPDB name = 
        let maybePDB = pdbMemoryRepo |> List.tryFind (fun p -> p.Name = name)
        match maybePDB with
        | None -> Error (Application.PDBRepository.Error.PDBDoesNotExist name)
        | Some pdb -> Ok pdb
    ``Add and find`` cleanMasterPDBRepo registerMasterPDB getMasterPDB

let instance1State = { 
    MasterPDBs = [ Domain.PDB.newMasterPDB "test1" [ Domain.PDB.newSchema "user" "password" "FusionInvest" ] ]
    MasterPDBVersions = [ newPDBVersion "test1" 1 "me" "no comment" ]
    LockedMasterPDBs = Map.empty
    UnusedMasterPDBVersions = Set.empty
}

[<Fact>]
let ``Get state`` () =
    let currentState = DTO.stateToDTO instance1State
    test <@ currentState.MasterPDBs.Length = 1 @>
    test <@ currentState.MasterPDBs.[0].Name = "test1" @>
    test <@ currentState.MasterPDBs.[0].Versions.Length = 1 @>
    test <@ currentState.MasterPDBs.[0].Versions.[0].Version = 1 @>
    test <@ currentState.MasterPDBs.[0].Locked = false @>
    ()

