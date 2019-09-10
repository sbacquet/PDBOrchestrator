module Tests.All

open Xunit
open Domain.PDB
open Tests.PDBRepositoryGenericTests
open Swensen.Unquote
open Akkling
open Application.State
open Application

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

let domainState : Domain.State.State = { 
    Domain.State.MasterPDBs = [ Domain.PDB.newMasterPDB "test1" [ Domain.PDB.newSchema "user" "password" "FusionInvest" ] ]
    Domain.State.MasterPDBVersions = [ newPDBVersion "test1" 1 "me" "no comment" ]
    Domain.State.LockedMasterPDBs = Map.empty
}

[<Fact>]
let ``Get state`` () =
    let currentState = DTO.stateToDTO domainState
    test <@ currentState.MasterPDBs.Length = 1 @>
    test <@ currentState.MasterPDBs.[0].Name = "test1" @>
    test <@ currentState.MasterPDBs.[0].Versions.Length = 1 @>
    test <@ currentState.MasterPDBs.[0].Versions.[0].Version = 1 @>
    test <@ currentState.MasterPDBs.[0].Locked = false @>
    ()

[<Fact>]
let ``Test Akkling`` () =
    use system = System.create "my-system" <| Configuration.defaultConfig()
    let aref = spawn system "StateAgent" <| props (stateAgentBody domainState)

    let response = aref <? GetState |> Async.RunSynchronously
    let tResp = box response :?> Application.DTO.State
    System.Diagnostics.Debug.Print("-> State: PDB count = {0}\n", tResp.MasterPDBs.Length) |> ignore

    System.Threading.Thread.Sleep 5000
