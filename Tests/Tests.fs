module Tests.All

open Xunit
open Domain.PDB
open Tests.PDBRepositoryGenericTests
open Swensen.Unquote
open Akkling
open Application.State
open Application
open Domain.State

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
    MasterPDBs = [ Domain.PDB.newMasterPDB "test1" [ Domain.PDB.newSchema "user" "password" "FusionInvest" ] ]
    MasterPDBVersions = [ newPDBVersion "test1" 1 "me" "no comment" ]
    LockedMasterPDBs = Map.empty
    UnusedMasterPDBVersions = Set.empty
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

    let response = 
        aref <? GetState 
        |> Async.RunSynchronously

    let tResp = box response :?> Application.DTO.State
    System.Diagnostics.Debug.Print("-> State: PDB count = {0}\n", tResp.MasterPDBs.Length)

    let response = 
        aref <? MasterPDBCreated ("test2", [ "user", "password", "FusionInvest" ], "me", "comment")
        |> Async.RunSynchronously |> box

    let tResp = response :?> Result<State, StateError>
    match tResp with
    | Ok state -> 
        System.Diagnostics.Debug.Print("-> State: PDB count = {0}\n", state.MasterPDBs.Length)
        System.Diagnostics.Debug.Print("-> State: PDB version count = {0}\n", state.MasterPDBVersions.Length)
    | Error error -> System.Diagnostics.Debug.Print("Error! {0}\n", buildStateErrorMessage error)

    let response = 
        aref <? MasterPDBVersionCreated ("test2", 2, "me", "comment")
        |> Async.RunSynchronously |> box

    let tResp = response :?> Result<State, StateError>
    match tResp with
    | Ok state -> 
        System.Diagnostics.Debug.Print("-> State: PDB version count = {0}\n", state.MasterPDBVersions.Length)
    | Error error -> System.Diagnostics.Debug.Print("Error! {0}\n", buildStateErrorMessage error)
