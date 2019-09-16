module Tests.Actors

open Akkling
open Akkling.TestKit
open Xunit
open Application.StateActor
open Domain.PDB
open Domain.State

let domainState : Domain.State.State = { 
    MasterPDBs = [ Domain.PDB.newMasterPDB "test1" [ Domain.PDB.newSchema "user" "password" "FusionInvest" ] ]
    MasterPDBVersions = [ newPDBVersion "test1" 1 "me" "no comment" ]
    LockedMasterPDBs = Map.empty
    UnusedMasterPDBVersions = Set.empty
}

[<Fact>]
let ``Test StateAgent basics`` () = testDefault <| fun tck ->
    let aref = spawn tck "StateAgent1" <| props (stateActorBody domainState)

    let response : Application.DTO.State = 
        aref <? GetState
        |> Async.RunSynchronously
        
    System.Diagnostics.Debug.Print("-> State: PDB count = {0}\n", response.MasterPDBs.Length)

    let response = 
        aref <? MasterPDBCreated ("test2", [ "user", "password", "FusionInvest" ], "me", "comment")
        |> Async.RunSynchronously

    match response with
    | Ok state -> 
        System.Diagnostics.Debug.Print("-> State: PDB count = {0}\n", state.MasterPDBs.Length)
        System.Diagnostics.Debug.Print("-> State: PDB version count = {0}\n", state.MasterPDBVersions.Length)
    | Error error -> System.Diagnostics.Debug.Print("Error! {0}\n", buildStateErrorMessage error)

    let response = 
        aref <? MasterPDBVersionCreated ("test2", 2, "me", "comment")
        |> Async.RunSynchronously

    match response with
    | Ok state -> 
        System.Diagnostics.Debug.Print("-> State: PDB version count = {0}\n", state.MasterPDBVersions.Length)
    | Error error -> System.Diagnostics.Debug.Print("Error! {0}\n", buildStateErrorMessage error)

[<Fact>]
let ``Test state transfer`` () = testDefault <| fun tck ->
    let aref1 = spawn tck "StateAgent1" <| props (stateActorBody domainState)
    let aref2 = spawn tck "StateAgent2" <| props (stateActorBody (newState ()))

    aref1 <! TransferState "StateAgent2"

    expectMsg tck StateSet

