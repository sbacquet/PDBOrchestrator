module Application.State

open Akkling

type StateMessage =
    | GetState
    | SetState of Application.DTO.State
    | MasterPDBCreated of string * (string * string * string) list * string * string
    | MasterPDBVersionCreated of string * int * string * string

let getNewState oldState newStateResult =
    match newStateResult with
    | Ok state -> state
    | Error error -> 
        System.Diagnostics.Debug.Print("Error: {0}\n", error.ToString()) |> ignore
        oldState

let stateAgentBody initialState (ctx : Actor<_>) =
    let rec loop (state : Domain.State.State) = actor {
        let! n = ctx.Receive()
        match n with
        | GetState -> 
            System.Diagnostics.Debug.Print("State: PDB count = {0}\n", state.MasterPDBs.Length) |> ignore
            ctx.Sender() <! DTO.stateToDTO state
        | SetState newState -> 
            return! loop (DTO.DTOtoState newState)
        | MasterPDBCreated (name, schemas, user, comment) -> 
            let newStateResult = 
                state 
                |> Domain.State.addMasterPDBToState 
                    name 
                    (schemas |> List.map (fun (user, password, t) -> Domain.PDB.newSchema user password t)) 
                    user 
                    comment
            ctx.Sender() <! newStateResult
            return! loop (getNewState state newStateResult)
        | MasterPDBVersionCreated (pdb, version, createdBy, comment) ->
            let newStateResult =
                state |> Domain.State.addMasterPDBVersionToState pdb version createdBy comment 
            ctx.Sender() <! newStateResult
            return! loop (getNewState state newStateResult)
    }
    loop initialState

type MasterPDBMessage =
| PrepareForModification
| Commit of string * string
| Rollback

type OracleDiskIntensiveAction =
| InstanciateMasterPDB of string (* manifest *)
| InstanciateTestPDB of string (* manifest *)
| ExportPDB