module Application.State

open Akkling

type StateMessage =
    | GetState
    | SetState of Application.DTO.State
    | AddMasterPDB of string * (string * string * string) list * string * string


let stateAgentBody initialState (ctx : Actor<_>) =
    let rec loop (state : Domain.State.State) = actor {
        let! n = ctx.Receive()
        match n with
        | GetState -> 
            System.Diagnostics.Debug.Print("State: PDB count = {0}\n", state.MasterPDBs.Length) |> ignore
            ctx.Sender() <! DTO.stateToDTO state
        | SetState newState -> 
            return! loop (DTO.DTOtoState newState)
        | AddMasterPDB (name, schemas, user, comment) -> 
            return! loop (
                state 
                |> Domain.State.addMasterPDB 
                    name 
                    (schemas |> List.map (fun (user, password, t) -> Domain.PDB.newSchema user password t)) 
                    user 
                    comment
            )
    }
    loop initialState

