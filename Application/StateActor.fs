module Application.StateActor

open Akkling

type ReplyTo =
| Nobody
| Sender
| Actor of Akka.Actor.IActorRef

let reply replyTo (sender : Akka.Actor.ICanTell) from message =
    match replyTo with
    | Nobody -> ()
    | Sender -> sender.Tell(message, from)
    | Actor actor -> actor.Tell(message, from)

type StateMessage =
| GetState
| SetState of Domain.State.State
| StateSet
| MasterPDBCreated of string * (string * string * string) list * string * string
| MasterPDBVersionCreated of string * int * string * string
| Transfer of string

let getNewState oldState newStateResult =
    match newStateResult with
    | Ok state -> state
    | Error error -> 
        System.Diagnostics.Debug.Print("Error: {0}\n", error.ToString()) |> ignore
        oldState

let stateActorBody initialState (ctx : Actor<_>) =
    let rec loop (state : Domain.State.State) = actor {
        let! n = ctx.Receive()
        match n with
        | GetState -> 
            System.Diagnostics.Debug.Print("State: PDB count = {0}\n", state.MasterPDBs.Length) |> ignore
            ctx.Sender() <! DTO.stateToDTO state
            return! loop state
        | SetState newState -> 
            ctx.Sender() <! StateSet
            return! loop newState
        | StateSet ->
            printfn "State set"
            return! loop state
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
        | Transfer target ->
            let targetActor =
                (select ctx <| sprintf "../%s" target).ResolveOne(System.TimeSpan.FromSeconds(1.))
                |> Async.RunSynchronously 
            targetActor <<! SetState state
            return! loop state
    }
    loop initialState

