module Application.MasterPDBActor

open Akkling

type Command =
| PrepareForModification
| Commit of string * string
| Rollback

type Event = 
| MasterPDBCreated of string * (string * string * string) list * string * string
| MasterPDBVersionCreated of string * int * string * string

let masterPDBActorBody (ctx : Actor<Command>) =
    let rec loop () = actor {
        let! msg = ctx.Receive()
        match msg with
        | PrepareForModification -> // TODO
            return! loop ()
        | Commit (user, comment) ->
            return! loop ()
        | Rollback ->
            return! loop ()
    }
    loop ()
