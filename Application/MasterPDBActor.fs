module Application.MasterPDBActor

open Akkling

type Command =
| PrepareForModification
| Commit of string * string
| Rollback

type Event = 
| MasterPDBCreated of string * (string * string * string) list * string * string
| MasterPDBVersionCreated of string * int * string * string

let masterPDBActorBody masterPDB (ctx : Actor<Command>) =
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

let masterPDBActorName (masterPDB:string) = Common.ActorName (sprintf "MasterPDB='%s'" (masterPDB.ToUpper() |> System.Uri.EscapeDataString))

let spawn masterPDB actorFactory =
    let (Common.ActorName actorName) = masterPDBActorName masterPDB
    logDebugf actorFactory "Spawning actor %s for master PDB %s" actorName masterPDB
    Akkling.Spawn.spawn actorFactory actorName <| props (masterPDBActorBody masterPDB)

