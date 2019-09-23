module Application.MasterPDBActor

open Akkling
open Domain.MasterPDB

type Command =
| GetState
| PrepareForModification
| Commit of string * string
| Rollback

type Event = 
| MasterPDBCreated of string * (string * string * string) list * string * string
| MasterPDBVersionCreated of string * int * string * string

type Collaborators = {
    MasterPDBVersionActors: Map<string, IActorRef<Command>>
}


let masterPDBActorBody (initialMasterPDB : Domain.MasterPDB.MasterPDB) (ctx : Actor<Command>) =
    let rec loop masterPDB = actor {
        let! msg = ctx.Receive()
        match msg with
        | GetState -> 
            ctx.Sender() <! (masterPDB |> Application.DTO.MasterPDB.toDTO)
            return! loop masterPDB
        | PrepareForModification -> // TODO
            return! loop masterPDB
        | Commit (user, comment) -> // TODO
            return! loop masterPDB
        | Rollback -> // TODO
            return! loop masterPDB
    }
    loop initialMasterPDB

let masterPDBActorName (masterPDB:string) = Common.ActorName (sprintf "MasterPDB='%s'" (masterPDB.ToUpper() |> System.Uri.EscapeDataString))

let spawn (masterPDB : Domain.MasterPDB.MasterPDB) actorFactory =
    let (Common.ActorName actorName) = masterPDBActorName masterPDB.Name
    logDebugf actorFactory "Spawning actor %s for master PDB %s" actorName masterPDB.Name
    Akkling.Spawn.spawn actorFactory actorName <| props (masterPDBActorBody masterPDB)

