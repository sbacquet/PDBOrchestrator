module Application.MasterPDBActor

open Akkling
open Domain.MasterPDB
open Domain.OracleInstance
open Application.PendingRequest
open Application.Oracle
open Akka.Actor

type Command =
| GetState
| GetInternalState // responds with MasterPDB
| SetInternalState of MasterPDB
| PrepareForModification of WithRequestId<int, string> // responds with WithRequestId<PrepareForModificationResult>
| Commit of WithRequestId<string, string>
| Rollback of RequestId // responds with WithRequestId<RollbackResult>

type PrepareForModificationResult = 
| Locked of MasterPDB
| Prepared of MasterPDB
| PreparationFailure of string

type RollbackResult =
| RolledBack of MasterPDB
| RollbackFailure of string

type Collaborators = {
    MasterPDBVersionActors: Map<string, IActorRef<Command>>
}

let spawnCollaborators masterPDB ctx = {
    MasterPDBVersionActors = Map.empty
}

let masterPDBActorBody (instance:OracleInstance) longTaskExecutor (initialMasterPDB : Domain.MasterPDB.MasterPDB) (ctx : Actor<_>) =

    let collaborators = spawnCollaborators initialMasterPDB ctx
    let manifestPath = sprintf "%s/%s" instance.MasterPDBManifestsPath

    let rec loop masterPDB (requests : RequestMap<Command>) = actor {

        let! (msg:obj) = ctx.Receive()
        
        match msg with
        | :? Command as command -> 
            match command with
            | GetState -> 
                ctx.Sender() <! (masterPDB |> Application.DTO.MasterPDB.toDTO)
                return! loop masterPDB requests

            | GetInternalState ->
                ctx.Sender() <! masterPDB
                return! loop masterPDB requests

            | SetInternalState state ->
                return! loop state requests

            | PrepareForModification (requestId, version, locker) as command ->
                let sender = ctx.Sender().Retype<WithRequestId<PrepareForModificationResult>>()
                let latestVersion = masterPDB |> getLatestAvailableVersion
                if (latestVersion.Number <> version) then sender <! (requestId, PreparationFailure (sprintf "version %d is not the latest version (%d) of \"%s\"" version latestVersion.Number masterPDB.Name))
                let newMasterPDBMaybe = masterPDB |> lock locker System.DateTime.Now
                match newMasterPDBMaybe with
                | Ok newMasterPDB -> 
                    let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                    longTaskExecutor <! OracleLongTaskExecutor.ImportPDB (requestId, (manifestPath masterPDB.Manifest), instance.MasterPDBManifestsPath, masterPDB.Name)
                    sender <! (requestId, Locked newMasterPDB)
                    return! loop newMasterPDB newRequests
                | Error error -> 
                    sender <! (requestId, PreparationFailure error)
                    return! loop masterPDB requests

            | Commit (requestId, user, comment) -> // TODO
                return! loop masterPDB requests

            | Rollback requestId ->
                let sender = ctx.Sender().Retype<WithRequestId<RollbackResult>>()
                let newMasterPDBMaybe = masterPDB |> unlock
                match newMasterPDBMaybe with
                | Ok _ ->
                    let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                    longTaskExecutor <! OracleLongTaskExecutor.DeletePDB (requestId, masterPDB.Name)
                    return! loop masterPDB newRequests
                | Error error -> 
                    sender <! (requestId, RollbackFailure error)
                    return! loop masterPDB requests

        | :? WithRequestId<OraclePDBResult> as requestResponse ->

            let (requestId, result) = requestResponse
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId

            match requestMaybe with
            | None -> 
                logWarningf ctx "Request %s not found" <| requestId.ToString()
                return! loop masterPDB requests

            | Some request -> 
                match request.Command with
                | PrepareForModification _ -> 
                    let sender = request.Requester.Retype<WithRequestId<PrepareForModificationResult>>()
                    match result with
                    | Ok _ ->
                        sender <! (requestId, Prepared masterPDB)
                    | Error error ->
                        sender <! (requestId, PreparationFailure (error.ToString()))
                    return! loop masterPDB newRequests
                | Rollback _ ->
                    let sender = request.Requester.Retype<WithRequestId<RollbackResult>>()
                    let newMasterPDBMaybe = masterPDB |> unlock
                    match newMasterPDBMaybe with
                    | Ok newMasterPDB ->
                        match result with
                        | Ok _ ->
                            sender <! (requestId, RolledBack newMasterPDB)
                        | Error error ->
                            sender <! (requestId, RollbackFailure (error.ToString()))
                        return! loop newMasterPDB newRequests
                    | Error error -> failwithf "Fatal error"
                | _ -> failwithf "Fatal error"

        | _ -> return! loop masterPDB requests
    }

    loop initialMasterPDB Map.empty

let masterPDBActorName (masterPDB:string) = Common.ActorName (sprintf "MasterPDB='%s'" (masterPDB.ToUpper() |> System.Uri.EscapeDataString))

let spawn (instance:OracleInstance) (longTaskExecutor:IActorRef<Application.OracleLongTaskExecutor.Command>) (masterPDB : Domain.MasterPDB.MasterPDB) (actorFactory:IActorRefFactory) =
    let (Common.ActorName actorName) = masterPDBActorName masterPDB.Name
    Akkling.Spawn.spawn actorFactory actorName <| props (masterPDBActorBody instance longTaskExecutor masterPDB)

