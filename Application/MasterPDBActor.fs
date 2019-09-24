module Application.MasterPDBActor

open Akkling
open Domain.MasterPDB
open Domain.OracleInstance
open Application.PendingRequest
open Application.Oracle
open Akka.Actor
open Domain.MasterPDBVersion

type Command =
| GetState
| GetInternalState // responds with MasterPDB
| SetInternalState of MasterPDB
| PrepareForModification of WithRequestId<int, string> // responds with WithRequestId<PrepareForModificationResult>
| Commit of WithRequestId<string, string>
| Rollback of RequestId // responds with WithRequestId<RollbackResult>
| SnapshotVersion of WithRequestId<int, string> // responds with WithRequest<SnapshotResult>

type PrepareForModificationResult = 
| Locked of MasterPDB
| Prepared of MasterPDB
| PreparationFailure of string

type RollbackResult =
| RolledBack of MasterPDB
| RollbackFailure of string

type SnapshotResult =
| Snapshot of int * string
| SnapshotFailure of string

type Collaborators = {
    OracleAPI: IOracleAPI
    MasterPDBVersionActors: Map<int, IActorRef<MasterPDBVersionActor.Command>>
    OracleLongTaskExecutor: IActorRef<OracleLongTaskExecutor.Command>
    OracleDiskIntensiveTaskExecutor : IActorRef<OracleDiskIntensiveActor.Command>
}

let getOrSpawnVersionActor (version:MasterPDBVersion) collaborators ctx =
    let versionActorMaybe = collaborators.MasterPDBVersionActors |> Map.tryFind version.Number
    match versionActorMaybe with
    | Some versionActor -> collaborators, versionActor
    | None -> 
        let versionActor = 
            ctx |> MasterPDBVersionActor.spawn 
                collaborators.OracleAPI
                collaborators.OracleLongTaskExecutor
                collaborators.OracleDiskIntensiveTaskExecutor
                version
        
        { collaborators with MasterPDBVersionActors = collaborators.MasterPDBVersionActors.Add(version.Number, versionActor) }, 
        versionActor

let masterPDBActorBody oracleAPI (instance:OracleInstance) oracleLongTaskExecutor oracleDiskIntensiveTaskExecutor (initialMasterPDB : Domain.MasterPDB.MasterPDB) (ctx : Actor<_>) =

    let manifestPath = sprintf "%s/%s" instance.MasterPDBManifestsPath

    let rec loop masterPDB (requests : RequestMap<Command>) collaborators = actor {

        let! (msg:obj) = ctx.Receive()
        
        match msg with
        | :? Command as command -> 
            match command with
            | GetState -> 
                ctx.Sender() <! (masterPDB |> Application.DTO.MasterPDB.toDTO)
                return! loop masterPDB requests collaborators

            | GetInternalState ->
                ctx.Sender() <! masterPDB
                return! loop masterPDB requests collaborators

            | SetInternalState state ->
                return! loop state requests collaborators

            | PrepareForModification (requestId, version, locker) as command ->
                let sender = ctx.Sender().Retype<WithRequestId<PrepareForModificationResult>>()
                let latestVersion = masterPDB |> getLatestAvailableVersion
                if (latestVersion.Number <> version) then sender <! (requestId, PreparationFailure (sprintf "version %d is not the latest version (%d) of \"%s\"" version latestVersion.Number masterPDB.Name))
                let newMasterPDBMaybe = masterPDB |> lock locker System.DateTime.Now
                match newMasterPDBMaybe with
                | Ok newMasterPDB -> 
                    let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                    oracleDiskIntensiveTaskExecutor <! OracleDiskIntensiveActor.ImportPDB (requestId, (manifestPath masterPDB.Manifest), instance.MasterPDBDestPath, masterPDB.Name)
                    sender <! (requestId, Locked newMasterPDB)
                    return! loop newMasterPDB newRequests collaborators
                | Error error -> 
                    sender <! (requestId, PreparationFailure error)
                    return! loop masterPDB requests collaborators

            | Commit (requestId, user, comment) -> // TODO
                return! loop masterPDB requests collaborators

            | Rollback requestId ->
                let sender = ctx.Sender().Retype<WithRequestId<RollbackResult>>()
                let newMasterPDBMaybe = masterPDB |> unlock
                match newMasterPDBMaybe with
                | Ok _ ->
                    let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                    oracleLongTaskExecutor <! OracleLongTaskExecutor.DeletePDB (requestId, masterPDB.Name)
                    return! loop masterPDB newRequests collaborators
                | Error error -> 
                    sender <! (requestId, RollbackFailure error)
                    return! loop masterPDB requests collaborators
            
            | SnapshotVersion (requestId, versionNumber, snapshotName) ->
                let sender = ctx.Sender().Retype<WithRequestId<SnapshotResult>>()
                let versionMaybe = masterPDB.Versions.TryFind(versionNumber)
                match versionMaybe with
                | None -> 
                    sender <! (requestId, SnapshotFailure (sprintf "version %d of master PDB %s does not exist" versionNumber masterPDB.Name))
                    return! loop masterPDB requests collaborators
                | Some version -> 
                    let newCollabs, versionActor = getOrSpawnVersionActor version collaborators ctx
                    let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                    versionActor <! MasterPDBVersionActor.Snapshot (requestId, masterPDB.Manifest, instance.SnapshotSourcePDBDestPath, snapshotName, instance.SnapshotPDBDestPath)
                    return! loop masterPDB newRequests newCollabs

        | :? WithRequestId<OraclePDBResult> as requestResponse ->

            let (requestId, result) = requestResponse
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId

            match requestMaybe with
            | None -> 
                logWarningf ctx "Request %s not found" <| requestId.ToString()
                return! loop masterPDB requests collaborators

            | Some request -> 
                match request.Command with
                | PrepareForModification _ -> 
                    let sender = request.Requester.Retype<WithRequestId<PrepareForModificationResult>>()
                    match result with
                    | Ok _ ->
                        sender <! (requestId, Prepared masterPDB)
                    | Error error ->
                        sender <! (requestId, PreparationFailure (error.ToString()))
                    return! loop masterPDB newRequests collaborators

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
                        return! loop newMasterPDB newRequests collaborators
                    | Error error -> failwithf "Fatal error"

                | SnapshotVersion (_, versionNumber, snapshotName) ->
                    let sender = request.Requester.Retype<WithRequestId<SnapshotResult>>()
                    match result with
                    | Ok _ ->
                        sender <! (requestId, Snapshot (versionNumber, snapshotName))
                    | Error error ->
                        sender <! (requestId, SnapshotFailure (error.ToString()))
                    return! loop masterPDB newRequests collaborators

                | _ -> failwithf "Fatal error"

        | _ -> return! loop masterPDB requests collaborators
    }

    let collaborators = { 
        OracleAPI = oracleAPI
        MasterPDBVersionActors = Map.empty
        OracleLongTaskExecutor = oracleLongTaskExecutor
        OracleDiskIntensiveTaskExecutor = oracleDiskIntensiveTaskExecutor 
    }
    loop initialMasterPDB Map.empty collaborators

let masterPDBActorName (masterPDB:string) = Common.ActorName (sprintf "MasterPDB='%s'" (masterPDB.ToUpper() |> System.Uri.EscapeDataString))

let spawn oracleAPI (instance:OracleInstance) (longTaskExecutor:IActorRef<Application.OracleLongTaskExecutor.Command>) (oracleDiskIntensiveTaskExecutor:IActorRef<Application.OracleDiskIntensiveActor.Command>) (masterPDB : Domain.MasterPDB.MasterPDB) (actorFactory:IActorRefFactory) =
    
    let (Common.ActorName actorName) = masterPDBActorName masterPDB.Name
    
    Akkling.Spawn.spawn actorFactory actorName 
        <| props (
            masterPDBActorBody 
                oracleAPI
                instance 
                longTaskExecutor 
                oracleDiskIntensiveTaskExecutor 
                masterPDB
        )

