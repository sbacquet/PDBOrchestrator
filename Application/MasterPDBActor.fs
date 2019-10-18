﻿module Application.MasterPDBActor

open Akkling
open Domain.MasterPDB
open Domain.OracleInstance
open Application.PendingRequest
open Application.Oracle
open Akka.Actor
open Domain.MasterPDBVersion
open Application.GlobalParameters

type Command =
| GetState // responds with StateResult
| GetInternalState // responds with MasterPDB
| SetInternalState of MasterPDB // no response
| PrepareForModification of WithRequestId<int, string> // responds with WithRequestId<PrepareForModificationResult>
| Commit of WithRequestId<string, string> // responds with WithRequestId<EditionDone>
| Rollback of WithRequestId<string> // responds with WithRequestId<EditionDone>
| SnapshotVersion of WithRequestId<int, string> // responds with WithRequest<SnapshotResult>
| CollectGarbage // no response

type PrepareForModificationResult = 
| Prepared of MasterPDB
| PreparationFailure of string

type StateResult = Result<Application.DTO.MasterPDB.MasterPDBState, string>
let stateOk state : StateResult = Ok state
let stateError error : StateResult = Error error

type EditionDone = Result<MasterPDB, string>

type SnapshotResult = Result<string * int * string, string>

type private Collaborators = {
    OracleAPI: IOracleAPI
    MasterPDBVersionActors: Map<int, IActorRef<MasterPDBVersionActor.Command>>
    OracleLongTaskExecutor: IActorRef<OracleLongTaskExecutor.Command>
    OracleDiskIntensiveTaskExecutor : IActorRef<OracleDiskIntensiveActor.Command>
}

let private getOrSpawnVersionActor parameters (masterPDBName:string) (version:MasterPDBVersion) collaborators ctx =
    let versionActorMaybe = collaborators.MasterPDBVersionActors |> Map.tryFind version.Number
    match versionActorMaybe with
    | Some versionActor -> collaborators, versionActor
    | None -> 
        let versionActor = 
            ctx |> MasterPDBVersionActor.spawn 
                parameters
                collaborators.OracleAPI
                collaborators.OracleLongTaskExecutor
                collaborators.OracleDiskIntensiveTaskExecutor
                masterPDBName
                version
        
        { collaborators with MasterPDBVersionActors = collaborators.MasterPDBVersionActors.Add(version.Number, versionActor) }, 
        versionActor

type private TransientState = {
    EditionOperationInProgress: bool
}

let private masterPDBActorBody (parameters:GlobalParameters) oracleAPI (instance:OracleInstance) oracleLongTaskExecutor oracleDiskIntensiveTaskExecutor (initialMasterPDB : Domain.MasterPDB.MasterPDB) (ctx : Actor<_>) =

    let rec loop (masterPDB:MasterPDB) (requests:RequestMap<Command>) collaborators transientState = actor {

        let manifestPath = Domain.MasterPDB.manifestPath instance.MasterPDBManifestsPath masterPDB.Name

        ctx.Log.Value.Debug("Number of pending requests : {0}", requests.Count)
        let! (msg:obj) = ctx.Receive()
        
        match msg with
        | :? Command as command -> 
            match command with
            | GetState -> 
                let sender = ctx.Sender().Retype<StateResult>()
                sender <! stateOk (masterPDB |> Application.DTO.MasterPDB.toDTO)
                return! loop masterPDB requests collaborators transientState

            | GetInternalState ->
                ctx.Sender() <! masterPDB
                return! loop masterPDB requests collaborators transientState

            | SetInternalState state ->
                return! loop state requests collaborators transientState

            | PrepareForModification (requestId, version, _) as command ->
                let sender = ctx.Sender().Retype<WithRequestId<PrepareForModificationResult>>()

                if masterPDB |> isLocked then
                    sender <! (requestId, PreparationFailure (sprintf "PDB %s is already locked" masterPDB.Name))
                    return! loop masterPDB requests collaborators transientState
                else
                    let latestVersion = masterPDB |> getLatestAvailableVersion
                    if (latestVersion.Number <> version) then 
                        sender <! (requestId, PreparationFailure (sprintf "version %d is not the latest version (%d) of \"%s\"" version latestVersion.Number masterPDB.Name))
                        return! loop masterPDB requests collaborators transientState
                    elif (transientState.EditionOperationInProgress) then
                        sender <! (requestId, PreparationFailure (sprintf "PDB %s has a pending edition operation in progress" masterPDB.Name))
                        return! loop masterPDB requests collaborators transientState
                    else
                        let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                        oracleDiskIntensiveTaskExecutor <! OracleDiskIntensiveActor.ImportPDB (Some requestId, (manifestPath version), instance.MasterPDBDestPath, masterPDB.Name)
                        return! loop masterPDB newRequests collaborators { transientState with EditionOperationInProgress = true }

            | Commit (requestId, unlocker, _) ->
                let sender = ctx.Sender().Retype<WithRequestId<EditionDone>>()
                let lockInfoMaybe = masterPDB.LockState
                match lockInfoMaybe with
                | None -> 
                    sender <! (requestId, Error (sprintf "the master PDB %s is not being edited" masterPDB.Name))
                    return! loop masterPDB requests collaborators transientState
                | Some lockInfo ->
                    if (lockInfo.Locker <> unlocker) then
                        sender <! (requestId, Error (sprintf "you (%s) are not the editor (%s) of master PDB %s" unlocker lockInfo.Locker masterPDB.Name))
                        return! loop masterPDB requests collaborators transientState
                    elif (transientState.EditionOperationInProgress) then
                        sender <! (requestId, Error (sprintf "PDB %s has a pending edition operation in progress" masterPDB.Name))
                        return! loop masterPDB requests collaborators transientState
                    else
                        let manifest = manifestPath (getNextAvailableVersion masterPDB)
                        oracleLongTaskExecutor <! OracleLongTaskExecutor.ExportPDB (Some requestId, manifest, masterPDB.Name)
                        let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                        return! loop masterPDB newRequests collaborators { transientState with EditionOperationInProgress = true }

            | Rollback (requestId, unlocker) ->
                let sender = ctx.Sender().Retype<WithRequestId<EditionDone>>()
                let lockInfoMaybe = masterPDB.LockState
                match lockInfoMaybe with
                | None -> 
                    sender <! (requestId, Error (sprintf "the master PDB %s is not being edited" masterPDB.Name))
                    return! loop masterPDB requests collaborators transientState
                | Some lockInfo ->
                    if (lockInfo.Locker <> unlocker) then
                        sender <! (requestId, Error (sprintf "you (%s) are not the editor (%s) of master PDB %s" unlocker lockInfo.Locker masterPDB.Name))
                        return! loop masterPDB requests collaborators transientState
                    elif (transientState.EditionOperationInProgress) then
                        sender <! (requestId, Error (sprintf "PDB %s has a pending edition operation in progress" masterPDB.Name))
                        return! loop masterPDB requests collaborators transientState
                    else
                        let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                        oracleLongTaskExecutor <! OracleLongTaskExecutor.DeletePDB (Some requestId, masterPDB.Name)
                        return! loop masterPDB newRequests collaborators { transientState with EditionOperationInProgress = true }
            
            | SnapshotVersion (requestId, versionNumber, snapshotName) ->
                let sender = ctx.Sender().Retype<WithRequestId<SnapshotResult>>()
                let versionMaybe = masterPDB.Versions.TryFind(versionNumber)
                match versionMaybe with
                | None -> 
                    sender <! (requestId, Error (sprintf "version %d of master PDB %s does not exist" versionNumber masterPDB.Name))
                    return! loop masterPDB requests collaborators transientState
                | Some version -> 
                    let newCollabs, versionActor = getOrSpawnVersionActor parameters masterPDB.Name version collaborators ctx
                    let newRequests = requests |> registerRequest requestId command (ctx.Sender())
                    versionActor <! MasterPDBVersionActor.Snapshot (requestId, (manifestPath versionNumber), instance.SnapshotSourcePDBDestPath, snapshotName, instance.SnapshotPDBDestPath)
                    return! loop masterPDB newRequests newCollabs transientState

            | CollectGarbage ->
                let! sourceVersionPDBsMaybe = collaborators.OracleAPI.GetPDBNamesLike (sprintf "%s_V%%_%%" masterPDB.Name)
                match sourceVersionPDBsMaybe with
                | Ok sourceVersionPDBs -> 
                    ctx.Log.Value.Info("Garbage collection of PDB {pdb} requested", masterPDB.Name)
                    let regex = System.Text.RegularExpressions.Regex((sprintf "^%s_V([\\d]+)_.+$" masterPDB.Name))
                    let garbageVersion collabs sourceVersionPDB = 
                        let ok, version = System.Int32.TryParse(regex.Replace(sourceVersionPDB, "$1"))
                        if ok then 
                            let versionPDBMaybe = masterPDB.Versions |> Map.tryFind version
                            match versionPDBMaybe with
                            | Some versionPDB -> 
                                let newCollabs, versionActor = getOrSpawnVersionActor parameters masterPDB.Name versionPDB collabs ctx
                                versionActor <! MasterPDBVersionActor.CollectGarbage
                                newCollabs
                            | None -> 
                                ctx.Log.Value.Error("Cannot garbage PDB {0} because it does not correspond to a PDB version of {pdb}", sourceVersionPDB, masterPDB.Name)
                                collabs
                        else
                            ctx.Log.Value.Error("PDB {0} has not a valid PDB version name", sourceVersionPDB)
                            collabs
                    let newCollabs = sourceVersionPDBs |> List.fold garbageVersion collaborators
                    return! loop masterPDB requests newCollabs transientState
                | Error error ->
                    ctx.Log.Value.Error("Unexpected error while garbaging {pdb} : {0}", masterPDB.Name, error)
                    return! loop masterPDB requests collaborators transientState

        | :? MasterPDBVersionActor.CommandToParent as commandToParent->
            match commandToParent with
            | MasterPDBVersionActor.KillVersion version ->
                let versionActorMaybe = collaborators.MasterPDBVersionActors |> Map.tryFind version
                match versionActorMaybe with
                | Some versionActor -> 
                    versionActor <! MasterPDBVersionActor.HaraKiri
                    let newCollabs = { collaborators with MasterPDBVersionActors = collaborators.MasterPDBVersionActors.Remove(version) }
                    return! loop masterPDB requests newCollabs transientState
                | None -> 
                    ctx.Log.Value.Error("cannot find actor for PDB {pdb} version {pdbversion}", masterPDB.Name, version)
                    return! loop masterPDB requests collaborators transientState

        | :? OraclePDBResultWithReqId as requestResponse ->

            let (requestId, result) = requestResponse
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId

            match requestMaybe with
            | None -> 
                logWarningf ctx "internal error : request %s not found" <| requestId.ToString()
                return! loop masterPDB requests collaborators transientState

            | Some request -> 
                match request.Command with
                | PrepareForModification (_, _, locker) -> 
                    let sender = request.Requester.Retype<WithRequestId<PrepareForModificationResult>>()
                    match result with
                    | Ok _ ->
                        let newMasterPDB = masterPDB |> lock locker
                        sender <! (requestId, Prepared newMasterPDB)
                        return! loop newMasterPDB newRequests collaborators { transientState with EditionOperationInProgress = false }
                    | Error error ->
                        sender <! (requestId, PreparationFailure (error.ToString()))
                        return! loop masterPDB newRequests collaborators { transientState with EditionOperationInProgress = false }

                | Commit (_, unlocker, comment) ->
                    let sender = request.Requester.Retype<WithRequestId<EditionDone>>()
                    match result with
                    | Ok _ ->
                        let newMasterPDBMaybe = masterPDB |> addVersionToMasterPDB unlocker comment |> unlock
                        match newMasterPDBMaybe with
                        | Ok newMasterPDB ->
                            sender <! (requestId, Ok newMasterPDB)
                            return! loop newMasterPDB newRequests collaborators { transientState with EditionOperationInProgress = false }
                        | Error error -> 
                            sender <! (requestId, Error (sprintf "cannot unlock %s : %s" masterPDB.Name (error.ToString())))
                            return! loop masterPDB newRequests collaborators { transientState with EditionOperationInProgress = false }
                    | Error error ->
                        sender <! (requestId, Error (sprintf "cannot commit %s : %s" masterPDB.Name (error.ToString())))
                        return! loop masterPDB newRequests collaborators { transientState with EditionOperationInProgress = false }

                | Rollback _ ->
                    let sender = request.Requester.Retype<WithRequestId<EditionDone>>()
                    match result with
                    | Ok _ ->
                        let newMasterPDBMaybe = masterPDB |> unlock
                        match newMasterPDBMaybe with
                        | Ok newMasterPDB ->
                            sender <! (requestId, Ok newMasterPDB)
                            return! loop newMasterPDB newRequests collaborators { transientState with EditionOperationInProgress = false }
                        | Error error -> 
                            sender <! (requestId, Error (sprintf "cannot unlock %s : %s" masterPDB.Name (error.ToString())))
                            return! loop masterPDB newRequests collaborators { transientState with EditionOperationInProgress = false }
                    | Error error ->
                        sender <! (requestId, Error (sprintf "cannot rollback %s : %s" masterPDB.Name (error.ToString())))
                        return! loop masterPDB newRequests collaborators { transientState with EditionOperationInProgress = false }

                | SnapshotVersion (_, versionNumber, snapshotName) ->
                    let sender = request.Requester.Retype<WithRequestId<SnapshotResult>>()
                    match result with
                    | Ok _ ->
                        sender <! (requestId, Ok (masterPDB.Name, versionNumber, snapshotName))
                    | Error error ->
                        sender <! (requestId, Error (error.ToString()))
                    return! loop masterPDB newRequests collaborators transientState

                | _ -> failwithf "Fatal error"

        | _ -> return! loop masterPDB requests collaborators transientState
    }

    let collaborators = { 
        OracleAPI = oracleAPI
        MasterPDBVersionActors = Map.empty
        OracleLongTaskExecutor = oracleLongTaskExecutor
        OracleDiskIntensiveTaskExecutor = oracleDiskIntensiveTaskExecutor 
    }
    loop initialMasterPDB Map.empty collaborators { EditionOperationInProgress = false }

let private masterPDBActorName (masterPDB:string) = Common.ActorName (sprintf "MasterPDB='%s'" (masterPDB.ToUpper() |> System.Uri.EscapeDataString))

let spawn parameters oracleAPI (instance:OracleInstance) (longTaskExecutor:IActorRef<Application.OracleLongTaskExecutor.Command>) (oracleDiskIntensiveTaskExecutor:IActorRef<Application.OracleDiskIntensiveActor.Command>) (masterPDB : Domain.MasterPDB.MasterPDB) (actorFactory:IActorRefFactory) =
    
    let (Common.ActorName actorName) = masterPDBActorName masterPDB.Name
    
    Akkling.Spawn.spawn actorFactory actorName 
        <| props (
            masterPDBActorBody 
                parameters
                oracleAPI
                instance 
                longTaskExecutor 
                oracleDiskIntensiveTaskExecutor 
                masterPDB
        )

