﻿module Application.OracleInstanceActor

open Akkling
open Application.Oracle
open Application.OracleLongTaskExecutor
open Application.PendingRequest
open Domain
open Domain.OracleInstance
open Domain.Common.Validation
open Domain.Common
open System
open Application.Common
open Application.GlobalParameters

type CreateMasterPDBParams = {
    Name: string
    Dump: string
    Schemas: string list
    TargetSchemas: (string * string * string) list
    User: string
    Date: DateTime
    Comment: string
}

let consCreateMasterPDBParams name dump schemas targetSchemas user date comment = 
    { 
        Name=name
        Dump=dump
        Schemas=schemas
        TargetSchemas=targetSchemas
        User=user
        Date=date
        Comment=comment 
    }

let newCreateMasterPDBParams name dump schemas targetSchemas user comment = 
    consCreateMasterPDBParams name dump schemas targetSchemas user System.DateTime.Now comment

type CreateMasterPDBParamsValidation = Validation<CreateMasterPDBParams, string>

[<RequireQualifiedAccess>]
module Validation =
    let validatePDB oracleInstance name =
        if (oracleInstance |> masterPDBAlreadyExists name) then
            Invalid [ sprintf "a PDB named \"%s\" already exists" name ]
        else
            Valid name

    let validateDump dump =
        if (System.IO.File.Exists(dump)) then
            Valid dump
        else
            Invalid [ sprintf "the dump file \"%s\" does not exist" dump ]

    let validateSchemas schemas =
        if schemas = [] then
            Invalid [ "at least 1 schema must be provided" ]
        else    
            Valid schemas

    let validateTargetSchemas (schemas:_ list) (targetSchemas : _ list) =
        if schemas.Length <> targetSchemas.Length then
            Invalid [ "the number of target schemas must be equal to the number of source schemas" ]
        else    
            Valid targetSchemas

    let validateUser (user:string) =
        if (user = "") then
            Invalid [ "the user cannot be empty" ]
        else
            Valid user

    let validateComment (comment:string) =
        if (comment = "") then
            Invalid [ "the comment cannot be empty" ]
        else
            Valid comment

    let validateCreateMasterPDBParams (parameters : CreateMasterPDBParams) (state : Domain.OracleInstance.OracleInstance) : CreateMasterPDBParamsValidation =
        let pdb = validatePDB state parameters.Name
        let dump = validateDump parameters.Dump
        let schemas = validateSchemas parameters.Schemas
        let targetSchemas = validateTargetSchemas parameters.Schemas parameters.TargetSchemas
        let user = validateUser parameters.User
        let date = Valid parameters.Date
        let comment = validateComment parameters.Comment
        retn consCreateMasterPDBParams <*> pdb <*> dump <*> schemas <*> targetSchemas <*> user <*> date <*> comment

type OracleInstanceExpanded = {
    Name: string
    Server: string
    Port: int option
    DBAUser: string
    DBAPassword: string
    MasterPDBManifestsPath: string
    MasterPDBDestPath: string
    SnapshotSourcePDBDestPath: string
    SnapshotPDBDestPath: string
    OracleDirectoryForDumps: string
    MasterPDBs: Domain.MasterPDB.MasterPDB list
}

type Command =
| GetState // responds with StateResult
| GetMasterPDBState of string // responds with Application.MasterPDBActor.StateResult
| SetInternalState of OracleInstanceExpanded // responds with StateResult
| TransferInternalState of IActorRef<obj> // responds with StateResult
| CreateMasterPDB of WithRequestId<CreateMasterPDBParams> // responds with WithRequestId<MasterPDBCreationResult>
| PrepareMasterPDBForModification of WithRequestId<string, int, string> // responds with WithRequestId<MasterPDBActor.PrepareForModificationResult>
| CommitMasterPDB of WithRequestId<string, string, string> // responds with WithRequestId<MasterPDBActor.EditionDone>
| RollbackMasterPDB of WithRequestId<string, string> // responds with WithRequestId<MasterPDBActor.EditionDone>
| SnapshotMasterPDBVersion of WithRequestId<string, int, string> // responds with WithRequest<MasterPDBActor.SnapshotResult>
| CollectGarbage // no response

type StateResult = Result<Application.DTO.OracleInstance.OracleInstanceState, string>
let stateOk state : StateResult = Ok state
let stateError error : StateResult = Error error

type MasterPDBCreationResult = 
| InvalidRequest of string list
| MasterPDBCreated of Domain.MasterPDB.MasterPDB
| MasterPDBCreationFailure of string

type Collaborators = {
    OracleAPI: IOracleAPI
    OracleLongTaskExecutor: IActorRef<Application.OracleLongTaskExecutor.Command>
    OracleDiskIntensiveTaskExecutor : IActorRef<Application.OracleDiskIntensiveActor.Command>
    MasterPDBActors: Map<string, IActorRef<obj>>
}

let addMasterPDBToCollaborators parameters ctx (instance : OracleInstance) (masterPDB:Domain.MasterPDB.MasterPDB) collaborators = 
    logDebugf ctx "Adding MasterPDB %s to collaborators" masterPDB.Name
    { collaborators with 
        MasterPDBActors = 
            collaborators.MasterPDBActors.Add(
                masterPDB.Name, 
                ctx |> MasterPDBActor.spawn 
                    parameters
                    collaborators.OracleAPI
                    instance 
                    collaborators.OracleLongTaskExecutor 
                    collaborators.OracleDiskIntensiveTaskExecutor 
                    masterPDB
            )
    }

// Spawn actor for a new master PDBs
let addNewMasterPDB parameters (ctx : Actor<obj>) (instance : OracleInstance) collaborators (masterPDB:Domain.MasterPDB.MasterPDB) (masterPDBRepo:IMasterPDBRepository) = result {
    let! newState = instance |> OracleInstance.addMasterPDB masterPDB.Name
    let newMasterPDBRepo = masterPDBRepo.Put masterPDB.Name masterPDB
    let newCollaborators = 
        { collaborators with 
            MasterPDBActors = 
                collaborators.MasterPDBActors.Add(
                    masterPDB.Name, 
                    ctx |> MasterPDBActor.spawn 
                        parameters
                        collaborators.OracleAPI
                        instance 
                        collaborators.OracleLongTaskExecutor 
                        collaborators.OracleDiskIntensiveTaskExecutor 
                        masterPDB) 
        }
    return newState, newCollaborators, newMasterPDBRepo
}

let expand timeout (masterPDBActors: Map<string, IActorRef<obj>>) (instance:OracleInstance) = 
    let masterPDBsMaybe = 
        instance.MasterPDBs 
        |> List.map (fun pdb -> retype masterPDBActors.[pdb] <? MasterPDBActor.GetInternalState)
        |> Async.Parallel |> runWithinElseDefaultError timeout
    masterPDBsMaybe |> Result.map (fun masterPDBs ->
    {
        Name = instance.Name
        Server = instance.Server
        Port = instance.Port
        DBAUser = instance.DBAUser
        DBAPassword = instance.DBAPassword
        MasterPDBManifestsPath = instance.MasterPDBManifestsPath
        MasterPDBDestPath = instance.MasterPDBDestPath
        SnapshotPDBDestPath = instance.SnapshotPDBDestPath
        SnapshotSourcePDBDestPath = instance.SnapshotSourcePDBDestPath
        OracleDirectoryForDumps = instance.OracleDirectoryForDumps
        MasterPDBs = masterPDBs |> List.ofArray
    })

    
let collapse (expandedInstance:OracleInstanceExpanded) : OracleInstance = {
    Name = expandedInstance.Name
    Server = expandedInstance.Server
    Port = expandedInstance.Port
    DBAUser = expandedInstance.DBAUser
    DBAPassword = expandedInstance.DBAPassword
    MasterPDBManifestsPath = expandedInstance.MasterPDBManifestsPath
    MasterPDBDestPath = expandedInstance.MasterPDBDestPath
    SnapshotPDBDestPath = expandedInstance.SnapshotPDBDestPath
    SnapshotSourcePDBDestPath = expandedInstance.SnapshotSourcePDBDestPath
    OracleDirectoryForDumps = expandedInstance.OracleDirectoryForDumps
    MasterPDBs = expandedInstance.MasterPDBs |> List.map (fun pdb -> pdb.Name)
}


let updateMasterPDBs parameters (ctx : Actor<obj>) (instanceToImport : OracleInstanceExpanded) (collaborators:Collaborators) (masterPDBRepo:IMasterPDBRepository) (instance : OracleInstance) = result {
    let masterPDBs = instanceToImport.MasterPDBs
    let existingMasterPDBs = collaborators.MasterPDBActors |> Map.toSeq |> Seq.map (fun (name, _) -> name) |> Set.ofSeq
    let newMasterPDBs = masterPDBs |> List.map (fun pdb -> pdb.Name) |> Set.ofList
    let masterPDBsToAdd = Set.difference newMasterPDBs existingMasterPDBs

    let masterPDBMap = masterPDBs |> List.map (fun pdb -> (pdb.Name, pdb)) |> Map.ofList

    let folder result pdb = 
        result |> Result.bind (fun (inst, collabs, repo) -> addNewMasterPDB parameters ctx inst collabs masterPDBMap.[pdb] repo)
        
    let! x = masterPDBsToAdd |> Set.fold folder (Ok (instance, collaborators, masterPDBRepo))

    let masterPDBsToUpdate = Set.intersect existingMasterPDBs newMasterPDBs
    let folder2 result pdb = 
        result |> Result.bind (fun (inst, collabs, (repo:IMasterPDBRepository)) -> 
            retype collabs.MasterPDBActors.[pdb] <! Application.MasterPDBActor.SetInternalState masterPDBMap.[pdb]
            Ok (inst, collabs, (repo.Put pdb masterPDBMap.[pdb]))
        )
    return! masterPDBsToUpdate |> Set.fold folder2 (Ok x)
}

// Spawn actors for master PDBs that already exist
let spawnCollaborators parameters getOracleAPI (masterPDBRepo:IMasterPDBRepository) (instance : OracleInstance) (ctx : Actor<obj>) : Collaborators = 
    let oracleAPI = getOracleAPI instance
    let oracleLongTaskExecutor = ctx |> OracleLongTaskExecutor.spawn parameters oracleAPI
    let oracleDiskIntensiveTaskExecutor = ctx |> Application.OracleDiskIntensiveActor.spawn parameters oracleAPI
    {
        OracleAPI = oracleAPI
        OracleLongTaskExecutor = oracleLongTaskExecutor
        OracleDiskIntensiveTaskExecutor = oracleDiskIntensiveTaskExecutor
        MasterPDBActors = 
            instance.MasterPDBs 
            |> List.map (fun pdb -> (pdb, ctx |> MasterPDBActor.spawn parameters oracleAPI instance oracleLongTaskExecutor oracleDiskIntensiveTaskExecutor (masterPDBRepo.Get pdb)))
            |> Map.ofList
    }

let oracleInstanceActorName (instance : OracleInstance) = 
    Common.ActorName 
        (sprintf "OracleInstance='%s'" (instance.Name.ToUpper() |> System.Uri.EscapeDataString))

let oracleInstanceActorBody (parameters:GlobalParameters) getOracleAPI (initialMasterPDBRepo:IMasterPDBRepository) initialInstance (ctx : Actor<obj>) =

    let rec loop collaborators (instance : OracleInstance) (requests : RequestMap<Command>) (masterPDBRepo:IMasterPDBRepository) = actor {

        ctx.Log.Value.Debug("Number of pending requests : {0}", requests.Count)
        let! msg = ctx.Receive()

        match msg with
        | :? Command as command ->
            match command with
            | GetState ->
                let sender = ctx.Sender().Retype<StateResult>()
                let! state = instance |> Application.DTO.OracleInstance.toDTO collaborators.MasterPDBActors
                sender <! stateOk state
                return! loop collaborators instance requests masterPDBRepo

            | GetMasterPDBState pdb ->
                let sender = ctx.Sender().Retype<Application.MasterPDBActor.StateResult>()
                let masterPDBOk = instance.MasterPDBs |> List.contains pdb
                if (instance.MasterPDBs |> List.contains pdb) then 
                    let masterPDBActor:IActorRef<MasterPDBActor.Command> = retype collaborators.MasterPDBActors.[pdb]
                    masterPDBActor <<! MasterPDBActor.GetState
                else
                    sender <! MasterPDBActor.stateError (sprintf "master PDB %s does not exist on instance %s" pdb instance.Name)
                return! loop collaborators instance requests masterPDBRepo

            | SetInternalState newState -> 
                let updateResult = updateMasterPDBs parameters ctx newState collaborators masterPDBRepo instance
                match updateResult with
                | Ok (inst, collabs, repo) ->
                    let! state = inst |> Application.DTO.OracleInstance.toDTO collabs.MasterPDBActors
                    ctx.Sender() <! stateOk state
                    return! loop collabs inst requests repo
                | Error error ->
                    ctx.Sender() <! stateError error
                    return! loop collaborators instance requests masterPDBRepo

            | TransferInternalState target ->
                let expandedInstanceMaybe = instance |> expand parameters.ShortTimeout collaborators.MasterPDBActors
                match expandedInstanceMaybe with
                | Ok expandedInstance -> retype target <<! SetInternalState expandedInstance
                | Error error -> ctx.Sender() <! stateError error
                return! loop collaborators instance requests masterPDBRepo

            | CreateMasterPDB (requestId, parameters) as command ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBCreationResult>>()
                let validation = Validation.validateCreateMasterPDBParams parameters instance
                match validation with
                | Valid _ -> 
                    let parameters2 = {
                        Name = parameters.Name
                        AdminUserName = instance.DBAUser
                        AdminUserPassword = instance.DBAPassword
                        Destination = instance.MasterPDBDestPath
                        DumpPath = parameters.Dump
                        Schemas = parameters.Schemas
                        TargetSchemas = parameters.TargetSchemas |> List.map (fun (u, p, _) -> (u, p))
                        Directory = instance.OracleDirectoryForDumps
                    }
                    let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                    collaborators.OracleLongTaskExecutor <! OracleLongTaskExecutor.CreatePDBFromDump (Some requestId, parameters2)
                    return! loop collaborators instance newRequests masterPDBRepo
                | Invalid errors -> 
                    sender <! (requestId, InvalidRequest errors)
                    return! loop collaborators instance requests masterPDBRepo

            | PrepareMasterPDBForModification (requestId, pdb, version, user) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.PrepareForModificationResult>>()
                let masterPDBOk = instance.MasterPDBs |> List.contains pdb
                if (not masterPDBOk) then 
                    sender <! (requestId, MasterPDBActor.PreparationFailure (sprintf "master PDB %s does not exist on instance %s" pdb instance.Name))
                    return! loop collaborators instance requests masterPDBRepo
                let masterPDBActor = collaborators.MasterPDBActors.[pdb]
                let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                retype masterPDBActor <! MasterPDBActor.PrepareForModification (requestId, version, user)
                return! loop collaborators instance newRequests masterPDBRepo

            | CommitMasterPDB (requestId, pdb, locker, comment) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.EditionDone>>()
                let masterPDBOk = instance.MasterPDBs |> List.contains pdb
                if (not masterPDBOk) then 
                    sender <! (requestId, Error (sprintf "master PDB %s does not exist on instance %s" pdb instance.Name))
                    return! loop collaborators instance requests masterPDBRepo
                let masterPDBActor = collaborators.MasterPDBActors.[pdb]
                let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                retype masterPDBActor <! MasterPDBActor.Commit (requestId, locker, comment)
                return! loop collaborators instance newRequests masterPDBRepo

            | RollbackMasterPDB (requestId, user, pdb) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.EditionDone>>()
                let masterPDBOk = instance.MasterPDBs |> List.contains pdb
                if (not masterPDBOk) then 
                    sender <! (requestId, Error (sprintf "master PDB %s does not exist on instance %s" pdb instance.Name))
                    return! loop collaborators instance requests masterPDBRepo
                let masterPDBActor = collaborators.MasterPDBActors.[pdb]
                let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                retype masterPDBActor <! MasterPDBActor.Rollback (requestId, user)
                return! loop collaborators instance newRequests masterPDBRepo

            | SnapshotMasterPDBVersion (requestId, masterPDBName, versionNumber, snapshotName) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.SnapshotResult>>()
                let masterPDBOk = instance.MasterPDBs |> List.contains masterPDBName
                if (not masterPDBOk) then 
                    sender <! (requestId, Error (sprintf "master PDB %s does not exist on instance %s" masterPDBName instance.Name))
                    return! loop collaborators instance requests masterPDBRepo
                let masterPDBActor = collaborators.MasterPDBActors.[masterPDBName]
                retype masterPDBActor <<! MasterPDBActor.SnapshotVersion (requestId, versionNumber, snapshotName)
                return! loop collaborators instance requests masterPDBRepo

            | CollectGarbage ->
                collaborators.MasterPDBActors |> Map.iter (fun _ pdbActor -> retype pdbActor <! MasterPDBActor.CollectGarbage)
                ctx.Log.Value.Info("Garbage collection of instance {instance} requested", instance.Name)
                return! loop collaborators instance requests masterPDBRepo

        // Callback from Oracle executor
        | :? OraclePDBResultWithReqId as requestResponse ->
            let (requestId, result) = requestResponse
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId
            match requestMaybe with
            | None -> 
                logError ctx (sprintf "internal error : request %s not found" (requestId.ToString()))
                return! loop collaborators instance newRequests masterPDBRepo

            | Some request ->
                match request.Command with 
                | CreateMasterPDB (_, commandParameters) ->
                    let requester = request.Requester.Retype<WithRequestId<MasterPDBCreationResult>>()
                    match result with
                    | Ok _ -> 
                        logDebugf ctx "PDB %s created successfully" commandParameters.Name
                        let newMasterPDB = 
                            Domain.MasterPDB.newMasterPDB 
                                commandParameters.Name 
                                (commandParameters.TargetSchemas |> List.map Domain.MasterPDB.consSchemaFromTuple)
                        let masterPDB = newMasterPDB commandParameters.User commandParameters.Comment
                        let newStateResult = 
                            addNewMasterPDB 
                                parameters
                                ctx 
                                instance
                                collaborators 
                                masterPDB
                                masterPDBRepo
                        requester <! (requestId, MasterPDBCreated masterPDB)
                        let newState, newCollabs, newMasterPDBRepo = 
                            match newStateResult with
                            | Ok s -> s
                            | Error error -> 
                                logErrorf ctx "error when registering new master PDB %s : %s" commandParameters.Name error
                                instance, collaborators, masterPDBRepo
                        return! loop newCollabs newState newRequests newMasterPDBRepo
                    | Error error -> 
                        logErrorf ctx "PDB %s failed to create with error %A" commandParameters.Name error
                        requester <! (requestId, MasterPDBCreationFailure (error.ToString()))
                        return! loop collaborators instance newRequests masterPDBRepo
                | _ -> 
                    ctx.Log.Value.Error "critical error"
                    return! loop collaborators instance requests masterPDBRepo

        // Callback from Master PDB actor in response to PrepareForModification
        | :? WithRequestId<MasterPDBActor.PrepareForModificationResult> as preparationResult ->
            let (requestId, result) = preparationResult
            match result with
            | MasterPDBActor.Prepared lockedMasterPDB ->
                let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId
                match requestMaybe with
                | Some request -> 
                    // Persist the state of the PDB
                    let newMasterPDBRepo = masterPDBRepo.Put lockedMasterPDB.Name lockedMasterPDB
                    retype request.Requester <! preparationResult
                    return! loop collaborators instance newRequests newMasterPDBRepo
                | None -> 
                    logError ctx "internal error"
                    return! loop collaborators instance newRequests masterPDBRepo

            | MasterPDBActor.PreparationFailure _ ->
                let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId
                match requestMaybe with
                | Some request -> 
                    retype request.Requester <! preparationResult
                    return! loop collaborators instance newRequests masterPDBRepo
                | None -> 
                    logError ctx "internal error"
                    return! loop collaborators instance newRequests masterPDBRepo

        // Callback from Master PDB actor in response to Commit or Rollback
        | :? WithRequestId<MasterPDBActor.EditionDone> as editionResult ->
            let (requestId, result) = editionResult
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId
            match requestMaybe with
            | None -> 
                logWarningf ctx "internal error : request %s not found" <| requestId.ToString()
                return! loop collaborators instance requests masterPDBRepo

            | Some request -> 
            match result with
                | Ok unlockedMasterPDB -> 
                    // Persist the state of the PDB
                    let newMasterPDBRepo = masterPDBRepo.Put unlockedMasterPDB.Name unlockedMasterPDB
                    retype request.Requester <! editionResult
                    return! loop collaborators instance newRequests newMasterPDBRepo

                | Error error ->
                    retype request.Requester <! editionResult
                    return! loop collaborators instance newRequests masterPDBRepo

        | _ -> return! loop collaborators instance requests masterPDBRepo
    }
    let collaborators = ctx |> spawnCollaborators parameters getOracleAPI initialMasterPDBRepo initialInstance
    loop collaborators initialInstance Map.empty initialMasterPDBRepo

let spawn parameters getOracleAPI initialMasterPDBRepo initialInstance actorFactory =
    let (Common.ActorName actorName) = oracleInstanceActorName initialInstance
    Akkling.Spawn.spawn actorFactory actorName 
    <| props (oracleInstanceActorBody parameters getOracleAPI initialMasterPDBRepo initialInstance)

