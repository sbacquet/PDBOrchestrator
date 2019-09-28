module Application.OracleInstanceActor

open Akkling
open Application.Oracle
open Application.OracleLongTaskExecutor
open Application.PendingRequest
open Domain
open Domain.OracleInstance
open Domain.Common.Validation
open Domain.Common.Result
open System
open Application.Common

type MasterPDBRepo = Common.Repository<string, Domain.MasterPDB.MasterPDB>

type CreateMasterPDBParams = {
    Name: string
    Dump: string
    Schemas: string list
    TargetSchemas: (string * string * string) list
    User: string
    Date: DateTime
    Comment: string
}

let newCreateMasterPDBParams name dump schemas targetSchemas user date comment = 
    { 
        Name=name
        Dump=dump
        Schemas=schemas
        TargetSchemas=targetSchemas
        User=user
        Date=date
        Comment=comment 
    }

type CreateMasterPDBParamsValidation = Validation<CreateMasterPDBParams, string>

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
    retn newCreateMasterPDBParams <*> pdb <*> dump <*> schemas <*> targetSchemas <*> user <*> date <*> comment

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
| GetState // responds with Application.DTO.OracleInstance
| SetInternalState of OracleInstanceExpanded // responds with Application.DTO.OracleInstance.OracleInstanceState
| TransferInternalState of IActorRef<obj> // responds with (StateSet state)
| CreateMasterPDB of WithRequestId<CreateMasterPDBParams> // responds with WithRequestId<MasterPDBCreationResult>
| PrepareMasterPDBForModification of WithRequestId<string, int, string> // responds with WithRequestId<MasterPDBActor.PrepareForModificationResult>
| RollbackMasterPDB of WithRequestId<string> // responds with WithRequestId<MasterPDBActor.RollbackResult>
| SnapshotMasterPDBVersion of WithRequestId<string, int, string> // responds with WithRequest<MasterPDBActor.SnapshotResult>

type StateSet = Result<Application.DTO.OracleInstance.OracleInstanceState, string>
let stateSetOk state : StateSet = Ok state
let stateSetError error : StateSet = Error error

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

let addMasterPDBToCollaborators ctx (instance : OracleInstance) (masterPDB:Domain.MasterPDB.MasterPDB) collaborators = 
    logDebugf ctx "Adding MasterPDB %s to collaborators" masterPDB.Name
    { collaborators with 
        MasterPDBActors = 
            collaborators.MasterPDBActors.Add(
                masterPDB.Name, 
                ctx |> MasterPDBActor.spawn 
                    collaborators.OracleAPI
                    instance 
                    collaborators.OracleLongTaskExecutor 
                    collaborators.OracleDiskIntensiveTaskExecutor 
                    masterPDB
            )
    }

// Spawn actor for a new master PDBs
let addNewMasterPDB (ctx : Actor<obj>) (instance : OracleInstance) collaborators (masterPDB:Domain.MasterPDB.MasterPDB) (masterPDBRepo:MasterPDBRepo) = result {
    let! newState = instance |> OracleInstance.addMasterPDB masterPDB.Name
    let newMasterPDBRepo = masterPDBRepo.Put masterPDB.Name masterPDB
    let newCollaborators = 
        { collaborators with 
            MasterPDBActors = 
                collaborators.MasterPDBActors.Add(
                    masterPDB.Name, 
                    ctx |> MasterPDBActor.spawn 
                        collaborators.OracleAPI
                        instance 
                        collaborators.OracleLongTaskExecutor 
                        collaborators.OracleDiskIntensiveTaskExecutor 
                        masterPDB) 
        }
    return newState, newCollaborators, newMasterPDBRepo
}

let expand (masterPDBActors: Map<string, IActorRef<obj>>) (instance:OracleInstance) = 
    let masterPDBsMaybe = 
        instance.MasterPDBs 
        |> List.map (fun pdb -> retype masterPDBActors.[pdb] <? MasterPDBActor.GetInternalState)
        |> Async.Parallel |> runWithinElseDefaultError 1000 // TODO
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


let updateMasterPDBs (ctx : Actor<obj>) (instanceToImport : OracleInstanceExpanded) (collaborators:Collaborators) (masterPDBRepo:MasterPDBRepo) (instance : OracleInstance) = result {
    let masterPDBs = instanceToImport.MasterPDBs
    let existingMasterPDBs = collaborators.MasterPDBActors |> Map.toSeq |> Seq.map (fun (name, _) -> name) |> Set.ofSeq
    let newMasterPDBs = masterPDBs |> List.map (fun pdb -> pdb.Name) |> Set.ofList
    let masterPDBsToAdd = Set.difference newMasterPDBs existingMasterPDBs

    let masterPDBMap = masterPDBs |> List.map (fun pdb -> (pdb.Name, pdb)) |> Map.ofList

    let folder result pdb = 
        result |> Result.bind (fun (inst, collabs, repo) -> addNewMasterPDB ctx inst collabs masterPDBMap.[pdb] repo)
        
    let! x = masterPDBsToAdd |> Set.fold folder (Ok (instance, collaborators, masterPDBRepo))

    let masterPDBsToUpdate = Set.intersect existingMasterPDBs newMasterPDBs
    let folder2 result pdb = 
        result |> Result.bind (fun (inst, collabs, (repo:MasterPDBRepo)) -> 
            retype collabs.MasterPDBActors.[pdb] <! Application.MasterPDBActor.SetInternalState masterPDBMap.[pdb]
            Ok (inst, collabs, (repo.Put pdb masterPDBMap.[pdb]))
        )
    return! masterPDBsToUpdate |> Set.fold folder2 (Ok x)
}

// Spawn actors for master PDBs that already exist
let spawnCollaborators getOracleAPI (masterPDBRepo:MasterPDBRepo) (instance : OracleInstance) (ctx : Actor<obj>) : Collaborators = 
    let oracleAPI = getOracleAPI instance
    let oracleLongTaskExecutor = ctx |> OracleLongTaskExecutor.spawn oracleAPI
    let oracleDiskIntensiveTaskExecutor = ctx |> Application.OracleDiskIntensiveActor.spawn oracleAPI
    {
        OracleAPI = oracleAPI
        OracleLongTaskExecutor = oracleLongTaskExecutor
        OracleDiskIntensiveTaskExecutor = oracleDiskIntensiveTaskExecutor
        MasterPDBActors = 
            instance.MasterPDBs 
            |> List.map (fun pdb -> (pdb, ctx |> MasterPDBActor.spawn oracleAPI instance oracleLongTaskExecutor oracleDiskIntensiveTaskExecutor (masterPDBRepo.Get pdb)))
            |> Map.ofList
    }

let oracleInstanceActorName (instance : OracleInstance) = 
    Common.ActorName 
        (sprintf "OracleInstance='%s'" (instance.Name.ToUpper() |> System.Uri.EscapeDataString))

let oracleInstanceActorBody getOracleAPI (initialMasterPDBRepo:MasterPDBRepo) initialInstance (ctx : Actor<obj>) =

    let rec loop collaborators (instance : OracleInstance) (requests : RequestMap<Command>) (masterPDBRepo:MasterPDBRepo) = actor {

        logDebugf ctx "Number of pending requests : %d" requests.Count
        let! msg = ctx.Receive()

        match msg with
        | :? Command as command ->
            match command with
            | GetState ->
                let! state = instance |> Application.DTO.OracleInstance.toDTO collaborators.MasterPDBActors
                ctx.Sender() <! state
                return! loop collaborators instance requests masterPDBRepo

            | SetInternalState newState -> 
                let updateResult = updateMasterPDBs ctx newState collaborators masterPDBRepo instance
                match updateResult with
                | Ok (inst, collabs, repo) ->
                    let! state = inst |> Application.DTO.OracleInstance.toDTO collabs.MasterPDBActors
                    ctx.Sender() <! stateSetOk state
                    return! loop collabs inst requests repo
                | Error error ->
                    ctx.Sender() <! stateSetError error
                    return! loop collaborators instance requests masterPDBRepo

            | TransferInternalState target ->
                let expandedInstanceMaybe = instance |> expand collaborators.MasterPDBActors
                match expandedInstanceMaybe with
                | Ok expandedInstance -> retype target <<! SetInternalState expandedInstance
                | Error error -> ctx.Sender() <! stateSetError error
                return! loop collaborators instance requests masterPDBRepo

            | CreateMasterPDB (requestId, parameters) as command ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBCreationResult>>()
                let validation = validateCreateMasterPDBParams parameters instance
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
                    collaborators.OracleLongTaskExecutor <! OracleLongTaskExecutor.CreatePDBFromDump (requestId, parameters2)
                    return! loop collaborators instance newRequests masterPDBRepo
                | Invalid errors -> 
                    sender <! (requestId, InvalidRequest errors)
                    return! loop collaborators instance requests masterPDBRepo

            | PrepareMasterPDBForModification (requestId, pdb, version, user) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.PrepareForModificationResult>>()
                let masterPDBOk = instance.MasterPDBs |> List.contains pdb
                if (not masterPDBOk) then sender <! (requestId, MasterPDBActor.PreparationFailure (sprintf "master PDB %s does not exist on instance %s" pdb instance.Name))
                let masterPDBActor = collaborators.MasterPDBActors.[pdb]
                let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                retype masterPDBActor <! MasterPDBActor.PrepareForModification (requestId, version, user)
                return! loop collaborators instance newRequests masterPDBRepo

            | RollbackMasterPDB (requestId, pdb) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.RollbackResult>>()
                let masterPDBOk = instance.MasterPDBs |> List.contains pdb
                if (not masterPDBOk) then sender <! (requestId, Error (sprintf "master PDB %s does not exist on instance %s" pdb instance.Name))
                let masterPDBActor = collaborators.MasterPDBActors.[pdb]
                let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                retype masterPDBActor <! MasterPDBActor.Rollback requestId
                return! loop collaborators instance newRequests masterPDBRepo

            | SnapshotMasterPDBVersion (requestId, masterPDBName, versionNumber, snapshotName) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.SnapshotResult>>()
                let masterPDBOk = instance.MasterPDBs |> List.contains masterPDBName
                if (not masterPDBOk) then sender <! (requestId, Error (sprintf "master PDB %s does not exist on instance %s" masterPDBName instance.Name))
                let masterPDBActor = collaborators.MasterPDBActors.[masterPDBName]
                retype masterPDBActor <<! MasterPDBActor.SnapshotVersion (requestId, versionNumber, snapshotName)
                return! loop collaborators instance requests masterPDBRepo

        // Callback from Oracle executor
        | :? WithRequestId<OraclePDBResult> as requestResponse ->
            let (requestId, result) = requestResponse
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId
            match requestMaybe with
            | None -> 
                logError ctx (sprintf "internal error : request %s not found" (requestId.ToString()))
                return! loop collaborators instance newRequests masterPDBRepo

            | Some request ->
                match request.Command with 
                | CreateMasterPDB (_, parameters) ->
                    let requester = request.Requester.Retype<WithRequestId<MasterPDBCreationResult>>()
                    match result with
                    | Ok _ -> 
                        logDebugf ctx "PDB %s created successfully" parameters.Name
                        let newMasterPDB = 
                            Domain.MasterPDB.newMasterPDB 
                                parameters.Name 
                                (parameters.TargetSchemas |> List.map Domain.MasterPDB.consSchemaFromTuple)
                        let masterPDB = newMasterPDB parameters.User parameters.Date parameters.Comment
                        let newStateResult = 
                            addNewMasterPDB 
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
                                logErrorf ctx "error when registering new master PDB %s : %s" parameters.Name error
                                instance, collaborators, masterPDBRepo
                        return! loop newCollabs newState newRequests newMasterPDBRepo
                    | Error error -> 
                        logErrorf ctx "PDB %s failed to create with error %A" parameters.Name error
                        requester <! (requestId, MasterPDBCreationFailure (error.ToString()))
                        return! loop collaborators instance newRequests masterPDBRepo
                | _ -> failwith "critical error"

        // Callback from Master PDB actor in response to PrepareForModification
        | :? WithRequestId<MasterPDBActor.PrepareForModificationResult> as preparationResult ->
            let (requestId, result) = preparationResult
            match result with
            | MasterPDBActor.Locked lockedMasterPDB -> 
                // Persist the state of the PDB
                let newMasterPDBRepo = masterPDBRepo.Put lockedMasterPDB.Name lockedMasterPDB
                // Keep the request in the map, because Prepared or PreparationFailure will come last
                return! loop collaborators instance requests newMasterPDBRepo

            | MasterPDBActor.Prepared _ ->
                let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId
                match requestMaybe with
                | Some request -> 
                    retype request.Requester <! preparationResult
                    return! loop collaborators instance newRequests masterPDBRepo
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

        // Callback from Master PDB actor in response to Rollback
        | :? WithRequestId<MasterPDBActor.RollbackResult> as rollbackResult ->
            let (requestId, result) = rollbackResult
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
                    retype request.Requester <! rollbackResult
                    return! loop collaborators instance newRequests newMasterPDBRepo

                | Error error ->
                    retype request.Requester <! rollbackResult
                    return! loop collaborators instance newRequests masterPDBRepo

        | _ -> return! loop collaborators instance requests masterPDBRepo
    }
    let collaborators = ctx |> spawnCollaborators getOracleAPI initialMasterPDBRepo initialInstance
    loop collaborators initialInstance Map.empty initialMasterPDBRepo

let spawn getOracleAPI initialMasterPDBRepo initialInstance actorFactory =
    let (Common.ActorName actorName) = oracleInstanceActorName initialInstance
    Akkling.Spawn.spawn actorFactory actorName 
    <| props (oracleInstanceActorBody getOracleAPI initialMasterPDBRepo initialInstance)

