module Application.OracleInstanceActor

open Akkling
open Application.Oracle
open Application.PendingRequest
open Domain
open Domain.OracleInstance
open Domain.Common.Validation
open Domain.Common
open System
open Application.Common
open Application.Parameters
open Domain.MasterPDB
open Application.DTO
open Domain.MasterPDBWorkingCopy

type CreateMasterPDBParams = {
    Name: string
    Dump: string
    Schemas: string list
    TargetSchemas: (string * string * string) list
    User: string
    Date: DateTime
    Comment: string
}

let consCreateMasterPDBParams name dump schemas targetSchemas user (date:System.DateTime) comment = 
    { 
        Name=name
        Dump=dump
        Schemas=schemas
        TargetSchemas=targetSchemas
        User=user
        Date=date.ToUniversalTime()
        Comment=comment 
    }

let newCreateMasterPDBParams name dump schemas targetSchemas user comment = 
    consCreateMasterPDBParams name dump schemas targetSchemas user System.DateTime.Now comment

type private CreateMasterPDBParamsValidation = Validation<CreateMasterPDBParams, string>

[<RequireQualifiedAccess>]
module private Validation =
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

type Command =
| GetState // responds with StateResult
| GetMasterPDBState of string // responds with Application.MasterPDBActor.StateResult
| SetInternalState of OracleInstance.OracleInstanceFullDTO // responds with StateResult
| TransferInternalState of IActorRef<obj> // responds with StateResult
| CreateMasterPDB of WithRequestId<CreateMasterPDBParams> // responds with WithRequestId<MasterPDBCreationResult>
| PrepareMasterPDBForModification of WithRequestId<string, int, string> // responds with WithRequestId<MasterPDBActor.PrepareForModificationResult>
| CommitMasterPDB of WithRequestId<string, string, string> // responds with WithRequestId<MasterPDBActor.EditionCommitted>
| RollbackMasterPDB of WithRequestId<string, string> // responds with WithRequestId<MasterPDBActor.EditionRolledBack>
| CreateWorkingCopy of WithRequestId<string, int, string, bool, bool, bool> // responds with WithRequest<CreateWorkingCopyResult>
| DeleteWorkingCopy of WithRequestId<string, int, string> // responds with OraclePDBResultWithReqId
| CreateWorkingCopyOfEdition of WithRequestId<string, string, bool, bool> // responds with RequestValidation
| DeleteWorkingCopyOfEdition of WithRequestId<string, string> // responds with RequestValidation
| CollectGarbage // no response
| GetDumpTransferInfo // responds with DumpTransferInfo

type StateResult = Result<OracleInstance.OracleInstanceDTO, string>
let stateOk state : StateResult = Ok state
let stateError error : StateResult = Error error

type MasterPDBCreationResult = 
| InvalidRequest of string list
| MasterPDBCreated of Domain.MasterPDB.MasterPDB
| MasterPDBCreationFailure of string * string

type CreateWorkingCopyResult = Result<string * int * string * string * string, string>

type DumpTransferInfo = {
    ImpDpLogin: string
    OracleDirectory: string
    OraclePort: int
    RemoteFolder: string
    Server: string
    ServerUser: string
    ServerPassword: string
    ServerHostkeyMD5: string
    ServerHostkeySHA256: string
}

type private Collaborators = {
    OracleShortTaskExecutor: IActorRef<Application.OracleShortTaskExecutor.Command>
    OracleLongTaskExecutor: IActorRef<Application.OracleLongTaskExecutor.Command>
    OracleDiskIntensiveTaskExecutor : IActorRef<Application.OracleDiskIntensiveActor.Command>
    MasterPDBActors: Map<string, IActorRef<obj>>
}

// Spawn actor for a new master PDBs
let private addNewMasterPDB parameters (oracleAPI:IOracleAPI) (ctx : Actor<obj>) (instance : OracleInstance) collaborators (masterPDB:MasterPDB) (newMasterPDBRepo:OracleInstance->MasterPDB->IMasterPDBRepository) = result {
    let! newState = instance |> OracleInstance.addMasterPDB masterPDB.Name
    let newCollaborators = 
        { collaborators with 
            MasterPDBActors = 
                collaborators.MasterPDBActors.Add(
                    masterPDB.Name, 
                    ctx |> MasterPDBActor.spawnNew 
                        parameters
                        instance 
                        collaborators.OracleShortTaskExecutor
                        collaborators.OracleLongTaskExecutor 
                        collaborators.OracleDiskIntensiveTaskExecutor 
                        newMasterPDBRepo
                        masterPDB) 
        }
    return newState, newCollaborators
}

let private expand timeout (masterPDBActors: Map<string, IActorRef<obj>>) (instance:OracleInstance) : Result<OracleInstance.OracleInstanceFullDTO, string> = 
    let masterPDBsMaybe : Result<MasterPDB[],string> = 
        instance.MasterPDBs 
        |> List.map (fun pdb -> retype masterPDBActors.[pdb] <? MasterPDBActor.GetInternalState)
        |> Async.Parallel |> runWithinElseDefaultError timeout
    masterPDBsMaybe 
    |> Result.map (fun masterPDBs -> 
        instance 
        |> Application.DTO.OracleInstance.toFullDTO 
            (masterPDBs |> Array.map Application.DTO.MasterPDB.toDTO |> List.ofArray)
       )

let private updateMasterPDBs parameters (oracleAPI:IOracleAPI) (ctx : Actor<obj>) (instanceToImport : OracleInstance.OracleInstanceFullDTO) (collaborators:Collaborators) (newMasterPDBRepo:OracleInstance->MasterPDB->IMasterPDBRepository) (instance : OracleInstance) = result {
    let masterPDBs = instanceToImport.MasterPDBs
    let existingMasterPDBs = collaborators.MasterPDBActors |> Map.toSeq |> Seq.map (fun (name, _) -> name) |> Set.ofSeq
    let newMasterPDBs = masterPDBs |> List.map (fun pdb -> pdb.Name) |> Set.ofList
    let masterPDBsToAdd = Set.difference newMasterPDBs existingMasterPDBs

    let masterPDBMap = masterPDBs |> List.map (fun pdb -> (pdb.Name, pdb |> MasterPDB.fromDTO)) |> Map.ofList

    let folder result pdb = 
        result |> Result.bind (fun (inst, collabs) -> addNewMasterPDB parameters oracleAPI ctx inst collabs masterPDBMap.[pdb] newMasterPDBRepo)
        
    let! x = masterPDBsToAdd |> Set.fold folder (Ok (instance, collaborators))
    let newCollabs = snd x

    let masterPDBsToUpdate = Set.intersect existingMasterPDBs newMasterPDBs
    masterPDBsToUpdate |> Set.iter (fun pdb -> retype newCollabs.MasterPDBActors.[pdb] <! Application.MasterPDBActor.SetInternalState masterPDBMap.[pdb])

    return x
}

// Spawn actors for master PDBs that already exist
let private spawnCollaborators parameters oracleAPI (getMasterPDBRepo:OracleInstance->string->IMasterPDBRepository) (instance : OracleInstance) (ctx : Actor<obj>) : Collaborators = 
    let oracleShortTaskExecutor = ctx |> OracleShortTaskExecutor.spawn parameters oracleAPI
    let oracleLongTaskExecutor = ctx |> OracleLongTaskExecutor.spawn parameters oracleAPI
    let oracleDiskIntensiveTaskExecutor = ctx |> Application.OracleDiskIntensiveActor.spawn parameters oracleAPI
    let collaborators = {
        OracleShortTaskExecutor = oracleShortTaskExecutor
        OracleLongTaskExecutor = oracleLongTaskExecutor
        OracleDiskIntensiveTaskExecutor = oracleDiskIntensiveTaskExecutor
        MasterPDBActors = 
            instance.MasterPDBs 
            |> List.map (fun pdb -> (pdb, ctx |> MasterPDBActor.spawn parameters instance oracleShortTaskExecutor oracleLongTaskExecutor oracleDiskIntensiveTaskExecutor getMasterPDBRepo pdb))
            |> Map.ofList
    }
    collaborators.OracleLongTaskExecutor |> monitor ctx |> ignore
    collaborators.OracleDiskIntensiveTaskExecutor |> monitor ctx |> ignore
    collaborators.MasterPDBActors |> Map.iter (fun _ actor -> actor |> monitor ctx |> ignore)
    collaborators

let private oracleInstanceActorName (instanceName : string) = 
    Common.ActorName 
        (sprintf "OracleInstance='%s'" (instanceName.ToUpper() |> System.Uri.EscapeDataString))

type private State = {
    Instance: OracleInstance
    PreviousInstance: OracleInstance
    Collaborators: Collaborators
    Requests: RequestMap<Command>
    Repository: IOracleInstanceRepository
}

let private oracleInstanceActorBody 
    (parameters:Parameters) 
    getOracleAPI 
    (getMasterPDBRepo:OracleInstance -> string -> IMasterPDBRepository) 
    (newMasterPDBRepo:OracleInstance -> MasterPDB -> IMasterPDBRepository) 
    (initialRepo:IOracleInstanceRepository)
    (ctx : Actor<obj>) =

    let initialInstance = initialRepo.Get()
    let oracleAPI = getOracleAPI initialInstance

    let rec loop state = actor {

        let instance = state.Instance
        let collaborators = state.Collaborators
        let requests = state.Requests

        if state.PreviousInstance <> instance then
            ctx.Log.Value.Debug("Persisted modified Oracle instance {instance}", instance.Name)
            return! loop { state with Repository = state.Repository.Put instance; PreviousInstance = instance }
        else

        if requests.Count > 0 then ctx.Log.Value.Debug("Number of pending requests : {0}", requests.Count)
        let! msg = ctx.Receive()

        try
        match msg with
        | :? Command as command ->
            match command with
            | GetState ->
                let sender = ctx.Sender().Retype<StateResult>()
                let! instanceDTO = instance |> OracleInstance.toDTO collaborators.MasterPDBActors
                sender <! stateOk instanceDTO
                return! loop state

            | GetMasterPDBState pdb ->
                let sender = ctx.Sender().Retype<Application.MasterPDBActor.StateResult>()
                match instance |> containsMasterPDB pdb with
                | Some pdb ->
                    let masterPDBActor:IActorRef<MasterPDBActor.Command> = retype collaborators.MasterPDBActors.[pdb]
                    masterPDBActor <<! MasterPDBActor.GetState
                | None ->
                    sender <! MasterPDBActor.stateError (sprintf "master PDB %s does not exist on instance %s" pdb instance.Name)
                return! loop state

            | SetInternalState newInstance -> 
                let updateResult = updateMasterPDBs parameters oracleAPI ctx newInstance collaborators newMasterPDBRepo instance
                match updateResult with
                | Ok (inst, collabs) ->
                    let! instanceDTO = inst |> OracleInstance.toDTO collabs.MasterPDBActors
                    ctx.Sender() <! stateOk instanceDTO
                    return! loop { state with Instance = inst; Collaborators = collabs }
                | Error error ->
                    ctx.Sender() <! stateError error
                    return! loop state

            | TransferInternalState target ->
                let expandedInstanceMaybe = instance |> expand parameters.ShortTimeout collaborators.MasterPDBActors
                match expandedInstanceMaybe with
                | Ok expandedInstance -> retype target <<! SetInternalState expandedInstance
                | Error error -> ctx.Sender() <! stateError error
                return! loop state

            | CreateMasterPDB (requestId, parameters) as command ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBCreationResult>>()
                let validation = Validation.validateCreateMasterPDBParams parameters instance
                match validation with
                | Valid _ -> 
                    let (parameters2:OracleLongTaskExecutor.CreatePDBFromDumpParams) = {
                        Name = parameters.Name
                        DumpPath = parameters.Dump
                        Schemas = parameters.Schemas
                        TargetSchemas = parameters.TargetSchemas |> List.map (fun (u, p, _) -> (u, p))
                    }
                    let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                    collaborators.OracleLongTaskExecutor <! OracleLongTaskExecutor.CreatePDBFromDump (Some requestId, parameters2)
                    return! loop { state with Requests = newRequests }
                | Invalid errors -> 
                    sender <! (requestId, InvalidRequest errors)
                    return! loop state

            | PrepareMasterPDBForModification (requestId, pdb, version, user) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.PrepareForModificationResult>>()
                match instance |> containsMasterPDB pdb with
                | None ->
                    sender <! (requestId, MasterPDBActor.PreparationFailure (pdb, sprintf "master PDB %s does not exist on instance %s" pdb instance.Name))
                    return! loop state
                | Some pdb ->
                    let masterPDBActor = collaborators.MasterPDBActors.[pdb]
                    let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                    retype masterPDBActor <! MasterPDBActor.PrepareForModification (requestId, version, user)
                    return! loop { state with Requests = newRequests }

            | CommitMasterPDB (requestId, pdb, locker, comment) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.EditionCommitted>>()
                match instance |> containsMasterPDB pdb with
                | None ->
                    sender <! (requestId, Error (sprintf "master PDB %s does not exist on instance %s" pdb instance.Name))
                    return! loop state
                | Some pdb ->
                    let masterPDBActor = collaborators.MasterPDBActors.[pdb]
                    let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                    retype masterPDBActor <! MasterPDBActor.Commit (requestId, locker, comment)
                    return! loop { state with Requests = newRequests }

            | RollbackMasterPDB (requestId, user, pdb) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.EditionRolledBack>>()
                match instance |> containsMasterPDB pdb with
                | None ->
                    sender <! (requestId, Error (sprintf "master PDB %s does not exist on instance %s" pdb instance.Name))
                    return! loop state
                | Some pdb ->
                    let masterPDBActor = collaborators.MasterPDBActors.[pdb]
                    let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                    retype masterPDBActor <! MasterPDBActor.Rollback (requestId, user)
                    return! loop { state with Requests = newRequests }

            | CreateWorkingCopy (requestId, masterPDBName, versionNumber, wcName, snapshot, durable, force) ->
                let sender = ctx.Sender().Retype<WithRequestId<CreateWorkingCopyResult>>()
                match instance |> containsMasterPDB masterPDBName with
                | None ->
                    sender <! (requestId, Error (sprintf "master PDB %s does not exist on instance %s" masterPDBName instance.Name))
                    return! loop state
                | Some masterPDBName ->
                    let masterPDBActor = collaborators.MasterPDBActors.[masterPDBName]
                    let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                    retype masterPDBActor <! MasterPDBActor.CreateWorkingCopy (requestId, versionNumber, wcName, snapshot, durable, force)
                    return! loop { state with Requests = newRequests }

            | DeleteWorkingCopy (requestId, _, _, wcName) // TODO
            | DeleteWorkingCopyOfEdition (requestId, _, wcName) ->
                let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()
                match instance |> getWorkingCopy wcName with
                | None ->
                    sender <! (requestId, sprintf "working copy %s does not exist on instance %s" wcName instance.Name |> exn |> Error)
                    return! loop state
                | Some workingCopy ->
                    let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                    let garbageCollector = OracleInstanceGarbageCollector.spawn parameters state.Collaborators.OracleShortTaskExecutor state.Collaborators.OracleLongTaskExecutor instance ctx
                    garbageCollector <! OracleInstanceGarbageCollector.DeleteWorkingCopy (requestId, workingCopy)
                    retype garbageCollector <! Akka.Actor.PoisonPill.Instance
                    return! loop { state with Requests = newRequests }

            | CreateWorkingCopyOfEdition (requestId, masterPDBName, wcName, durable, force) ->
                let sender = ctx.Sender().Retype<WithRequestId<CreateWorkingCopyResult>>()
                match instance |> containsMasterPDB masterPDBName with
                | None ->
                    sender <! (requestId, Error (sprintf "master PDB %s does not exist on instance %s" masterPDBName instance.Name))
                    return! loop state
                | Some masterPDBName ->
                    let masterPDBActor = collaborators.MasterPDBActors.[masterPDBName]
                    let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                    retype masterPDBActor <! MasterPDBActor.CreateWorkingCopyOfEdition (requestId, wcName, durable, force)
                    return! loop { state with Requests = newRequests }

            | CollectGarbage ->
                ctx.Log.Value.Info("Garbage collection of instance {instance} requested.", instance.Name)
                let garbageCollector = OracleInstanceGarbageCollector.spawn parameters state.Collaborators.OracleShortTaskExecutor state.Collaborators.OracleLongTaskExecutor instance ctx
                garbageCollector <! OracleInstanceGarbageCollector.CollectGarbage
                retype garbageCollector <! Akka.Actor.PoisonPill.Instance
                //if instance.SnapshotCapable then
                //    collaborators.MasterPDBActors |> Map.iter (fun _ pdbActor -> retype pdbActor <! MasterPDBActor.CollectGarbage)
                //else
                //    collaborators.OracleLongTaskExecutor <! OracleLongTaskExecutor.DeleteOldPDBsInFolder (getWorkingCopyPath instance false)
                return! loop state

            | GetDumpTransferInfo ->
                let transferInfo = {
                    ImpDpLogin = sprintf "%s/%s" instance.UserForImport instance.UserForImportPassword
                    OracleDirectory = instance.OracleDirectoryForDumps
                    OraclePort = instance.Port |> getOracleServerPort
                    RemoteFolder = instance.OracleDirectoryPathForDumps
                    Server = instance.Server
                    ServerUser = instance.UserForFileTransfer
                    ServerPassword = instance.UserForFileTransferPassword
                    ServerHostkeySHA256 = instance.ServerHostkeySHA256
                    ServerHostkeyMD5 = instance.ServerHostkeyMD5
                }
                ctx.Sender() <! transferInfo
                return! loop state
                
        // Callback from Oracle executor
        | :? OraclePDBResultWithReqId as requestResponse ->
            let (requestId, result) = requestResponse
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId
            match requestMaybe with
            | None -> 
                ctx.Log.Value.Error("internal error : request {0} not found", requestId)
                return! loop { state with Requests = newRequests }

            | Some request ->
                match request.Command with 
                | CreateMasterPDB (_, commandParameters) ->
                    let requester = request.Requester.Retype<WithRequestId<MasterPDBCreationResult>>()
                    match result with
                    | Ok _ -> 
                        ctx.Log.Value.Debug("PDB {pdb} created successfully", commandParameters.Name)
                        let newMasterPDB = 
                            Domain.MasterPDB.newMasterPDB 
                                commandParameters.Name 
                                (commandParameters.TargetSchemas |> List.map Domain.MasterPDB.consSchemaFromTuple)
                        let masterPDB = newMasterPDB commandParameters.User commandParameters.Comment
                        let newStateResult = 
                            addNewMasterPDB 
                                parameters
                                oracleAPI
                                ctx 
                                instance
                                collaborators 
                                masterPDB
                                newMasterPDBRepo
                        requester <! (requestId, MasterPDBCreated masterPDB)
                        let newInstance, newCollabs = 
                            match newStateResult with
                            | Ok s -> s
                            | Error error -> 
                                ctx.Log.Value.Error("error when registering new master PDB {pdb} : {error}", commandParameters.Name, error)
                                instance, collaborators
                        return! loop { state with Instance = newInstance; Collaborators = newCollabs; Requests = newRequests }
                    | Error error -> 
                        ctx.Log.Value.Error("PDB {pdb} failed to create with error : {error}", commandParameters.Name, error.Message)
                        requester <! (requestId, MasterPDBCreationFailure (commandParameters.Name, error.Message))
                        return! loop { state with Requests = newRequests }

                | CreateWorkingCopy (requestId, masterPDBName, versionNumber, wcName, snapshot, durable, _) ->
                    let sender = request.Requester.Retype<WithRequestId<CreateWorkingCopyResult>>()
                    match result with
                    | Ok _ ->
                        let wcService = sprintf "%s%s/%s" instance.Server (oracleInstancePortString instance.Port) wcName
                        sender <! (requestId, Ok (masterPDBName, versionNumber, wcName, wcService, instance.Name))
                        let wc = 
                            if durable then 
                                newDurableWorkingCopy "userTODO" (SpecificVersion versionNumber) masterPDBName wcName // TODO
                            else    
                                newTempWorkingCopy parameters.GarbageCollectionDelay "userTODO" (SpecificVersion versionNumber) masterPDBName wcName // TODO
                        return! loop { state with Requests = newRequests; Instance = state.Instance |> addWorkingCopy wc }
                    | Error error ->
                        sender <! (requestId, Error error.Message)
                        return! loop { state with Requests = newRequests }

                | DeleteWorkingCopy (_, _, _, wcName)
                | DeleteWorkingCopyOfEdition (_, _, wcName) ->
                    let sender = request.Requester.Retype<OraclePDBResultWithReqId>()
                    sender <! requestResponse
                    return! loop { state with Requests = newRequests; Instance = instance |> removeWorkingCopy wcName }

                | CreateWorkingCopyOfEdition (requestId, masterPDBName, wcName, durable, _) ->
                    let sender = request.Requester.Retype<WithRequestId<CreateWorkingCopyResult>>()
                    match result with
                    | Ok _ ->
                        let wcService = sprintf "%s%s/%s" instance.Server (oracleInstancePortString instance.Port) wcName
                        sender <! (requestId, Ok (masterPDBName, 0, wcName, wcService, instance.Name))
                        let wc = 
                            if durable then 
                                newDurableWorkingCopy "userTODO" Edition masterPDBName wcName // TODO
                            else    
                                newTempWorkingCopy parameters.GarbageCollectionDelay "userTODO" Edition masterPDBName wcName // TODO
                        return! loop { state with Requests = newRequests; Instance = state.Instance |> addWorkingCopy wc }
                    | Error error ->
                        sender <! (requestId, Error error.Message)
                        return! loop { state with Requests = newRequests }

                | _ -> 
                    ctx.Log.Value.Error "critical error"
                    return! loop state

        // Callback from Master PDB actor in response to PrepareForModification
        | :? WithRequestId<MasterPDBActor.PrepareForModificationResult> as preparationResponse ->
            let (requestId, _) = preparationResponse
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId

            match requestMaybe with
            | Some request -> 
                retype request.Requester <! preparationResponse
                return! loop { state with Requests = newRequests }
            | None -> 
                ctx.Log.Value.Error("internal error : request {0} not found", requestId)
                return! loop { state with Requests = newRequests }

        // Callback from Master PDB actor in response to Commit or Rollback
        | :? WithRequestId<MasterPDBActor.EditionCommitted> as editionResponse ->
            let (requestId, _) = editionResponse
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId

            match requestMaybe with
            | Some request -> 
                retype request.Requester <! editionResponse
                return! loop { state with Requests = newRequests }
            | None -> 
                ctx.Log.Value.Error("internal error : request {0} not found", requestId)
                return! loop state

        // Callback from Master PDB actor in response to Commit or Rollback
        | :? WithRequestId<MasterPDBActor.EditionRolledBack> as editionResponse ->
            let (requestId, _) = editionResponse
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId

            match requestMaybe with
            | Some request -> 
                retype request.Requester <! editionResponse
                return! loop { state with Requests = newRequests }
            | None -> 
                ctx.Log.Value.Error("internal error : request {0} not found", requestId)
                return! loop state

        | :? OracleInstanceGarbageCollector.CommandToParent as commandToParent ->
            match commandToParent with
            | OracleInstanceGarbageCollector.CollectVersionsGarbage (masterPDBName, versions)->
                if (instance.MasterPDBs |> List.contains masterPDBName) then
                    retype state.Collaborators.MasterPDBActors.[masterPDBName] <! MasterPDBActor.CollectVersionsGarbage versions
                else
                    ctx.Log.Value.Warning("Cannot collect garbage for {pdb} because it does not exist on instance {instance}", masterPDBName, instance.Name)
                return! loop state

            | OracleInstanceGarbageCollector.WorkingCopiesDeleted deletionResults ->
                let deletedWorkingCopies, undeletedWorkingCopies = deletionResults |> List.partition (fun (_, errorMaybe) -> errorMaybe |> Option.isNone)
                undeletedWorkingCopies |> List.iter (fun (wc, error) ->
                    ctx.Log.Value.Warning("Cannot delete working copy {workingCopy} : {error}", wc.Name,error.Value.Message)
                )
                let deletedWorkingCopies = deletedWorkingCopies |> List.map (fun (wc, _) -> wc.Name) |> Set.ofList
                return! loop { state with Instance = { state.Instance with WorkingCopies = state.Instance.WorkingCopies |> Map.filter (fun name _ -> not (deletedWorkingCopies |> Set.contains name)) } }

        | Terminated (child, _, _) ->
            ctx.Log.Value.Error("Child actor {0} is down => disabling Oracle instance {1}...", child.Path, initialInstance.Name)
            return! Stop
            
        | _ -> return! loop state
        
        with ex -> 
            ctx.Log.Value.Error("Unexpected error : {0}", ex)
            return! loop state
    }

    let collaborators = ctx |> spawnCollaborators parameters oracleAPI getMasterPDBRepo initialInstance

    loop { 
        Instance = initialInstance
        PreviousInstance = initialInstance
        Collaborators = collaborators
        Requests = Map.empty
        Repository = initialRepo
    }


let spawn 
        parameters 
        getOracleAPI 
        getRepository
        getMasterPDBRepo 
        newMasterPDBRepo 
        actorFactory 
        instanceName =

    let initialRepo:IOracleInstanceRepository = getRepository instanceName

    let (Common.ActorName actorName) = oracleInstanceActorName instanceName

    Akkling.Spawn.spawn actorFactory actorName 
    <| props (oracleInstanceActorBody parameters getOracleAPI getMasterPDBRepo newMasterPDBRepo initialRepo)

let spawnNew 
        parameters 
        getOracleAPI 
        newRepository
        getMasterPDBRepo 
        newMasterPDBRepo 
        actorFactory 
        (instance:OracleInstance) =

    let initialRepo:IOracleInstanceRepository = newRepository instance

    let (Common.ActorName actorName) = oracleInstanceActorName instance.Name

    Akkling.Spawn.spawn actorFactory actorName 
    <| props (oracleInstanceActorBody parameters getOracleAPI getMasterPDBRepo newMasterPDBRepo (initialRepo.Put instance))

