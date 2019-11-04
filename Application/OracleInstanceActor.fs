module Application.OracleInstanceActor

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
open Application.Parameters
open Domain.MasterPDB
open Application.DTO

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
| CommitMasterPDB of WithRequestId<string, string, string> // responds with WithRequestId<MasterPDBActor.EditionDone>
| RollbackMasterPDB of WithRequestId<string, string> // responds with WithRequestId<MasterPDBActor.EditionDone>
| CreateWorkingCopy of WithRequestId<string, int, string, bool> // responds with WithRequest<MasterPDBActor.CreateWorkingCopyResult>
| DeleteWorkingCopy of WithRequestId<string, int, string> // responds with OraclePDBResultWithReqId
| CollectGarbage // no response

type StateResult = Result<OracleInstance.OracleInstanceDTO, string>
let stateOk state : StateResult = Ok state
let stateError error : StateResult = Error error

type MasterPDBCreationResult = 
| InvalidRequest of string list
| MasterPDBCreated of Domain.MasterPDB.MasterPDB
| MasterPDBCreationFailure of string * string

type private Collaborators = {
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
                        oracleAPI
                        instance 
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
    let oracleLongTaskExecutor = ctx |> OracleLongTaskExecutor.spawn parameters oracleAPI
    let oracleDiskIntensiveTaskExecutor = ctx |> Application.OracleDiskIntensiveActor.spawn parameters oracleAPI
    {
        OracleLongTaskExecutor = oracleLongTaskExecutor
        OracleDiskIntensiveTaskExecutor = oracleDiskIntensiveTaskExecutor
        MasterPDBActors = 
            instance.MasterPDBs 
            |> List.map (fun pdb -> (pdb, ctx |> MasterPDBActor.spawn parameters oracleAPI instance oracleLongTaskExecutor oracleDiskIntensiveTaskExecutor getMasterPDBRepo pdb))
            |> Map.ofList
    }

let private oracleInstanceActorName (instance : OracleInstance) = 
    Common.ActorName 
        (sprintf "OracleInstance='%s'" (instance.Name.ToUpper() |> System.Uri.EscapeDataString))

type private State = {
    Instance: OracleInstance
    PreviousInstance: OracleInstance option
    Collaborators: Collaborators
    Requests: RequestMap<Command>
    Repository: IOracleInstanceRepository
}

let private oracleInstanceActorBody (parameters:Parameters) (oracleAPI:IOracleAPI) (getMasterPDBRepo:OracleInstance -> string -> IMasterPDBRepository) (newMasterPDBRepo:OracleInstance -> MasterPDB -> IMasterPDBRepository) initialRepo initialInstance (ctx : Actor<obj>) =

    let rec loop state = actor {

        let instance = state.Instance
        let collaborators = state.Collaborators
        let requests = state.Requests

        if (state.PreviousInstance.IsNone || state.PreviousInstance.Value <> instance) then
            ctx.Log.Value.Debug("Persisted modified Oracle instance {instance}", instance.Name)
            return! loop { state with Repository = state.Repository.Put instance; PreviousInstance = Some instance }
        else

        ctx.Log.Value.Debug("Number of pending requests : {0}", requests.Count)
        let! msg = ctx.Receive()

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
                let masterPDBOk = instance.MasterPDBs |> List.contains pdb
                if (instance.MasterPDBs |> List.contains pdb) then 
                    let masterPDBActor:IActorRef<MasterPDBActor.Command> = retype collaborators.MasterPDBActors.[pdb]
                    masterPDBActor <<! MasterPDBActor.GetState
                else
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
                    return! loop { state with Requests = newRequests }
                | Invalid errors -> 
                    sender <! (requestId, InvalidRequest errors)
                    return! loop state

            | PrepareMasterPDBForModification (requestId, pdb, version, user) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.PrepareForModificationResult>>()
                let masterPDBOk = instance.MasterPDBs |> List.contains pdb
                if (not masterPDBOk) then 
                    sender <! (requestId, MasterPDBActor.PreparationFailure (pdb, sprintf "master PDB %s does not exist on instance %s" pdb instance.Name))
                    return! loop state
                else
                    let masterPDBActor = collaborators.MasterPDBActors.[pdb]
                    let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                    retype masterPDBActor <! MasterPDBActor.PrepareForModification (requestId, version, user)
                    return! loop { state with Requests = newRequests }

            | CommitMasterPDB (requestId, pdb, locker, comment) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.EditionCommitted>>()
                let masterPDBOk = instance.MasterPDBs |> List.contains pdb
                if (not masterPDBOk) then 
                    sender <! (requestId, Error (sprintf "master PDB %s does not exist on instance %s" pdb instance.Name))
                    return! loop state
                else
                    let masterPDBActor = collaborators.MasterPDBActors.[pdb]
                    let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                    retype masterPDBActor <! MasterPDBActor.Commit (requestId, locker, comment)
                    return! loop { state with Requests = newRequests }

            | RollbackMasterPDB (requestId, user, pdb) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.EditionRolledBack>>()
                let masterPDBOk = instance.MasterPDBs |> List.contains pdb
                if (not masterPDBOk) then 
                    sender <! (requestId, Error (sprintf "master PDB %s does not exist on instance %s" pdb instance.Name))
                    return! loop state
                else
                    let masterPDBActor = collaborators.MasterPDBActors.[pdb]
                    let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                    retype masterPDBActor <! MasterPDBActor.Rollback (requestId, user)
                    return! loop { state with Requests = newRequests }

            | CreateWorkingCopy (requestId, masterPDBName, versionNumber, wcName, force) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.CreateWorkingCopyResult>>()
                let masterPDBOk = instance.MasterPDBs |> List.contains masterPDBName
                if (not masterPDBOk) then 
                    sender <! (requestId, Error (sprintf "master PDB %s does not exist on instance %s" masterPDBName instance.Name))
                    return! loop state
                else
                    let masterPDBActor = collaborators.MasterPDBActors.[masterPDBName]
                    retype masterPDBActor <<! MasterPDBActor.CreateWorkingCopy (requestId, versionNumber, wcName, force)
                    return! loop state

            | DeleteWorkingCopy (requestId, masterPDBName, versionNumber, wcName) ->
                let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.CreateWorkingCopyResult>>()
                let masterPDBOk = instance.MasterPDBs |> List.contains masterPDBName
                if (not masterPDBOk) then 
                    sender <! (requestId, Error (sprintf "master PDB %s does not exist on instance %s" masterPDBName instance.Name))
                    return! loop state
                else
                    let masterPDBActor = collaborators.MasterPDBActors.[masterPDBName]
                    retype masterPDBActor <<! MasterPDBActor.DeleteWorkingCopy (requestId, wcName, versionNumber)
                    return! loop state

            | CollectGarbage ->
                if instance.SnapshotCapable then
                    collaborators.MasterPDBActors |> Map.iter (fun _ pdbActor -> retype pdbActor <! MasterPDBActor.CollectGarbage)
                    ctx.Log.Value.Info("Garbage collection of instance {instance} requested", instance.Name)
                    return! loop state
                else
                    collaborators.OracleLongTaskExecutor <! GarbageWorkingCopies instance
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
                | _ -> 
                    ctx.Log.Value.Error "critical error"
                    return! loop state

        // Callback from Master PDB actor in response to PrepareForModification
        | :? WithRequestId<MasterPDBActor.PrepareForModificationResult> as preparationResult ->
            let (requestId, _) = preparationResult
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId

            match requestMaybe with
            | Some request -> 
                retype request.Requester <! preparationResult
                return! loop { state with Requests = newRequests }
            | None -> 
                ctx.Log.Value.Error("internal error : request {0} not found", requestId)
                return! loop { state with Requests = newRequests }

        // Callback from Master PDB actor in response to Commit or Rollback
        | :? WithRequestId<MasterPDBActor.EditionCommitted> as editionResult ->
            let (requestId, _) = editionResult
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId

            match requestMaybe with
            | Some request -> 
                retype request.Requester <! editionResult
                return! loop { state with Requests = newRequests }
            | None -> 
                ctx.Log.Value.Error("internal error : request {0} not found", requestId)
                return! loop state

        // Callback from Master PDB actor in response to Commit or Rollback
        | :? WithRequestId<MasterPDBActor.EditionRolledBack> as editionResult ->
            let (requestId, _) = editionResult
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId

            match requestMaybe with
            | Some request -> 
                retype request.Requester <! editionResult
                return! loop { state with Requests = newRequests }
            | None -> 
                ctx.Log.Value.Error("internal error : request {0} not found", requestId)
                return! loop state

        | _ -> return! loop state
    }

    let collaborators = ctx |> spawnCollaborators parameters oracleAPI getMasterPDBRepo initialInstance

    loop { 
        Instance = initialInstance
        PreviousInstance = Some initialInstance
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
    let initialInstance = initialRepo.Get()

    let (Common.ActorName actorName) = oracleInstanceActorName initialInstance

    let oracleAPI = getOracleAPI initialInstance

    Akkling.Spawn.spawn actorFactory actorName 
    <| props (oracleInstanceActorBody parameters oracleAPI getMasterPDBRepo newMasterPDBRepo initialRepo initialInstance)

let spawnNew 
        parameters 
        getOracleAPI 
        newRepository
        getMasterPDBRepo 
        newMasterPDBRepo 
        actorFactory 
        instance =

    let initialRepo:IOracleInstanceRepository = newRepository instance

    let (Common.ActorName actorName) = oracleInstanceActorName instance

    let oracleAPI = getOracleAPI instance

    Akkling.Spawn.spawn actorFactory actorName 
    <| props (oracleInstanceActorBody parameters oracleAPI getMasterPDBRepo newMasterPDBRepo initialRepo instance)

