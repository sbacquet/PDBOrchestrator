﻿module Application.OracleInstanceActor

open Akkling
open Application.Oracle
open Application.PendingRequest
open Domain
open Domain.OracleInstance
open Domain.Common.Validation
open Domain.Common
open Domain.Common.Exceptional
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

let consCreateMasterPDBParams (name:string) dump schemas targetSchemas (user:string) (date:System.DateTime) comment = 
    { 
        Name=name.ToUpper()
        Dump=dump
        Schemas=schemas
        TargetSchemas=targetSchemas
        User=user.ToLower()
        Date=date.ToUniversalTime()
        Comment=comment 
    }

let newCreateMasterPDBParams name dump schemas targetSchemas user comment = 
    consCreateMasterPDBParams name dump schemas targetSchemas user System.DateTime.Now comment

type private CreateMasterPDBParamsValidation = Validation<CreateMasterPDBParams, string>

[<RequireQualifiedAccess>]
module private Validation =
    let validatePDB oracleInstance (name:string) =
        if (oracleInstance |> masterPDBAlreadyExists name) then
            Invalid [ sprintf "a PDB named \"%s\" already exists" (name.ToUpper()) ]
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
| GetBasicState // responds with BasicStateResult
| GetMasterPDBState of string // responds with Application.MasterPDBActor.StateResult
| GetMasterPDBEditionInfo of string // responds with Application.MasterPDBActor.EditionInfoResult
| SetInternalState of OracleInstance.OracleInstanceFullDTO // responds with StateResult
| TransferInternalState of IActorRef<obj> // responds with StateResult
| CreateMasterPDB of WithRequestId<CreateMasterPDBParams> // responds with WithRequestId<MasterPDBCreationResult>
| PrepareMasterPDBForModification of WithRequestId<string, int, string> // responds with WithRequestId<MasterPDBActor.PrepareForModificationResult>
| CommitMasterPDB of WithRequestId<string, string, string> // responds with WithRequestId<MasterPDBActor.EditionCommitted>
| RollbackMasterPDB of WithRequestId<string, string> // responds with WithRequestId<MasterPDBActor.EditionRolledBack>
| CreateWorkingCopy of WithRequestId<string, string, int, string, bool, bool, bool> // responds with WithRequest<CreateWorkingCopyResult>
| DeleteWorkingCopy of WithRequestId<string, bool> // responds with OraclePDBResultWithReqId
| CreateWorkingCopyOfEdition of WithRequestId<string, string, string, bool, bool> // responds with RequestValidation
| ExtendWorkingCopy of string // responds with Result<MasterPDBWorkingCopy, string>
| CollectGarbage // no response
| GetDumpTransferInfo // responds with DumpTransferInfo
| DeleteVersion of string * int * bool // responds with Application.MasterPDBActor.DeleteVersionResult
| SwitchLock of string

type StateResult = Result<OracleInstance.OracleInstanceDTO, string>
let stateOk state : StateResult = Ok state
let stateError error : StateResult = Error error

type BasicStateResult = Result<OracleInstance.OracleInstanceBasicDTO, string>

type MasterPDBCreationResult = 
| InvalidRequest of string list
| MasterPDBCreated of string * Domain.MasterPDB.MasterPDB
| MasterPDBCreationFailure of string * string * string

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
let private addNewMasterPDB parameters (ctx : Actor<obj>) (instance : OracleInstance) collaborators (masterPDB:MasterPDB) (newMasterPDBRepo:OracleInstance->MasterPDB->IMasterPDBRepository) = result {
    let! newState = instance |> OracleInstance.addMasterPDB masterPDB.Name
    let newCollaborators = 
        { collaborators with 
            MasterPDBActors = 
                collaborators.MasterPDBActors.Add(
                    masterPDB.Name.ToUpper(), 
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

let getMasterPDBActor (pdb:string) (masterPDBActors:Map<string, IActorRef<obj>>) = masterPDBActors.[pdb.ToUpper()]

let private expand timeout (masterPDBActors: Map<string, IActorRef<obj>>) (instance:OracleInstance) : Result<OracleInstance.OracleInstanceFullDTO, string> = 
    let masterPDBsMaybe : Result<MasterPDB[],string> = 
        instance.MasterPDBs 
        |> List.map (fun pdb -> retype (masterPDBActors |> getMasterPDBActor pdb) <? MasterPDBActor.GetInternalState)
        |> Async.Parallel |> runWithinElseDefaultError timeout
    masterPDBsMaybe 
    |> Result.map (fun masterPDBs -> 
        instance 
        |> Application.DTO.OracleInstance.toFullDTO 
            (masterPDBs |> Array.map Application.DTO.MasterPDB.toDTO |> List.ofArray)
       )

let private updateMasterPDBs parameters (ctx : Actor<obj>) (instanceToImport : OracleInstance.OracleInstanceFullDTO) (collaborators:Collaborators) (newMasterPDBRepo:OracleInstance->MasterPDB->IMasterPDBRepository) (instance : OracleInstance) = result {
    let masterPDBs = instanceToImport.MasterPDBs
    let existingMasterPDBs = collaborators.MasterPDBActors |> Map.toSeq |> Seq.map (fun (name, _) -> name.ToUpper()) |> Set.ofSeq
    let newMasterPDBs = masterPDBs |> List.map (fun pdb -> pdb.Name.ToUpper()) |> Set.ofList
    let masterPDBsToAdd = Set.difference newMasterPDBs existingMasterPDBs

    let masterPDBMap = masterPDBs |> List.map (fun pdb -> (pdb.Name.ToUpper(), pdb |> MasterPDB.fromDTO)) |> Map.ofList

    let folder result pdb = 
        result |> Result.bind (fun (inst, collabs) -> addNewMasterPDB parameters ctx inst collabs masterPDBMap.[pdb] newMasterPDBRepo)
        
    let! x = masterPDBsToAdd |> Set.fold folder (Ok (instance, collaborators))
    let newCollabs = snd x

    let masterPDBsToUpdate = Set.intersect existingMasterPDBs newMasterPDBs
    masterPDBsToUpdate |> Set.iter (fun pdb -> retype (newCollabs.MasterPDBActors |> getMasterPDBActor pdb) <! Application.MasterPDBActor.SetInternalState masterPDBMap.[pdb])

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
    // Monitor death of master PDB actors : if one of them dies, this Oracle instance will be disabled
    collaborators.MasterPDBActors |> Map.iter (fun _ actor -> actor |> monitor ctx |> ignore)
    collaborators

let private oracleInstanceActorName (instanceName : string) = 
    Common.ActorName 
        (sprintf "OracleInstance='%s'" (instanceName.ToLower() |> System.Uri.EscapeDataString))

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

    let rec loop state =

        let instance = state.Instance
        let collaborators = state.Collaborators
        let requests = state.Requests
        let pdbService = pdbServiceFromInstance instance

        if state.PreviousInstance <> instance then
            ctx.Log.Value.Debug("Persisted modified Oracle instance {instance}", instance.Name)
            loop { state with Repository = state.Repository.Put instance; PreviousInstance = instance }

        else 
            
            if requests.Count > 0 then ctx.Log.Value.Debug("Number of pending requests : {0}", requests.Count)

            actor {

            let! msg = ctx.Receive()

            //try
            match msg with
            | :? LifecycleEvent as event ->
                match event with
                | LifecycleEvent.PreStart ->
                    ctx.Log.Value.Info("Checking integrity of Oracle instance {instance}...", state.Instance.Name)
                    let pdbExists pdb : Async<Exceptional<bool>> = state.Collaborators.OracleShortTaskExecutor <? OracleShortTaskExecutor.PDBExists pdb
                    let isWorkingCopyValid name = async {
                        let! exists = pdbExists name
                        match exists with
                        | Ok exists -> return (name, exists)
                        | Error _ -> return (name, true) // ignore errors
                    }
                    let! workingCopyValidations = 
                        instance.WorkingCopies 
                        |> Map.toList 
                        |> Async.traverseP (fst >> isWorkingCopyValid)
                    let workingCopyValidationsMap = workingCopyValidations |> Map.ofList
                    let existingWCs, nonExistingWCs = instance.WorkingCopies |> Map.partition (fun key _ -> workingCopyValidationsMap.[key])
                    nonExistingWCs |> Map.iter (fun name _ -> ctx.Log.Value.Warning("PDB for working copy {pdb} does not exist => removed from the list of Oracle instance {instance}", name, instance.Name))
                    ctx.Log.Value.Info("Integrity of Oracle instance {instance} checked.", state.Instance.Name)
                    return! loop { state with Instance = { state.Instance with WorkingCopies = existingWCs } }
                | _ -> return! loop state

            | :? Command as command ->
                match command with
                | GetState ->
                    let sender = ctx.Sender().Retype<StateResult>()
                    let! instanceDTO = instance |> OracleInstance.toDTO collaborators.MasterPDBActors
                    sender <! stateOk instanceDTO
                    return! loop state

                | GetBasicState ->
                    let sender = ctx.Sender().Retype<BasicStateResult>()
                    let instanceDTO = instance |> Application.DTO.OracleInstance.toBasicDTO
                    sender <! Ok instanceDTO
                    return! loop state

                | GetMasterPDBState pdb ->
                    let sender = ctx.Sender().Retype<Application.MasterPDBActor.StateResult>()
                    match instance |> containsMasterPDB pdb with
                    | Some pdb ->
                        let masterPDBActor:IActorRef<MasterPDBActor.Command> = retype (collaborators.MasterPDBActors |> getMasterPDBActor pdb)
                        masterPDBActor <<! MasterPDBActor.GetState
                    | None ->
                        sender <! MasterPDBActor.stateError (sprintf "master PDB %s does not exist on Oracle instance %s" pdb instance.Name)
                    return! loop state

                | GetMasterPDBEditionInfo pdb ->
                    let sender = ctx.Sender().Retype<Application.MasterPDBActor.EditionInfoResult>()
                    match instance |> containsMasterPDB pdb with
                    | Some pdb ->
                        let masterPDBActor:IActorRef<MasterPDBActor.Command> = retype (collaborators.MasterPDBActors |> getMasterPDBActor pdb)
                        masterPDBActor <<! MasterPDBActor.GetEditionInfo
                    | None ->
                        let error:Application.MasterPDBActor.EditionInfoResult = sprintf "master PDB %s does not exist on Oracle instance %s" pdb instance.Name |> Error
                        sender <! error
                    return! loop state

                | SetInternalState newInstance -> 
                    let updateResult = updateMasterPDBs parameters ctx newInstance collaborators newMasterPDBRepo instance
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
                        sender <! (requestId, MasterPDBActor.PreparationFailure (pdb, sprintf "master PDB %s does not exist on Oracle instance %s" pdb instance.Name))
                        return! loop state
                    | Some pdb ->
                        let masterPDBActor = collaborators.MasterPDBActors |> getMasterPDBActor pdb
                        let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                        retype masterPDBActor <! MasterPDBActor.PrepareForModification (requestId, version, user)
                        return! loop { state with Requests = newRequests }

                | CommitMasterPDB (requestId, pdb, locker, comment) ->
                    let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.EditionCommitted>>()
                    match instance |> containsMasterPDB pdb with
                    | None ->
                        sender <! (requestId, Error (sprintf "master PDB %s does not exist on Oracle instance %s" pdb instance.Name))
                        return! loop state
                    | Some pdb ->
                        let masterPDBActor = collaborators.MasterPDBActors |> getMasterPDBActor pdb
                        let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                        retype masterPDBActor <! MasterPDBActor.Commit (requestId, locker, comment)
                        return! loop { state with Requests = newRequests }

                | RollbackMasterPDB (requestId, user, pdb) ->
                    let sender = ctx.Sender().Retype<WithRequestId<MasterPDBActor.EditionRolledBack>>()
                    match instance |> containsMasterPDB pdb with
                    | None ->
                        sender <! (requestId, Error (sprintf "master PDB %s does not exist on Oracle instance %s" pdb instance.Name))
                        return! loop state
                    | Some pdb ->
                        let masterPDBActor = collaborators.MasterPDBActors |> getMasterPDBActor pdb
                        let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                        retype masterPDBActor <! MasterPDBActor.Rollback (requestId, user)
                        return! loop { state with Requests = newRequests }

                | CreateWorkingCopy (requestId, user, masterPDBName, versionNumber, wcName, snapshot, durable, force) ->
                    let sender = ctx.Sender().Retype<WithRequestId<CreateWorkingCopyResult>>()
                    let cancel = instance |> getWorkingCopy wcName |> Option.bind (fun wc ->
                        if not (wc.MasterPDBName =~ masterPDBName) || 
                           not (wc.CreatedBy =~ user) || 
                           (isDurable wc.Lifetime <> durable)
                        then
                            sprintf "working copy %s already exists but for a different master user/PDB/lifetime (%s/%s/%s)" wcName wc.CreatedBy wc.MasterPDBName (lifetimeType wc.Lifetime) |> Error |> Some
                        else
                            match wc.Source with
                            | SpecificVersion version ->
                                if version = versionNumber then
                                    if force then None else Some (Ok wc)
                                else
                                    sprintf "working copy %s of %s already exists, but for a different version (%d)" wcName wc.MasterPDBName version |> Error |> Some
                            | Edition ->
                                sprintf "working copy %s already exists, but for an edition of %s" wcName wc.MasterPDBName |> Error |> Some
                    )
                    match cancel with
                    | Some result ->
                        match result with
                        | Ok wc -> 
                            sender <! (requestId, Ok (masterPDBName, versionNumber, wcName, pdbService wcName, instance.Name))
                            return! loop { state with Instance = state.Instance |> addWorkingCopy (wc |> extendWorkingCopy parameters.TemporaryWorkingCopyLifetime) }
                        | Error error -> 
                            sender <! (requestId, Error error)
                            return! loop state
                    | None ->
                        match instance |> containsMasterPDB masterPDBName with
                        | None ->
                            sender <! (requestId, Error (sprintf "master PDB %s does not exist on Oracle instance %s" masterPDBName instance.Name))
                            return! loop state
                        | Some masterPDBName ->
                            let masterPDBActor = collaborators.MasterPDBActors |> getMasterPDBActor masterPDBName
                            let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                            retype masterPDBActor <! MasterPDBActor.CreateWorkingCopy (requestId, versionNumber, wcName, snapshot, durable)
                            return! loop { state with Requests = newRequests }

                | DeleteWorkingCopy (requestId, wcName, durable) ->
                    let sender = ctx.Sender().Retype<OraclePDBResultWithReqId>()
                    let workingCopy = instance |> getWorkingCopy wcName
                    match workingCopy with
                    | Some workingCopy -> 
                        if durable <> (workingCopy.Lifetime |> Domain.MasterPDBWorkingCopy.isDurable) then
                            sender <! (requestId, Error <| (sprintf "cannot delete a working copy of durability different from requested; to delete a durable copy, please confirm by providing ?durable=true" |> exn))
                            return! loop state
                        else
                            let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                            let masterPDBActor = collaborators.MasterPDBActors |> getMasterPDBActor workingCopy.MasterPDBName
                            retype masterPDBActor <! MasterPDBActor.DeleteWorkingCopy (requestId, workingCopy)
                            return! loop { state with Requests = newRequests }
                    | None ->
                        let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                        let garbageCollector = OracleInstanceGarbageCollector.spawn parameters state.Collaborators.OracleShortTaskExecutor state.Collaborators.OracleLongTaskExecutor instance ctx
                        garbageCollector <! OracleInstanceGarbageCollector.DeleteWorkingCopy (requestId, wcName)
                        return! loop { state with Requests = newRequests }

                | ExtendWorkingCopy name ->
                    let sender = ctx.Sender().Retype<Result<MasterPDBWorkingCopy, string>>()
                    let workingCopy = instance |> getWorkingCopy name
                    match workingCopy with
                    | Some workingCopy -> 
                        let extendedWorkingCopy = workingCopy |> extendWorkingCopy parameters.TemporaryWorkingCopyLifetime
                        sender <! Ok extendedWorkingCopy
                        return! loop { state with Instance = { state.Instance with WorkingCopies = state.Instance.WorkingCopies |> Map.add name extendedWorkingCopy } }
                    | None ->
                        sender <! Error (sprintf "working copy %s does not exist on Oracle instance %s" name instance.Name)
                        return! loop state

                | CreateWorkingCopyOfEdition (requestId, user, masterPDBName, wcName, durable, force) ->
                    let sender = ctx.Sender().Retype<WithRequestId<CreateWorkingCopyResult>>()
                    let cancel = instance |> getWorkingCopy wcName |> Option.bind (fun wc ->
                        if not (wc.MasterPDBName =~ masterPDBName) || not (wc.CreatedBy =~ user) then
                            sprintf "working copy %s already exists but for a different master PDB/user (%s/%s)" wcName wc.MasterPDBName wc.CreatedBy |> Error |> Some
                        else
                            match wc.Source with
                            | Edition ->
                                if force then None else Some (Ok wc)
                            | SpecificVersion version ->
                                sprintf "working copy %s already exists but for a specific version (%d) of %s" wcName version wc.MasterPDBName |> Error |> Some
                    )
                    match cancel with
                    | Some result ->
                        match result with
                        | Ok wc -> 
                            sender <! (requestId, Ok (masterPDBName, 0, wcName, pdbService wcName, instance.Name))
                            return! loop { state with Instance = state.Instance |> addWorkingCopy (wc |> extendWorkingCopy parameters.TemporaryWorkingCopyLifetime) }
                        | Error error -> 
                            sender <! (requestId, Error error)
                            return! loop state
                    | None ->
                        match instance |> containsMasterPDB masterPDBName with
                        | None ->
                            sender <! (requestId, Error (sprintf "master PDB %s does not exist on Oracle instance %s" masterPDBName instance.Name))
                            return! loop state
                        | Some masterPDBName ->
                            let masterPDBActor = collaborators.MasterPDBActors |> getMasterPDBActor masterPDBName
                            let newRequests = requests |> registerRequest requestId command (retype (ctx.Sender()))
                            retype masterPDBActor <! MasterPDBActor.CreateWorkingCopyOfEdition (requestId, wcName, durable)
                            return! loop { state with Requests = newRequests }

                | CollectGarbage ->
                    ctx.Log.Value.Info("Garbage collection of Oracle instance {instance} requested.", instance.Name)
                    let garbageCollector = OracleInstanceGarbageCollector.spawn parameters state.Collaborators.OracleShortTaskExecutor state.Collaborators.OracleLongTaskExecutor instance ctx
                    garbageCollector <! OracleInstanceGarbageCollector.CollectGarbage
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

                | DeleteVersion (pdb, version, force) ->
                    let sender = ctx.Sender().Retype<Application.MasterPDBActor.DeleteVersionResult>()
                    match instance |> containsMasterPDB pdb with
                    | Some pdb ->
                        let workingCopiesOfThisVersion = instance |> getWorkingCopiesOfVersion pdb version
                        match workingCopiesOfThisVersion, force with
                        | [], _
                        | _::_, true ->
                            let masterPDBActor:IActorRef<MasterPDBActor.Command> = retype (collaborators.MasterPDBActors |> getMasterPDBActor pdb)
                            masterPDBActor <<! MasterPDBActor.DeleteVersion version
                        | _::_, false ->
                            sender <! Error "working copies exist and 'force' was not specified"
                    | None ->
                        sender <! Error (sprintf "master PDB %s does not exist on Oracle instance %s" pdb instance.Name)
                    return! loop state

                | SwitchLock pdb ->
                    let sender = ctx.Sender().Retype<Result<bool,string>>()
                    match instance |> containsMasterPDB pdb with
                    | Some pdb -> 
                        let masterPDBActor:IActorRef<MasterPDBActor.Command> = retype (collaborators.MasterPDBActors |> getMasterPDBActor pdb)
                        masterPDBActor <<! MasterPDBActor.SwitchLock
                    | None ->
                        sender <! Error (sprintf "master PDB %s does not exist on Oracle instance %s" pdb instance.Name)
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
                                    ctx 
                                    instance
                                    collaborators 
                                    masterPDB
                                    newMasterPDBRepo
                            requester <! (requestId, MasterPDBCreated (instance.Name, masterPDB))
                            let newInstance, newCollabs = 
                                match newStateResult with
                                | Ok s -> s
                                | Error error -> 
                                    ctx.Log.Value.Error("error when registering new master PDB {pdb} : {error}", commandParameters.Name, error)
                                    instance, collaborators
                            return! loop { state with Instance = newInstance; Collaborators = newCollabs; Requests = newRequests }
                        | Error error -> 
                            ctx.Log.Value.Error("PDB {pdb} failed to create with error : {error}", commandParameters.Name, error.Message)
                            requester <! (requestId, MasterPDBCreationFailure (instance.Name, commandParameters.Name, error.Message))
                            return! loop { state with Requests = newRequests }

                    | CreateWorkingCopy (requestId, user, masterPDBName, versionNumber, wcName, snapshot, durable, _) ->
                        let sender = request.Requester.Retype<WithRequestId<CreateWorkingCopyResult>>()
                        match result with
                        | Ok _ ->
                            sender <! (requestId, Ok (masterPDBName, versionNumber, wcName, pdbService wcName, instance.Name))
                            let wc = 
                                if durable then 
                                    newDurableWorkingCopy user (SpecificVersion versionNumber) masterPDBName wcName
                                else    
                                    newTempWorkingCopy parameters.TemporaryWorkingCopyLifetime user (SpecificVersion versionNumber) masterPDBName wcName
                            return! loop { state with Requests = newRequests; Instance = state.Instance |> addWorkingCopy wc }
                        | Error error ->
                            sender <! (requestId, Error error.Message)
                            return! loop { state with Requests = newRequests }

                    | DeleteWorkingCopy (_, wcName, _) ->
                        let sender = request.Requester.Retype<OraclePDBResultWithReqId>()
                        sender <! requestResponse
                        return! loop { state with Requests = newRequests; Instance = instance |> removeWorkingCopy wcName }

                    | CreateWorkingCopyOfEdition (requestId, user, masterPDBName, wcName, durable, _) ->
                        let sender = request.Requester.Retype<WithRequestId<CreateWorkingCopyResult>>()
                        match result with
                        | Ok _ ->
                            sender <! (requestId, Ok (masterPDBName, 0, wcName, pdbService wcName, instance.Name))
                            let wc = 
                                if durable then 
                                    newDurableWorkingCopy user Edition masterPDBName wcName
                                else    
                                    newTempWorkingCopy parameters.TemporaryWorkingCopyLifetime user Edition masterPDBName wcName
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
                        retype (state.Collaborators.MasterPDBActors |> getMasterPDBActor masterPDBName) <! MasterPDBActor.CollectVersionsGarbage versions
                    else
                        ctx.Log.Value.Warning("Cannot collect garbage for {pdb} because it does not exist on Oracle instance {instance}", masterPDBName, instance.Name)
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
        (instanceName:string) =

    let instanceName = instanceName.ToLower()

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

