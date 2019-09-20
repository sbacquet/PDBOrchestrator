module Application.OracleInstanceActor

open Akkling
open Application.Oracle
open Application.OracleLongTaskExecutor
open Application.PendingRequest
open Domain
open Domain.OracleInstance
open Application.MasterPDBActor
open Domain.Common.Validation
open Domain.Common.Result
open System

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

type Command =
| GetState // returns Domain.OracleInstanceState.OracleInstanceState
| SetState of OracleInstance // returns StateSet
| TransferState of (* toInstance : *) IActorRef<obj> // returns (StateSet state)
| CreateMasterPDB of (* withParams : *) CreateMasterPDBParams // returns MasterPDBCreationResult

type StateSet = Result<OracleInstance, string>
let stateSetOk state : StateSet = Ok state
let stateSetError error : StateSet = Error error

type MasterPDBCreationResult = 
| MasterPDBCreated of OraclePDBResult
| InvalidRequest of string list

type Collaborators = {
    OracleLongTaskExecutor: IActorRef<Application.OracleLongTaskExecutor.Command>
    MasterPDBActors: Map<string, IActorRef<Application.MasterPDBActor.Command>>
}

let addMasterPDBToCollaborators ctx getMasterPDBVersion (masterPDB:Domain.MasterPDB.MasterPDB) collaborators = 
    logDebugf ctx "Adding MasterPDB %s to collaborators" masterPDB.Name
    { collaborators with MasterPDBActors = collaborators.MasterPDBActors.Add(masterPDB.Name, ctx |> MasterPDBActor.spawn getMasterPDBVersion masterPDB)
}

// Spawn actor for a new master PDBs
let addNewMasterPDB (ctx : Actor<obj>) getMasterPDBVersion collaborators (masterPDB:Domain.MasterPDB.MasterPDB) user date comments (state : OracleInstance) = result {
    let! newState = state |> OracleInstance.addMasterPDB masterPDB.Name
    let version = Domain.MasterPDBVersion.newPDBVersion masterPDB.Name user date comments
    // TODO: store version, using a new putMasterPDBVersion function
    let newCollaborators = { collaborators with MasterPDBActors = collaborators.MasterPDBActors.Add(masterPDB.Name, ctx |> MasterPDBActor.spawn getMasterPDBVersion masterPDB) }
    // TODO: store masterPDB, using a new putMasterPDB function
    return newState, newCollaborators
}

// Spawn actors for master PDBs that already exist
let spawnCollaborators getOracleAPI getMasterPDB getMasterPDBVersion (instance : OracleInstance) (ctx : Actor<obj>) : Collaborators = 
    let oracleAPI = getOracleAPI instance
    {
        OracleLongTaskExecutor = ctx |> OracleLongTaskExecutor.spawn oracleAPI
        MasterPDBActors = 
            instance.MasterPDBs 
            |> List.map (fun pdb -> (pdb, ctx |> MasterPDBActor.spawn getMasterPDBVersion (getMasterPDB pdb)))
            |> Map.ofList
    }
    //let p = Akka.Actor.Props.Create(typeof<FunActor<'M>>, [ oracleLogTaskExecutorBody ]).WithRouter(Akka.Routing.FromConfig())
    //spawn ctx "oracleLongTaskExecutor" <| Props.From(p) |> ignore

let oracleInstanceActorName (instance : OracleInstance) = 
    Common.ActorName 
        (sprintf "OracleInstance='%s'" (instance.Name.ToUpper() |> System.Uri.EscapeDataString))

let oracleInstanceActorBody getOracleAPI getMasterPDB getMasterPDBVersion initialInstance (ctx : Actor<obj>) =
    let rec loop collaborators (instance : OracleInstance) (requests : RequestMap<Command>) = actor {
        let! msg = ctx.Receive()
        match msg with
        | :? Command as command ->
            match command with
            | GetState -> 
                logDebugf ctx "State: PDB count = %d" instance.MasterPDBs.Length
                ctx.Sender() <! instance
                return! loop collaborators instance requests
            | SetState newState -> 
                ctx.Sender() <! stateSetOk newState
                return! loop collaborators newState requests
            | TransferState target ->
                retype target <<! SetState instance
                return! loop collaborators instance requests
            | CreateMasterPDB parameters as command ->
                let validation = validateCreateMasterPDBParams parameters instance
                match validation with
                | Valid _ -> 
                    let parameters2 = {
                        Name = parameters.Name
                        AdminUserName = instance.DBAUser
                        AdminUserPassword = instance.DBAPassword
                        Destination = instance.MasterPDBManifestsPath
                        DumpPath = parameters.Dump
                        Schemas = parameters.Schemas
                        TargetSchemas = parameters.TargetSchemas |> List.map (fun (u, p, _) -> (u, p))
                        Directory = instance.OracleDirectoryForDumps
                    }
                    let requestId = newRequestId()
                    collaborators.OracleLongTaskExecutor <! OracleLongTaskExecutor.CreatePDBFromDump (requestId, parameters2)
                    return! loop collaborators instance <| registerRequest requestId command (retype (ctx.Sender())) requests
                | Invalid errors -> 
                    ctx.Sender() <! InvalidRequest errors
                    return! loop collaborators instance requests
        | :? WithRequestId<OraclePDBResult> as requestResponse ->
            let (requestId, result) = requestResponse
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId
            match requestMaybe with
            | None -> 
                logWarningf ctx "Request %s not found" <| requestId.ToString()
                return! loop collaborators instance newRequests
            | Some request ->
                match request.Command with 
                | CreateMasterPDB parameters ->
                    match result with
                    | Ok _ -> 
                        logDebugf ctx "PDB %s created successfully" parameters.Name
                        let newMasterPDB = 
                            Domain.MasterPDB.newMasterPDB 
                                parameters.Name 
                                (parameters.TargetSchemas |> List.map Domain.MasterPDB.consSchemaFromTuple)
                        let newStateResult = 
                            addNewMasterPDB 
                                ctx 
                                getMasterPDBVersion 
                                collaborators 
                                newMasterPDB
                                parameters.User 
                                parameters.Date
                                parameters.Comment
                                instance
                        retype request.Requester <! MasterPDBCreated result
                        let newState, newCollabs = 
                            match newStateResult with
                            | Ok s -> s
                            | Error error -> 
                                logErrorf ctx "error when registering new master PDB %s : %s" parameters.Name error
                                instance, collaborators
                        return! loop newCollabs newState newRequests
                    | Error e -> 
                        logErrorf ctx "PDB %s failed to create with error %A" parameters.Name e
                        retype request.Requester <! MasterPDBCreated result
                        return! loop collaborators instance newRequests
                | _ -> failwith "critical error"
        | _ -> return! loop collaborators instance requests
    }
    let collaborators = ctx |> spawnCollaborators getOracleAPI getMasterPDB getMasterPDBVersion initialInstance
    loop collaborators initialInstance Map.empty

let spawn getOracleAPI getMasterPDB getMasterPDBVersion initialInstance actorFactory =
    let (Common.ActorName actorName) = oracleInstanceActorName initialInstance
    Akkling.Spawn.spawn actorFactory actorName 
    <| props (oracleInstanceActorBody getOracleAPI getMasterPDB getMasterPDBVersion initialInstance)

