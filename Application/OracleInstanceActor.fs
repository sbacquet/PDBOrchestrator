module Application.OracleInstanceActor

open Akkling
open Application.Oracle
open Application.OracleLongTaskExecutor
open Application.PendingRequest
open Domain
open Domain.OracleInstance
open Application.MasterPDBActor
open Domain.Common.Validation

type CreateMasterPDBParams = {
    Name: string
    Dump: string
    Schemas: string list
    TargetSchemas: (string * string * string) list
    User: string
    Comment: string
}

let newCreateMasterPDBParams name dump schemas targetSchemas user comment = 
    { Name=name; Dump=dump; Schemas=schemas; TargetSchemas=targetSchemas; User=user; Comment=comment }

type CreateMasterPDBParamsValidation = Validation<CreateMasterPDBParams, string>

let validatePDB state name =
    match (state |> findMasterPDB name) with
    | Ok _ -> Invalid [ sprintf "a PDB named \"%s\" already exists" name ]
    | Error _ -> Valid name

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

let validateCreateMasterPDBParams (parameters : CreateMasterPDBParams) (state : Domain.OracleInstance.OracleInstanceState) : CreateMasterPDBParamsValidation =
    let pdb = validatePDB state parameters.Name
    let dump = validateDump parameters.Dump
    let schemas = validateSchemas parameters.Schemas
    let targetSchemas = validateTargetSchemas parameters.Schemas parameters.TargetSchemas
    let user = validateUser parameters.User
    let comment = validateComment parameters.Comment
    retn newCreateMasterPDBParams <*> pdb <*> dump <*> schemas <*> targetSchemas <*> user <*> comment

type Command =
| GetState // returns Domain.OracleInstanceState.OracleInstanceState
| SetState of OracleInstanceState // returns StateSet
| TransferState of (* toInstance : *) IActorRef<obj> // returns (StateSet state)
| CreateMasterPDB of (* withParams : *) CreateMasterPDBParams // returns MasterPDBCreationResult

type StateSet = Result<OracleInstanceState, string>
let stateSetOk state : StateSet = Ok state
let stateSetError error : StateSet = Error error

type MasterPDBCreationResult = 
| MasterPDBCreated of OraclePDBResult
| InvalidRequest of string list

let getNewState oldState newStateResult =
    match newStateResult with
    | Ok state -> state
    | Error error -> 
        System.Diagnostics.Debug.Print("Error: {0}\n", error.ToString()) |> ignore
        oldState

type Collaborators = {
    OracleLongTaskExecutor: IActorRef<Application.OracleLongTaskExecutor.Command>
    MasterPDBActors: Map<string, IActorRef<Application.MasterPDBActor.Command>>
}

let spawnCollaborators oracleAPI (state : OracleInstanceState) (ctx : Actor<obj>) : Collaborators = {
    OracleLongTaskExecutor = ctx |> OracleLongTaskExecutor.spawn oracleAPI
    MasterPDBActors = 
        state.MasterPDBs 
        |> List.map (fun pdb -> (pdb.Name, ctx |> MasterPDBActor.spawn pdb.Name))
        |> Map.ofList
}
    //let p = Akka.Actor.Props.Create(typeof<FunActor<'M>>, [ oracleLogTaskExecutorBody ]).WithRouter(Akka.Routing.FromConfig())
    //spawn ctx "oracleLongTaskExecutor" <| Props.From(p) |> ignore

let oracleInstanceActorName (name:string) = Common.ActorName (sprintf "OracleInstance='%s'" (name.ToUpper() |> System.Uri.EscapeDataString))

let oracleInstanceActorBody getInstance getInstanceState getOracleAPI instanceName (ctx : Actor<obj>) =

    let instanceMaybe, instanceStateMaybe = getInstance instanceName, getInstanceState instanceName
    match instanceMaybe, instanceStateMaybe with
    | Some instance, Some initialState ->
        let oracleAPI = getOracleAPI instance
        let collaborators = ctx |> spawnCollaborators oracleAPI initialState
        let rec loop (state : OracleInstanceState) (requests : RequestMap<Command>) = actor {
            let! msg = ctx.Receive()
            match msg with
            | :? Command as command ->
                match command with
                | GetState -> 
                    logDebugf ctx "State: PDB count = %d" state.MasterPDBs.Length
                    ctx.Sender() <! state
                    return! loop state requests
                | SetState newState -> 
                    ctx.Sender() <! stateSetOk newState
                    return! loop newState requests
                //| MasterPDBVersionCreated (pdb, version, createdBy, comment) ->
                //    let newStateResult =
                //        state |> Domain.State.addMasterPDBVersionToState pdb version createdBy comment 
                //    ctx.Sender() <! newStateResult
                //    return! loop (getNewState state newStateResult)
                | TransferState target ->
                    retype target <<! SetState state
                    return! loop state requests
                | CreateMasterPDB parameters as command ->
                    let validation = validateCreateMasterPDBParams parameters state
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
                        return! loop state <| registerRequest requestId command (retype (ctx.Sender())) requests
                    | Invalid errors -> 
                        ctx.Sender() <! InvalidRequest errors
                        return! loop state requests
            | :? WithRequestId<OraclePDBResult> as requestResponse ->
                let (requestId, result) = requestResponse
                let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId
                match requestMaybe with
                | None -> 
                    logWarningf ctx "Request %s not found" <| requestId.ToString()
                    return! loop state newRequests
                | Some request ->
                    match request.Command with 
                    | CreateMasterPDB parameters ->
                        match result with
                        | Ok _ -> 
                            logDebugf ctx "PDB %s created successfully" parameters.Name
                            let newStateResult = 
                                state 
                                |> Domain.OracleInstance.addMasterPDBToState 
                                    parameters.Name 
                                    (parameters.TargetSchemas |> List.map (fun (user, password, t) -> Domain.PDB.newSchema user password t)) 
                                    parameters.User 
                                    parameters.Comment
                            retype request.Requester <! MasterPDBCreated result
                            return! loop (getNewState state newStateResult) newRequests
                        | Error e -> 
                            logErrorf ctx "PDB %s failed to create with error %A" parameters.Name e
                            retype request.Requester <! MasterPDBCreated result
                            return! loop state newRequests
                    | _ -> failwith "critical error"
            | x -> return! loop state requests
        }
        loop initialState Map.empty
    | _ ->
        failwithf "Oracle instance %s cannot be created" instanceName

let spawn getInstance getInstanceState getOracleAPI instanceName actorFactory =
    let (Common.ActorName actorName) = oracleInstanceActorName instanceName
    Akkling.Spawn.spawn actorFactory actorName <| props (oracleInstanceActorBody getInstance getInstanceState getOracleAPI instanceName)

