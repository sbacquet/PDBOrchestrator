module Application.OracleInstanceActor

open Akkling
open Application.Oracle
open Application.OracleLongTaskExecutor
open Application.PendingRequest
open Domain
open Domain.OracleInstanceState
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

let validateCreateMasterPDBParams (parameters : CreateMasterPDBParams) (state : Domain.OracleInstanceState.OracleInstanceState) : CreateMasterPDBParamsValidation =
    let pdb = validatePDB state parameters.Name
    let dump = validateDump parameters.Dump
    let schemas = validateSchemas parameters.Schemas
    let targetSchemas = validateTargetSchemas parameters.Schemas parameters.TargetSchemas
    let user = validateUser parameters.User
    let comment = validateComment parameters.Comment
    retn newCreateMasterPDBParams <*> pdb <*> dump <*> schemas <*> targetSchemas <*> user <*> comment

type Command =
| GetState
| SetState of Domain.OracleInstanceState.OracleInstanceState
| TransferState of string
| CreateMasterPDB of WithRequestId<CreateMasterPDBParams>

type StateSet = | StateSet of string

type MasterPDBCreationResult = 
| MasterPDBCreated of OraclePDBResult
| InvalidRequest of string list


let getNewState oldState newStateResult =
    match newStateResult with
    | Ok state -> state
    | Error error -> 
        System.Diagnostics.Debug.Print("Error: {0}\n", error.ToString()) |> ignore
        oldState

//let masterPDBActorName pdb = sprintf "MasterPDB[%s]" pdb
let masterPDBActorName = id

let spawnChildActors oracleAPI (state : OracleInstanceState) (ctx : Actor<obj>) =
    spawn ctx "oracleLongTaskExecutor" <| props (oracleLongTaskExecutorBody oracleAPI) |> ignore
    state.MasterPDBs |> List.iter (fun pdb -> 
        spawn ctx (masterPDBActorName pdb.Name) <| props (masterPDBActorBody) |> ignore
    )
    //let p = Akka.Actor.Props.Create(typeof<FunActor<'M>>, [ oracleLogTaskExecutorBody ]).WithRouter(Akka.Routing.FromConfig())
    //spawn ctx "oracleLongTaskExecutor" <| Props.From(p) |> ignore

let oracleInstanceActorBody oracleAPI initialState (ctx : Actor<obj>) =
    let rec loop (state : OracleInstanceState) (requests : RequestMap<Command>) = actor {
        let! msg = ctx.Receive()
        match msg with
        | :? Command as command ->
            match command with
            | GetState -> 
                logDebugf ctx "State: PDB count = %d" state.MasterPDBs.Length
                ctx.Sender() <! DTO.stateToDTO state
                return! loop state requests
            | SetState newState -> 
                ctx.Sender() <! StateSet ctx.Self.Path.Name
                return! loop newState requests
            //| MasterPDBVersionCreated (pdb, version, createdBy, comment) ->
            //    let newStateResult =
            //        state |> Domain.State.addMasterPDBVersionToState pdb version createdBy comment 
            //    ctx.Sender() <! newStateResult
            //    return! loop (getNewState state newStateResult)
            | TransferState target ->
                let targetActor = 
                    (select ctx <| sprintf "../%s" target).ResolveOne(System.TimeSpan.FromSeconds(1.))
                    |> Async.RunSynchronously 
                targetActor <<! SetState state
                return! loop state requests
            | CreateMasterPDB (requestId, parameters) as command ->
                let validation = validateCreateMasterPDBParams parameters state
                match validation with
                | Valid _ -> 
                    let oracleExecutor = 
                        (select ctx <| "oracleLongTaskExecutor").ResolveOne(System.TimeSpan.FromSeconds(1.))
                        |> Async.RunSynchronously 
                    let parameters2 = {
                        Name = parameters.Name
                        AdminUserName = "dbadmin"
                        AdminUserPassword = "pass"
                        Destination = "/u01/blabla"
                        DumpPath = parameters.Dump
                        Schemas = parameters.Schemas
                        TargetSchemas = parameters.TargetSchemas |> List.map (fun (u, p, _) -> (u, p))
                        Directory = "blabla"
                    }
                    oracleExecutor <! OracleLongTaskExecutor.CreatePDBFromDump (requestId, parameters2)
                    return! loop state (requests |> registerRequest requestId command (retype (ctx.Sender())))
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
                let parameters = match request.Command with | CreateMasterPDB command -> snd command | _ -> failwith "critical"
                match result with
                | Ok _ -> 
                    logDebugf ctx "PDB %s created successfully" parameters.Name
                    let newStateResult = 
                        state 
                        |> Domain.OracleInstanceState.addMasterPDBToState 
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
        | x -> return! loop state requests
    }
    ctx |> spawnChildActors oracleAPI initialState
    loop initialState Map.empty

