module Application.OracleInstanceActor

open Akkling
open Application.Oracle
open Application.OracleLongTaskExecutor
open Application.PendingRequest
open Domain
open Domain.OracleInstanceState
open Application.MasterPDBActor

type CreateMasterPDBParams = {
    Name: string
    Dump: string
    Schemas: string list
    TargetSchemas: (string * string * string) list
    User: string
    Comment: string
}

type CreateMasterPDBParamsValidation =
| Valid
| PDBAlreadyExists of string

let validationMessage validation =
    match validation with
    | Valid -> "parameters are valid"
    | PDBAlreadyExists pdb -> sprintf "a PDB with name %s already exists" pdb

let validateCreateMasterPDBParams (parameters : CreateMasterPDBParams) (state : Domain.OracleInstanceState.OracleInstanceState) =
    match (state.MasterPDBs |> List.exists (fun x -> x.Name = parameters.Name)) with
    | true -> PDBAlreadyExists parameters.Name
    | false -> Valid

type Command =
| GetState
| SetState of Domain.OracleInstanceState.OracleInstanceState
| TransferState of string
| CreateMasterPDB of WithRequestId<CreateMasterPDBParams>

type StateSet = | StateSet of string

type MasterPDBCreationResult = 
| MasterPDBCreated of OraclePDBResult
| InvalidRequest of string


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
                System.Diagnostics.Debug.Print("State: PDB count = {0}\n", state.MasterPDBs.Length) |> ignore
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
                | Valid -> 
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
                | error -> 
                    ctx.Sender() <! InvalidRequest (validationMessage error)
                    return! loop state requests
        | :? WithRequestId<OraclePDBResult> as requestResponse ->
            let (requestId, result) = requestResponse
            let (requestMaybe, newRequests) = requests |> getAndUnregisterRequest requestId
            match requestMaybe with
            | None -> return! loop state newRequests
            | Some request ->
                let parameters = match request.Command with | CreateMasterPDB command -> snd command | _ -> failwith "critical"
                match result with
                | Ok _ -> 
                    printfn "PDB %s created" parameters.Name
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
                    printfn "PDB %s failed to create with error %A" parameters.Name e
                    retype request.Requester <! MasterPDBCreated result
                    return! loop state newRequests
        | _ -> return! loop state requests
    }
    ctx |> spawnChildActors oracleAPI initialState
    loop initialState Map.empty

