module Application.OracleServerActor

open Akkling
open Application.Oracle
open Application.OracleLongTaskExecutor

type CreateMasterPDBParams = {
    Name: string
    Dump: string
    Schemas: string list
    TargetSchemas: (string * string * string) list
    User: string
    Comment: string
}

type Command =
| GetState
| SetState of Domain.State.State
| TransferState of string
| CreateMasterPDB of CreateMasterPDBParams
| PDBCreated of CreateMasterPDBParams * OraclePDBResult * IActorRef<OraclePDBResult>

type StateSet = | StateSet of string
type MasterPDBCreated = | MasterPDBCreated of OraclePDBResult

let getNewState oldState newStateResult =
    match newStateResult with
    | Ok state -> state
    | Error error -> 
        System.Diagnostics.Debug.Print("Error: {0}\n", error.ToString()) |> ignore
        oldState

let spawnChildActors<'M> (ctx : Actor<'M>) =
    spawn ctx "oracleLongTaskExecutor" <| props oracleLogTaskExecutorBody |> ignore
    //let p = Akka.Actor.Props.Create(typeof<FunActor<'M>>, [ oracleLogTaskExecutorBody ]).WithRouter(Akka.Routing.FromConfig())
    //spawn ctx "oracleLongTaskExecutor" <| Props.From(p) |> ignore

let oracleServerActorBody initialState (ctx : Actor<_>) =
    let rec loop (state : Domain.State.State) = actor {
        let! msg = ctx.Receive()
        match msg with
        | GetState -> 
            System.Diagnostics.Debug.Print("State: PDB count = {0}\n", state.MasterPDBs.Length) |> ignore
            ctx.Sender() <! DTO.stateToDTO state
            return! loop state
        | SetState newState -> 
            ctx.Sender() <! StateSet ctx.Self.Path.Name
            return! loop newState
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
            return! loop state
        | CreateMasterPDB parameters ->
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
                Callback = fun sender r -> 
                    typed(sender) <! PDBCreated (parameters, r, ctx.Sender())                
            }
            oracleExecutor <! OracleLongTaskExecutor.CreatePDBFromDump parameters2
            return! loop state
        | PDBCreated (parameters, result, replyTo) ->
            match result with
            | Ok _ -> 
                printfn "PDB %s created" parameters.Name
                let newStateResult = 
                    state 
                    |> Domain.State.addMasterPDBToState 
                        parameters.Name 
                        (parameters.TargetSchemas |> List.map (fun (user, password, t) -> Domain.PDB.newSchema user password t)) 
                        parameters.User 
                        parameters.Comment
                replyTo <! result
                return! loop (getNewState state newStateResult)
            | Error e -> 
                printfn "PDB %s failed to create with error %A" parameters.Name e
                replyTo <! result
                return! loop state
    }
    ctx |> spawnChildActors<Command>
    loop initialState

