module Application.OracleServerActor

open Akkling
open Application.Oracle
open Application.OracleLongTaskExecutor

type Command =
| GetState
| SetState of Domain.State.State
| TransferState of string
| CreateMasterPDB of string

type Event =
| StateSet of string
| MasterPDBCreated of OraclePDBResult

let getNewState oldState newStateResult =
    match newStateResult with
    | Ok state -> state
    | Error error -> 
        System.Diagnostics.Debug.Print("Error: {0}\n", error.ToString()) |> ignore
        oldState

type TransientState = {
    PendingPDBRequests: Map<PendingRequest.RequestId, PendingRequest.PendingRequest<Command>>
}

let newTransientState () = { PendingPDBRequests = Map.empty }

let registerCommand command requester transientState =
    let id = System.Guid.NewGuid()
    let request = { PendingRequest.Id = id; PendingRequest.Command = command; PendingRequest.Requester = requester }
    id, { transientState with PendingPDBRequests = transientState.PendingPDBRequests.Add(id, request) }

let getRequest id transientState =
    transientState.PendingPDBRequests.TryFind id

let spawnChildActors<'M> (ctx : Actor<'M>) =
    spawn ctx "oracleLongTaskExecutor" <| props oracleLogTaskExecutorBody |> ignore
    //let p = Akka.Actor.Props.Create(typeof<FunActor<'M>>, [ oracleLogTaskExecutorBody ]).WithRouter(Akka.Routing.FromConfig())
    //spawn ctx "oracleLongTaskExecutor" <| Props.From(p) |> ignore

let oracleServerActorBody initialDomainState (ctx : Actor<Command>) =
    let rec loop (state : Domain.State.State * TransientState) = actor {
        let! msg = ctx.Receive()
        let domainState, transientState = state
        match msg with
        //| :? Command as command ->
        //    match command with
            | GetState -> 
                System.Diagnostics.Debug.Print("State: PDB count = {0}\n", domainState.MasterPDBs.Length) |> ignore
                ctx.Sender() <! DTO.stateToDTO domainState
                return! loop state
            | SetState newState -> 
                ctx.Sender() <! StateSet ctx.Self.Path.Name
                return! loop (newState, transientState)
            //| MasterPDBCreated (name, schemas, user, comment) -> 
            //    let newStateResult = 
            //        state 
            //        |> Domain.State.addMasterPDBToState 
            //            name 
            //            (schemas |> List.map (fun (user, password, t) -> Domain.PDB.newSchema user password t)) 
            //            user 
            //            comment
            //    ctx.Sender() <! newStateResult
            //    return! loop (getNewState state newStateResult)
            //| MasterPDBVersionCreated (pdb, version, createdBy, comment) ->
            //    let newStateResult =
            //        state |> Domain.State.addMasterPDBVersionToState pdb version createdBy comment 
            //    ctx.Sender() <! newStateResult
            //    return! loop (getNewState state newStateResult)
            | TransferState target ->
                let targetActor = 
                    (select ctx <| sprintf "../%s" target).ResolveOne(System.TimeSpan.FromSeconds(1.))
                    |> Async.RunSynchronously 
                targetActor <<! SetState domainState
                return! loop state
            | CreateMasterPDB name as command ->
                //let requestId, newTransientState = registerCommand command (untyped (ctx.Sender())) transientState
                let oracleExecutor = 
                    (select ctx <| "oracleLongTaskExecutor").ResolveOne(System.TimeSpan.FromSeconds(1.))
                    |> Async.RunSynchronously 
                oracleExecutor <<! OracleLongTaskExecutor.CreatePDB ("dbadmin", "pass", "/u01/blabla", name)
                return! loop state
        //| :? PendingRequest.RequestResult<OraclePDBResult> as requestResult ->
        //    let (requestId, result) = requestResult
        //    let request = transientState |> getRequest requestId
        //    match request with
        //    | Some r ->
        //        match r.Command with
        //        | CreateMasterPDB name -> 
        //            (typed r.Requester) <! MasterPDBCreated result
        //            return! loop (domainState, transientState)
        //        | _ -> return! loop state
        //    | None -> 
        //        printfn "!!! Request with id %s not found !!!" (requestId.ToString())
        //        return! loop state
        //| _ -> return! loop state
    }
    ctx |> spawnChildActors<Command>
    loop (initialDomainState, newTransientState())

