module Application.DTO.Orchestrator

open Akkling
open Application

type OrchestratorState = {
    OracleInstances : OracleInstance.OracleInstanceState list
    PrimaryInstance : string
}

let getResult (state:OracleInstanceActor.StateResult) : OracleInstance.OracleInstanceState =
    match state with
    | Ok result -> result
    | Error _ -> failwith "should never happen" // TODO

let toDTO (instanceActors:Map<string, IActorRef<_>>) (orchestrator:Domain.Orchestrator.Orchestrator) = async {
    let! instances = 
        orchestrator.OracleInstanceNames 
        |> List.map (fun instance -> async {
            let! (state:OracleInstanceActor.StateResult) = instanceActors.[instance] <? Application.OracleInstanceActor.GetState
            return getResult state
           })
        |> Async.Parallel
    return { OracleInstances = instances |> Array.toList; PrimaryInstance = orchestrator.PrimaryInstance }
}
