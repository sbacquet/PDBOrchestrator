module Application.DTO.Orchestrator

open Akkling
open Application

type OrchestratorState = {
    OracleInstances : OracleInstance.OracleInstanceDTO list
    PrimaryInstance : string
}

let consOrchestratorState instances primary = { 
    OracleInstances = instances |> Seq.toList
    PrimaryInstance = primary
}

let getResult (state:OracleInstanceActor.StateResult) : OracleInstance.OracleInstanceDTO =
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
    return consOrchestratorState instances orchestrator.PrimaryInstance
}
