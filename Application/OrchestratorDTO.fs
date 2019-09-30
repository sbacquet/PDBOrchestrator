module Application.DTO.Orchestrator

open Akkling

type OrchestratorState = {
    OracleInstances : OracleInstance.OracleInstanceState list
    PrimaryInstance : string
}

let toDTO (instanceActors:Map<string, IActorRef<_>>) (orchestrator:Domain.Orchestrator.Orchestrator) = async {
    let! instances = 
        orchestrator.OracleInstanceNames 
        |> List.map (fun instance -> instanceActors.[instance] <? Application.OracleInstanceActor.GetState)
        |> Async.Parallel
    return { OracleInstances = instances |> Array.toList; PrimaryInstance = orchestrator.PrimaryInstance }
}
