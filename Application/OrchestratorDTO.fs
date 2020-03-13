module Application.DTO.Orchestrator

open Application

type OrchestratorDTO = {
    OracleInstances : string list
    PrimaryInstance : string
}

let consOrchestratorDTO instances primary = { 
    OracleInstances = instances
    PrimaryInstance = primary
}

let getResult (state:OracleInstanceActor.StateResult) : OracleInstance.OracleInstanceDTO =
    match state with
    | Ok result -> result
    | Error _ -> failwith "should never happen" // TODO

let toDTO (orchestrator:Domain.Orchestrator.Orchestrator) =
    consOrchestratorDTO orchestrator.OracleInstanceNames orchestrator.PrimaryInstance
