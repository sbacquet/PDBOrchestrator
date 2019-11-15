module Domain.Orchestrator

open Domain.Common

type Orchestrator = {
    OracleInstanceNames : string list
    PrimaryInstance : string
}

let consOrchestrator instances primary = { OracleInstanceNames = instances; PrimaryInstance = primary }

let containsOracleInstance (instance:string) orchestrator =
    orchestrator.OracleInstanceNames |> List.tryFind ((=~)instance)
