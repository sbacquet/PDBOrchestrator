module Domain.Orchestrator

type Orchestrator = {
    OracleInstanceNames : string list
    PrimaryInstance : string
}

let consOrchestrator instances primary = { OracleInstanceNames = instances; PrimaryInstance = primary }
