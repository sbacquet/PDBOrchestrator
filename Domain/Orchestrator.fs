module Domain.Orchestrator

type Orchestrator = {
    OracleInstanceNames : string list
    PrimaryInstance : string
}

let consOrchestrator instances (primary:string) = { OracleInstanceNames = instances |> List.map (fun (instance:string) -> instance.ToLower()); PrimaryInstance = primary.ToLower() }

let containsOracleInstance (instance:string) orchestrator =
    orchestrator.OracleInstanceNames |> List.tryFind ((=)(instance.ToLower()))
