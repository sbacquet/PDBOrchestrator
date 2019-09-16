module Domain.OrchestratorState

type OrchestratorState = {
    ServerStates : Map<string, Domain.State.State>
    PrimaryServer : string
}
