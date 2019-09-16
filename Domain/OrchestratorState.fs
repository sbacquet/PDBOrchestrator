module Domain.OrchestratorState

type OracleInstance = {
    Name: string
    Server: string
    DBAUser: string
    DBAPassword: string
}

type OrchestratorState = {
    OracleInstances : OracleInstance list
    PrimaryServer : string
}
