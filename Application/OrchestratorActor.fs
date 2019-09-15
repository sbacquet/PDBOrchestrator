module Application.OrchestratorActor

type Message =
| Synchronize of string
| StateSet of Domain.State.State
