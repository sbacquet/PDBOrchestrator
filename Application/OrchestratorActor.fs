module Application.OrchestratorActor

open Akkling
open Application.OracleInstanceActor
open Domain.Orchestrator

type OnInstance<'C> = string * 'C

type InstanceResult<'R> = Result<'R, string>

type Command =
| Synchronize of string
| GetState
| CreateMasterPDB of (* withParams : *) CreateMasterPDBParams // returns MasterPDBCreationResult

// TODO : try/catch ActorNotFoundException and return option
let spawnCollaborators getOracleAPI getInstance getMasterPDB getMasterPDBVersion state (ctx : Actor<_>) =
    state.OracleInstanceNames 
    |> List.map (fun instanceName -> 
        instanceName,
        ctx |> OracleInstanceActor.spawn getOracleAPI getMasterPDB getMasterPDBVersion (getInstance instanceName))
    |> Map.ofList

let createMasterPDBError error : MasterPDBCreationResult = InvalidRequest [ error ]

let orchestratorActorBody getOracleAPI getInstance getMasterPDB getMasterPDBVersion initialState (ctx : Actor<_>) =
    let collaborators = ctx |> spawnCollaborators getOracleAPI getInstance getMasterPDB getMasterPDBVersion initialState
    let rec loop (state : Orchestrator) = actor {
        let! msg = ctx.Receive()
        match msg with
        | Synchronize targetInstance ->
            if (state.OracleInstanceNames |> List.contains targetInstance) then
                let primaryInstance = collaborators.[state.PrimaryServer]
                let target = collaborators.[targetInstance]
                retype primaryInstance <<! TransferState target
            else
                ctx.Sender() <! stateSetError (sprintf "cannot find actor of instance %s" targetInstance)
            return! loop state
        | GetState ->
            ctx.Sender() <! state
            return! loop state
        | CreateMasterPDB parameters ->
            let primaryInstance = collaborators.[state.PrimaryServer]
            retype primaryInstance <<! Application.OracleInstanceActor.CreateMasterPDB parameters
    }
    loop initialState

let [<Literal>]cOrchestratorActorName = "Orchestrator"

let spawn getOracleAPI getInstance getMasterPDB getMasterPDBVersion initialState actorFactory =
    Akkling.Spawn.spawn actorFactory cOrchestratorActorName <| props (orchestratorActorBody getOracleAPI getInstance getMasterPDB getMasterPDBVersion initialState)
