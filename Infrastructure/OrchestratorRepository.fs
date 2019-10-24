module Infrastructure.OrchestratorRepository

open Domain.Orchestrator
open System.IO
open Chiron
open Infrastructure
open Application.Common

let orchestratorPath = sprintf "%s\%s.json"

let loadOrchestrator folder name : Orchestrator =
    use stream = new StreamReader (orchestratorPath folder name)
    let content = stream.ReadToEnd()
    let result = content |> OrchestratorJson.jsonToOrchestrator
    match result with
    | JPass orchestrator -> orchestrator
    | JFail error -> failwith (error.ToString())

let saveOrchestrator folder name orchestrator = 
    Directory.CreateDirectory folder |> ignore
    use stream = File.CreateText (orchestratorPath folder name)
    let json = orchestrator |> OrchestratorJson.orchestratorToJson
    stream.Write json
    stream.Flush()

type OrchestratorRepository(folder, name) = 
    interface IOrchestratorRepository with
        member __.Get () = loadOrchestrator folder name
        member __.Put orchestrator = 
            saveOrchestrator folder name orchestrator
            upcast __
