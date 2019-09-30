module Infrastructure.OrchestratorRepository

open Domain.Orchestrator
open System.IO
open Chiron
open Infrastructure
open Application.Common

let loadOrchestrator rootFolder name : Orchestrator =
    use stream = new StreamReader (sprintf "%s\%s.json" rootFolder name)
    let content = stream.ReadToEnd()
    let result = content |> OrchestratorJson.jsonToOrchestrator
    match result with
    | JPass orchestrator -> orchestrator
    | JFail error -> failwith (error.ToString())

let saveOrchestrator rootFolder name orchestrator = 
    Directory.CreateDirectory rootFolder |> ignore
    use stream = File.CreateText (sprintf "%s\%s.json" rootFolder name)
    let json = orchestrator |> OrchestratorJson.orchestratorToJson
    stream.Write json
    stream.Flush()

type OrchestratorRepository(rootFolder) = 
    interface IOrchestratorRepository with
        member this.Get name = loadOrchestrator rootFolder name
        member this.Put name orchestrator = saveOrchestrator rootFolder name orchestrator; upcast this
