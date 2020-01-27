module Infrastructure.OrchestratorRepository

open Domain.Orchestrator
open System.IO
open Chiron
open Infrastructure
open Application.Common

let orchestratorPath folder name = Path.Combine(folder, sprintf "%s.json" name)

let loadOrchestrator folder name : Orchestrator =
    let file = orchestratorPath folder name
    use stream = new StreamReader(file)
    let content = stream.ReadToEnd()
    let result = content |> OrchestratorJson.jsonToOrchestrator
    match result with
    | JPass orchestrator -> orchestrator
    | JFail error -> 
        error |> JsonFailure.summarize |> failwithf "%s cannot be loaded from JSON file %s :\n%s" name file

let saveOrchestrator folder name orchestrator = 
    Directory.CreateDirectory folder |> ignore
    use stream = File.CreateText (orchestratorPath folder name)
    let json = orchestrator |> OrchestratorJson.orchestratorToJson
    stream.Write json
    stream.Flush()

type GitParams = {
    LogError : string -> string -> unit // error -> ()
    GetModifyComment : string -> string
}

type OrchestratorRepository(logFailure, gitParams, folder, name) = 
    interface IOrchestratorRepository with
        member __.Get () = loadOrchestrator folder name
        member __.Put orchestrator = 
            try
                // 1. Save instance to file
                saveOrchestrator folder name orchestrator
                // 2. Commit file to Git
                gitParams |> Option.map (fun gitParams ->
                orchestratorPath "." name
                |> GIT.commitFile folder (gitParams.GetModifyComment name)
                |> Result.mapError (gitParams.LogError name))
                |> ignore
            with 
            | ex -> logFailure (orchestratorPath folder name) ex
            upcast __
