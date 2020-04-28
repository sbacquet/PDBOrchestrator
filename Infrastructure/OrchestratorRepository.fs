module Infrastructure.OrchestratorRepository

open Domain.Orchestrator
open System.IO
open Chiron
open Infrastructure
open Application.Common
open Akkling

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
    let filePath = orchestratorPath folder name
    use stream = File.CreateText filePath
    let json = orchestrator |> OrchestratorJson.orchestratorToJson
    stream.Write json
    stream.Flush()
    filePath

type GitParams = {
    LogError : string -> string -> unit // error -> ()
    GetModifyComment : string -> string
}

type OrchestratorRepository(logFailure, gitActor, gitParams, folder, name) = 
    interface IOrchestratorRepository with
        member __.Get () = loadOrchestrator folder name
        member this.Put orchestrator = 
            try
                // 1. Save orchestrator to file
                let filePath = saveOrchestrator folder name orchestrator
                // 2. Commit file to Git
                gitActor <! GITActor.Commit (folder, filePath, name, (gitParams.GetModifyComment name), gitParams.LogError)
            with 
            | ex -> logFailure (orchestratorPath folder name) ex
            upcast this

type NewOrchestratorRepository(logFailure, gitActor, gitParams, folder, orchestrator) = 
    interface IOrchestratorRepository with
        member __.Get () = orchestrator
        member this.Put _ =
            let name = "orchestrator"
            try
                // 1. Save orchestrator to file
                let filePath = saveOrchestrator folder name orchestrator
                // 2. Commit file to Git
                gitActor <! GITActor.AddAndCommit (folder, filePath, name, (gitParams.GetModifyComment name), gitParams.LogError)
                OrchestratorRepository(logFailure, gitActor, gitParams, folder, name) :> IOrchestratorRepository
            with 
            | ex ->
                logFailure (orchestratorPath folder name) ex
                upcast this
