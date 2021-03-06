﻿module Infrastructure.OracleInstanceRepository

open Domain.OracleInstance
open System.IO
open Chiron
open Infrastructure
open Application.Common
open Domain.MasterPDBWorkingCopy
open Akkling

let instanceFolder folder name = Path.Combine(folder, name)
let instancePath folder name = Path.Combine(instanceFolder folder name, sprintf "%s.json" name)
let instanceWorkingCopiesPath folder name (suffix:string) = Path.Combine(instanceFolder folder name, sprintf "%s_working_copies_%s.json" name suffix)

let loadOracleInstance temporaryTimespan folder name suffix : OracleInstance =
    let file = instancePath folder name
    use stream = new StreamReader(file)
    let content = stream.ReadToEnd()
    let result = content |> OracleInstanceJson.jsonToOracleInstance
    let instance = 
        match result with
        | JPass instance -> instance
        | JFail error -> error |> JsonFailure.summarize |> failwithf "Oracle instance %s cannot be loaded from JSON file %s :\n%s" name file
    let tempWorkingCopies = 
        let file = instanceWorkingCopiesPath folder name suffix
        if File.Exists(file) then
            use stream = new StreamReader(file)
            let content = stream.ReadToEnd()
            let result = content |> OracleInstanceJson.jsonToWorkingCopies (Some temporaryTimespan)
            match result with
            | JPass workingCopies -> workingCopies
            | JFail error -> error |> JsonFailure.summarize |> failwithf "Temporary working copies on Oracle instance %s cannot be loaded from JSON file %s :\n%s" name file
        else
            []
    { instance with WorkingCopies = instance.WorkingCopies |> Map.fold (fun m k v -> m |> Map.add k v) (tempWorkingCopies |> List.map (fun wc -> wc.Name, wc) |> Map.ofList) }

let saveTemporaryWorkingCopies folder name suffix instance =
    let file = instanceWorkingCopiesPath folder name suffix
    let tempWorkingCopies = instance.WorkingCopies |> Map.toList |> List.map snd |> List.filter isTemporary
    if tempWorkingCopies |> List.isEmpty then
        File.Delete(file)
    else
        use stream = File.CreateText(file)
        let json = tempWorkingCopies |> OracleInstanceJson.workingCopiesToJson
        stream.Write json
        stream.Flush()

let saveOracleInstance folder name suffix instance =
    Directory.CreateDirectory(instanceFolder folder name) |> ignore
    let filePath = instancePath folder name
    use stream = File.CreateText(filePath)
    let json = instance |> OracleInstanceJson.oracleInstanceToJson
    stream.Write json
    stream.Flush()
    instance |> saveTemporaryWorkingCopies folder name suffix
    filePath

type GitParams = {
    LogError : string -> string -> unit // instance -> error -> ()
    GetModifyComment : string -> string
    GetAddComment : string -> string
}

type OracleInstanceRepository(temporaryTimespan, logFailure, gitActor:IActorRef<GITActor.Command>, gitParams, folder, name, suffix) = 
    interface IOracleInstanceRepository with

        member __.Get () = loadOracleInstance temporaryTimespan folder name suffix

        member this.Put instance = 
            try
                // 1. Save instance to file
                let filePath = instance |> saveOracleInstance folder name suffix
                // 2. Commit file to Git
                gitActor <! GITActor.Commit (folder, filePath, name, (gitParams.GetModifyComment name), gitParams.LogError)
            with
            | ex -> logFailure instance.Name (instancePath folder name) ex
            upcast this

        member this.PutWorkingCopiesOnly instance = 
            try
                instance |> saveTemporaryWorkingCopies folder name suffix
            with
            | ex -> logFailure instance.Name (instancePath folder name) ex
            upcast this

type NewOracleInstanceRepository(temporaryTimespan, logFailure, gitActor, gitParams, folder, instance, suffix) = 
    interface IOracleInstanceRepository with

        member __.Get () = instance

        member this.Put _ = 
            try
                // 1. Save instance to file
                let filePath = instance |> saveOracleInstance folder instance.Name suffix
                // 2. Add and commit file to Git
                gitActor <! GITActor.AddAndCommit (folder, filePath, instance.Name, (gitParams.GetAddComment instance.Name), gitParams.LogError)
                // 3. Return a repository ready to use
                OracleInstanceRepository(temporaryTimespan, logFailure, gitActor, gitParams, folder, instance.Name, suffix) :> IOracleInstanceRepository
            with
            | ex ->
                logFailure instance.Name (instancePath folder instance.Name) ex
                upcast this

        member this.PutWorkingCopiesOnly _ = upcast this