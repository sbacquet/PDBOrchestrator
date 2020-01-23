module Infrastructure.OracleInstanceRepository

open Domain.OracleInstance
open System.IO
open Chiron
open Infrastructure
open Application.Common

let instanceFolder folder name = Path.Combine(folder, name)
let instancePath folder name = Path.Combine(instanceFolder folder name, sprintf "%s.json" name)
let instanceWorkingCopiesPath folder name (suffix:string) = Path.Combine(instanceFolder folder name, sprintf "%s_working_copies_%s.json" name suffix)

let loadOracleInstance folder name suffix : OracleInstance =
    let file = instancePath folder name
    use stream = new StreamReader(file)
    let content = stream.ReadToEnd()
    let result = content |> OracleInstanceJson.jsonToOracleInstance
    let instance = 
        match result with
        | JPass instance -> instance
        | JFail error -> error |> JsonFailure.summarize |> failwithf "Oracle instance %s cannot be loaded from JSON file %s :\n%s" name file
    let workingCopies = 
        let file = instanceWorkingCopiesPath folder name suffix
        if File.Exists(file) then
            use stream = new StreamReader(file)
            let content = stream.ReadToEnd()
            let result = content |> OracleInstanceJson.jsonToWorkingCopies
            match result with
            | JPass workingCopies -> workingCopies
            | JFail error -> error |> JsonFailure.summarize |> failwithf "Working copies on Oracle instance %s cannot be loaded from JSON file %s :\n%s" name file
        else
            []
    { instance with WorkingCopies = instance.WorkingCopies |> Map.fold (fun m k v -> m |> Map.add k v) (workingCopies |> List.map (fun wc -> wc.Name, wc) |> Map.ofList) }

let saveWorkingCopies folder name suffix instance =
    let file = instanceWorkingCopiesPath folder name suffix
    if instance.WorkingCopies |> Map.isEmpty then
        File.Delete(file)
    else
        use stream = File.CreateText(file)
        let json = instance.WorkingCopies |> Map.toList |> List.map snd |> OracleInstanceJson.workingCopiesToJson
        stream.Write json
        stream.Flush()

let saveOracleInstance folder name suffix instance =
    Directory.CreateDirectory(instanceFolder folder name) |> ignore
    use stream = File.CreateText(instancePath folder name)
    let json = instance |> OracleInstanceJson.oracleInstanceToJson
    stream.Write json
    stream.Flush()
    instance |> saveWorkingCopies folder name suffix

type OracleInstanceRepository(logFailure, folder, name, suffix) = 
    interface IOracleInstanceRepository with
        member __.Get () = loadOracleInstance folder name suffix
        member __.Put instance = 
            try
                instance |> saveOracleInstance folder name suffix
            with
            | ex -> logFailure instance.Name (instancePath folder name) ex
            upcast __
        member __.PutWorkingCopiesOnly instance = 
            try
                instance |> saveWorkingCopies folder name suffix
            with
            | ex -> logFailure instance.Name (instancePath folder name) ex
            upcast __

type NewOracleInstanceRepository(logFailure, folder, instance, suffix) = 
    interface IOracleInstanceRepository with
        member __.Get () = instance
        member __.Put inst = 
            let newRepo = OracleInstanceRepository(logFailure, folder, instance.Name, suffix) :> IOracleInstanceRepository
            newRepo.Put inst
        member __.PutWorkingCopiesOnly inst = 
            let newRepo = OracleInstanceRepository(logFailure, folder, instance.Name, suffix) :> IOracleInstanceRepository
            newRepo.PutWorkingCopiesOnly inst
