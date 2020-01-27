module Infrastructure.MasterPDBRepository

open Domain.MasterPDB
open System.IO
open Chiron
open Infrastructure
open Application.Common

let masterPDBFolder folder _ = Path.Combine(folder, "masterPDBs")
let masterPDBPath folder name = Path.Combine(masterPDBFolder folder name, sprintf "%s.json"  name)

let loadMasterPDB folder name : MasterPDB =
    let file = masterPDBPath folder name
    use stream = new StreamReader(file)
    let content = stream.ReadToEnd()
    let result = content |> MasterPDBJson.jsonToMasterPDB
    match result with
    | JPass masterPDB -> masterPDB
    | JFail error -> error |> JsonFailure.summarize |> failwithf "master PDB %s cannot be loaded from JSON file %s :\n%s" name file

let saveMasterPDB folder name pdb = 
    Directory.CreateDirectory (masterPDBFolder folder name) |> ignore
    use stream = File.CreateText (masterPDBPath folder name)
    let json = pdb |> MasterPDBJson.masterPDBtoJson
    stream.Write json
    stream.Flush()
    
type GitParams = {
    LogError : string -> string -> unit
    GetModifyComment : string -> string
    GetAddComment : string -> string
}

type MasterPDBRepository(logFailure, gitParams, folder, name) = 
    interface IMasterPDBRepository with
        member __.Get () = loadMasterPDB folder name
        member __.Put pdb =
            try
                // 1. Save instance to file
                pdb |> saveMasterPDB folder name
                // 2. Commit file to Git
                gitParams |> Option.map (fun gitParams ->
                masterPDBPath "." name
                |> GIT.commitFile folder (gitParams.GetModifyComment name)
                |> Result.mapError (gitParams.LogError pdb.Name)) 
                |> ignore
            with
            | ex -> logFailure pdb.Name (masterPDBPath folder name) ex
            upcast __

type NewMasterPDBRepository(logFailure, gitParams, folder, pdb) = 
    interface IMasterPDBRepository with
        member __.Get () = pdb
        member __.Put _ = 
            let filePath = masterPDBPath "." pdb.Name
            // 1. Add file to Git
            gitParams |> Option.map (fun gitParams ->
            filePath 
            |> GIT.addFile folder
            |> Result.mapError (gitParams.LogError pdb.Name))
            |> ignore
            // 2. Save and commit it
            try
                pdb |> saveMasterPDB folder pdb.Name
                gitParams |> Option.map (fun gitParams ->
                filePath
                |> GIT.commitFile folder (gitParams.GetAddComment pdb.Name)
                |> Result.mapError (gitParams.LogError pdb.Name))
                |> ignore
            with
            | ex -> logFailure pdb.Name (masterPDBPath folder pdb.Name) ex
            // Return a repository ready to use
            MasterPDBRepository(logFailure, gitParams, folder, pdb.Name) :> IMasterPDBRepository
