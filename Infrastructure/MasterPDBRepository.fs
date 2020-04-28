module Infrastructure.MasterPDBRepository

open Domain.MasterPDB
open System.IO
open Chiron
open Infrastructure
open Application.Common
open Akkling

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
    let filePath = masterPDBPath folder name
    use stream = File.CreateText filePath
    let json = pdb |> MasterPDBJson.masterPDBtoJson
    stream.Write json
    stream.Flush()
    filePath
    
type GitParams = {
    LogError : string -> string -> unit
    GetModifyComment : string -> string
    GetAddComment : string -> string
}

type MasterPDBRepository(logFailure, gitActor, gitParams, folder, name) = 
    interface IMasterPDBRepository with
        member __.Get () = loadMasterPDB folder name
        member this.Put pdb =
            try
                // 1. Save master PDB to file
                let filePath = pdb |> saveMasterPDB folder name
                // 2. Commit file to Git
                gitActor <! GITActor.Commit (folder, filePath, name, (gitParams.GetModifyComment name), gitParams.LogError)
            with
            | ex -> logFailure pdb.Name (masterPDBPath folder name) ex
            upcast this

type NewMasterPDBRepository(logFailure, gitActor, gitParams, folder, pdb) = 
    interface IMasterPDBRepository with
        member __.Get () = pdb
        member this.Put _ = 
            try
                // 1. Save master PDB to file
                let filePath = pdb |> saveMasterPDB folder pdb.Name
                // 2. Add and commit file to Git
                gitActor <! GITActor.AddAndCommit (folder, filePath, pdb.Name, (gitParams.GetAddComment pdb.Name), gitParams.LogError)
                // 3. Return a repository ready to use
                MasterPDBRepository(logFailure, gitActor, gitParams, folder, pdb.Name) :> IMasterPDBRepository
            with
            | ex ->
                logFailure pdb.Name (masterPDBPath folder pdb.Name) ex
                upcast this
