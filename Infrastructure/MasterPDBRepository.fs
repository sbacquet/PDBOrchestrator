module Infrastructure.MasterPDBRepository

open Domain.MasterPDB
open System.IO
open Chiron
open Infrastructure
open Application.Common

let masterPDBFolder folder _ = sprintf "%s\masterPDBs" folder
let masterPDBPath folder name = sprintf "%s\%s.json" (masterPDBFolder folder name) name

let loadMasterPDB folder name : MasterPDB =
    use stream = new StreamReader (masterPDBPath folder name)
    let content = stream.ReadToEnd()
    let result = content |> MasterPDBJson.jsonToMasterPDB
    match result with
    | JPass masterPDB -> masterPDB
    | JFail error -> failwith (error.ToString())

let loadMasterPDBs rootfolder (names:string list) =
    names 
    |> List.map (fun name -> name, loadMasterPDB rootfolder name)
    |> Map.ofList

let saveMasterPDB folder (cache:Map<string,MasterPDB>) name pdb = 
    Directory.CreateDirectory (masterPDBFolder folder name) |> ignore
    use stream = File.CreateText (masterPDBPath folder name)
    let json = pdb |> MasterPDBJson.masterPDBtoJson
    stream.Write json
    stream.Flush()
    cache |> Map.add name pdb
    
type MasterPDBRepository(folder, cache) = 
    interface IMasterPDBRepository with
        member this.Get name = cache |> Map.find name
        member this.Put name pdb = upcast MasterPDBRepository(folder, pdb |> saveMasterPDB folder cache name)

let loadMasterPDBRepository folder names = 
    MasterPDBRepository(folder, loadMasterPDBs folder names)
