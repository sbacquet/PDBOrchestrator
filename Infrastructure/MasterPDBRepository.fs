module Infrastructure.MasterPDBRepository

open Application.DTO.MasterPDB
open Domain.MasterPDB
open System.IO
open Chiron
open Infrastructure

let loadMasterPDB rootFolder name : MasterPDB =
    use stream = new StreamReader (sprintf "%s\%s.json" rootFolder name)
    let content = stream.ReadToEnd()
    let result = content |> MasterPDBJson.jsonToDTO
    match result with
    | JPass masterPDB -> masterPDB |> fromDTO
    | JFail error -> failwith (error.ToString())

let loadMasterPDBs rootfolder (names:string list) =
    names 
    |> List.map (fun name -> name, loadMasterPDB rootfolder name)
    |> Map.ofList

let saveMasterPDB rootFolder (cache:Map<string,MasterPDB>) name pdb = 
    Directory.CreateDirectory rootFolder |> ignore
    use stream = File.CreateText (sprintf "%s\%s.json" rootFolder name)
    let json = pdb |> toDTO |> MasterPDBJson.DTOtoJson
    stream.Write json
    stream.Flush()
    cache |> Map.add name pdb
    
type MasterPDBRepository(rootFolder, cache) = 
    interface Application.Common.Repository<string, MasterPDB> with
        member this.Get name = cache |> Map.find name
        member this.Put name pdb = upcast MasterPDBRepository(rootFolder, pdb |> saveMasterPDB rootFolder cache name)

let loadMasterPDBRepository rootFolder names = 
    MasterPDBRepository(rootFolder, loadMasterPDBs rootFolder names)
