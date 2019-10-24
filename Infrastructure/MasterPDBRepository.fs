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

let saveMasterPDB folder name pdb = 
    Directory.CreateDirectory (masterPDBFolder folder name) |> ignore
    use stream = File.CreateText (masterPDBPath folder name)
    let json = pdb |> MasterPDBJson.masterPDBtoJson
    stream.Write json
    stream.Flush()
    
type MasterPDBRepository(folder, name) = 
    interface IMasterPDBRepository with
        member __.Get () = loadMasterPDB folder name
        member __.Put pdb = 
            pdb |> saveMasterPDB folder name
            upcast __

type NewMasterPDBRepository(folder, pdb) = 
    interface IMasterPDBRepository with
        member __.Get () = pdb
        member __.Put pdb = 
            let newRepo = MasterPDBRepository(folder, pdb.Name) :> IMasterPDBRepository
            newRepo.Put pdb
