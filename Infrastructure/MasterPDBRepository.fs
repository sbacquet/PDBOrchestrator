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
