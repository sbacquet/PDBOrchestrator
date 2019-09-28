module Infrastructure.MasterPDBRepository

open System
open Application.DTO.MasterPDB
open Domain.MasterPDB
open System.IO
open Chiron

let getMasterPDB rootFolder name : Lazy<MasterPDB> = lazy(
    use stream = new StreamReader (sprintf "%s\%s.json" rootFolder name)
    let content = stream.ReadToEnd()
    let state:Application.DTO.MasterPDB.MasterPDBState = failwith ""
    state |> fromDTO
)

let putMasterPDB rootFolder name pdb = 
    () // TODO
    
type MasterPDBRepository(rootFolder : string) =
    //member this.Cache : Map<string, MasterPDBState> = load rootFolder
    interface Application.Common.Repository<string, Domain.MasterPDB.MasterPDB> with
        member this.Get name = (getMasterPDB rootFolder name).Value
        member this.Put name pdb = 
            putMasterPDB rootFolder name pdb
            upcast this