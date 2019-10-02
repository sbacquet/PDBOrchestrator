module Application.DTO.OracleInstance

open Akkling
open Application
open Application.DTO.MasterPDB

type OracleInstanceState = {
    Name: string
    Server: string
    Port: int option
    MasterPDBManifestsPath: string
    TestPDBManifestsPath: string
    OracleDirectoryForDumps: string
    MasterPDBs: MasterPDBState list
}

let getResult (state:MasterPDBActor.StateResult) : MasterPDBState =
    match state with
    | Ok result -> result
    | Error _ -> failwith "should never happen" // TODO

let toDTO (masterPDBActors:Map<string, IActorRef<obj>>) (oracleInstance : Domain.OracleInstance.OracleInstance) = async {
    let! masterPDBs = 
        oracleInstance.MasterPDBs 
            |> List.map (fun name -> async {
                let! (state:MasterPDBActor.StateResult) = retype (masterPDBActors |> Map.find name) <? MasterPDBActor.GetState
                return getResult state
               })
            |> Async.Parallel
    return {
        Name = oracleInstance.Name
        Server = oracleInstance.Server
        Port = oracleInstance.Port
        MasterPDBManifestsPath = oracleInstance.MasterPDBManifestsPath
        TestPDBManifestsPath = oracleInstance.SnapshotPDBDestPath
        OracleDirectoryForDumps = oracleInstance.OracleDirectoryForDumps
        MasterPDBs = masterPDBs |> Array.toList
    }
}
