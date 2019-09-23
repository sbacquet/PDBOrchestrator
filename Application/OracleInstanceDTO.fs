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

let toDTO (masterPDBActors:Map<string, IActorRef<obj>>) (oracleInstance : Domain.OracleInstance.OracleInstance) = async {
    let! masterPDBs = 
        oracleInstance.MasterPDBs 
            |> List.map (fun name -> retype (masterPDBActors |> Map.find name) <? MasterPDBActor.GetState)
            |> Async.Parallel
    return {
        Name = oracleInstance.Name
        Server = oracleInstance.Server
        Port = oracleInstance.Port
        MasterPDBManifestsPath = oracleInstance.MasterPDBManifestsPath
        TestPDBManifestsPath = oracleInstance.TestPDBManifestsPath
        OracleDirectoryForDumps = oracleInstance.OracleDirectoryForDumps
        MasterPDBs = masterPDBs |> Array.toList
    }
}
