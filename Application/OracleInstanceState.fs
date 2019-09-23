module Application.OracleInstanceState

open Akkling

type OracleInstanceState = {
    Name: string
    Server: string
    Port: int option
    MasterPDBManifestsPath: string
    TestPDBManifestsPath: string
    OracleDirectoryForDumps: string
    MasterPDBs: Domain.MasterPDB.MasterPDB list
}

let getState (oracleInstance : Domain.OracleInstance.OracleInstance) (masterPDBActors:Map<string, IActorRef<MasterPDBActor.Command>>) actor = {
    Name = oracleInstance.Name
    Server = oracleInstance.Server
    Port = oracleInstance.Port
    MasterPDBManifestsPath = oracleInstance.MasterPDBManifestsPath
    TestPDBManifestsPath = oracleInstance.TestPDBManifestsPath
    OracleDirectoryForDumps = oracleInstance.OracleDirectoryForDumps
    MasterPDBs = 
        oracleInstance.MasterPDBs 
        |> List.map (fun name -> (masterPDBActors |> Map.find name) <? MasterPDBActor.GetState )
        |> Async.Parallel |> Async.RunSynchronously |> Array.toList
}