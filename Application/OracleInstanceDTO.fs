module Application.DTO.OracleInstance

open Akkling
open Application
open Application.DTO.MasterPDB

type OracleInstanceDTO = {
    Name: string
    MasterPDBs: MasterPDBDTO list
}

let consOracleInstanceDTO name masterPDBs = 
    { 
        Name = name
        MasterPDBs = masterPDBs 
    }


let getResult (state:MasterPDBActor.StateResult) : MasterPDBDTO =
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
    return 
        consOracleInstanceDTO 
            oracleInstance.Name 
            (masterPDBs |> Array.toList)
}

type OracleInstanceFullDTO = {
    Name: string
    Server: string
    Port: int option
    DBAUser: string
    DBAPassword: string
    MasterPDBManifestsPath: string
    MasterPDBDestPath: string
    SnapshotSourcePDBDestPath: string
    SnapshotPDBDestPath: string
    OracleDirectoryForDumps: string
    MasterPDBs: MasterPDBDTO list
    SnapshotCapable: bool
}

let consOracleInstanceFullDTO name server port dbaUser dbaPassword mp dp sdp ssdp odd masterPDBs snapshotCapable =
    {
        Name = name
        Server = server
        Port = port
        DBAUser = dbaUser
        DBAPassword = dbaPassword
        MasterPDBManifestsPath = mp
        MasterPDBDestPath = dp
        SnapshotPDBDestPath = sdp
        SnapshotSourcePDBDestPath = ssdp
        OracleDirectoryForDumps = odd
        MasterPDBs = masterPDBs
        SnapshotCapable = snapshotCapable
    }

let toFullDTO masterPDBs (instance:Domain.OracleInstance.OracleInstance) =
    {
        Name = instance.Name
        Server = instance.Server
        Port = instance.Port
        DBAUser = instance.DBAUser
        DBAPassword = instance.DBAPassword
        MasterPDBManifestsPath = instance.MasterPDBManifestsPath
        MasterPDBDestPath = instance.MasterPDBDestPath
        SnapshotPDBDestPath = instance.WorkingCopyDestPath
        SnapshotSourcePDBDestPath = instance.SnapshotSourcePDBDestPath
        OracleDirectoryForDumps = instance.OracleDirectoryForDumps
        MasterPDBs = masterPDBs
        SnapshotCapable = instance.SnapshotCapable
    }
