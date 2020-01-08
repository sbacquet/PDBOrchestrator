module Application.DTO.OracleInstance

open Akkling
open Application
open Application.DTO.MasterPDB
open Application.DTO.MasterPDBWorkingCopy
open Domain.MasterPDBWorkingCopy
open Domain.OracleInstance
open Application

type OracleInstanceDTO = {
    Name: string
    ServerUri: string
    MasterPDBs: MasterPDBDTO list
    WorkingCopies: MasterPDBWorkingCopyDTO list
}

let consOracleInstanceDTO name serverUri masterPDBs workingCopies = 
    { 
        Name = name
        ServerUri = serverUri
        MasterPDBs = masterPDBs 
        WorkingCopies = workingCopies
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
    let schemasByMasterPDB = masterPDBs |> Array.map (fun pdb -> (pdb.Name, pdb.Schemas)) |> Map.ofSeq
    let wcToDTO (wc:MasterPDBWorkingCopy) =
        let wcService = pdbServiceFromInstance oracleInstance wc.Name
        wc |> toWorkingCopyDTO wcService (schemasByMasterPDB |> Map.tryFind wc.MasterPDBName |> Option.defaultValue List.empty)
    return 
        consOracleInstanceDTO 
            oracleInstance.Name 
            (oracleInstanceUri oracleInstance.Server oracleInstance.Port)
            (masterPDBs |> Array.toList)
            (oracleInstance.WorkingCopies |> Map.toList |> List.map (snd >> wcToDTO))
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
    WorkingCopies: MasterPDBWorkingCopyDTO list
    SnapshotCapable: bool
}

let consOracleInstanceFullDTO name server port dbaUser dbaPassword mp dp sdp ssdp odd masterPDBs wcList snapshotCapable =
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
        WorkingCopies = wcList
        SnapshotCapable = snapshotCapable
    }

let toFullDTO (masterPDBs:MasterPDBDTO list) (instance:Domain.OracleInstance.OracleInstance) =
    let schemasByMasterPDB = masterPDBs |> List.map (fun pdb -> (pdb.Name, pdb.Schemas)) |> Map.ofList
    let wcToDTO (wc:MasterPDBWorkingCopy) =
        let wcService = pdbServiceFromInstance instance wc.Name
        wc |> toWorkingCopyDTO wcService (schemasByMasterPDB |> Map.tryFind wc.MasterPDBName |> Option.defaultValue List.empty)
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
        WorkingCopies = instance.WorkingCopies |> Map.toList |> List.map (snd >> wcToDTO)
        SnapshotCapable = instance.SnapshotCapable
    }
