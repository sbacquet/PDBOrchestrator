module Domain.OracleInstance

open Domain.Common
open Domain.MasterPDBWorkingCopy

type OracleInstance = {
    Name: string
    Server: string
    Port: int option
    DBAUser: string
    DBAPassword: string
    UserForImport: string
    UserForImportPassword: string
    UserForFileTransfer: string
    UserForFileTransferPassword: string
    ServerHostkeySHA256: string
    ServerHostkeyMD5: string
    MasterPDBManifestsPath: string
    MasterPDBDestPath: string
    SnapshotSourcePDBDestPath: string
    WorkingCopyDestPath: string
    OracleDirectoryForDumps: string
    OracleDirectoryPathForDumps: string
    MasterPDBs: string list
    SnapshotCapable: bool
    WorkingCopies: Map<string,MasterPDBWorkingCopy>
}

let consOracleInstance 
    masterPDBs 
    (workingCopies:MasterPDBWorkingCopy list)
    name 
    server port 
    dbaUser dbaPassword 
    userForImport userForImportPassword 
    userForFileTransfer userForFileTransferPassword serverHostkeySHA256 serverHostkeyMD5
    mPath mdPath wcPath ssdPath 
    directory directoryPath
    snapshotCapable
    =
    { 
        Name = name
        Server = server
        Port = port
        DBAUser = dbaUser
        DBAPassword = dbaPassword
        UserForImport = userForImport
        UserForImportPassword = userForImportPassword
        UserForFileTransfer = userForFileTransfer
        UserForFileTransferPassword = userForFileTransferPassword
        ServerHostkeySHA256 = serverHostkeySHA256
        ServerHostkeyMD5 = serverHostkeyMD5
        MasterPDBManifestsPath = mPath
        MasterPDBDestPath = mdPath
        WorkingCopyDestPath = wcPath
        SnapshotSourcePDBDestPath = ssdPath
        OracleDirectoryForDumps = directory
        OracleDirectoryPathForDumps = directoryPath
        MasterPDBs = masterPDBs 
        SnapshotCapable = snapshotCapable
        WorkingCopies = workingCopies |> List.map (fun wc -> (wc.Name, wc)) |> Map.ofList
    }

let newOracleInstance = consOracleInstance List.empty List.empty

let masterPDBAlreadyExists pdb oracleInstance = oracleInstance.MasterPDBs |> List.tryFind (fun name -> name = pdb) |> Option.isSome

let addMasterPDB masterPDB oracleInstance =
    if (oracleInstance |> masterPDBAlreadyExists masterPDB) then
        Error (sprintf "master PDB %s already exists" masterPDB)
    else
        Ok { oracleInstance with MasterPDBs = masterPDB :: oracleInstance.MasterPDBs }

let containsMasterPDB (pdb:string) instance =
    instance.MasterPDBs |> List.tryFind ((=~)pdb)

let getWorkingCopy name (instance:OracleInstance) =
    instance.WorkingCopies |> Map.tryFind name

let addWorkingCopy (wc:MasterPDBWorkingCopy) (instance:OracleInstance) =
    { instance with WorkingCopies = instance.WorkingCopies |> Map.add wc.Name wc }

let removeWorkingCopy name (instance:OracleInstance) =
    { instance with WorkingCopies = instance.WorkingCopies |> Map.remove name }
