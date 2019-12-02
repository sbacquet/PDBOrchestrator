module Domain.OracleInstance

open Domain.Common

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
}

let consOracleInstance 
    masterPDBs 
    name 
    server port 
    dbaUser dbaPassword 
    userForImport userForImportPassword 
    userForFileTransfer userForFileTransferPassword serverHostkeySHA256 serverHostkeyMD5
    mPath mdPath wcPath ssdPath 
    directory directoryPath
    snapshotCapable = 
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
    }

let newOracleInstance = consOracleInstance []

let masterPDBAlreadyExists pdb oracleInstance = oracleInstance.MasterPDBs |> List.tryFind (fun name -> name = pdb) |> Option.isSome

let addMasterPDB masterPDB oracleInstance =
    if (oracleInstance |> masterPDBAlreadyExists masterPDB) then
        Error (sprintf "master PDB %s already exists" masterPDB)
    else
        Ok { oracleInstance with MasterPDBs = masterPDB :: oracleInstance.MasterPDBs }

let containsMasterPDB (pdb:string) instance =
    instance.MasterPDBs |> List.tryFind ((=~)pdb)
