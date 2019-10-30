﻿module Domain.OracleInstance

type OracleInstance = {
    Name: string
    Server: string
    Port: int option
    DBAUser: string
    DBAPassword: string
    MasterPDBManifestsPath: string
    MasterPDBDestPath: string
    SnapshotSourcePDBDestPath: string
    WorkingCopyDestPath: string
    OracleDirectoryForDumps: string
    MasterPDBs: string list
    SnapshotCapable: bool
}

let consOracleInstance masterPDBs name server port dbaUser dbaPassword mPath mdPath wcPath ssdPath directory snapshotCapable = 
    { 
        Name = name
        Server = server
        Port = port
        DBAUser = dbaUser
        DBAPassword = dbaPassword
        MasterPDBManifestsPath = mPath
        MasterPDBDestPath = mdPath
        WorkingCopyDestPath = wcPath
        SnapshotSourcePDBDestPath = ssdPath
        OracleDirectoryForDumps = directory
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
