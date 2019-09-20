﻿module Domain.OracleInstance

type OracleInstance = {
    Name: string
    Server: string
    Port: int option
    DBAUser: string
    DBAPassword: string
    MasterPDBManifestsPath: string
    TestPDBManifestsPath: string
    OracleDirectoryForDumps: string
    MasterPDBs: string list
}

let consOracleInstance masterPDBs name server port dbaUser dbaPassword mPath tPath directory = 
    { 
        Name = name
        Server = server
        Port = port
        DBAUser = dbaUser
        DBAPassword = dbaPassword
        MasterPDBManifestsPath = mPath
        TestPDBManifestsPath = tPath
        OracleDirectoryForDumps = directory
        MasterPDBs = masterPDBs 
    }

let newOracleInstance = consOracleInstance []

let masterPDBAlreadyExists pdb oracleInstance = oracleInstance.MasterPDBs |> List.tryFind (fun name -> name = pdb) |> Option.isSome

let addMasterPDB masterPDB oracleInstance =
    if (oracleInstance |> masterPDBAlreadyExists masterPDB) then
        Error (sprintf "master PDB %s already exists" masterPDB)
    else
        Ok { oracleInstance with MasterPDBs = masterPDB :: oracleInstance.MasterPDBs }
