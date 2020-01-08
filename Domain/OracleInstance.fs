module Domain.OracleInstance

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
    (name:string)
    server port 
    dbaUser dbaPassword 
    userForImport userForImportPassword 
    userForFileTransfer userForFileTransferPassword serverHostkeySHA256 serverHostkeyMD5
    mPath mdPath wcPath ssdPath 
    directory directoryPath
    snapshotCapable
    =
    { 
        Name = name.ToLower()
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
        MasterPDBs = masterPDBs |> List.map (fun (pdb:string) -> pdb.ToUpper())
        SnapshotCapable = snapshotCapable
        WorkingCopies = workingCopies |> List.map (fun wc -> (wc.Name.ToUpper(), wc)) |> Map.ofList
    }

let newOracleInstance = consOracleInstance List.empty List.empty

let masterPDBAlreadyExists (pdb:string) (oracleInstance:OracleInstance) = 
    oracleInstance.MasterPDBs |> List.contains (pdb.ToUpper())

let addMasterPDB (masterPDB:string) (oracleInstance:OracleInstance) =
    if (oracleInstance |> masterPDBAlreadyExists masterPDB) then
        Error (sprintf "master PDB %s already exists" masterPDB)
    else
        Ok { oracleInstance with MasterPDBs = (masterPDB.ToUpper()) :: oracleInstance.MasterPDBs }

let containsMasterPDB (pdb:string) (instance:OracleInstance) =
    instance.MasterPDBs |> List.tryFind ((=)(pdb.ToUpper()))

let getWorkingCopy (name:string) (instance:OracleInstance) =
    instance.WorkingCopies |> Map.tryFind (name.ToUpper())

let addWorkingCopy (wc:MasterPDBWorkingCopy) (instance:OracleInstance) =
    { instance with WorkingCopies = instance.WorkingCopies |> Map.add (wc.Name.ToUpper()) wc }

let removeWorkingCopy (name:string) (instance:OracleInstance) =
    { instance with WorkingCopies = instance.WorkingCopies |> Map.remove (name.ToUpper()) }

let oracleInstancePortString port =
    port |> Option.map (fun port -> sprintf ":%d" port) |> Option.defaultValue ""

let pdbService uri = sprintf "%s/%s" uri

let oracleInstanceUri server port = sprintf "%s%s" server (oracleInstancePortString port)

let pdbServiceFromInstance (instance:OracleInstance) = pdbService (oracleInstanceUri instance.Server instance.Port)
