module Domain.MasterPDB

open Domain.Common.Result

type Schema = {
    User: string
    Password: string
    Type: string
}

let consSchema user password t = { User = user; Password = password; Type = t }
let consSchemaFromTuple (user, password, t) = consSchema user password t

type LockInfo = {
    Locker: string
    Date: System.DateTime
}

let consLockInfo locker date = 
    { 
        Locker = locker
        Date = date
    }

type MasterPDB = {
    Name: string
    Schemas: Schema list
    Versions : MasterPDBVersion.VersionNumber list
    DeletedVersions: Set<MasterPDBVersion.VersionNumber>
    LockState : LockInfo option
}

let consMasterPDB name schemas versions deletedVersions lockState = 
    { 
        Name = name
        Schemas = schemas 
        Versions = versions
        DeletedVersions = deletedVersions
        LockState = lockState
    }

let newMasterPDB name schemas =
    { 
        Name = name
        Schemas = schemas
        Versions = [ 1 ]
        DeletedVersions = Set.empty
        LockState = None 
    }

let addVersionToMasterPDB version createdBy comment masterPDB =
    if (masterPDB.Versions |> List.contains version) || (masterPDB.DeletedVersions.Contains version) then
        Error (sprintf "version %d is already used for PDB %s" version masterPDB.Name)
    else
        Ok { masterPDB with Versions = version :: masterPDB.Versions }

let isVersionDeleted version masterPDB =
    masterPDB.DeletedVersions |> Set.contains version

let getLatestAvailableVersion masterPDB =
    masterPDB.Versions 
    |> List.find (fun version -> not (masterPDB.DeletedVersions |> Set.contains version)) 

let getNextAvailableVersion masterPDB =
    let highestVersionUsed = [ masterPDB.Versions.[0]; masterPDB.DeletedVersions.MaximumElement ] |> List.max
    highestVersionUsed + 1

let getCurrentVersion masterPDB = masterPDB.Versions.[0]

let getPreviousAvailableVersion masterPDB = 
    match masterPDB.Versions.Length with
    | 1 -> None
    | _ -> Some masterPDB.Versions.[1]

let deleteVersion version masterPDB =
    if (version = 1) 
    then Error "version 1 cannot be deleted"
    else
        if (not (masterPDB.Versions |> List.contains version))
        then Error (sprintf "version %d does not exist" version)
        else 
            Ok { masterPDB with 
                    Versions = masterPDB.Versions |> List.filter (fun v -> v <> version)
                    DeletedVersions = masterPDB.DeletedVersions.Add version 
               }

let lock masterPDB user comment =
    match masterPDB.LockState with
    | Some lockInfo -> Error (sprintf "%s is already locked by %s" masterPDB.Name lockInfo.Locker)
    | None -> Ok { masterPDB with LockState = Some (consLockInfo user comment) }

let unlock masterPDB =
    match masterPDB.LockState with
    | None -> Error (sprintf "%s is not locked" masterPDB.Name)
    | Some _ -> Ok { masterPDB with LockState = None }
