module Domain.MasterPDB

open Domain.Common.Result
open Domain.MasterPDBVersion

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
    Versions: Map<MasterPDBVersion.VersionNumber, MasterPDBVersion.MasterPDBVersion>
    LockState : LockInfo option
}

let consMasterPDB name schemas versions lockState = 
    { 
        Name = name
        Schemas = schemas 
        Versions = versions |> List.map (fun version -> version.Number, version) |> Map.ofList
        LockState = lockState
    }

let newMasterPDB name schemas createdBy creationDate comment =
    { 
        Name = name
        Schemas = schemas
        Versions = [ 1, newPDBVersion createdBy creationDate comment ] |> Map.ofList
        LockState = None 
    }

let isVersionDeleted version masterPDB =
    masterPDB.Versions |> Map.tryFind version |> Option.exists (fun v -> v.Deleted)

let getLatestAvailableVersion masterPDB =
    masterPDB.Versions 
    |> Map.toList
    |> List.findBack (fun (_, version) -> not (version.Deleted)) 
    |> snd

let getNextAvailableVersion masterPDB =
    let highestVersionUsed = masterPDB.Versions |> Map.toList |> List.last |> snd
    highestVersionUsed.Number + 1

let addVersionToMasterPDB createdBy comment masterPDB =
    let newVersionNumber = getNextAvailableVersion masterPDB
    let version:MasterPDBVersion.MasterPDBVersion = consPDBVersion newVersionNumber false createdBy System.DateTime.Now comment
    { masterPDB with Versions = masterPDB.Versions.Add(newVersionNumber, version) }

let deleteVersion versionNumber masterPDB =
    if (versionNumber = 1) 
    then 
        Error "version 1 cannot be deleted"
    else
        let versionMaybe = masterPDB.Versions |> Map.tryFind versionNumber
        match versionMaybe with
        | Some version ->
            if not version.Deleted then
                Ok { 
                    masterPDB with 
                        Versions = masterPDB.Versions 
                        |> Map.map (fun number version -> if (number = versionNumber) then { version with Deleted = true } else version)
                }
            else
                Error (sprintf "version %d of master PDB %s is already deleted" versionNumber masterPDB.Name)
        | None -> 
            Error (sprintf "version %d of master PDB %s does not exist" versionNumber masterPDB.Name)

let lock user masterPDB =
    match masterPDB.LockState with
    | Some lockInfo -> Error (sprintf "%s is already locked by %s" masterPDB.Name lockInfo.Locker)
    | None -> Ok { masterPDB with LockState = Some (consLockInfo user System.DateTime.Now) }

let unlock masterPDB =
    match masterPDB.LockState with
    | None -> Error (sprintf "%s is not locked" masterPDB.Name)
    | Some _ -> Ok { masterPDB with LockState = None }

let isLocked masterPDB = masterPDB.LockState.IsSome

let manifestFile = sprintf "%s_v%03d.xml"
let manifestPath baseFolder name version = sprintf "%s/%s" baseFolder <| manifestFile name version
