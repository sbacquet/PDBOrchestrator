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
    Manifest: string
    Schemas: Schema list
    Versions: Map<MasterPDBVersion.VersionNumber, MasterPDBVersion.MasterPDBVersion>
    LockState : LockInfo option
}

let consMasterPDB name manifest schemas versions lockState = 
    { 
        Name = name
        Manifest = manifest
        Schemas = schemas 
        Versions = versions |> List.map (fun version -> version.Number, version) |> Map.ofList
        LockState = lockState
    }

let masterPDBManifest = sprintf "%s_v%03d.xml"

let newMasterPDB name schemas createdBy creationDate comment =
    { 
        Name = name
        Manifest = masterPDBManifest name 1
        Schemas = schemas
        Versions = [ 1, newPDBVersion createdBy creationDate comment ] |> Map.ofList
        LockState = None 
    }

let addVersionToMasterPDB (version:MasterPDBVersion.MasterPDBVersion) createdBy comment masterPDB =

    if (masterPDB.Versions |> Map.tryFind version.Number |> Option.isSome) then
        Error (sprintf "version %d is already used for PDB %s" version.Number masterPDB.Name)
    else
        Ok { masterPDB with Versions = masterPDB.Versions.Add(version.Number, version) }

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

let getCurrentVersion masterPDB = masterPDB.Versions.[0]

let deleteVersion versionNumber masterPDB =
    if (versionNumber = 1) 
    then Error "version 1 cannot be deleted"
    else
        let versionMaybe = masterPDB.Versions |> Map.tryFind versionNumber
        match versionMaybe with
        | Some version -> Ok { 
            masterPDB with Versions = masterPDB.Versions |> Map.map (fun number version -> if (number = versionNumber) then { version with Deleted = true } else version)
          }
        | None -> Error (sprintf "version %d of master PDB %s does not exist" versionNumber masterPDB.Name)

let lock user comment masterPDB =
    match masterPDB.LockState with
    | Some lockInfo -> Error (sprintf "%s is already locked by %s" masterPDB.Name lockInfo.Locker)
    | None -> Ok { masterPDB with LockState = Some (consLockInfo user comment) }

let unlock masterPDB =
    match masterPDB.LockState with
    | None -> Error (sprintf "%s is not locked" masterPDB.Name)
    | Some _ -> Ok { masterPDB with LockState = None }

let isLocked masterPDB = masterPDB.LockState.IsSome
