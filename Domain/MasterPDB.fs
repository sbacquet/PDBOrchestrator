module Domain.MasterPDB

open Domain.MasterPDBVersion
open Domain.MasterPDBWorkingCopy

type Schema = {
    User: string
    Password: string
    Type: string
}

let consSchema user password t = { User = user; Password = password; Type = t }
let consSchemaFromTuple (user, password, t) = consSchema user password t

type EditionInfo = {
    Editor: string
    Date: System.DateTime
}

let consEditionInfo editor (date:System.DateTime) = 
    { 
        Editor = editor
        Date = date.ToUniversalTime()
    }

let newEditionInfo locker = consEditionInfo locker System.DateTime.Now

type MasterPDB = {
    Name: string
    Schemas: Schema list
    Versions: Map<MasterPDBVersion.VersionNumber, MasterPDBVersion.MasterPDBVersion>
    EditionState : EditionInfo option
    EditionDisabled: bool
    Properties: Map<string, string>
}

let consMasterPDB name schemas versions (editionState:EditionInfo option) editionDisabled properties = 
    { 
        Name = name
        Schemas = schemas 
        Versions = versions |> List.map (fun version -> version.Number, version) |> Map.ofList
        EditionState = editionState |> Option.map (fun editionState -> { editionState with Date = editionState.Date.ToUniversalTime() })
        EditionDisabled = editionDisabled
        Properties = properties
    }

let newMasterPDB name schemas createdBy comment =
    { 
        Name = name
        Schemas = schemas
        Versions = [ 1, newPDBVersion createdBy comment ] |> Map.ofList
        EditionState = None 
        EditionDisabled = false
        Properties = Map.empty
    }

let hasVersion version masterPDB =
    masterPDB.Versions |> Map.tryFind version |> Option.isSome

let isVersionDeleted version masterPDB =
    masterPDB.Versions |> Map.tryFind version |> Option.exists (fun v -> v.Deleted)

let getLatestAvailableVersion masterPDB =
    masterPDB.Versions 
    |> Map.toList
    |> List.tryFindBack (fun (_, version) -> not (version.Deleted)) 
    |> Option.map snd

let getLatestAvailableVersionNumber masterPDB =
    getLatestAvailableVersion masterPDB
    |> Option.map (fun v -> v.Number)
    |> Option.defaultValue 0

let getNextAvailableVersion masterPDB =
    let highestVersionUsed = masterPDB.Versions |> Map.toList |> List.last |> snd
    highestVersionUsed.Number + 1

let addVersionToMasterPDB createdBy comment masterPDB =
    let newVersionNumber = getNextAvailableVersion masterPDB
    let version = consPDBVersion newVersionNumber false createdBy System.DateTime.Now comment Map.empty
    { masterPDB with Versions = masterPDB.Versions |> Map.add newVersionNumber version }, newVersionNumber

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

let lockForEdition user masterPDB =
    if masterPDB.EditionDisabled then failwith (sprintf "master PDB %s cannot be edited !" masterPDB.Name)
    if masterPDB.EditionState.IsSome then failwith (sprintf "master PDB %s should not already be edited !" masterPDB.Name)
    { masterPDB with EditionState = Some (consEditionInfo user System.DateTime.Now) }

let unlock masterPDB =
    match masterPDB.EditionState with
    | None -> Error (sprintf "%s is not locked" masterPDB.Name)
    | Some _ -> Ok { masterPDB with EditionState = None }

let isLockedForEdition masterPDB = masterPDB.EditionState.IsSome
