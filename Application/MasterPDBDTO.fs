module Application.DTO.MasterPDB

open System
open Domain
open Domain.MasterPDBWorkingCopy

type SchemaDTO = {
    User: string
    Password: string
    Type: string
}

type MasterPDBVersionDTO = {
    Number: int
    CreatedBy: string
    CreationDate: DateTime
    Comment: string
    Deleted : bool
    Manifest : string
    Properties: Map<string, string>
    WorkingCopyCount: int
}

let consMasterPDBVersionDTO version createdBy (creationDate:DateTime) comment deleted manifest properties wcCount = {
    Number = version
    CreatedBy = createdBy
    CreationDate = creationDate.ToUniversalTime()
    Comment = comment
    Deleted = deleted
    Manifest = manifest
    Properties = properties
    WorkingCopyCount = wcCount
}

let toMasterPDBVersionDTO manifest wcCount (version:Domain.MasterPDBVersion.MasterPDBVersion) =
    consMasterPDBVersionDTO 
        version.Number 
        version.CreatedBy 
        version.CreationDate
        version.Comment 
        version.Deleted 
        manifest 
        version.Properties
        wcCount

type EditionInfoDTO = {
    Editor: string
    Date: System.DateTime
}

let consEditionInfoDTO editor (date:DateTime) = { Editor = editor; Date = date.ToUniversalTime() }

let toEditionInfoDTO (lockInfo:Domain.MasterPDB.EditionInfo option) =
    lockInfo |> Option.map (fun lock -> consEditionInfoDTO lock.Editor lock.Date)

type MasterPDBDTO = {
    Name: string
    Schemas: SchemaDTO list
    LatestVersion: int
    Versions: MasterPDBVersionDTO list
    EditionState: EditionInfoDTO option
    EditionDisabled: bool
    Properties: Map<string, string>
    WorkingCopyCount: int
}

let consMasterPDBDTO name schemas latestVersion versions editionState editionDisabled properties wcCount = {
    Name = name
    Schemas = schemas
    LatestVersion = latestVersion
    Versions = versions
    EditionState = editionState |> Option.map (fun editionState -> { editionState with Date = editionState.Date.ToUniversalTime() })
    EditionDisabled = editionDisabled
    Properties = properties
    WorkingCopyCount = wcCount
}

let toDTO (masterPDB:Domain.MasterPDB.MasterPDB) =
    consMasterPDBDTO
        masterPDB.Name
        (masterPDB.Schemas |> List.map (fun schema -> { User = schema.User; Password = schema.Password; Type = schema.Type }))
        (masterPDB |> Domain.MasterPDB.getLatestAvailableVersionNumber)
        (masterPDB.Versions 
         |> Map.map (fun _ version -> version |> toMasterPDBVersionDTO (Domain.MasterPDBVersion.manifestFile masterPDB.Name version.Number) (masterPDB.WorkingCopies |> List.filter (fun wc -> match wc.Source with | SpecificVersion v -> v = version.Number | _ -> false) |> List.length))
         |> Map.toList |> List.map snd)
        (masterPDB.EditionState |> toEditionInfoDTO)
        masterPDB.EditionDisabled
        masterPDB.Properties
        masterPDB.WorkingCopies.Length

let fromDTO (dto:MasterPDBDTO) : Domain.MasterPDB.MasterPDB = 
    MasterPDB.consMasterPDB
        dto.Name
        (dto.Schemas |> List.map (fun schema -> { User = schema.User; Password = schema.Password; Type = schema.Type }))
        (dto.Versions |> List.map (fun version -> 
            MasterPDBVersion.consPDBVersion 
                version.Number
                version.Deleted
                version.CreatedBy
                version.CreationDate
                version.Comment
                version.Properties))
        (dto. EditionState |> Option.map (fun lock -> { Editor = lock.Editor; Date = lock.Date }))
        dto.EditionDisabled
        dto.Properties
        List.Empty


