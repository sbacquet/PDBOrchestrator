module Application.DTO.MasterPDB

open System
open Domain
open Application.DTO.MasterPDBVersion

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
}

let consMasterPDBDTO name schemas latestVersion versions editionState editionDisabled properties = {
    Name = name
    Schemas = schemas
    LatestVersion = latestVersion
    Versions = versions
    EditionState = editionState |> Option.map (fun editionState -> { editionState with Date = editionState.Date.ToUniversalTime() })
    EditionDisabled = editionDisabled
    Properties = properties
}

let toDTO (masterPDB:Domain.MasterPDB.MasterPDB) =
    consMasterPDBDTO
        masterPDB.Name
        (masterPDB.Schemas |> List.map toSchemaDTO)
        (masterPDB |> Domain.MasterPDB.getLatestAvailableVersionNumber)
        (masterPDB.Versions |> Map.toList |> List.map (fun (_, version) -> version |> toDTO (Domain.MasterPDBVersion.manifestFile masterPDB.Name version.VersionNumber)))
        (masterPDB.EditionState |> toEditionInfoDTO)
        masterPDB.EditionDisabled
        masterPDB.Properties

let fromDTO (dto:MasterPDBDTO) : Domain.MasterPDB.MasterPDB = 
    MasterPDB.consMasterPDB
        dto.Name
        (dto.Schemas |> List.map (fun schema -> { User = schema.User; Password = schema.Password; Type = schema.Type }))
        (dto.Versions |> List.map (fun version -> 
            MasterPDBVersion.consPDBVersion 
                version.VersionNumber
                version.Deleted
                version.CreatedBy
                version.CreationDate
                version.Comment
                version.Properties))
        (dto. EditionState |> Option.map (fun lock -> { Editor = lock.Editor; Date = lock.Date }))
        dto.EditionDisabled
        dto.Properties


let toFullDTO (masterPDB:MasterPDBDTO) (version:MasterPDBVersionDTO) =
    consMasterPDBVersionFullDTO
        masterPDB.Name
        masterPDB.Schemas
        version.VersionNumber
        version.CreatedBy
        version.CreationDate
        version.Comment
        version.Deleted
        version.Manifest
        version.Properties
