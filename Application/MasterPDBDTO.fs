module Application.DTO.MasterPDB

open System
open Domain
open Application.DTO.MasterPDBVersion
open Domain.OracleInstance
open Domain.MasterPDB

type EditionInfoDTO = {
    Editor: string
    Date: System.DateTime
}

let consEditionInfoDTO editor (date:DateTime) = 
    {
        Editor = editor
        Date = date.ToUniversalTime() 
    }

let toEditionInfoDTO (lockInfo:Domain.MasterPDB.EditionInfo option) =
    lockInfo |> Option.map (fun lock -> consEditionInfoDTO lock.Editor lock.Date)

type MasterPDBDTO = {
    Name: string
    Schemas: SchemaDTO list
    LatestVersion: int
    Versions: MasterPDBVersionDTO list
    EditionState: EditionInfoDTO option
    EditionDisabled: bool
    EditionRole: string option
    Properties: Map<string, string>
}

let consMasterPDBDTO name schemas latestVersion versions editionState editionDisabled editionRole properties = {
    Name = name
    Schemas = schemas
    LatestVersion = latestVersion
    Versions = versions
    EditionState = editionState |> Option.map (fun editionState -> { editionState with Date = editionState.Date.ToUniversalTime() })
    EditionDisabled = editionDisabled
    EditionRole = editionRole
    Properties = properties
}

let toDTO (masterPDB:MasterPDB) =
    consMasterPDBDTO
        masterPDB.Name
        (masterPDB.Schemas |> List.map (toSchemaDTO None))
        (masterPDB |> Domain.MasterPDB.getLatestAvailableVersionNumber)
        (masterPDB.Versions |> Map.toList |> List.map (fun (_, version) -> version |> toDTO (Domain.MasterPDBVersion.manifestFile masterPDB.Name version.VersionNumber)))
        (masterPDB.EditionState |> toEditionInfoDTO)
        masterPDB.EditionDisabled
        masterPDB.EditionRole
        masterPDB.Properties

let fromDTO (dto:MasterPDBDTO) : MasterPDB = 
    MasterPDB.consMasterPDB
        dto.Name
        (dto.Schemas |> List.map (fun schema -> consSchema schema.User schema.Password schema.Type))
        (dto.Versions |> List.map (fun version -> 
            MasterPDBVersion.consPDBVersion 
                version.VersionNumber
                version.Deleted
                version.CreatedBy
                version.CreationDate
                version.Comment
                version.Properties))
        (dto. EditionState |> Option.map (fun lock -> consEditionInfo lock.Editor lock.Date))
        dto.EditionDisabled
        dto.EditionRole
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


type MasterPDBEditionDTO = {
    MasterPDBName: string
    EditionInfo: EditionInfoDTO
    Schemas: SchemaDTO list
}

let toMasterPDBEditionDTO (instance:OracleInstance) (masterPDB:MasterPDB) : MasterPDBEditionDTO =
    let pdbService = pdbServiceFromInstance instance (masterPDBEditionName masterPDB.Name)
    {
        MasterPDBName = masterPDB.Name
        EditionInfo = (masterPDB.EditionState |> toEditionInfoDTO).Value
        Schemas = masterPDB.Schemas |> List.map (fun schema -> schema |> toSchemaDTO (sprintf "%s/%s@%s" schema.User schema.Password pdbService |> Some))
    }

