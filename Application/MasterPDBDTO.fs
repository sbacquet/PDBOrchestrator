module Application.DTO.MasterPDB

open System

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
}

let consMasterPDBVersionDTO version createdBy creationDate comment deleted manifest = {
    Number = version
    CreatedBy = createdBy
    CreationDate = creationDate
    Comment = comment
    Deleted = deleted
    Manifest = manifest
}

let toMasterPDBVersionDTO manifest (version:Domain.MasterPDBVersion.MasterPDBVersion) =
    consMasterPDBVersionDTO version.Number version.CreatedBy version.CreationDate version.Comment version.Deleted manifest

type EditionInfoDTO = {
    Editor: string
    Date: System.DateTime
}

let consEditionInfoDTO editor date = { Editor = editor; Date = date }

let toEditionInfoDTO (lockInfo:Domain.MasterPDB.EditionInfo option) =
    lockInfo |> Option.map (fun lock -> consEditionInfoDTO lock.Editor lock.Date)

type MasterPDBDTO = {
    Name: string
    Schemas: SchemaDTO list
    Versions: MasterPDBVersionDTO list
    EditionState: EditionInfoDTO option
    EditionDisabled: bool
}

let consMasterPDBDTO name schemas versions editionState editionDisabled = {
    Name = name
    Schemas = schemas
    Versions = versions
    EditionState = editionState
    EditionDisabled = editionDisabled
}

let toDTO (masterPDB:Domain.MasterPDB.MasterPDB) = { 
    Name = masterPDB.Name
    Schemas = masterPDB.Schemas |> List.map (fun schema -> { User = schema.User; Password = schema.Password; Type = schema.Type })
    Versions = masterPDB.Versions 
        |> Map.map (fun _ version -> version |> toMasterPDBVersionDTO (Domain.MasterPDB.manifestFile masterPDB.Name version.Number))
        |> Map.toList |> List.map snd
    EditionState = masterPDB.EditionState |> toEditionInfoDTO
    EditionDisabled = masterPDB.EditionDisabled
}

let fromDTO (dto:MasterPDBDTO) : Domain.MasterPDB.MasterPDB = { 
    Name = dto.Name
    Schemas = dto.Schemas |> List.map (fun schema -> { User = schema.User; Password = schema.Password; Type = schema.Type })
    Versions = dto.Versions 
        |> List.map (fun version -> 
            let v:Domain.MasterPDBVersion.MasterPDBVersion = { 
                Number = version.Number
                CreatedBy = version.CreatedBy
                CreationDate = version.CreationDate
                Comment = version.Comment
                Deleted = version.Deleted
            }
            version.Number, v)
        |> Map.ofList
    EditionState = dto.EditionState |> Option.map (fun lock -> { Editor = lock.Editor; Date = lock.Date })
    EditionDisabled = dto.EditionDisabled
}

