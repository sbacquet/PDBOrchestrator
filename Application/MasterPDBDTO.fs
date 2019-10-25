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

type LockInfoDTO = {
    Locker: string
    Date: System.DateTime
}

let consLockInfoDTO locker date = { Locker = locker; Date = date }

let toLockInfoDTO (lockInfo:Domain.MasterPDB.LockInfo option) =
    lockInfo |> Option.map (fun lock -> consLockInfoDTO lock.Locker lock.Date)

type MasterPDBDTO = {
    Name: string
    Schemas: SchemaDTO list
    Versions: MasterPDBVersionDTO list
    LockState : LockInfoDTO option
}

let consMasterPDBDTO name schemas versions lockState = {
    Name = name
    Schemas = schemas
    Versions = versions
    LockState = lockState
}

let toDTO (masterPDB:Domain.MasterPDB.MasterPDB) = { 
    Name = masterPDB.Name
    Schemas = masterPDB.Schemas |> List.map (fun schema -> { User = schema.User; Password = schema.Password; Type = schema.Type })
    Versions = masterPDB.Versions 
        |> Map.map (fun _ version -> version |> toMasterPDBVersionDTO (Domain.MasterPDB.manifestFile masterPDB.Name version.Number))
        |> Map.toList |> List.map snd
    LockState = masterPDB.LockState |> toLockInfoDTO
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
    LockState = dto.LockState |> Option.map (fun lock -> { Locker = lock.Locker; Date = lock.Date })
}

