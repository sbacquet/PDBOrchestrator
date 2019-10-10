module Application.DTO.MasterPDB

open System

type Schema = {
    User: string
    Password: string
    Type: string
}

type MasterPDBVersion = {
    Number: Domain.MasterPDBVersion.VersionNumber
    CreatedBy: string
    CreationDate: DateTime
    Comment: string
    Deleted : bool
    Manifest : string
}

let consMasterPDBVersion pdb version createdBy creationDate comment deleted = {
    Number = version
    CreatedBy = createdBy
    CreationDate = creationDate
    Comment = comment
    Deleted = deleted
    Manifest = Domain.MasterPDB.manifestFile pdb version
}

type MasterPDBState = {
    Name: string
    Schemas: Schema list
    Versions: MasterPDBVersion list
    LockState : Domain.MasterPDB.LockInfo option
}

let consMasterPDBState name schemas versions lockState = {
    Name = name
    Schemas = schemas
    Versions = versions
    LockState = lockState
}

let toDTO (masterPDB:Domain.MasterPDB.MasterPDB) = { 
    Name = masterPDB.Name
    Schemas = masterPDB.Schemas |> List.map (fun schema -> { User = schema.User; Password = schema.Password; Type = schema.Type })
    Versions = masterPDB.Versions 
        |> Map.map (fun _ version -> 
            consMasterPDBVersion masterPDB.Name version.Number version.CreatedBy version.CreationDate version.Comment version.Deleted)
        |> Map.toList |> List.map snd
    LockState = masterPDB.LockState
}

let fromDTO (dto:MasterPDBState) : Domain.MasterPDB.MasterPDB = { 
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

