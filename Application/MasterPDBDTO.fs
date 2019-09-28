module Application.DTO.MasterPDB

open System

type Schema = {
    User: string
    Password: string
    Type: string
} (*with
    static member ToJson (x:Schema) = json {
        do! Json.write "user" x.User
        do! Json.writeWith encryptPassword "password" x.Password
        do! Json.write "type" x.Type
    }*)

type LockInfo = {
    Locker: string
    Date: System.DateTime
}

let consLockInfo locker date = { Locker = locker; Date = date }

type MasterPDBVersion = {
    Number: Domain.MasterPDBVersion.VersionNumber
    CreatedBy: string
    CreationDate: DateTime
    Comment: string
    Deleted : bool
}

type MasterPDBState = {
    Name: string
    Manifest: string
    Schemas: Schema list
    Versions: MasterPDBVersion list
    LockState : LockInfo option
    _iv : byte[]
}

let consMasterPDBState name manifest schemas versions lockState iv  = {
    _iv = iv
    Name = name
    Manifest = manifest
    Schemas = schemas
    Versions = versions
    LockState = lockState
}

let toDTO (masterPDB:Domain.MasterPDB.MasterPDB) = { 
    Name = masterPDB.Name
    Manifest = masterPDB.Manifest
    Schemas = masterPDB.Schemas |> List.map (fun schema -> { User = schema.User; Password = schema.Password; Type = schema.Type })
    Versions = masterPDB.Versions 
        |> Map.map (fun _ version -> 
            { 
                Number = version.Number
                CreatedBy = version.CreatedBy
                CreationDate = version.CreationDate
                Comment = version.Comment
                Deleted = version.Deleted
            })
        |> Map.toList |> List.map snd
    LockState = masterPDB.LockState |> Option.map (fun lock -> { Locker = lock.Locker; Date = lock.Date })
    _iv = Array.empty
}

let fromDTO (dto:MasterPDBState) : Domain.MasterPDB.MasterPDB = { 
    Name = dto.Name
    Manifest = dto.Manifest
    Schemas = dto.Schemas |> List.map (fun schema -> { User = schema.User; Password = schema.Password; Type = schema.Type })
    Versions = dto.Versions 
        |> List.map (fun version -> 
            let v:Domain.MasterPDBVersion.MasterPDBVersion = { 
                MasterPDBName = dto.Name
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

