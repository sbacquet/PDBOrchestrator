module Application.DTO.MasterPDB

open System

type Schema = {
    User: string
    Type: string
}

type LockInfo = {
    Locker: string
    Date: System.DateTime
}

type MasterPDBVersion = {
    Number: Domain.MasterPDBVersion.VersionNumber
    CreatedBy: string
    CreationDate: DateTime
    Comment: string
}

type MasterPDBState = {
    Name: string
    Schemas: Schema list
    Versions: MasterPDBVersion list
    LockState : LockInfo option
}

let toDTO (masterPDB:Domain.MasterPDB.MasterPDB) = { 
    Name = masterPDB.Name
    Schemas = masterPDB.Schemas |> List.map (fun schema -> { User = schema.User; Type = schema.Type })
    Versions = masterPDB.Versions 
        |> Map.filter (fun _ version -> not version.Deleted)
        |> Map.map (fun _ version -> { Number = version.Number; CreatedBy = version.CreatedBy; CreationDate = version.CreationDate; Comment = version.Comment})
        |> Map.toList |> List.map snd
    LockState = masterPDB.LockState |> Option.map (fun lock -> { Locker = lock.Locker; Date = lock.Date })
}