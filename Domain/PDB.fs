namespace Domain

module PDB =

    type Version = int

    type MasterPDBVersion = {
        Name: string
        Version: Version
        CreatedBy: string
        Comment: string
    }

    let newPDBVersion name version createdBy comment =
        { Name = name; Version = version; CreatedBy = createdBy; Comment = comment }

    type Schema = {
        User: string
        Password: string
        Type: string
    }

    let newSchema user password t = { User = user; Password = password; Type = t }

    type MasterPDB = {
        Name: string
        Schemas: Schema list
    }

    let newMasterPDB name schemas = { Name = name; Schemas = schemas }

    type LockInfo = {
        Locker: string
        Date: System.DateTime
    }

    let newLockInfo locker date = { Locker = locker; Date = date }