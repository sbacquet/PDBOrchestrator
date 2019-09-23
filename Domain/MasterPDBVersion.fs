module Domain.MasterPDBVersion

open System

type VersionNumber = int

type MasterPDBVersion = {
    MasterPDBName: string
    Number: VersionNumber
    CreatedBy: string
    CreationDate: DateTime
    Comment: string
    Deleted : bool
}

let consPDBVersion version deleted name createdBy creationDate comment =
    { 
        MasterPDBName = name
        Number = version
        CreatedBy = createdBy
        CreationDate = creationDate
        Comment = comment 
        Deleted = deleted
    }

let newPDBVersion = consPDBVersion 1 false
