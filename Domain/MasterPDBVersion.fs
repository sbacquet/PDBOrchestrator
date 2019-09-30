module Domain.MasterPDBVersion

open System

type VersionNumber = int

type MasterPDBVersion = {
    Number: VersionNumber
    CreatedBy: string
    CreationDate: DateTime
    Comment: string
    Deleted : bool
}

let consPDBVersion version deleted createdBy creationDate comment =
    { 
        Number = version
        CreatedBy = createdBy
        CreationDate = creationDate
        Comment = comment 
        Deleted = deleted
    }

let newPDBVersion = consPDBVersion 1 false
