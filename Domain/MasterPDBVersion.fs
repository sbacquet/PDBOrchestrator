module Domain.MasterPDBVersion

open System

type VersionNumber = int

type MasterPDBVersion = {
    MasterPDBName: string
    Number: VersionNumber
    CreatedBy: string
    CreationDate: DateTime
    Comment: string
    Instanciated : bool
}

let consPDBVersion version instanciated name createdBy creationDate comment =
    { 
        MasterPDBName = name
        Number = version
        CreatedBy = createdBy
        CreationDate = creationDate
        Comment = comment 
        Instanciated = instanciated
    }

let newPDBVersion = consPDBVersion 1 false
