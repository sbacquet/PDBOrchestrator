module Domain.MasterPDBVersion

open System

type VersionNumber = int

type MasterPDBVersion = {
    VersionNumber: VersionNumber
    CreatedBy: string
    CreationDate: DateTime
    Comment: string
    Deleted: bool
    Properties: Map<string, string>
}

let consPDBVersion version deleted createdBy (creationDate:DateTime) comment properties =
    { 
        VersionNumber = version
        CreatedBy = createdBy
        CreationDate = creationDate.ToUniversalTime()
        Comment = comment 
        Deleted = deleted
        Properties = properties
    }

let newPDBVersion createdBy comment = 
    consPDBVersion 1 false createdBy System.DateTime.Now comment Map.empty

let manifestFile (name:string) = sprintf "%s_V%03d.XML" (name.ToUpper())
