module Application.DTO.MasterPDBVersion

open System
open Domain

type SchemaDTO = {
    User: string
    Password: string
    Type: string
    ConnectionString: string option
}

let toSchemaDTO connectionString (schema:Domain.MasterPDB.Schema) = { 
    User = schema.User
    Password = schema.Password
    Type = schema.Type
    ConnectionString = connectionString
}

type MasterPDBVersionDTO = {
    VersionNumber: int
    CreatedBy: string
    CreationDate: DateTime
    Comment: string
    Deleted : bool
    Manifest : string
    Properties: Map<string, string>
}

let consMasterPDBVersionDTO version createdBy (creationDate:DateTime) comment deleted manifest properties = {
    VersionNumber = version
    CreatedBy = createdBy
    CreationDate = creationDate.ToUniversalTime()
    Comment = comment
    Deleted = deleted
    Manifest = manifest
    Properties = properties
}

let toDTO manifest (version:Domain.MasterPDBVersion.MasterPDBVersion) =
    consMasterPDBVersionDTO 
        version.VersionNumber 
        version.CreatedBy 
        version.CreationDate
        version.Comment 
        version.Deleted 
        manifest 
        version.Properties

let fromDTO (version:MasterPDBVersionDTO) =
    MasterPDBVersion.consPDBVersion 
        version.VersionNumber
        version.Deleted
        version.CreatedBy
        version.CreationDate
        version.Comment
        version.Properties

type MasterPDBVersionFullDTO = {
    Name: string
    Schemas: SchemaDTO list
    VersionNumber: int
    CreatedBy: string
    CreationDate: DateTime
    Comment: string
    Deleted : bool
    Manifest : string
    Properties: Map<string, string>
}


let consMasterPDBVersionFullDTO name schemas version createdBy (creationDate:DateTime) comment deleted manifest properties = {
    Name = name
    Schemas = schemas
    VersionNumber = version
    CreatedBy = createdBy
    CreationDate = creationDate.ToUniversalTime()
    Comment = comment
    Deleted = deleted
    Manifest = manifest
    Properties = properties
}
