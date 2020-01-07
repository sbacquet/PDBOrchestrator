module Infrastructure.DTOJSON.MasterPDBVersion

open Chiron
open Application.DTO.MasterPDBVersion
open Chiron.Serialization.Json
open Chiron.JsonTransformer

let encodeSchema = Encode.buildWith (fun (x:SchemaDTO) ->
    Encode.required Encode.string "user" x.User >>
    Encode.required Encode.string "password" x.Password >>
    Encode.required Encode.string "type" x.Type
)

let encodeMasterPDBVersion = Encode.buildWith (fun (x:MasterPDBVersionDTO) ->
    Encode.required Encode.int "versionNumber" x.VersionNumber >>
    Encode.required Encode.string "createdBy" x.CreatedBy >>
    Encode.required Encode.dateTime "creationDate" x.CreationDate >>
    Encode.required Encode.string "comment" x.Comment >>
    Encode.ifNotEqual false Encode.bool "deleted" x.Deleted >>
    Encode.required Encode.string "manifest" x.Manifest >>
    Encode.ifNotEqual Map.empty (Encode.mapWith Encode.string) "properties" x.Properties
)

let encodeMasterPDBVersionFull = Encode.buildWith (fun (x:MasterPDBVersionFullDTO) ->
    Encode.required Encode.string "name" x.Name >>
    Encode.required (Encode.listWith encodeSchema) "schemas" x.Schemas >>
    Encode.required Encode.int "versionNumber" x.VersionNumber >>
    Encode.required Encode.string "createdBy" x.CreatedBy >>
    Encode.required Encode.dateTime "creationDate" x.CreationDate >>
    Encode.required Encode.string "comment" x.Comment >>
    Encode.ifNotEqual false Encode.bool "deleted" x.Deleted >>
    Encode.required Encode.string "manifest" x.Manifest >>
    Encode.ifNotEqual Map.empty (Encode.mapWith Encode.string) "properties" x.Properties
)

let versionFulltoJson versionFullDTO =
    versionFullDTO |> Json.serializeWith encodeMasterPDBVersionFull JsonFormattingOptions.Pretty
