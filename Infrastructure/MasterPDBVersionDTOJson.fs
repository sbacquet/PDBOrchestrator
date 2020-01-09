module Infrastructure.DTOJSON.MasterPDBVersion

open Chiron
open Application.DTO.MasterPDBVersion
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Infrastructure.Common

let encodeSchemaDTO = Encode.buildWith (fun (x:SchemaDTO) ->
    Encode.required Encode.string "user" x.User >>
    Encode.required Encode.string "password" x.Password >>
    Encode.required Encode.string "type" x.Type >>
    Encode.optional Encode.string "connectionString" x.ConnectionString
)

let encodeMasterPDBVersionDTO culture = Encode.buildWith (fun (x:MasterPDBVersionDTO) ->
    Encode.required Encode.int "versionNumber" x.VersionNumber >>
    Encode.required Encode.string "createdBy" x.CreatedBy >>
    Encode.required Encode.dateTime "creationDate" x.CreationDate >>
    Encode.required Encode.string "creationLocalDate" (toLocalTimeString culture x.CreationDate) >>
    Encode.required Encode.string "comment" x.Comment >>
    Encode.ifNotEqual false Encode.bool "deleted" x.Deleted >>
    Encode.required Encode.string "manifest" x.Manifest >>
    Encode.ifNotEqual Map.empty (Encode.mapWith Encode.string) "properties" x.Properties
)

let encodeMasterPDBVersionFullDTO culture = Encode.buildWith (fun (x:MasterPDBVersionFullDTO) ->
    Encode.required Encode.string "name" x.Name >>
    Encode.required (Encode.listWith encodeSchemaDTO) "schemas" x.Schemas >>
    Encode.required Encode.int "versionNumber" x.VersionNumber >>
    Encode.required Encode.string "createdBy" x.CreatedBy >>
    Encode.required Encode.dateTime "creationDate" x.CreationDate >>
    Encode.required Encode.string "creationLocalDate" (toLocalTimeString culture x.CreationDate) >>
    Encode.required Encode.string "comment" x.Comment >>
    Encode.ifNotEqual false Encode.bool "deleted" x.Deleted >>
    Encode.required Encode.string "manifest" x.Manifest >>
    Encode.ifNotEqual Map.empty (Encode.mapWith Encode.string) "properties" x.Properties
)

let versionFulltoJson culture versionFullDTO =
    versionFullDTO |> Json.serializeWith (encodeMasterPDBVersionFullDTO culture) JsonFormattingOptions.Pretty
