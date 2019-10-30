module Infrastructure.DTOJSON.MasterPDB

open Chiron
open Application.DTO.MasterPDB
open Chiron.Serialization.Json
open Chiron.JsonTransformer

let encodeSchema = Encode.buildWith (fun (x:SchemaDTO) ->
    Encode.required Encode.string "user" x.User >>
    Encode.required Encode.string "password" x.Password >>
    Encode.required Encode.string "type" x.Type
)

let encodeMasterPDBVersion = Encode.buildWith (fun (x:MasterPDBVersionDTO) ->
    Encode.required Encode.int "number" x.Number >>
    Encode.required Encode.string "createdBy" x.CreatedBy >>
    Encode.required Encode.dateTime "creationDate" x.CreationDate >>
    Encode.required Encode.string "comment" x.Comment >>
    Encode.required Encode.bool "deleted" x.Deleted >>
    Encode.required Encode.string "manifest" x.Manifest >>
    Encode.ifNotEqual Map.empty (Encode.mapWith Encode.string) "properties" x.Properties
)

let encodeLockInfo = Encode.buildWith (fun (x:EditionInfoDTO) ->
    Encode.required Encode.string "editor" x.Editor >>
    Encode.required Encode.dateTime "date" x.Date
)

let encodeMasterPDB = Encode.buildWith (fun (x:MasterPDBDTO) ->
    Encode.required Encode.string "name" x.Name >>
    Encode.required (Encode.listWith encodeSchema) "schemas" x.Schemas >>
    Encode.ifNotEqual Map.empty (Encode.mapWith Encode.string) "properties" x.Properties >>
    Encode.ifNotEqual false Encode.bool "editionDisabled" x.EditionDisabled >>
    Encode.optional encodeLockInfo "edition" x.EditionState >>
    Encode.required (Encode.listWith encodeMasterPDBVersion) "versions" x.Versions
)

let masterPDBStatetoJson pdb =
    pdb |> Json.serializeWith encodeMasterPDB JsonFormattingOptions.Pretty
