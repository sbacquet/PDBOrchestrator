module Infrastructure.DTOJSON.MasterPDB

open Chiron
open Application.DTO.MasterPDB
open Chiron.Serialization.Json
open Chiron.JsonTransformer

let encodeSchema = Encode.buildWith (fun (x:Schema) jObj ->
    jObj 
    |> Encode.required Encode.string "user" x.User
    |> Encode.required Encode.string "password" x.Password
    |> Encode.required Encode.string "type" x.Type
)

let encodeMasterPDBVersion = Encode.buildWith (fun (x:MasterPDBVersion) jObj ->
    jObj
    |> Encode.required Encode.int "number" x.Number
    |> Encode.required Encode.string "createdby" x.CreatedBy
    |> Encode.required Encode.dateTime "creationdate" x.CreationDate
    |> Encode.required Encode.string "comment" x.Comment
    |> Encode.required Encode.bool "deleted" x.Deleted
    |> Encode.required Encode.string "manifest" x.Manifest
)

let encodeMasterPDB = Encode.buildWith (fun (x:MasterPDBState) jObj ->
    jObj
    |> Encode.required Encode.string "name" x.Name
    |> Encode.required (Encode.listWith encodeSchema) "schemas" x.Schemas
    |> Encode.required (Encode.listWith encodeMasterPDBVersion) "versions" x.Versions
    |> Encode.optional Infrastructure.MasterPDBJson.encodeLockInfo "lockstate" x.LockState
)

let masterPDBStatetoJson pdb =
    pdb |> Json.serializeWith encodeMasterPDB JsonFormattingOptions.Pretty
