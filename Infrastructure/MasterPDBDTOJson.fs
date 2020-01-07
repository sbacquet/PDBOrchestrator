module Infrastructure.DTOJSON.MasterPDB

open Chiron
open Application.DTO.MasterPDB
open Infrastructure.DTOJSON.MasterPDBVersion
open Chiron.Serialization.Json
open Chiron.JsonTransformer

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
    Encode.required Encode.int "latestVersion" x.LatestVersion >>
    Encode.required (Encode.listWith encodeMasterPDBVersion) "versions" x.Versions
)

let masterPDBStatetoJson (pdb:MasterPDBDTO) =
    pdb |> Json.serializeWith encodeMasterPDB JsonFormattingOptions.Pretty

let masterPDBVersionstoJson (pdb:MasterPDBDTO) =
    pdb.Versions |> Json.serializeWith (Encode.listWith encodeMasterPDBVersion) JsonFormattingOptions.Pretty
