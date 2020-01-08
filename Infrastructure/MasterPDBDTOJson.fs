module Infrastructure.DTOJSON.MasterPDB

open Chiron
open Application.DTO.MasterPDB
open Infrastructure.DTOJSON.MasterPDBVersion
open Chiron.Serialization.Json
open Chiron.JsonTransformer

let encodeLockInfoDTO = Encode.buildWith (fun (x:EditionInfoDTO) ->
    Encode.required Encode.string "editor" x.Editor >>
    Encode.required Encode.dateTime "date" x.Date
)

let encodeMasterPDBDTO = Encode.buildWith (fun (x:MasterPDBDTO) ->
    Encode.required Encode.string "name" x.Name >>
    Encode.required (Encode.listWith encodeSchemaDTO) "schemas" x.Schemas >>
    Encode.ifNotEqual Map.empty (Encode.mapWith Encode.string) "properties" x.Properties >>
    Encode.ifNotEqual false Encode.bool "editionDisabled" x.EditionDisabled >>
    Encode.optional encodeLockInfoDTO "edition" x.EditionState >>
    Encode.required Encode.int "latestVersion" x.LatestVersion >>
    Encode.required (Encode.listWith encodeMasterPDBVersionDTO) "versions" x.Versions
)

let masterPDBStatetoJson (pdb:MasterPDBDTO) =
    pdb |> Json.serializeWith encodeMasterPDBDTO JsonFormattingOptions.Pretty

let masterPDBVersionstoJson (pdb:MasterPDBDTO) =
    pdb.Versions |> Json.serializeWith (Encode.listWith encodeMasterPDBVersionDTO) JsonFormattingOptions.Pretty

let encodeMasterPDBEditionDTO = Encode.buildWith (fun (x:MasterPDBEditionDTO) ->
    Encode.required Encode.string "editedMasterPDB" x.MasterPDBName >>
    Encode.required encodeLockInfoDTO "edition" x.EditionInfo >>
    Encode.required (Encode.listWith encodeSchemaDTO) "schemas" x.Schemas
)

let masterPDBEditionDTOToJson (edition:MasterPDBEditionDTO) =
    edition |> Json.serializeWith encodeMasterPDBEditionDTO JsonFormattingOptions.Pretty

