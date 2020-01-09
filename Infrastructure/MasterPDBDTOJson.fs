module Infrastructure.DTOJSON.MasterPDB

open Chiron
open Application.DTO.MasterPDB
open Infrastructure.DTOJSON.MasterPDBVersion
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Infrastructure.Common

let encodeLockInfoDTO culture = Encode.buildWith (fun (x:EditionInfoDTO) ->
    Encode.required Encode.string "editor" x.Editor >>
    Encode.required Encode.dateTime "date" x.Date >>
    Encode.required Encode.string "localDate" (toLocalTimeString culture x.Date)
)

let encodeMasterPDBDTO culture = Encode.buildWith (fun (x:MasterPDBDTO) ->
    Encode.required Encode.string "name" x.Name >>
    Encode.required (Encode.listWith encodeSchemaDTO) "schemas" x.Schemas >>
    Encode.ifNotEqual Map.empty (Encode.mapWith Encode.string) "properties" x.Properties >>
    Encode.ifNotEqual false Encode.bool "editionDisabled" x.EditionDisabled >>
    Encode.optional (encodeLockInfoDTO culture) "edition" x.EditionState >>
    Encode.required Encode.int "latestVersion" x.LatestVersion >>
    Encode.required (Encode.listWith (encodeMasterPDBVersionDTO culture)) "versions" x.Versions
)

let masterPDBStatetoJson culture (pdb:MasterPDBDTO) =
    pdb |> Json.serializeWith (encodeMasterPDBDTO culture) JsonFormattingOptions.Pretty

let masterPDBVersionstoJson culture (pdb:MasterPDBDTO) =
    pdb.Versions |> Json.serializeWith (Encode.listWith (encodeMasterPDBVersionDTO culture)) JsonFormattingOptions.Pretty

let encodeMasterPDBEditionDTO culture = Encode.buildWith (fun (x:MasterPDBEditionDTO) ->
    Encode.required Encode.string "editedMasterPDB" x.MasterPDBName >>
    Encode.required (encodeLockInfoDTO culture) "edition" x.EditionInfo >>
    Encode.required (Encode.listWith encodeSchemaDTO) "schemas" x.Schemas
)

let masterPDBEditionDTOToJson culture (edition:MasterPDBEditionDTO) =
    edition |> Json.serializeWith (encodeMasterPDBEditionDTO culture) JsonFormattingOptions.Pretty

