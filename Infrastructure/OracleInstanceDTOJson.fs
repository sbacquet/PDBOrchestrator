module Infrastructure.DTOJSON.OracleInstance

open Chiron
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Application.DTO.OracleInstance
open Domain.MasterPDBWorkingCopy
open Application.DTO.MasterPDBWorkingCopy
open Infrastructure.DTOJSON.MasterPDBVersion
open Infrastructure.Common

let encodeMasterPDBDTOs = Encode.listWith MasterPDB.encodeMasterPDBDTO

let encodeWorkingCopyDTO = Encode.buildWith (fun (x:MasterPDBWorkingCopyDTO) ->
    Encode.required Encode.string "name" x.Name >>
    Encode.required Encode.string "masterPDBName" x.MasterPDBName >>
    Encode.required Encode.string "createdBy" x.CreatedBy >>
    Encode.required Encode.dateTime "creationDate" x.CreationDate >>
    Encode.required Encode.string "creationLocalDate" (toLocalTimeString x.CreationDate) >>
    (match x.Source with
    | SpecificVersion version -> 
        Encode.required Encode.string "sourceType" "SpecificVersion" >>
        Encode.required Encode.int "sourceVersion" version
    | Edition ->
        Encode.required Encode.string "sourceType" "Edition"
    ) >>
    (match x.Lifetime with
    | Temporary expiry -> 
        Encode.required Encode.string "lifetimeType" "Temporary" >>
        Encode.required Encode.dateTime "expiryDate" expiry >>
        Encode.required Encode.string "expiryLocalDate" (toLocalTimeString expiry) >>
        Encode.required Encode.float "hoursBeforeExpiry" (countdownInHours expiry)
    | Durable ->
        Encode.required Encode.string "lifetimeType" "Durable"
    ) >>
    Encode.required (Encode.listWith encodeSchemaDTO) "schemas" x.Schemas
)

let encodeOracleInstanceDTO = Encode.buildWith (fun (x:OracleInstanceDTO) ->
    Encode.required Encode.string "name" x.Name >>
    Encode.required Encode.string "serverUri" x.ServerUri >>
    Encode.required encodeMasterPDBDTOs "masterPDBs" x.MasterPDBs >>
    Encode.ifNotEqual List.empty (Encode.listWith encodeWorkingCopyDTO) "workingCopies" x.WorkingCopies
)

let oracleInstanceDTOToJson instanceDTO =
    instanceDTO |> Json.serializeWith encodeOracleInstanceDTO JsonFormattingOptions.Pretty

let masterPDBsToJson (instanceDTO:OracleInstanceDTO) =
    instanceDTO.MasterPDBs |> Json.serializeWith encodeMasterPDBDTOs JsonFormattingOptions.Pretty

let workingCopiesToJson (instanceDTO:OracleInstanceDTO) =
    instanceDTO.WorkingCopies |> Json.serializeWith (Encode.listWith encodeWorkingCopyDTO) JsonFormattingOptions.Pretty

let workingCopyDTOToJson (wc:MasterPDBWorkingCopyDTO) =
    wc |> Json.serializeWith encodeWorkingCopyDTO JsonFormattingOptions.Pretty

