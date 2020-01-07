module Infrastructure.DTOJSON.OracleInstance

open Chiron
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Application.DTO.OracleInstance
open Domain.MasterPDBWorkingCopy

let encodeMasterPDBs = Encode.listWith MasterPDB.encodeMasterPDB

let encodeOracleInstance = Encode.buildWith (fun (x:OracleInstanceDTO) ->
    Encode.required Encode.string "name" x.Name >>
    Encode.required encodeMasterPDBs "masterPDBs" x.MasterPDBs >>
    Encode.ifNotEqual List.empty (Encode.listWith Infrastructure.OracleInstanceJson.encodeWorkingCopy) "workingCopies" x.WorkingCopies
)

let oracleInstanceToJson instanceDTO =
    instanceDTO |> Json.serializeWith encodeOracleInstance JsonFormattingOptions.Pretty

let masterPDBsToJson (instanceDTO:OracleInstanceDTO) =
    instanceDTO.MasterPDBs |> Json.serializeWith encodeMasterPDBs JsonFormattingOptions.Pretty

let workingCopiesToJson  (instanceDTO:OracleInstanceDTO) =
    instanceDTO.WorkingCopies |> Json.serializeWith (Encode.listWith Infrastructure.OracleInstanceJson.encodeWorkingCopy) JsonFormattingOptions.Pretty

let workingCopyToJson  (wc:MasterPDBWorkingCopy) =
    wc |> Json.serializeWith Infrastructure.OracleInstanceJson.encodeWorkingCopy JsonFormattingOptions.Pretty
