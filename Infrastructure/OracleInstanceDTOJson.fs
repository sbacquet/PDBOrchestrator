module Infrastructure.DTOJSON.OracleInstance

open Chiron
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Application.DTO.OracleInstance

let encodeMasterPDBs = Encode.listWith MasterPDB.encodeMasterPDB

let encodeOracleInstance = Encode.buildWith (fun (x:OracleInstanceDTO) ->
    Encode.required Encode.string "name" x.Name >>
    Encode.required encodeMasterPDBs "masterPDBs" x.MasterPDBs
)

let oracleInstanceToJson pdb =
    pdb |> Json.serializeWith encodeOracleInstance JsonFormattingOptions.Pretty

let masterPDBsToJson (pdb:OracleInstanceDTO) =
    pdb.MasterPDBs |> Json.serializeWith encodeMasterPDBs JsonFormattingOptions.Pretty
