module Infrastructure.DTOJSON.OracleInstance

open Chiron
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Application.DTO.OracleInstance

let encodeOracleInstance = Encode.buildWith (fun (x:OracleInstanceDTO) ->
    Encode.required Encode.string "name" x.Name >>
    Encode.required (Encode.listWith MasterPDB.encodeMasterPDB) "masterPDBs" x.MasterPDBs
)

let oracleInstanceToJson pdb =
    pdb |> Json.serializeWith encodeOracleInstance JsonFormattingOptions.Pretty
