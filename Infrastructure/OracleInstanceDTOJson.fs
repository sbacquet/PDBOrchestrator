module Infrastructure.DTOJSON.OracleInstance

open Chiron
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Application.DTO.OracleInstance

let encodeOracleInstance = Encode.buildWith (fun (x:OracleInstanceState) jObj ->
    jObj
    |> Encode.required Encode.string "name" x.Name
    |> Encode.required Encode.string "server" x.Server
    |> Encode.required Encode.int "port" (x.Port |> Option.defaultValue 1521)
    |> Encode.required Encode.string "oracleDirectoryForDumps" x.OracleDirectoryForDumps
    |> Encode.required (Encode.listWith MasterPDB.encodeMasterPDB) "masterPDBs" x.MasterPDBs
)

let oracleInstanceToJson pdb =
    pdb |> Json.serializeWith encodeOracleInstance JsonFormattingOptions.Pretty
