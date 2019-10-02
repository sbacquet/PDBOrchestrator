﻿module Infrastructure.OracleInstanceJson

open Chiron
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Domain.OracleInstance
open System.Security.Cryptography

let encryptPassword (algo:SymmetricAlgorithm) (clearPassword:string) =
    clearPassword |> Encryption.encryptString algo |> Json.String

let decryptPassword (algo:SymmetricAlgorithm) (encryptedPasswordJson:Json) = 
    match encryptedPasswordJson with
    | String encryptedPassword ->
        Encryption.decryptString algo encryptedPassword 
        |> JPass
    | json ->
        Json.formatWith JsonFormattingOptions.SingleLine json 
        |> sprintf "Expected a string, but got: %s" |> exn 
        |> JsonFailureReason.OtherError 
        |> JsonFailure.SingleFailure 
        |> JFail

let decodeOracleInstance (algo:SymmetricAlgorithm) = jsonDecoder {
    let! iv = Decode.required Decode.bytes "_iv"
    algo.IV <- iv
    let! name = Decode.required Decode.string "name" 
    let! server = Decode.required Decode.string "server" 
    let! port = Decode.optional Decode.int "port"
    let! dbaUser = Decode.required Decode.string "dbaUser" 
    let! dbaPassword = Decode.required (decryptPassword algo) "dbaPassword"
    let! masterPDBManifestsPath = Decode.required Decode.string "masterPDBManifestsPath" 
    let! masterPDBDestPath = Decode.required Decode.string "masterPDBDestPath" 
    let! snapshotSourcePDBDestPath = Decode.required Decode.string "snapshotSourcePDBDestPath" 
    let! snapshotPDBDestPath = Decode.required Decode.string "snapshotPDBDestPath" 
    let! oracleDirectoryForDumps = Decode.required Decode.string "oracleDirectoryForDumps" 
    let! masterPDBs = Decode.required Decode.stringList "masterPDBs" 
    return 
        consOracleInstance 
            masterPDBs 
            name 
            server 
            port 
            dbaUser 
            dbaPassword 
            masterPDBManifestsPath 
            masterPDBDestPath 
            snapshotPDBDestPath 
            snapshotSourcePDBDestPath 
            oracleDirectoryForDumps
}

let encodeOracleInstance (algo:SymmetricAlgorithm) = Encode.buildWith (fun (x:OracleInstance) jObj ->
    jObj
    |> Encode.required Encode.bytes "_iv" algo.IV
    |> Encode.required Encode.string "name" x.Name
    |> Encode.required Encode.string "server" x.Server
    |> Encode.optional Encode.int "port" x.Port
    |> Encode.required Encode.string "dbaUser" x.DBAUser
    |> Encode.required (encryptPassword algo) "dbaPassword" x.DBAPassword
    |> Encode.required Encode.string "masterPDBManifestsPath" x.MasterPDBManifestsPath
    |> Encode.required Encode.string "masterPDBDestPath" x.MasterPDBDestPath
    |> Encode.required Encode.string "snapshotSourcePDBDestPath" x.SnapshotSourcePDBDestPath
    |> Encode.required Encode.string "snapshotPDBDestPath" x.SnapshotPDBDestPath
    |> Encode.required Encode.string "oracleDirectoryForDumps" x.OracleDirectoryForDumps
    |> Encode.required Encode.stringList "masterPDBs" x.MasterPDBs
)

let jsonToOracleInstance json = 
    use aesAlg = Aes.Create()
    json |> Json.deserializeWith (decodeOracleInstance aesAlg) 

let oracleInstancetoJson pdb =
    use aesAlg = Aes.Create()
    pdb |> Json.serializeWith (encodeOracleInstance aesAlg) JsonFormattingOptions.Pretty