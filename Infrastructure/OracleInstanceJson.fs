module Infrastructure.OracleInstanceJson

open Chiron
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Domain.OracleInstance
open System.Security.Cryptography

let [<Literal>] private cCurrentJsonVersion = 1

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
    let! version = Decode.required Decode.int "_version"
    match version with
    | 1 ->
        let! ivMaybe = Decode.optional Decode.bytes "_iv"
        let decoder = 
            match ivMaybe with
            | Some iv -> 
                algo.IV <- iv
                decryptPassword algo
            | None -> Decode.string
        let! name = Decode.required Decode.string "name" 
        let! server = Decode.required Decode.string "server" 
        let! port = Decode.optional Decode.int "port"
        let! dbaUser = Decode.required Decode.string "dbaUser" 
        let! dbaPassword = Decode.required decoder "dbaPassword"
        let! snapshotCapable = Decode.optional Decode.bool "snapshotCapable"
        let! masterPDBManifestsPath = Decode.required Decode.string "masterPDBManifestsPath" 
        let! masterPDBDestPath = Decode.required Decode.string "masterPDBDestPath" 
        let! snapshotSourcePDBDestPath = Decode.required Decode.string "snapshotSourcePDBDestPath" 
        let! workingCopyDestPath = Decode.required Decode.string "workingCopyDestPath" 
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
                workingCopyDestPath 
                snapshotSourcePDBDestPath 
                oracleDirectoryForDumps
                (snapshotCapable |> Option.defaultValue true)
    | _ -> 
        return! Decoder.alwaysFail (JsonFailure.SingleFailure (JsonFailureReason.InvalidJson (sprintf "unknown Oracle instance JSON version %d" version)))
}

let encodeOracleInstance (algo:SymmetricAlgorithm) = Encode.buildWith (fun (x:OracleInstance) ->
    Encode.required Encode.string "name" x.Name >>
    Encode.required Encode.string "server" x.Server >>
    Encode.optional Encode.int "port" x.Port >>
    Encode.required Encode.string "dbaUser" x.DBAUser >>
    Encode.required (encryptPassword algo) "dbaPassword" x.DBAPassword >>
    Encode.ifNotEqual true Encode.bool "snapshotCapable" x.SnapshotCapable >>
    Encode.required Encode.string "masterPDBManifestsPath" x.MasterPDBManifestsPath >>
    Encode.required Encode.string "masterPDBDestPath" x.MasterPDBDestPath >>
    Encode.required Encode.string "snapshotSourcePDBDestPath" x.SnapshotSourcePDBDestPath >>
    Encode.required Encode.string "workingCopyDestPath" x.WorkingCopyDestPath >>
    Encode.required Encode.string "oracleDirectoryForDumps" x.OracleDirectoryForDumps >>
    Encode.required Encode.stringList "masterPDBs" x.MasterPDBs >>
    Encode.required Encode.int "_version" cCurrentJsonVersion >>
    Encode.required Encode.bytes "_iv" algo.IV
)

let jsonToOracleInstance json = 
    use aesAlg = Aes.Create()
    json |> Json.deserializeWith (decodeOracleInstance aesAlg) 

let oracleInstanceToJson pdb =
    use aesAlg = Aes.Create()
    pdb |> Json.serializeWith (encodeOracleInstance aesAlg) JsonFormattingOptions.Pretty
