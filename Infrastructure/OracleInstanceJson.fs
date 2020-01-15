module Infrastructure.OracleInstanceJson

open Chiron
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Domain.OracleInstance
open Domain.MasterPDBWorkingCopy
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

let decodeWorkingCopy = jsonDecoder {
    let! name = Decode.required Decode.string "name"
    let! masterPDBName = Decode.required Decode.string "masterPDBName"
    let! createdBy = Decode.required Decode.string "createdBy"
    let! creationDate = Decode.required Decode.dateTime "creationDate"
    let! sourceType = Decode.required Decode.string "sourceType"
    let! sourceVersion = Decode.optional Decode.int "sourceVersion"
    let (source:Result<Source,string>) = 
        match sourceType with
        | "SpecificVersion" -> 
            match sourceVersion with
            | Some version -> SpecificVersion version |> Ok
            | None -> Error "sourceVersion is not specified"
        | "Edition" -> Ok Edition
        | _ -> Error "invalid sourceType"
    let! lifetimeType = Decode.required Decode.string "lifetimeType"
    let! lifetimeExpiry = Decode.optional Decode.dateTime "expiryDate"
    let (lifetime:Result<Lifetime,string>) = 
        match lifetimeType with
        | "Temporary" -> 
            match lifetimeExpiry with
            | Some expiry -> expiry |> Temporary |> Ok
            | None -> Error "expiryDate is not specified"
        | "Durable" -> Ok Durable
        | _ -> Error "invalid lifetimeType"
    match source, lifetime with
    | Ok source, Ok lifetime ->
        return 
            consWorkingCopy
                creationDate
                lifetime
                createdBy
                source
                masterPDBName
                name
    | Error error, Ok _
    | Ok _, Error error ->
        return! JsonFailureReason.InvalidJson error |> JsonFailure.SingleFailure |> Decoder.alwaysFail
    | Error error1, Error error2 ->
        return! [ 
            JsonFailureReason.InvalidJson error1 |> JsonFailure.SingleFailure
            JsonFailureReason.InvalidJson error2 |> JsonFailure.SingleFailure
        ] |> JsonFailure.MultipleFailures |> Decoder.alwaysFail
}

let encodeWorkingCopy = Encode.buildWith (fun (x:MasterPDBWorkingCopy) ->
    Encode.required Encode.string "name" x.Name >>
    Encode.required Encode.string "masterPDBName" x.MasterPDBName >>
    Encode.required Encode.string "createdBy" x.CreatedBy >>
    Encode.required Encode.dateTime "creationDate" x.CreationDate >>
    (match x.Source with
    | SpecificVersion version -> 
        Encode.required Encode.string "sourceType" "SpecificVersion" >>
        Encode.required Encode.int "sourceVersion" version
    | Edition ->
        Encode.required Encode.string "sourceType" "Edition"
    ) >>
    (match x.Lifetime with
    | Temporary lifetime -> 
        Encode.required Encode.string "lifetimeType" "Temporary" >>
        Encode.required Encode.dateTime "expiryDate" lifetime
    | Durable ->
        Encode.required Encode.string "lifetimeType" "Durable"
    )
)

let decodeWorkingCopies = Decode.listWith decodeWorkingCopy

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
        let! userForImport = Decode.required Decode.string "userForImport" 
        let! userForImportPassword = Decode.required decoder "userForImportPassword"
        let! userForFileTransfer = Decode.required Decode.string "userForFileTransfer" 
        let! userForFileTransferPassword = Decode.required decoder "userForFileTransferPassword"
        let! serverHostkeySHA256 = Decode.required decoder "serverHostkeySHA256"
        let! serverHostkeyMD5 = Decode.required decoder "serverHostkeyMD5"
        let! snapshotCapable = Decode.optional Decode.bool "snapshotCapable"
        let! masterPDBManifestsPath = Decode.required Decode.string "masterPDBManifestsPath" 
        let! masterPDBDestPath = Decode.required Decode.string "masterPDBDestPath" 
        let! snapshotSourcePDBDestPath = Decode.required Decode.string "snapshotSourcePDBDestPath" 
        let! workingCopyDestPath = Decode.required Decode.string "workingCopyDestPath" 
        let! oracleDirectoryForDumps = Decode.required Decode.string "oracleDirectoryForDumps" 
        let! oracleDirectoryPathForDumps = Decode.required Decode.string "oracleDirectoryPathForDumps" 
        let! masterPDBs = Decode.required Decode.stringList "masterPDBs"
        let! workingCopies = Decode.optional decodeWorkingCopies "workingCopies" // for compatibility, now stored in a dedicated file
        return 
            consOracleInstance 
                masterPDBs
                (workingCopies |> Option.defaultValue List.empty)
                name 
                server 
                port 
                dbaUser dbaPassword 
                userForImport userForImportPassword userForFileTransfer userForFileTransferPassword
                serverHostkeySHA256 serverHostkeyMD5
                masterPDBManifestsPath 
                masterPDBDestPath 
                workingCopyDestPath 
                snapshotSourcePDBDestPath 
                oracleDirectoryForDumps oracleDirectoryPathForDumps
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
    Encode.required Encode.string "userForImport" x.UserForImport >>
    Encode.required (encryptPassword algo) "userForImportPassword" x.UserForImportPassword >>
    Encode.required Encode.string "userForFileTransfer" x.UserForFileTransfer >>
    Encode.required (encryptPassword algo) "userForFileTransferPassword" x.UserForFileTransferPassword >>
    Encode.required (encryptPassword algo) "serverHostkeySHA256" x.ServerHostkeySHA256 >>
    Encode.required (encryptPassword algo) "serverHostkeyMD5" x.ServerHostkeyMD5 >>
    Encode.ifNotEqual true Encode.bool "snapshotCapable" x.SnapshotCapable >>
    Encode.required Encode.string "masterPDBManifestsPath" x.MasterPDBManifestsPath >>
    Encode.required Encode.string "masterPDBDestPath" x.MasterPDBDestPath >>
    Encode.required Encode.string "snapshotSourcePDBDestPath" x.SnapshotSourcePDBDestPath >>
    Encode.required Encode.string "workingCopyDestPath" x.WorkingCopyDestPath >>
    Encode.required Encode.string "oracleDirectoryForDumps" x.OracleDirectoryForDumps >>
    Encode.required Encode.string "oracleDirectoryPathForDumps" x.OracleDirectoryPathForDumps >>
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

let jsonToWorkingCopies json =
    json |> Json.deserializeWith decodeWorkingCopies

let workingCopiesToJson workingCopies =
    workingCopies |> Json.serializeWith (Encode.listWith encodeWorkingCopy) JsonFormattingOptions.Pretty
