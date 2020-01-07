module Infrastructure.MasterPDBJson

open Chiron
open System.Security.Cryptography
open Domain.MasterPDB
open Domain.MasterPDBVersion
open Domain.MasterPDBWorkingCopy
open Chiron.Serialization.Json
open Chiron.JsonTransformer

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

let encodeSchema algo = Encode.buildWith (fun (x:Schema) ->
    Encode.required Encode.string "user" x.User >>
    Encode.required (encryptPassword algo) "password" x.Password >>
    Encode.required Encode.string "type" x.Type
)

let decodeSchema decoder = jsonDecoder {
    let! user = Decode.required Decode.string "user"
    let! password = Decode.required (match decoder with | Some algo -> decryptPassword algo | None -> Decode.string) "password"
    let! t = Decode.required Decode.string "type"
    return { User = user; Password = password; Type = t }
}

let encodeLockInfo = Encode.buildWith (fun (x:EditionInfo) ->
    Encode.required Encode.string "editor" x.Editor >>
    Encode.required Encode.dateTime "date" x.Date
)

let decodeLockInfo = jsonDecoder {
    let! locker = Decode.required Decode.string "editor"
    let! date = Decode.required Decode.dateTime "date"
    return consEditionInfo locker date
}

let decodeMasterPDBVersion = jsonDecoder {
    let! number = Decode.required Decode.int "versionNumber"
    let! createdBy = Decode.required Decode.string "createdBy"
    let! creationDate = Decode.required Decode.dateTime "creationDate"
    let! comment = Decode.required Decode.string "comment"
    let! deleted = Decode.optional Decode.bool "deleted"
    let! properties = Decode.optional (Decode.mapWith Decode.string) "properties"
    return 
        consPDBVersion 
            number 
            (deleted |> Option.defaultValue false)
            createdBy 
            creationDate 
            comment 
            (properties |> Option.defaultValue Map.empty)
}

let encodeMasterPDBVersion = Encode.buildWith (fun (x:MasterPDBVersion) ->
    Encode.required Encode.int "versionNumber" x.VersionNumber >>
    Encode.required Encode.string "createdBy" x.CreatedBy >>
    Encode.required Encode.dateTime "creationDate" x.CreationDate >>
    Encode.required Encode.string "comment" x.Comment >>
    Encode.ifNotEqual false Encode.bool "deleted" x.Deleted >>
    Encode.ifNotEqual Map.empty (Encode.mapWith Encode.string) "properties" x.Properties
)

let decodeWorkingCopy masterPDBName = jsonDecoder {
    let! name = Decode.required Decode.string "name"
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

let decodeMasterPDB (algo:SymmetricAlgorithm) = jsonDecoder {
    let! version = Decode.required Decode.int "_version"
    match version with
    | 1 ->
        let! name = Decode.required Decode.string "name" 
        let! ivMaybe = Decode.optional Decode.bytes "_iv"
        let decoder = ivMaybe |> Option.map (fun iv -> algo.IV <- iv; algo)
        let! schemas = Decode.required (Decode.listWith (decodeSchema decoder)) "schemas"
        let! versions = Decode.required (Decode.listWith decodeMasterPDBVersion) "versions"
        let! lockState = Decode.optional decodeLockInfo "edition"
        let! editionDisabled = Decode.optional Decode.bool "editionDisabled"
        let! properties = Decode.optional (Decode.mapWith Decode.string) "properties"
        return 
            consMasterPDB 
                name 
                schemas 
                versions 
                lockState 
                (editionDisabled |> Option.defaultValue false)
                (properties |> Option.defaultValue Map.empty)
    | _ -> 
        return! Decoder.alwaysFail (JsonFailure.SingleFailure (JsonFailureReason.InvalidJson (sprintf "unknown master PDB JSON version %d" version)))
}

let encodeMasterPDB (algo:SymmetricAlgorithm) = Encode.buildWith (fun (x:MasterPDB) ->
    Encode.required Encode.string "name" x.Name >>
    Encode.required (Encode.listWith (encodeSchema algo)) "schemas" x.Schemas >>
    Encode.ifNotEqual Map.empty (Encode.mapWith Encode.string) "properties" x.Properties >>
    Encode.ifNotEqual false Encode.bool "editionDisabled" x.EditionDisabled >>
    Encode.optional encodeLockInfo "edition" x.EditionState >>
    Encode.required (Encode.listWith encodeMasterPDBVersion) "versions" (x.Versions |> Map.toList |> List.map snd) >>
    Encode.required Encode.int "_version" cCurrentJsonVersion >>
    Encode.required Encode.bytes "_iv" algo.IV
)

let jsonToMasterPDB json = 
    use aesAlg = Aes.Create()
    json |> Json.deserializeWith (decodeMasterPDB aesAlg) 

let masterPDBtoJson pdb =
    use aesAlg = Aes.Create()
    pdb |> Json.serializeWith (encodeMasterPDB aesAlg) JsonFormattingOptions.Pretty
