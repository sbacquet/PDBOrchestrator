module Infrastructure.MasterPDBJson

open Chiron
open System.Security.Cryptography
open Domain.MasterPDB
open Domain.MasterPDBVersion
open Chiron.Serialization.Json
open Chiron.JsonTransformer

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

let encodeLockInfo = Encode.buildWith (fun (x:LockInfo) ->
    Encode.required Encode.string "locker" x.Locker >>
    Encode.required Encode.dateTime "date" x.Date
)

let decodeLockInfo = jsonDecoder {
    let! locker = Decode.required Decode.string "locker"
    let! date = Decode.required Decode.dateTime "date"
    return consLockInfo locker date
}

let decodeMasterPDBVersion = jsonDecoder {
    let! number = Decode.required Decode.int "number"
    let! createdBy = Decode.required Decode.string "createdby"
    let! creationDate = Decode.required Decode.dateTime "creationdate"
    let! comment = Decode.required Decode.string "comment"
    let! deleted = Decode.required Decode.bool "deleted"
    return { Number = number; CreatedBy = createdBy; CreationDate = creationDate.ToLocalTime(); Comment = comment; Deleted = deleted}
}

let encodeMasterPDBVersion = Encode.buildWith (fun (x:MasterPDBVersion) ->
    Encode.required Encode.int "number" x.Number >>
    Encode.required Encode.string "createdby" x.CreatedBy >>
    Encode.required Encode.dateTime "creationdate" x.CreationDate >>
    Encode.required Encode.string "comment" x.Comment >>
    Encode.required Encode.bool "deleted" x.Deleted
)

let decodeMasterPDB (algo:SymmetricAlgorithm) = jsonDecoder {
    let! name = Decode.required Decode.string "name" 
    let! ivMaybe = Decode.optional Decode.bytes "_iv"
    let decoder = ivMaybe |> Option.map (fun iv -> algo.IV <- iv; algo)
    let! schemas = Decode.required (Decode.listWith (decodeSchema decoder)) "schemas"
    let! versions = Decode.required (Decode.listWith decodeMasterPDBVersion) "versions"
    let! lockState = Decode.optional decodeLockInfo "lockstate"
    return consMasterPDB name schemas versions lockState
}

let encodeMasterPDB (algo:SymmetricAlgorithm) = Encode.buildWith (fun (x:MasterPDB) ->
    Encode.required Encode.string "name" x.Name >>
    Encode.required Encode.bytes "_iv" algo.IV >>
    Encode.required (Encode.listWith (encodeSchema algo)) "schemas" x.Schemas >>
    Encode.required (Encode.listWith encodeMasterPDBVersion) "versions" (x.Versions |> Map.toList |> List.map snd) >>
    Encode.optional encodeLockInfo "lockstate" x.LockState
)

let jsonToMasterPDB json = 
    use aesAlg = Aes.Create()
    json |> Json.deserializeWith (decodeMasterPDB aesAlg) 

let masterPDBtoJson pdb =
    use aesAlg = Aes.Create()
    pdb |> Json.serializeWith (encodeMasterPDB aesAlg) JsonFormattingOptions.Pretty
