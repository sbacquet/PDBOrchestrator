module Infrastructure.MasterPDBJson

open System
open Chiron
open Chiron.Operators
open System.Security.Cryptography
open System.IO
open Application.DTO.MasterPDB
open System.Text
open Chiron.Serialization.Json
open Chiron
open Chiron.JsonTransformer

let encryptPassword (algo:SymmetricAlgorithm) (password:string) = 
    let encryptor = algo.CreateEncryptor(algo.Key, algo.IV)
    use msEncrypt = new MemoryStream()
    use csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write)
    let toEncrypt = Encoding.Unicode.GetBytes(password)
    csEncrypt.Write(toEncrypt, 0, toEncrypt.Length)
    csEncrypt.FlushFinalBlock()
    let encrypted = msEncrypt.ToArray()
    Json.String <| Convert.ToBase64String encrypted 

let decryptPassword (algo:SymmetricAlgorithm) (encryptedPasswordJson:Json) = 
    match encryptedPasswordJson with
    | String encryptedPassword ->
        let bytes = Convert.FromBase64String encryptedPassword
        let decryptor = algo.CreateDecryptor(algo.Key, algo.IV)
        use msDecrypt = new MemoryStream()
        use csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Write)
        csDecrypt.Write(bytes, 0, bytes.Length)
        csDecrypt.FlushFinalBlock();
        let decrypted = msDecrypt.ToArray()
        JPass (Encoding.Unicode.GetString(decrypted))
    | json ->
        Json.formatWith JsonFormattingOptions.SingleLine json 
        |> sprintf "Expected a string, but got: %s" |> exn 
        |> JsonFailureReason.OtherError 
        |> JsonFailure.SingleFailure 
        |> JFail

let encodeSchema algo = Encode.buildWith (fun (x:Schema) jObj ->
    jObj 
    |> Encode.required Encode.string "user" x.User
    |> Encode.required (encryptPassword algo) "password" x.Password
    |> Encode.required Encode.string "type" x.Type
)

let decodeSchema algo = jsonDecoder {
    let! user = Decode.required Decode.string "user"
    let! password = Decode.required (decryptPassword algo) "password"
    let! t = Decode.required Decode.string "type"
    return { User = user; Password = password; Type = t }
}

let encodeLockInfo = Encode.buildWith (fun (x:LockInfo) jobj ->
    jobj
    |> Encode.required Encode.string "locker" x.Locker
    |> Encode.required Encode.dateTime "date" x.Date
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
    return { Number = number; CreatedBy = createdBy; CreationDate = creationDate; Comment = comment; Deleted = deleted}
}

let encodeMasterPDBVersion = Encode.buildWith (fun (x:MasterPDBVersion) jObj ->
    jObj
    |> Encode.required Encode.int "number" x.Number
    |> Encode.required Encode.string "createdby" x.CreatedBy
    |> Encode.required Encode.dateTime "creationdate" x.CreationDate
    |> Encode.required Encode.string "comment" x.Comment
    |> Encode.required Encode.bool "deleted" x.Deleted
)

let decodeMasterPDBState (algo:SymmetricAlgorithm) = jsonDecoder {
    let! name = Decode.required Decode.string "name" 
    let! manifest =  Decode.required Decode.string "manifest" 
    let! iv = Decode.required Decode.bytes "_iv"
    algo.IV <- iv
    let! schemas = Decode.required (Decode.listWith (decodeSchema algo)) "schemas"
    let! versions = Decode.required (Decode.listWith decodeMasterPDBVersion) "versions"
    let! lockState = Decode.optional decodeLockInfo "lockstate"
    return consMasterPDBState name manifest schemas versions lockState iv
}

let encodeMasterPDBState (algo:SymmetricAlgorithm) = Encode.buildWith (fun (x:MasterPDBState) jObj ->
    jObj
    |> Encode.required Encode.string "name" x.Name
    |> Encode.required Encode.string "manifest" x.Manifest
    |> Encode.required Encode.bytes "_iv" algo.IV
    |> Encode.required (Encode.listWith (encodeSchema algo)) "schemas" x.Schemas
    |> Encode.required (Encode.listWith encodeMasterPDBVersion) "versions" x.Versions
    |> Encode.optional encodeLockInfo "lockstate" x.LockState
)

let secretKey = [| 195uy; 30uy; 150uy; 96uy; 231uy; 225uy; 203uy; 15uy; 5uy; 74uy; 227uy; 139uy; 77uy; 111uy; 105uy; 17uy |]

let deserializeMasterPDBState json = 
    use aesAlg = Aes.Create()
    aesAlg.Key <- secretKey
    json |> Json.deserializeWith (decodeMasterPDBState aesAlg) 

let serializeMasterPDBState pdb =
    use aesAlg = Aes.Create()
    aesAlg.Key <- secretKey
    pdb |> Json.serializeWith (encodeMasterPDBState aesAlg) JsonFormattingOptions.Pretty

let DTOtoJson = 
    toDTO >> serializeMasterPDBState

let jsonToDTO = 
    deserializeMasterPDBState
