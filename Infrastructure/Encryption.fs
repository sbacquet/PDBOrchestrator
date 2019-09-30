module Infrastructure.Encryption

open System
open System.IO
open System.Text
open System.Security.Cryptography

let secretKey = [| 195uy; 30uy; 150uy; 96uy; 231uy; 225uy; 203uy; 15uy; 5uy; 74uy; 227uy; 139uy; 77uy; 111uy; 105uy; 17uy |]

let encryptString (algo:SymmetricAlgorithm) (clearString:string) = 
    algo.Key <- secretKey
    let encryptor = algo.CreateEncryptor(algo.Key, algo.IV)
    use msEncrypt = new MemoryStream()
    use csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write)
    let toEncrypt = Encoding.Unicode.GetBytes(clearString)
    csEncrypt.Write(toEncrypt, 0, toEncrypt.Length)
    csEncrypt.FlushFinalBlock()
    let encrypted = msEncrypt.ToArray()
    Convert.ToBase64String encrypted 

let decryptString (algo:SymmetricAlgorithm) (encryptedString:string) = 
    algo.Key <- secretKey
    let bytes = Convert.FromBase64String encryptedString
    let decryptor = algo.CreateDecryptor(algo.Key, algo.IV)
    use msDecrypt = new MemoryStream()
    use csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Write)
    csDecrypt.Write(bytes, 0, bytes.Length)
    csDecrypt.FlushFinalBlock();
    Encoding.Unicode.GetString(msDecrypt.ToArray())

