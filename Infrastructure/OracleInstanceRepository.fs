module Infrastructure.OracleInstanceRepository

open Domain.OracleInstance
open System.IO
open Chiron
open Infrastructure
open Application.Common

let instanceFolder = sprintf "%s\%s"
let instancePath folder name = sprintf "%s\%s.json" (instanceFolder folder name) name

let loadOracleInstance folder name : OracleInstance =
    use stream = new StreamReader (instancePath folder name)
    let content = stream.ReadToEnd()
    let result = content |> OracleInstanceJson.jsonToOracleInstance
    match result with
    | JPass instance -> instance
    | JFail error -> failwith (error.ToString())

let saveOracleInstance folder name pdb = 
    Directory.CreateDirectory (instanceFolder folder name) |> ignore
    use stream = File.CreateText (instancePath folder name)
    let json = pdb |> OracleInstanceJson.oracleInstancetoJson
    stream.Write json
    stream.Flush()

type OracleInstanceRepository(folder) = 
    interface IOracleInstanceRepository with
        member this.Get name = loadOracleInstance folder name
        member this.Put name pdb = saveOracleInstance folder name pdb; upcast this
