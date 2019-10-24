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
    let json = pdb |> OracleInstanceJson.oracleInstanceToJson
    stream.Write json
    stream.Flush()

type OracleInstanceRepository(folder, name) = 
    interface IOracleInstanceRepository with
        member __.Get () = loadOracleInstance folder name
        member __.Put pdb = 
            pdb |> saveOracleInstance folder name
            upcast __
