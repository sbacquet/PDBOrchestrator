module Infrastructure.OracleInstanceRepository

open Domain.OracleInstance
open System.IO
open Chiron
open Infrastructure

let loadOracleInstance rootFolder name : OracleInstance =
    use stream = new StreamReader (sprintf "%s\%s.json" rootFolder name)
    let content = stream.ReadToEnd()
    let result = content |> OracleInstanceJson.jsonToOracleInstance
    match result with
    | JPass instance -> instance
    | JFail error -> failwith (error.ToString())

let saveOracleInstance rootFolder name pdb = 
    Directory.CreateDirectory rootFolder |> ignore
    use stream = File.CreateText (sprintf "%s\%s.json" rootFolder name)
    let json = pdb |> OracleInstanceJson.oracleInstancetoJson
    stream.Write json
    stream.Flush()

type OracleInstanceRepository(rootFolder) = 
    interface Application.Common.Repository<string, OracleInstance> with
        member this.Get name = loadOracleInstance rootFolder name
        member this.Put name pdb = saveOracleInstance rootFolder name pdb; upcast this
