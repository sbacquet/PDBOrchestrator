module Infrastructure.OrchestratorJson

open Chiron
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Domain.Orchestrator

let [<Literal>] private cCurrentJsonVersion = 1

let decodeOrchestrator = jsonDecoder {
    let! version = Decode.required Decode.int "_version"
    match version with
    | 1 ->
        let! instances = Decode.required Decode.stringList "oracleInstanceNames"
        let! primaryInstance = Decode.required Decode.string "primaryInstance" 
        return consOrchestrator instances primaryInstance
    | _ -> 
        return! Decoder.alwaysFail (JsonFailure.SingleFailure (JsonFailureReason.InvalidJson (sprintf "unknown orchestrator JSON version %d" version)))
}

let encodeOrchestrator = Encode.buildWith (fun (x:Orchestrator) ->
    Encode.required Encode.string "primaryInstance" x.PrimaryInstance >>
    Encode.required Encode.stringList "oracleInstanceNames" x.OracleInstanceNames >>
    Encode.required Encode.int "_version" cCurrentJsonVersion
)

let jsonToOrchestrator json = 
    json |> Json.deserializeWith decodeOrchestrator 

let orchestratorToJson pdb =
    pdb |> Json.serializeWith encodeOrchestrator JsonFormattingOptions.Pretty
