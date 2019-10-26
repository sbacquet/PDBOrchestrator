module Infrastructure.OrchestratorJson

open Chiron
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Domain.Orchestrator

let decodeOrchestrator = jsonDecoder {
    let! instances = Decode.required Decode.stringList "oracleInstanceNames"
    let! primaryInstance = Decode.required Decode.string "primaryInstance" 
    return consOrchestrator instances primaryInstance
}

let encodeOrchestrator = Encode.buildWith (fun (x:Orchestrator) ->
    Encode.required Encode.stringList "oracleInstanceNames" x.OracleInstanceNames >>
    Encode.required Encode.string "primaryInstance" x.PrimaryInstance
)

let jsonToOrchestrator json = 
    json |> Json.deserializeWith decodeOrchestrator 

let orchestratorToJson pdb =
    pdb |> Json.serializeWith encodeOrchestrator JsonFormattingOptions.Pretty
