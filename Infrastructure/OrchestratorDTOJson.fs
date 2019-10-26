module Infrastructure.DTOJSON.Orchestrator

open Chiron
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Application.DTO.Orchestrator

let encodeOrchestrator = Encode.buildWith (fun (x:OrchestratorState) ->
    Encode.required Encode.string "primaryInstance" x.PrimaryInstance >>
    Encode.required (Encode.listWith OracleInstance.encodeOracleInstance) "instances" x.OracleInstances
)

let orchestratorToJson pdb =
    pdb |> Json.serializeWith encodeOrchestrator JsonFormattingOptions.Pretty
