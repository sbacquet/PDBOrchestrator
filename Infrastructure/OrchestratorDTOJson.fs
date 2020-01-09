module Infrastructure.DTOJSON.Orchestrator

open Chiron
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Application.DTO.Orchestrator

let encodeOrchestrator culture = Encode.buildWith (fun (x:OrchestratorState) ->
    Encode.required Encode.string "primaryInstance" x.PrimaryInstance >>
    Encode.required (Encode.listWith (OracleInstance.encodeOracleInstanceDTO culture)) "instances" x.OracleInstances
)

let orchestratorToJson culture pdb =
    pdb |> Json.serializeWith (encodeOrchestrator culture) JsonFormattingOptions.Pretty
