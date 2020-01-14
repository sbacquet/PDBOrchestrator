module Infrastructure.DTOJSON.Orchestrator

open Chiron
open Chiron.Serialization.Json
open Chiron.JsonTransformer
open Application.DTO.Orchestrator

let encodeOrchestrator culture = Encode.buildWith (fun (x:OrchestratorDTO) ->
    Encode.required Encode.stringList "instances" x.OracleInstances >>
    Encode.required Encode.string "primaryInstance" x.PrimaryInstance
)

let orchestratorToJson culture pdb =
    pdb |> Json.serializeWith (encodeOrchestrator culture) JsonFormattingOptions.Pretty
