module Infrastructure.HttpHandlers

open Microsoft.AspNetCore.Http
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive
open Giraffe
open Application
open Application
open Chiron.Serialization.Json
open Chiron.Formatting
open Microsoft.Net.Http.Headers

let handleGetHello next (ctx:HttpContext) = task {
    //let! body = ctx.ReadBodyFromRequestAsync()
    ctx.GetLogger().Log(LogLevel.Information, "Hello request received with trace={Trace}", ctx.TraceIdentifier)
    return! text "hello" next ctx
}

let getAllInstances (apiCtx:API.APIContext) next (ctx:HttpContext) = task {
    let! state = API.getState apiCtx
    return! json state next ctx
}

let getInstance (apiCtx:API.APIContext) (name:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx name
    match stateMaybe with
    | Ok state -> return! json state next ctx
    | Error error -> return! RequestErrors.notFound (text error) next ctx
}

let getMasterPDB (apiCtx:API.APIContext) (instance:string, pdb:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getMasterPDBState apiCtx instance pdb
    match stateMaybe with
    | Ok state -> return! json state next ctx
    | Error error -> return! RequestErrors.notFound (text error) next ctx
}

let getRequestStatus (apiCtx:API.APIContext) (requestId:PendingRequest.RequestId) next (ctx:HttpContext) = task {
    let! (_, requestStatus) = API.getRequestStatus apiCtx requestId
    match requestStatus with
    | OrchestratorActor.NotFound -> 
        return! RequestErrors.notFound (text (sprintf "No request found with id = %O" requestId)) next ctx
    | _ ->
        let encodeRequestStatus = Encode.buildWith (fun (x:OrchestratorActor.RequestStatus) jObj ->
            let jObj = jObj |> Encode.required Encode.string "requestId" (sprintf "%O" requestId)
            match x with 
            | OrchestratorActor.Pending -> 
                jObj 
                |> Encode.required Encode.string "status" "Pending"
            | OrchestratorActor.CompletedOk data -> 
                jObj 
                |> Encode.required Encode.string "status" "Completed"
                |> Encode.required Encode.string "data" data
            | OrchestratorActor.CompletedWithError error -> 
                jObj 
                |> Encode.required Encode.string "status" "Completed"
                |> Encode.required Encode.string "error" error
            | _ -> jObj // cannot happen
        )

        return! text (requestStatus |> serializeWith encodeRequestStatus JsonFormattingOptions.Pretty) next ctx
}

open Domain.Common.Validation

let snapshot (apiCtx:API.APIContext) (user:string) endpoint (instance:string, masterPDB:string, version:int, name:string) next (ctx:HttpContext) = task {
    let! request = API.snapshotMasterPDBVersion apiCtx user instance masterPDB version name
    match request with
    | Valid reqId -> 
        ctx.SetHttpHeader HeaderNames.Location (sprintf "%s/request/%O" endpoint reqId) 
        return! Successful.accepted (text "Request accepted. Please poll the resource in response header's Location.") next ctx
    | Invalid errors -> 
        return! RequestErrors.badRequest (text (System.String.Join("; ", errors))) next ctx
}

let getPendingChanges (apiCtx:API.APIContext) next (ctx:HttpContext) = task {
    let! pendingChangesMaybe = API.getPendingChanges apiCtx
    match pendingChangesMaybe with
    | Error error -> return! ServerErrors.internalError (text error) next ctx
    | Ok pendingChangesPerhaps -> 
        match pendingChangesPerhaps with
        | None -> return! Successful.NO_CONTENT next ctx
        | Some pendingChanges -> return! json pendingChanges next ctx
}
