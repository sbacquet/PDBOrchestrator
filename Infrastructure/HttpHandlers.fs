module Infrastructure.HttpHandlers

open Microsoft.AspNetCore.Http
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive
open Giraffe
open Application
open Application

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
    | OrchestratorActor.Pending ->
        return! Successful.accepted (text (sprintf "The request with id %O is pending" requestId)) next ctx
    | OrchestratorActor.CompletedOk data -> 
        return! text data next ctx
    | OrchestratorActor.CompletedWithError error ->
        return! text error next ctx
}
