module Infrastructure.HttpHandlers

open Microsoft.AspNetCore.Http
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive
open Giraffe

let handleGetHello next (ctx:HttpContext) = task {
    //let! body = ctx.ReadBodyFromRequestAsync()
    ctx.GetLogger().Log(LogLevel.Information, "Hello request received with trace={Trace}", ctx.TraceIdentifier)
    return! text "hello" next ctx
}
