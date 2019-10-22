﻿module Infrastructure.HttpHandlers

open Microsoft.AspNetCore.Http
open Microsoft.Extensions.Logging
open FSharp.Control.Tasks.V2.ContextInsensitive
open Giraffe
open Application
open Application
open Chiron.Serialization.Json
open Chiron.Formatting
open Microsoft.Net.Http.Headers
open Infrastructure.DTOJSON

let withUser f : HttpHandler =
    fun next (ctx:HttpContext) -> task {
        let user = 
            if (ctx.User.Identity.Name = null) then None else Some ctx.User.Identity.Name
            |> Option.orElse (ctx.TryGetRequestHeader "user") // TODO: remove this line when authentication implemented
        match user with
        | Some user -> return! f user next ctx
        | None -> return! RequestErrors.badRequest (text "User cannot be determined.") next ctx
    }

let getAllInstances (apiCtx:API.APIContext) next (ctx:HttpContext) = task {
    let! state = API.getState apiCtx
    return! text (Orchestrator.orchestratorToJson state) next ctx
}

let getInstance (apiCtx:API.APIContext) (name:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx name
    match stateMaybe with
    | Ok state -> return! (text (OracleInstance.oracleInstanceToJson state)) next ctx
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
        return! RequestErrors.notFound (text <| sprintf "No request found with id = %O." requestId) next ctx
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
                |> Encode.required Encode.string "status" "Completed with error"
                |> Encode.required Encode.string "error" error
            | _ -> jObj // cannot happen
        )

        return! text (requestStatus |> serializeWith encodeRequestStatus JsonFormattingOptions.Pretty) next ctx
}

open Domain.Common.Validation

let returnRequest endpoint requestValidation : HttpHandler =
    match requestValidation with
    | Valid reqId -> 
        setHttpHeader HeaderNames.Location (sprintf "%s/request/%O" endpoint reqId) 
        >=> Successful.accepted (text "Your request is accepted. Please poll the resource in response header's Location.")
    | Invalid errors -> 
        RequestErrors.notAcceptable (text <| sprintf "Your request cannot be accepted : %s." (System.String.Join("; ", errors)))

let snapshot (apiCtx:API.APIContext) (instance:string, masterPDB:string, version:int, name:string) =
    withUser (fun user next ctx -> task {
        let! requestValidation = API.snapshotMasterPDBVersion apiCtx user instance masterPDB version name
        return! returnRequest apiCtx.Endpoint requestValidation next ctx
    })

let getPendingChanges (apiCtx:API.APIContext) next (ctx:HttpContext) = task {
    let! pendingChangesMaybe = API.getPendingChanges apiCtx
    match pendingChangesMaybe with
    | Error error -> return! ServerErrors.internalError (text error) next ctx
    | Ok pendingChangesPerhaps -> 
        match pendingChangesPerhaps with
        | None -> return! Successful.NO_CONTENT next ctx
        | Some pendingChanges -> 
            let encodeOrchestratorCommand = Encode.buildWith (fun (command:OrchestratorActor.Command) jObj ->
                let description = 
                    match command with
                    | OrchestratorActor.GetState ->
                        "Get global state"
                    | OrchestratorActor.GetInstanceState instance ->
                        sprintf "Get state of instance %s" instance
                    | OrchestratorActor.GetMasterPDBState (instance, pdb) ->
                        sprintf "Get state of master PDB %s on instance %s" pdb instance
                    | OrchestratorActor.Synchronize instance ->
                        sprintf "Synchronize state of primary instance with %s" instance
                    | OrchestratorActor.CreateMasterPDB (user, parameters) ->
                        sprintf "Create master PDB %s from dump %s" parameters.Name parameters.Dump
                    | OrchestratorActor.PrepareMasterPDBForModification (user, pdb, version) ->
                        sprintf "Prepare master PDB %s for modifications" pdb
                    | OrchestratorActor.CommitMasterPDB (user, pdb, comment) ->
                        sprintf "Commit modifications done in master PDB %s" pdb
                    | OrchestratorActor.RollbackMasterPDB (user, pdb) ->
                        sprintf "Roll back modification done in %s" pdb
                    | OrchestratorActor.SnapshotMasterPDBVersion (user, instance, pdb, version, name) ->
                        sprintf "Snapshot version %d of master PDB %s to PDB %s" version pdb name
                    | OrchestratorActor.GetRequest requestId ->
                        sprintf "Get request from id %O" requestId
                jObj 
                |> Encode.required Encode.string "description" description
            )
            let encodeOpenMasterPDB = Encode.buildWith (fun (x:string * Domain.MasterPDB.LockInfo) jObj ->
                let name, lockInfo = x
                jObj 
                |> Encode.required Encode.string "name" name
                |> Encode.required MasterPDBJson.encodeLockInfo "lock" lockInfo
            )
            let encodePendingChanges = Encode.buildWith (fun (x:OrchestratorActor.PendingChanges) jObj ->
                jObj 
                |> Encode.optional (Encode.listWith encodeOrchestratorCommand) "pendingChangeCommands" (if (x.Commands.IsEmpty) then None else Some x.Commands)
                |> Encode.optional (Encode.listWith encodeOpenMasterPDB) "lockedPDBs" (if (x.OpenMasterPDBs.IsEmpty) then None else Some x.OpenMasterPDBs)
            )
            return! text (pendingChanges |> serializeWith encodePendingChanges JsonFormattingOptions.Pretty) next ctx
}

let enterReadOnlyMode (apiCtx:API.APIContext) next (ctx:HttpContext) = task {
    let! switched = API.enterReadOnlyMode apiCtx
    if (switched) then
        return! text "The system is now in maintenance mode." next ctx
    else
        return! RequestErrors.notAcceptable (text "The system was already in maintenance mode.") next ctx
}

let enterNormalMode (apiCtx:API.APIContext) next (ctx:HttpContext) = task {
    let! switched = API.enterNormalMode apiCtx
    if (switched) then
        return! text "The system is now in normal mode." next ctx
    else
        return! RequestErrors.notAcceptable (text "The system was already in normal mode.") next ctx
}

let getMode (apiCtx:API.APIContext) next (ctx:HttpContext) = task {
    let! readOnly = API.isReadOnlyMode apiCtx
    return! text (if readOnly then @"""maintenance""" else @"""normal""") next ctx
}

let prepareMasterPDBForModification (apiCtx:API.APIContext) pdb = withUser (fun user next ctx -> task {
    let version = ctx.TryGetQueryStringValue "version"
    match version with
    | Some version -> 
        let (ok, version) = System.Int32.TryParse version
        if ok then
            let! requestValidation = API.prepareMasterPDBForModification apiCtx user pdb version
            return! returnRequest apiCtx.Endpoint requestValidation next ctx
        else 
            return! RequestErrors.badRequest (text "The current version must an integer.") next ctx
    | None ->
        return! RequestErrors.badRequest (text "The current version must be provided.") next ctx
})

let commitMasterPDB (apiCtx:API.APIContext) pdb = withUser (fun user next ctx -> task {
    let! comment = ctx.ReadBodyFromRequestAsync()
    if (comment <> "") then
        let! requestValidation = API.commitMasterPDB apiCtx user pdb comment
        return! returnRequest apiCtx.Endpoint requestValidation next ctx
    else
        return! RequestErrors.badRequest (text "A comment must be provided.") next ctx
})

let rollbackMasterPDB (apiCtx:API.APIContext) pdb = withUser (fun user next ctx -> task {
    let! requestValidation = API.rollbackMasterPDB apiCtx user pdb
    return! returnRequest apiCtx.Endpoint requestValidation next ctx
})

let collectGarbage (apiCtx:API.APIContext) = withUser (fun user next ctx -> task {
    API.collectGarbage apiCtx
    return! text "Garbage collecting initiated." next ctx
})
