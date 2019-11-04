module Infrastructure.HttpHandlers

open Microsoft.AspNetCore.Http
open FSharp.Control.Tasks.V2.ContextInsensitive
open Giraffe
open Application
open Chiron.Serialization.Json
open Chiron.Formatting
open Microsoft.Net.Http.Headers
open Infrastructure.DTOJSON
open Application.DTO

let json jsonStr : HttpHandler =
    fun _ (ctx : HttpContext) ->
        task {
            ctx.SetHttpHeader "Content-Type" "application/json"
            return! ctx.WriteStringAsync jsonStr
        }

let withUser f : HttpHandler =
    fun next (ctx:HttpContext) -> task {
        let user = 
            if (ctx.User.Identity.Name = null) then None else Some ctx.User.Identity.Name
            |> Option.orElse (ctx.TryGetRequestHeader "user") // TODO: remove this line when authentication implemented
        match user with
        | Some user -> return! f user next ctx
        | None -> return! RequestErrors.badRequest (text "User cannot be determined.") next ctx
    }

let getAllInstances apiCtx next (ctx:HttpContext) = task {
    let! state = API.getState apiCtx
    return! json (Orchestrator.orchestratorToJson state) next ctx
}

let getInstance apiCtx (name:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx name
    match stateMaybe with
    | Ok state -> return! (text (OracleInstance.oracleInstanceToJson state)) next ctx
    | Error error -> return! RequestErrors.notFound (text error) next ctx
}

let getMasterPDB apiCtx (instance:string, pdb:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getMasterPDBState apiCtx instance pdb
    match stateMaybe with
    | Ok state -> return! json (MasterPDB.masterPDBStatetoJson state) next ctx
    | Error error -> return! RequestErrors.notFound (text error) next ctx
}

let getRequestStatus apiCtx (requestId:PendingRequest.RequestId) next (ctx:HttpContext) = task {

    let completedRequestDataKeyValue = function
    | OrchestratorActor.PDBName name -> "PDB name", name
    | OrchestratorActor.PDBVersion version -> "PDB version", version.ToString()

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
            | OrchestratorActor.CompletedOk (message, data) -> 
                jObj 
                |> Encode.required Encode.string "status" "Completed"
                |> Encode.required Encode.string "message" message
                |> Encode.required (Encode.propertyListWith Encode.string) 
                    "data" 
                    (data |> List.map completedRequestDataKeyValue)
            | OrchestratorActor.CompletedWithError error -> 
                jObj 
                |> Encode.required Encode.string "status" "Completed with error"
                |> Encode.required Encode.string "error" error
            | _ -> jObj // cannot happen
        )
        return! json (requestStatus |> serializeWith encodeRequestStatus JsonFormattingOptions.Pretty) next ctx
}

open Domain.Common.Validation

let returnRequest endpoint requestValidation : HttpHandler =
    match requestValidation with
    | Valid reqId -> 
        setHttpHeader HeaderNames.Location (sprintf "%s/requests/%O" endpoint reqId) 
        >=> Successful.accepted (text "Your request is accepted. Please poll the resource in response header's Location.")
    | Invalid errors -> 
        RequestErrors.notAcceptable (text <| sprintf "Your request cannot be accepted : %s." (System.String.Join("; ", errors)))

let createWorkingCopy apiCtx (instance:string, masterPDB:string, version:int, name:string) =
    withUser (fun user next ctx -> task {
        let parsedOk, force = ctx.TryGetQueryStringValue "force" |> Option.defaultValue bool.FalseString |> bool.TryParse
        if not parsedOk then 
            return! RequestErrors.badRequest (text "When provided, the \"force\" query parameter must be \"true\" or \"false\".") next ctx
        else
            let! requestValidation = API.createWorkingCopy apiCtx user instance masterPDB version name force
            return! returnRequest apiCtx.Endpoint requestValidation next ctx
    })

let deleteWorkingCopy apiCtx (instance:string, masterPDB:string, version:int, name:string) =
    withUser (fun user next ctx -> task {
        let! requestValidation = API.deleteWorkingCopy apiCtx user instance masterPDB version name
        return! returnRequest apiCtx.Endpoint requestValidation next ctx
    })

let getPendingChanges apiCtx next (ctx:HttpContext) = task {
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
                    | OrchestratorActor.CreateMasterPDB (user, parameters) ->
                        sprintf "Create master PDB %s from dump %s" parameters.Name parameters.Dump
                    | OrchestratorActor.PrepareMasterPDBForModification (user, pdb, version) ->
                        sprintf "Prepare master PDB %s for modifications" pdb
                    | OrchestratorActor.CommitMasterPDB (user, pdb, comment) ->
                        sprintf "Commit modifications done in master PDB %s" pdb
                    | OrchestratorActor.RollbackMasterPDB (user, pdb) ->
                        sprintf "Roll back modification done in %s" pdb
                    | OrchestratorActor.CreateWorkingCopy (user, instance, pdb, version, name, force) ->
                        sprintf "Create a working copy named %s of master PDB %s version %d on instance %s" name pdb version instance
                    | OrchestratorActor.DeleteWorkingCopy (user, instance, pdb, version, name) ->
                        sprintf "Delete a working copy named %s of master PDB %s version %d on instance %s" name pdb version instance
                    | OrchestratorActor.GetRequest requestId ->
                        sprintf "Get request from id %O" requestId
                jObj 
                |> Encode.required Encode.string "description" description
            )
            let encodeOpenMasterPDB = Encode.buildWith (fun (x:string * MasterPDB.EditionInfoDTO) jObj ->
                let name, lockInfo = x
                jObj 
                |> Encode.required Encode.string "name" name
                |> Encode.required MasterPDB.encodeLockInfo "lock" lockInfo
            )
            let encodePendingChanges = Encode.buildWith (fun (x:OrchestratorActor.PendingChanges) jObj ->
                jObj 
                |> Encode.optional (Encode.listWith encodeOrchestratorCommand) "pendingChangeCommands" (if (x.Commands.IsEmpty) then None else Some x.Commands)
                |> Encode.optional (Encode.listWith encodeOpenMasterPDB) "lockedPDBs" (if (x.OpenMasterPDBs.IsEmpty) then None else Some x.OpenMasterPDBs)
            )
            return! json (pendingChanges |> serializeWith encodePendingChanges JsonFormattingOptions.Pretty) next ctx
}

let enterReadOnlyMode apiCtx next (ctx:HttpContext) = task {
    let! switched = API.enterReadOnlyMode apiCtx
    if switched then
        return! text "The system is now in maintenance mode." next ctx
    else
        return! RequestErrors.notAcceptable (text "The system was already in maintenance mode.") next ctx
}

let enterNormalMode apiCtx next (ctx:HttpContext) = task {
    let! switched = API.enterNormalMode apiCtx
    if switched then
        return! text "The system is now in normal mode." next ctx
    else
        return! RequestErrors.notAcceptable (text "The system was already in normal mode.") next ctx
}

let getMode apiCtx next (ctx:HttpContext) = task {
    let! readOnly = API.isReadOnlyMode apiCtx
    return! json (if readOnly then @"""maintenance""" else @"""normal""") next ctx
}

let prepareMasterPDBForModification apiCtx pdb = withUser (fun user next ctx -> task {
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

let commitMasterPDB apiCtx pdb = withUser (fun user next ctx -> task {
    let! comment = ctx.ReadBodyFromRequestAsync()
    if (comment <> "") then
        let! requestValidation = API.commitMasterPDB apiCtx user pdb comment
        return! returnRequest apiCtx.Endpoint requestValidation next ctx
    else
        return! RequestErrors.badRequest (text "A comment must be provided.") next ctx
})

let rollbackMasterPDB apiCtx pdb = withUser (fun user next ctx -> task {
    let! requestValidation = API.rollbackMasterPDB apiCtx user pdb
    return! returnRequest apiCtx.Endpoint requestValidation next ctx
})

let collectGarbage apiCtx = withUser (fun user next ctx -> task {
    API.collectGarbage apiCtx
    return! text "Garbage collecting initiated." next ctx
})

let synchronizePrimaryInstanceWith apiCtx instance next ctx = task {
    let! stateMaybe = API.synchronizePrimaryInstanceWith apiCtx instance
    match stateMaybe with
    | Ok state -> return! (json <| OracleInstance.oracleInstanceToJson state) next ctx
    | Error error -> return! RequestErrors.notAcceptable (text <| sprintf "Cannot synchronize %s with primary instance : %s." instance error) next ctx
}

let switchPrimaryOracleInstanceWith apiCtx next (ctx:HttpContext) = task {
    let! instance = ctx.BindJsonAsync<string>()
    if System.String.IsNullOrEmpty(instance) then
        return! RequestErrors.notAcceptable (text <| "The new primary Oracle instance name must be provided in the body as a JSON string.") next ctx
    else
        let! result = API.switchPrimaryOracleInstanceWith apiCtx instance
        match result with
        | Ok newInstance -> return! (text <| sprintf "New primary Oracle instance is now %s" newInstance) next ctx
        | Error (error, currentInstance) -> return! RequestErrors.notAcceptable (text <| sprintf "Cannot switch primary Oracle instance to %s : %s. The primary instance is unchanged (%s)." instance error currentInstance) next ctx
}
