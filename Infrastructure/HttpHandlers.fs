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
open Chiron

let json jsonStr : HttpHandler =
    fun _ (ctx : HttpContext) ->
        task {
            ctx.SetHttpHeader "Content-Type" "application/json"
            return! ctx.WriteStringAsync jsonStr
        }

let withUser f : HttpHandler =
    fun next (ctx:HttpContext) -> task {
        let userNameMaybe = 
            if (ctx.User.Identity.Name = null) then None else Some ctx.User.Identity.Name
            |> Option.orElse (ctx.TryGetRequestHeader "user") // TODO: remove this line when authentication implemented
        let isAdmin = ctx.TryGetRequestHeader "admin" |> Option.contains "true" // TODO
        match userNameMaybe with
        | Some userName -> 
            let user:Application.UserRights.User = { Name = userName.ToLower(); Roles = if isAdmin then [ "admin" ] else [] }
            return! f user next ctx
        | None -> return! RequestErrors.badRequest (text "User cannot be determined.") next ctx
    }

let withAdmin f : HttpHandler =
    let ff user next ctx =
        if UserRights.isAdmin user then
            f user next ctx
        else
            RequestErrors.forbidden (text "User must be admin.") next ctx
    withUser ff

let getAllInstances apiCtx next (ctx:HttpContext) = task {
    let! state = API.getState apiCtx
    return! json (Orchestrator.orchestratorToJson state) next ctx
}

let getInstance apiCtx (name:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx name
    match stateMaybe with
    | Ok state -> return! json (OracleInstance.oracleInstanceToJson state) next ctx
    | Error error -> return! RequestErrors.notFound (text error) next ctx
}

let getMasterPDBs apiCtx (instancename:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx instancename
    match stateMaybe with
    | Ok state -> return! json (OracleInstance.masterPDBsToJson state) next ctx
    | Error error -> return! RequestErrors.notFound (text error) next ctx
}

let getMasterPDB apiCtx (instance:string, pdb:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getMasterPDBState apiCtx instance pdb
    match stateMaybe with
    | Ok state -> return! json (MasterPDB.masterPDBStatetoJson state) next ctx
    | Error error -> return! RequestErrors.notFound (text error) next ctx
}

let getWorkingCopies apiCtx (instancename:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx instancename
    match stateMaybe with
    | Ok state -> return! json (OracleInstance.workingCopiesToJson state) next ctx
    | Error error -> return! RequestErrors.notFound (text error) next ctx
}

let getWorkingCopy apiCtx (instancename:string, workingCopyName:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx instancename
    match stateMaybe with
    | Ok state ->
        let workingCopy = state.WorkingCopies |> List.tryFind (fun wc -> wc.Name = workingCopyName.ToUpper())
        match workingCopy with
        | Some workingCopy -> 
            return! json (OracleInstance.workingCopyToJson workingCopy) next ctx
        | None ->
            return! RequestErrors.notFound (sprintf "Working copy %s not found on Oracle instance %s" (workingCopyName.ToUpper()) (instancename.ToLower()) |> text) next ctx
    | Error error -> 
        return! RequestErrors.notFound (text error) next ctx
}

let getRequestStatus apiCtx (requestId:PendingRequest.RequestId) next (ctx:HttpContext) = task {

    let completedRequestDataKeyValue = function
    | OrchestratorActor.PDBName name -> "PDB name", name.ToUpper()
    | OrchestratorActor.PDBVersion version -> "PDB version", version.ToString()
    | OrchestratorActor.PDBService service -> "PDB service", service
    | OrchestratorActor.SchemaLogon (schemaType, schemaLogon) -> sprintf "%s schema logon" schemaType, schemaLogon
    | OrchestratorActor.OracleInstance instance -> "Oracle instance", instance.ToLower()

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
            | OrchestratorActor.Done (OrchestratorActor.CompletedOk (message, data), duration) -> 
                jObj 
                |> Encode.required Encode.string "status" "Completed"
                |> Encode.required Encode.string "message" message
                |> Encode.required Encode.float "durationInSec" duration.TotalSeconds
                |> Encode.required (Encode.propertyListWith Encode.string) 
                    "data" 
                    (data |> List.map completedRequestDataKeyValue)
            | OrchestratorActor.Done (OrchestratorActor.CompletedWithError error, duration) -> 
                jObj 
                |> Encode.required Encode.string "status" "Completed with error"
                |> Encode.required Encode.string "error" error
                |> Encode.required Encode.float "durationInSec" duration.TotalSeconds
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
            let parsedOk, clone = ctx.TryGetQueryStringValue "clone" |> Option.defaultValue bool.FalseString |> bool.TryParse
            if not parsedOk then 
                return! RequestErrors.badRequest (text "When provided, the \"clone\" query parameter must be \"true\" or \"false\".") next ctx
            else
                let parsedOk, durable = ctx.TryGetQueryStringValue "durable" |> Option.defaultValue bool.FalseString |> bool.TryParse
                if not parsedOk then 
                    return! RequestErrors.badRequest (text "When provided, the \"durable\" query parameter must be \"true\" or \"false\".") next ctx
                else
                    let! requestValidation = API.createWorkingCopy apiCtx user.Name instance masterPDB version name (not clone) durable force
                    return! returnRequest apiCtx.Endpoint requestValidation next ctx
    })

let createWorkingCopyOfEdition apiCtx (masterPDB:string, name:string) =
    withUser (fun user next ctx -> task {
        let parsedOk, force = ctx.TryGetQueryStringValue "force" |> Option.defaultValue bool.FalseString |> bool.TryParse
        if not parsedOk then 
            return! RequestErrors.badRequest (text "When provided, the \"force\" query parameter must be \"true\" or \"false\".") next ctx
        else
            let parsedOk, durable = ctx.TryGetQueryStringValue "durable" |> Option.defaultValue bool.FalseString |> bool.TryParse
            if not parsedOk then 
                return! RequestErrors.badRequest (text "When provided, the \"durable\" query parameter must be \"true\" or \"false\".") next ctx
            else
                let! requestValidation = API.createWorkingCopyOfEdition apiCtx user.Name masterPDB name durable force
                return! returnRequest apiCtx.Endpoint requestValidation next ctx
    })

let deleteWorkingCopy apiCtx (instance:string, name:string) =
    withUser (fun user next ctx -> task {
        let! requestValidation = API.deleteWorkingCopy apiCtx user.Name instance name
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
                jObj 
                |> Encode.required Encode.string "description" (OrchestratorActor.describeCommand command)
            )
            let encodeOpenMasterPDB = Encode.buildWith (fun (x:string * MasterPDB.EditionInfoDTO) jObj ->
                let name, lockInfo = x
                jObj 
                |> Encode.required Encode.string "name" (name.ToUpper())
                |> Encode.required MasterPDB.encodeLockInfo "lock" lockInfo
            )
            let encodePendingChanges = Encode.buildWith (fun (x:OrchestratorActor.PendingChanges) jObj ->
                jObj 
                |> Encode.optional (Encode.listWith encodeOrchestratorCommand) "pendingChangeCommands" (if (x.Commands.IsEmpty) then None else Some x.Commands)
                |> Encode.optional (Encode.listWith encodeOpenMasterPDB) "lockedPDBs" (if (x.OpenMasterPDBs.IsEmpty) then None else Some x.OpenMasterPDBs)
            )
            return! json (pendingChanges |> serializeWith encodePendingChanges JsonFormattingOptions.Pretty) next ctx
}

let enterMaintenanceMode apiCtx next (ctx:HttpContext) = task {
    let! switched = API.enterMaintenanceMode apiCtx
    if switched then
        return! text "The system is now in maintenance mode." next ctx
    else
        return! text "The system was already in maintenance mode." next ctx
}

let enterNormalMode apiCtx next (ctx:HttpContext) = task {
    let! switched = API.enterNormalMode apiCtx
    if switched then
        return! text "The system is now in normal mode." next ctx
    else
        return! text "The system was already in normal mode." next ctx
}

let getMode apiCtx next (ctx:HttpContext) = task {
    let! readOnly = API.isMaintenanceMode apiCtx
    return! json (if readOnly then @"""maintenance""" else @"""normal""") next ctx
}

let prepareMasterPDBForModification apiCtx (pdb:string) = withUser (fun user next ctx -> task {
    let version = ctx.TryGetQueryStringValue "version"
    match version with
    | Some version -> 
        let (ok, version) = System.Int32.TryParse version
        if ok then
            let! requestValidation = API.prepareMasterPDBForModification apiCtx user.Name pdb version
            return! returnRequest apiCtx.Endpoint requestValidation next ctx
        else 
            return! RequestErrors.badRequest (text "The current version must an integer.") next ctx
    | None ->
        return! RequestErrors.badRequest (text "The current version must be provided.") next ctx
})

let commitMasterPDB apiCtx (pdb:string) = withUser (fun user next ctx -> task {
    let! comment = ctx.ReadBodyFromRequestAsync()
    if (comment <> "") then
        let! requestValidation = API.commitMasterPDB apiCtx user.Name pdb comment
        return! returnRequest apiCtx.Endpoint requestValidation next ctx
    else
        return! RequestErrors.badRequest (text "A comment must be provided.") next ctx
})

let rollbackMasterPDB apiCtx (pdb:string) = withUser (fun user next ctx -> task {
    let! requestValidation = API.rollbackMasterPDB apiCtx user.Name pdb
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
    | Error error -> return! RequestErrors.notAcceptable (text <| sprintf "Cannot synchronize %s with primary instance : %s." (instance.ToLower()) error) next ctx
}

let switchPrimaryOracleInstanceWith apiCtx next (ctx:HttpContext) = task {
    let! instance = ctx.BindJsonAsync<string>()
    if System.String.IsNullOrEmpty(instance) then
        return! RequestErrors.notAcceptable (text <| "The new primary Oracle instance name must be provided in the body as a JSON string.") next ctx
    else
        let! result = API.switchPrimaryOracleInstanceWith apiCtx instance
        match result with
        | Ok newInstance -> return! (text <| sprintf "New primary Oracle instance is now %s" (newInstance.ToLower())) next ctx
        | Error (error, currentInstance) -> return! RequestErrors.notAcceptable (text <| sprintf "Cannot switch primary Oracle instance to %s : %s. The primary instance is unchanged (%s)." (instance.ToLower()) error (currentInstance.ToLower())) next ctx
}

open Application.OracleInstanceActor

let decodeCreateMasterPDBParams user = jsonDecoder {
    let! name = Decode.required Decode.string "Name" 
    let! dump = Decode.required Decode.string "Dump" 
    let! schemas = Decode.required (Decode.listWith Decode.string) "Schemas"
    let! targetSchemas = Decode.required (Decode.listWith (Decode.tuple3With Decode.string Decode.string Decode.string)) "TargetSchemas"
    let! date = Decode.optional Decode.dateTime "Date"
    let! comment = Decode.required Decode.string "Comment"
    return 
        consCreateMasterPDBParams 
            name 
            dump
            schemas
            targetSchemas
            user
            (date |> Option.defaultValue System.DateTime.Now)
            comment
}

let encodeCreateMasterPDBParams = Encode.buildWith (fun (x:CreateMasterPDBParams) ->
    Encode.required Encode.string "Name" x.Name >>
    Encode.required Encode.string "Dump" x.Dump>>
    Encode.required (Encode.listWith Encode.string) "Schemas" x.Schemas >>
    Encode.required (Encode.listWith (Encode.tuple3 Encode.string Encode.string Encode.string)) "TargetSchemas" x.TargetSchemas >>
    Encode.required Encode.string "User" x.User >>
    Encode.required Encode.dateTime "Date" x.Date >>
    Encode.required Encode.string "Comment" x.Comment
)

let jsonToCreateMasterPDBParams user json = 
    json |> Json.deserializeWith (decodeCreateMasterPDBParams user)

let createNewPDB apiCtx = withAdmin (fun user next ctx -> task {
    let! body = ctx.ReadBodyFromRequestAsync()
    let pars = jsonToCreateMasterPDBParams user.Name body
    match pars with
    | JPass result ->
        let! requestValidation = API.createMasterPDB apiCtx result
        return! returnRequest apiCtx.Endpoint requestValidation next ctx
    | JFail error -> 
        return! RequestErrors.badRequest (error |> JsonFailure.summarize |> sprintf "JSON provided in body is not well-formed :\n%s" |> text) next ctx
})

let createMasterPDBParamsToJson pars = 
    pars |> Json.serializeWith encodeCreateMasterPDBParams JsonFormattingOptions.Pretty 

let encodeDumpTransferInfo = Encode.buildWith (fun (x:DumpTransferInfo) ->
    Encode.required Encode.string "ImpDpLogin" x.ImpDpLogin >>
    Encode.required Encode.string "OracleDirectory" x.OracleDirectory >>
    Encode.required Encode.int "OraclePort" x.OraclePort >>
    Encode.required Encode.string "RemoteFolder" x.RemoteFolder >>
    Encode.required Encode.string "Server" x.Server >>
    Encode.required Encode.string "ServerUser" x.ServerUser >>
    Encode.required Encode.string "ServerPassword" x.ServerPassword >>
    Encode.required Encode.string "ServerHostkeyMD5" x.ServerHostkeyMD5 >>
    Encode.required Encode.string "ServerHostkeySHA256" x.ServerHostkeySHA256
)

let dumpTransferInfoToJson json = 
    json |> Json.serializeWith encodeDumpTransferInfo JsonFormattingOptions.Pretty

let getDumpTransferInfo apiCtx instance next (ctx:HttpContext) = task {
    let! dumpTransferInfoMaybe = API.getDumpTransferInfo apiCtx instance
    match dumpTransferInfoMaybe with
    | Ok dumpTransferInfo -> return! (json <| dumpTransferInfoToJson dumpTransferInfo) next ctx
    | Error error -> return! RequestErrors.notAcceptable (text <| sprintf "Cannot get dump transfer info for instance %s : %s." (instance.ToLower()) error) next ctx
}
