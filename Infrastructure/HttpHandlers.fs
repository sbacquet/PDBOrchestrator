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
open Application.DTO.MasterPDBWorkingCopy
open Microsoft.AspNetCore.Authentication.JwtBearer
open System.Security.Claims

let notAuthenticated next (ctx : HttpContext) =
    RequestErrors.UNAUTHORIZED
        ctx.Request.Scheme
        "Finastra"
        "User/client must be authenticated."
        next
        ctx

let authenticationNeeded : HttpHandler =
    requiresAuthentication ((challenge JwtBearerDefaults.AuthenticationScheme) >=> notAuthenticated)

let json jsonStr : HttpHandler =
    fun _ (ctx : HttpContext) ->
        task {
            ctx.SetHttpHeader "Content-Type" "application/json"
            return! ctx.WriteStringAsync jsonStr
        }

let errorText error = sprintf "Error : %s." error |> text

let getCulture (ctx:HttpContext) =
    ctx.Features.Get<Microsoft.AspNetCore.Localization.IRequestCultureFeature>().RequestCulture.Culture

let withUser f : HttpHandler =
    fun next (ctx:HttpContext) -> task {
        let user : Application.UserRights.User =
            let identity = ctx.User.Identity :?> ClaimsIdentity
            let roles =
                identity.Claims 
                |> Seq.filter (fun claim -> claim.Type = identity.RoleClaimType)
                |> Seq.map (fun claim -> claim.Value)
                |> List.ofSeq
            identity.Name |> Application.UserRights.consUser (identity |> OracleInstanceAffinity.userAffinity) roles
        return! f user next ctx
    }

let withRole role : HttpHandler =
    Auth.requiresRole (UserRights.rolePrefix+role) (RequestErrors.forbidden (sprintf "User/client must have role '%s'." role |> text))

let withAdmin f = 
    withRole UserRights.adminRole >=> 
    withUser f

let getAllInstances apiCtx next (ctx:HttpContext) = task {
    let! state = API.getState apiCtx
    let culture = getCulture ctx
    return! json (Orchestrator.orchestratorToJson culture state) next ctx
}

let getInstance apiCtx (name:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx name
    match stateMaybe with
    | Ok state -> return! json (OracleInstance.oracleInstanceDTOToJson (getCulture ctx) state) next ctx
    | Error error -> return! RequestErrors.notFound (errorText error) next ctx
}

let getBasicInstance apiCtx (name:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceBasicState apiCtx name
    match stateMaybe with
    | Ok state -> return! json (OracleInstance.oracleInstanceBasicDTOToJson (getCulture ctx) state) next ctx
    | Error error -> return! RequestErrors.notFound (errorText error) next ctx
}

let getMasterPDBs apiCtx (instancename:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx instancename
    match stateMaybe with
    | Ok state -> return! json (OracleInstance.masterPDBsToJson (getCulture ctx) state) next ctx
    | Error error -> return! RequestErrors.notFound (errorText error) next ctx
}

let getMasterPDB apiCtx (instance:string, pdb:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getMasterPDBState apiCtx instance pdb
    match stateMaybe with
    | Ok state -> return! json (MasterPDB.masterPDBStatetoJson (getCulture ctx) state) next ctx
    | Error error -> return! RequestErrors.notFound (errorText error) next ctx
}

let getMasterPDBEditionInfo apiCtx (pdb:string) next (ctx:HttpContext) = task {
    let! editionInfo = API.getMasterPDBEditionInfo apiCtx pdb
    match editionInfo with
    | Ok editionInfo -> return! json (MasterPDB.masterPDBEditionDTOToJson (getCulture ctx) editionInfo) next ctx
    | Error error -> return! RequestErrors.notFound (errorText error) next ctx
}

let getMasterPDBVersions apiCtx (instance:string, pdb:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getMasterPDBState apiCtx instance pdb
    match stateMaybe with
    | Ok state -> return! json (MasterPDB.masterPDBVersionstoJson (getCulture ctx) state) next ctx
    | Error error -> return! RequestErrors.notFound (errorText error) next ctx
}

let getMasterPDBVersion apiCtx (instance:string, pdb:string, version:int) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getMasterPDBState apiCtx instance pdb
    match stateMaybe with
    | Ok state -> 
        let versionPDB = state.Versions |> List.tryFind (fun v -> v.VersionNumber = version)
        match versionPDB with
        | Some versionPDB -> return! json (MasterPDBVersion.versionFulltoJson (getCulture ctx) (Application.DTO.MasterPDB.toFullDTO state versionPDB)) next ctx
        | None -> return! RequestErrors.notFound (sprintf "Master PDB %s has no version %d on Oracle instance %s." pdb version instance |> text) next ctx
    | Error error -> return! RequestErrors.notFound (errorText error) next ctx
}

let getWorkingCopies apiCtx (instancename:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx instancename
    match stateMaybe with
    | Ok state -> return! json (state |> OracleInstance.workingCopiesToJson (getCulture ctx) None) next ctx
    | Error error -> return! RequestErrors.notFound (errorText error) next ctx
}

let getWorkingCopiesOfMasterPDB apiCtx (instancename:string, pdb:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx instancename
    match stateMaybe with
    | Ok state -> 
        let filter (wc:MasterPDBWorkingCopyDTO) = wc.MasterPDBName = pdb.ToUpper()
        return! json (state |> OracleInstance.workingCopiesToJson (getCulture ctx) (Some filter)) next ctx
    | Error error -> return! RequestErrors.notFound (errorText error) next ctx
}

let getWorkingCopyOfMasterPDB apiCtx (instancename:string, pdb:string, name:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx instancename
    match stateMaybe with
    | Ok state -> 
        let filter (wc:MasterPDBWorkingCopyDTO) = wc.Name = name.ToUpper() && wc.MasterPDBName = pdb.ToUpper()
        return! json (state |> OracleInstance.workingCopiesToJson (getCulture ctx) (Some filter)) next ctx
    | Error error -> return! RequestErrors.notFound (errorText error) next ctx
}

let getWorkingCopiesOfMasterPDBVersion apiCtx (instancename:string, pdb:string, version:int) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx instancename
    match stateMaybe with
    | Ok state -> 
        let filter (wc:MasterPDBWorkingCopyDTO) = 
            wc.MasterPDBName = pdb.ToUpper() && 
            (match wc.Source with | Domain.MasterPDBWorkingCopy.SpecificVersion v -> v = version | _ -> false)
        return! json (state |> OracleInstance.workingCopiesToJson (getCulture ctx) (Some filter)) next ctx
    | Error error -> return! RequestErrors.notFound (errorText error) next ctx
}

let getWorkingCopyOfMasterPDBVersion apiCtx (instancename:string, pdb:string, version:int, name:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx instancename
    match stateMaybe with
    | Ok state -> 
        let filter (wc:MasterPDBWorkingCopyDTO) = 
            wc.Name = name.ToUpper() && 
            wc.MasterPDBName = pdb.ToUpper() && 
            (match wc.Source with | Domain.MasterPDBWorkingCopy.SpecificVersion v -> v = version | _ -> false)
        return! json (state |> OracleInstance.workingCopiesToJson (getCulture ctx) (Some filter)) next ctx
    | Error error -> return! RequestErrors.notFound (errorText error) next ctx
}

let getWorkingCopiesOfMasterPDBEdition apiCtx (pdb:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx "primary"
    match stateMaybe with
    | Ok state -> 
        let filter (wc:MasterPDBWorkingCopyDTO) = 
            wc.MasterPDBName = pdb.ToUpper() && 
            (match wc.Source with | Domain.MasterPDBWorkingCopy.Edition -> true | _ -> false)
        return! json (state |> OracleInstance.workingCopiesToJson (getCulture ctx) (Some filter)) next ctx
    | Error error -> return! RequestErrors.notFound (errorText error) next ctx
}

let getWorkingCopyOfMasterPDBEdition apiCtx (pdb:string, name:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx "primary"
    match stateMaybe with
    | Ok state -> 
        let filter (wc:MasterPDBWorkingCopyDTO) = 
            wc.Name = name.ToUpper() &&
            wc.MasterPDBName = pdb.ToUpper() && 
            (match wc.Source with | Domain.MasterPDBWorkingCopy.Edition -> true | _ -> false)
        return! json (state |> OracleInstance.workingCopiesToJson (getCulture ctx) (Some filter)) next ctx
    | Error error -> return! RequestErrors.notFound (errorText error) next ctx
}

let getWorkingCopy apiCtx (instancename:string, workingCopyName:string) next (ctx:HttpContext) = task {
    let! stateMaybe = API.getInstanceState apiCtx instancename
    match stateMaybe with
    | Ok state ->
        let workingCopy = state.WorkingCopies |> List.tryFind (fun wc -> wc.Name = workingCopyName.ToUpper())
        match workingCopy with
        | Some workingCopy -> 
            return! json (OracleInstance.workingCopyDTOToJson (getCulture ctx) workingCopy) next ctx
        | None ->
            return! RequestErrors.notFound (sprintf "Working copy %s not found on Oracle instance %s." (workingCopyName.ToUpper()) (instancename.ToLower()) |> text) next ctx
    | Error error -> 
        return! RequestErrors.notFound (errorText error) next ctx
}

let getRequestStatus (apiCtx:API.APIContext) (requestId:PendingRequest.RequestId) next (ctx:HttpContext) = task {

    let completedRequestDataKeyValue = function
    | OrchestratorActor.PDBName name -> "PDB name", name.ToUpper()
    | OrchestratorActor.PDBVersion version -> "PDB version", version.ToString()
    | OrchestratorActor.PDBService service -> "PDB service", service
    | OrchestratorActor.SchemaLogon (schemaType, schemaLogon) -> sprintf "%s schema logon" schemaType, schemaLogon
    | OrchestratorActor.OracleInstance instance -> "Oracle instance", instance.ToLower()
    | OrchestratorActor.ResourceLink link -> "Resource link", apiCtx.Endpoint + link

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

let private createWorkingCopy apiCtx (instance:string, masterPDB:string, version:int, name:string) =
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
                    let! requestValidation = API.createWorkingCopy apiCtx user instance masterPDB version name (not clone) durable force
                    return! returnRequest apiCtx.Endpoint requestValidation next ctx
    })

let createWorkingCopyAuto apiCtx (masterPDB:string, version:int, name:string) =
    createWorkingCopy apiCtx ("auto", masterPDB, version, name)

let createWorkingCopyForcing apiCtx (instance:string, masterPDB:string, version:int, name:string) =
    withRole UserRights.forceInstance >=>
    createWorkingCopy apiCtx (instance, masterPDB, version, name)

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
                let! requestValidation = API.createWorkingCopyOfEdition apiCtx user masterPDB name durable force
                return! returnRequest apiCtx.Endpoint requestValidation next ctx
    })

let deleteWorkingCopy apiCtx (instance:string, name:string) =
    withUser (fun user next ctx -> task {
        let parsedOk, durable = ctx.TryGetQueryStringValue "durable" |> Option.defaultValue bool.FalseString |> bool.TryParse
        if not parsedOk then 
            return! RequestErrors.badRequest (text "When provided, the \"durable\" query parameter must be \"true\" or \"false\".") next ctx
        else
            let! requestValidation = API.deleteWorkingCopy apiCtx user instance name durable
            return! returnRequest apiCtx.Endpoint requestValidation next ctx
    })

let extendWorkingCopy apiCtx (instance:string, name:string) next (ctx:HttpContext) = task {
    let! extendedWorkingCopy = API.extendWorkingCopy apiCtx instance name
    match extendedWorkingCopy with
    | Ok _ ->
        return! text (sprintf "Extended lifetime of working copy %s on instance %s." name instance) next ctx
    | Error error ->
        return! RequestErrors.notFound (errorText error) next ctx
}

let getPendingChanges apiCtx next (ctx:HttpContext) = task {
    let! pendingChangesMaybe = API.getPendingChanges apiCtx
    match pendingChangesMaybe with
    | Error error -> return! ServerErrors.internalError (errorText error) next ctx
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
                |> Encode.required (MasterPDB.encodeLockInfoDTO (getCulture ctx)) "lock" lockInfo
            )
            let encodePendingChanges = Encode.buildWith (fun (x:OrchestratorActor.PendingChanges) jObj ->
                jObj 
                |> Encode.optional (Encode.listWith encodeOrchestratorCommand) "pendingChangeCommands" (if (x.Commands.IsEmpty) then None else Some x.Commands)
                |> Encode.optional (Encode.listWith encodeOpenMasterPDB) "lockedPDBs" (if (x.OpenMasterPDBs.IsEmpty) then None else Some x.OpenMasterPDBs)
            )
            return! json (pendingChanges |> serializeWith encodePendingChanges JsonFormattingOptions.Pretty) next ctx
}

let getPendingCommands apiCtx = withAdmin (fun _ next ctx -> task {
    let! commands = API.getPendingCommands apiCtx
    return! json (commands |> List.map OrchestratorActor.describeCommand |> serializeWith Encode.stringList JsonFormattingOptions.Pretty) next ctx
})

let getPendingCommandsCount apiCtx = withAdmin (fun _ next ctx -> task {
    let! commands = API.getPendingCommands apiCtx
    return! json (commands |> List.length |> serializeWith Encode.int JsonFormattingOptions.SingleLine) next ctx
})

let enterMaintenanceMode apiCtx = withAdmin (fun _ next ctx -> task {
    let! switched = API.enterMaintenanceMode apiCtx
    if switched then
        return! text "The system is now in maintenance mode." next ctx
    else
        return! text "The system was already in maintenance mode." next ctx
})

let enterNormalMode apiCtx = withAdmin (fun _ next ctx -> task {
    let! switched = API.enterNormalMode apiCtx
    if switched then
        return! text "The system is now in normal mode." next ctx
    else
        return! text "The system was already in normal mode." next ctx
})

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
            let! requestValidation = API.prepareMasterPDBForModification apiCtx user pdb version
            return! returnRequest apiCtx.Endpoint requestValidation next ctx
        else 
            return! RequestErrors.badRequest (text "The current version must an integer.") next ctx
    | None ->
        return! RequestErrors.badRequest (text "The current version must be provided.") next ctx
})

let commitMasterPDB apiCtx (pdb:string) = withUser (fun user next ctx -> task {
    let! comment = ctx.ReadBodyFromRequestAsync()
    if (comment <> "") then
        let! requestValidation = API.commitMasterPDB apiCtx user pdb comment
        return! returnRequest apiCtx.Endpoint requestValidation next ctx
    else
        return! RequestErrors.badRequest (text "A comment must be provided.") next ctx
})

let rollbackMasterPDB apiCtx (pdb:string) = withUser (fun user next ctx -> task {
    let! requestValidation = API.rollbackMasterPDB apiCtx user pdb
    return! returnRequest apiCtx.Endpoint requestValidation next ctx
})

let collectGarbage apiCtx = withAdmin (fun _ next ctx -> task {
    API.collectGarbage apiCtx
    return! text "Garbage collecting of all Oracle instances initiated." next ctx
})

let collectInstanceGarbage apiCtx (instance:string) = withAdmin (fun _ next ctx -> task {
    API.collectInstanceGarbage apiCtx instance
    return! text (sprintf "Garbage collecting of Oracle instance %s initiated." instance) next ctx
})

let synchronizePrimaryInstanceWith apiCtx instance = withAdmin (fun _ next ctx -> task {
    let! stateMaybe = API.synchronizePrimaryInstanceWith apiCtx instance
    match stateMaybe with
    | Ok state -> return! (json <| OracleInstance.oracleInstanceDTOToJson (getCulture ctx) state) next ctx
    | Error error -> return! RequestErrors.notAcceptable (text <| sprintf "Cannot synchronize %s with primary instance : %s." (instance.ToLower()) error) next ctx
})

let switchPrimaryOracleInstanceWith apiCtx = withAdmin (fun _ next ctx -> task {
    let! instance = ctx.BindJsonAsync<string>()
    if System.String.IsNullOrEmpty(instance) then
        return! RequestErrors.notAcceptable (text <| "The new primary Oracle instance name must be provided in the body as a JSON string.") next ctx
    else
        let! result = API.switchPrimaryOracleInstanceWith apiCtx instance
        match result with
        | Ok newInstance -> return! (text <| sprintf "New primary Oracle instance is now %s" (newInstance.ToLower())) next ctx
        | Error (error, currentInstance) -> return! RequestErrors.notAcceptable (text <| sprintf "Cannot switch primary Oracle instance to %s : %s. The primary instance is unchanged (%s)." (instance.ToLower()) error (currentInstance.ToLower())) next ctx
})

open Application.OracleInstanceActor

let decodeCreateMasterPDBParams user = jsonDecoder {
    let! name = Decode.required Decode.string "Name" 
    let! dump = Decode.required Decode.string "Dump" 
    let! schemas = Decode.required Decode.stringList "Schemas"
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
    Encode.required Encode.stringList "Schemas" x.Schemas >>
    Encode.required (Encode.listWith (Encode.tuple3 Encode.string Encode.string Encode.string)) "TargetSchemas" x.TargetSchemas >>
    Encode.required Encode.string "User" x.User.Name >>
    Encode.required Encode.dateTime "Date" x.Date >>
    Encode.required Encode.string "Comment" x.Comment
)

let jsonToCreateMasterPDBParams user json = 
    json |> Json.deserializeWith (decodeCreateMasterPDBParams user)

let createNewPDB apiCtx = withAdmin (fun user next ctx -> task {
    let! body = ctx.ReadBodyFromRequestAsync()
    let pars = jsonToCreateMasterPDBParams user body
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

let deleteMasterPDBVersion apiCtx (masterPDB:string, version:int) = withAdmin (fun _ next ctx -> task {
    let parsedOk, force = ctx.TryGetQueryStringValue "force" |> Option.defaultValue bool.FalseString |> bool.TryParse
    if not parsedOk then 
        return! RequestErrors.badRequest (text "When provided, the \"force\" query parameter must be \"true\" or \"false\".") next ctx
    else
        let! result = API.deleteMasterPDBVersion apiCtx masterPDB version force
        match result with
        | Ok invalidInstances ->
            match invalidInstances with
            | [] -> return! (text <| sprintf "Version %d of master PDB %s deleted on all instances." version (masterPDB.ToUpper())) next ctx
            | invalidInstances ->
                return!
                    (text <|
                        sprintf
                            "Version %d of master PDB %s deleted on primary instance, but not on : %s."
                            version
                            (masterPDB.ToUpper())
                            (invalidInstances |> List.map (fun (instance, error) -> sprintf "%s : %s" instance error) |> String.concat "; "))
                    next ctx
        | Error error -> return! RequestErrors.notAcceptable (text <| sprintf "Cannot delete version %d of master PDB %s : %s." version (masterPDB.ToUpper()) error) next ctx
})

let switchLock apiCtx (masterPDB:string) = withAdmin (fun _ next ctx -> task {
    let! lock = API.switchLock apiCtx masterPDB
    match lock with
    | Ok locked -> return! (text <| sprintf "Master PDB edition %s is now %s." masterPDB (if locked then "locked" else "unlocked")) next ctx
    | Error error -> return! RequestErrors.notAcceptable (text <| sprintf "Cannot switch lock for master PDB %s : %s." (masterPDB.ToUpper()) error) next ctx
})

let deleteRequest apiCtx (requestId:System.Guid) next (ctx:HttpContext) = task {
    API.deleteRequest apiCtx requestId
    return! text (sprintf "The request %O has been deleted, if any." requestId) next ctx
}

let declareMasterPDBVersionSynchronizedWithPrimary apiCtx (instance:string, masterPDB:string, version:int) = withAdmin (fun _ next ctx -> task {
    let! res = API.declareMasterPDBVersionSynchronizedWithPrimary apiCtx instance masterPDB version
    match res with
    | Ok _ -> return! (text <| sprintf "Master PDB %s version %d is now available on Oracle instance %s." masterPDB version instance) next ctx
    | Error error -> return! RequestErrors.notAcceptable (text <| sprintf "Error : %s." error) next ctx
})
