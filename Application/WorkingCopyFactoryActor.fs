module Application.WorkingCopyFactoryActor

open Akkling
open Akka.Actor
open Domain.MasterPDBVersion
open Application.PendingRequest
open Application.Oracle
open Application.Parameters
open Domain.Common
open Application.Common
open Application
open Akka.Routing
open Domain.MasterPDBWorkingCopy

type Command =
| CreateWorkingCopyBySnapshot of WithOptionalRequestId<string, string, string, bool, bool> // responds with OraclePDBResultWithReqId
| CreateWorkingCopyByClone of WithOptionalRequestId<string, string, string, bool, bool> // responds with OraclePDBResultWithReqId
| CreateWorkingCopyOfEdition of WithOptionalRequestId<string, string, string, bool, bool> // responds with OraclePDBResultWithReqId
| DeleteWorkingCopy of WithOptionalRequestId<string, bool>

let private workingCopyFactoryActorBody 
    (parameters:Parameters)
    instance
    (oracleShortTaskExecutor:IActorRef<Application.OracleShortTaskExecutor.Command>)
    (oracleLongTaskExecutor:IActorRef<OracleLongTaskExecutor.Command>) 
    (oracleDiskIntensiveTaskExecutor:IActorRef<OracleDiskIntensiveActor.Command>) 
    (ctx : Actor<Command>) =

    let deletePDB pdb : OraclePDBResult = 
        oracleLongTaskExecutor <? OracleLongTaskExecutor.DeletePDB (None, pdb)
        |> runWithin parameters.LongTimeout id (fun () -> sprintf "PDB %s cannot be deleted : timeout exceeded" pdb |> exn |> Error)
    let importPDB manifest path name : OraclePDBResult =
        oracleDiskIntensiveTaskExecutor <? OracleDiskIntensiveActor.ImportPDB (None, manifest, path, name)
        |> runWithin parameters.VeryLongTimeout id (fun () -> sprintf "cannot import PDB %s : timeout exceeded" name |> exn |> Error)
    let snapshotPDB source path name : OraclePDBResult =
        oracleLongTaskExecutor <? OracleLongTaskExecutor.SnapshotPDB (None, source, path, name)
        |> runWithin parameters.LongTimeout id (fun () -> sprintf "cannot snapshot PDB %s to %s : timeout exceeded" source name |> exn |> Error)
    let clonePDB editionPDBName path name : OraclePDBResult =
        oracleDiskIntensiveTaskExecutor <? OracleDiskIntensiveActor.ClonePDB (None, editionPDBName, path, name)
        |> runWithin parameters.VeryLongTimeout id (fun () -> sprintf "cannot clone PDB %s to %s : timeout exceeded" editionPDBName name |> exn |> Error)
    let isTempWorkingCopy (pdb:string) : Exceptional.Exceptional<bool> = result {
        let! (folder:string option) = 
            oracleShortTaskExecutor <? OracleShortTaskExecutor.GetPDBFilesFolder pdb
            |> runWithin parameters.ShortTimeout id (fun () -> "cannot get files folder : timeout exceeded" |> exn |> Error)
        return folder |> Option.map (Domain.OracleInstance.isTemporaryWorkingCopyFolder instance) |> Option.defaultValue false
    }
    let isDurableWorkingCopy (pdb:string) : Exceptional.Exceptional<bool> = result {
        let! (folder:string option) = 
            oracleShortTaskExecutor <? OracleShortTaskExecutor.GetPDBFilesFolder pdb
            |> runWithin parameters.ShortTimeout id (fun () -> "cannot get files folder : timeout exceeded" |> exn |> Error)
        return folder |> Option.map (Domain.OracleInstance.isDurableWorkingCopyFolder instance) |> Option.defaultValue false
    }
    let pdbExists pdb : Exceptional.Exceptional<bool> = 
        oracleShortTaskExecutor <? OracleShortTaskExecutor.PDBExists pdb
        |> runWithin parameters.ShortTimeout id (fun () -> sprintf "cannot check if PDB %s exists: timeout exceeded" pdb |> exn |> Error)
    let reply (requestId:RequestId option) (result:OraclePDBResult) =
        match requestId with
        | Some requestId -> ctx.Sender() <! (requestId, result)
        | None -> ctx.Sender() <! result
    let createWorkingCopyIfNeeded workingCopyName durable force builder = result {
        let! wcExists = pdbExists workingCopyName
        // if the working copy already exists and not forcing, keep it if same durability
        if wcExists && not force then
            let! isDurable = isDurableWorkingCopy workingCopyName
            if isDurable <> durable then
                return! Error <| (sprintf "working copy %s already exists but for a different durability (%s)" workingCopyName (Lifetime.text isDurable) |> exn)
            else
                return workingCopyName
        else
            let! _ = result {
                if wcExists then
                    let! canDelete = 
                        if durable then Ok true
                        else isTempWorkingCopy workingCopyName
                    return!
                        if canDelete then
                            deletePDB workingCopyName
                        else
                            Error <| (sprintf "PDB %s exists and is not a temporary working copy, so cannot be overwritten" workingCopyName |> exn)
                else
                    return ""
            }
            return! builder workingCopyName
    }

    let rec loop () = actor {

        let! command = ctx.Receive()

        match command with
        | CreateWorkingCopyBySnapshot (requestId, snapshotSourceName, destPath, workingCopyName, durable, force) -> 
            ctx.Log.Value.Info("Creating working copy {pdb} by snapshot...", workingCopyName)
            let result = createWorkingCopyIfNeeded workingCopyName durable force <| snapshotPDB snapshotSourceName destPath
            result |> reply requestId
            return! loop ()

        | CreateWorkingCopyByClone (requestId, sourceManifest, destPath, workingCopyName, durable, force) -> 
            ctx.Log.Value.Info("Creating working copy {pdb} by clone...", workingCopyName)
            let result = createWorkingCopyIfNeeded workingCopyName durable force <| importPDB sourceManifest destPath
            result |> reply requestId
            return! loop ()

        | CreateWorkingCopyOfEdition (requestId, editionPDBName, destPath, workingCopyName, durable, force) ->
            ctx.Log.Value.Info("Creating working copy {pdb} of edition...", workingCopyName)
            let result = createWorkingCopyIfNeeded workingCopyName durable force <| clonePDB editionPDBName destPath
            result |> reply requestId
            return! loop ()

        | DeleteWorkingCopy (requestId, workingCopyName, temporaryOnly) ->
            ctx.Log.Value.Info("Deleting working copy {pdb}...", workingCopyName)
            let result = result {
                let! delete = result {
                    if temporaryOnly then
                        return! isTempWorkingCopy workingCopyName
                    else
                        return true
                }
                return! 
                    if delete then deletePDB workingCopyName
                    else sprintf "PDB %s is not a temporary working copy" workingCopyName |> exn |> Error
            }
            ctx.Log.Value.Info("Working copy {pdb} deleted.", workingCopyName)
            result |> reply requestId
            return! loop ()

    }

    loop ()

let spawn parameters instance shortTaskExecutor longTaskExecutor oracleDiskIntensiveTaskExecutor (actorFactory:IActorRefFactory) =

    let hashMapping (command:obj) : obj =
        match command with
        | :? Command as command -> 
            match command with
            | CreateWorkingCopyBySnapshot (_, _, _, workingCopyName, _, _)
            | CreateWorkingCopyByClone (_, _, _, workingCopyName, _, _)
            | CreateWorkingCopyOfEdition (_, _, _, workingCopyName, _, _)
            | DeleteWorkingCopy (_, workingCopyName, _) 
                -> upcast workingCopyName
        | _ -> upcast ""
    (Akkling.Spawn.spawn actorFactory "WorkingCopyFactory"
        <| { props (workingCopyFactoryActorBody parameters instance shortTaskExecutor longTaskExecutor oracleDiskIntensiveTaskExecutor)
                with Router = Some (upcast ConsistentHashingPool(parameters.NumberOfWorkingCopyWorkers).WithHashMapping(ConsistentHashMapping(hashMapping))) }
    ).Retype<Command>()

