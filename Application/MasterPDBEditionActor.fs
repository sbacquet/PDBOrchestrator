module Application.MasterPDBEditionActor

open Akkling
open Akka.Actor
open Application.PendingRequest
open Application.Oracle
open Application.Parameters
open Domain.Common
open Domain.Common.Exceptional
open Domain.OracleInstance
open Application.Common
open Domain.MasterPDBWorkingCopy

type Command =
| CreateWorkingCopy of WithRequestId<string, bool, bool> // responds with OraclePDBResultWithReqId
| DeleteWorkingCopy of WithRequestId<MasterPDBWorkingCopy> // responds with OraclePDBResultWithReqId

let private masterPDBEditionActorBody 
    (instance:OracleInstance) 
    (workingCopyFactory:IActorRef<Application.WorkingCopyFactoryActor.Command>)
    (editionPDBName:string)
    (ctx : Actor<Command>) =

    let rec loop () = 
        
        actor {

        let! command = ctx.Receive()

        match command with
        | CreateWorkingCopy (requestId, workingCopyName, durable, force) -> 
            workingCopyFactory <<! WorkingCopyFactoryActor.CreateWorkingCopyOfEdition(Some requestId, editionPDBName, (instance |> getWorkingCopyFolder durable), workingCopyName, durable, force)
            return! loop ()
        
        | DeleteWorkingCopy (requestId, workingCopy) ->
            workingCopyFactory <<! WorkingCopyFactoryActor.DeleteWorkingCopy(Some requestId, workingCopy.Name, false)
            return! loop ()

        }

    loop ()

let spawn 
        instance 
        (workingCopyFactory:IActorRef<Application.WorkingCopyFactoryActor.Command>)
        (editionPDBName:string) (actorFactory:IActorRefFactory) =

    (Akkling.Spawn.spawn actorFactory "edition"
        <| props (
            masterPDBEditionActorBody
                instance
                workingCopyFactory
                editionPDBName
        )).Retype<Command>()

