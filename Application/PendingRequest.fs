module Application.PendingRequest

open Akkling.ActorRefs

type RequestId = System.Guid

type RequestResult<'R> = RequestId * 'R

type PendingRequest<'C> = {
    Id: System.Guid 
    Command: 'C
    Requester: Akka.Actor.IActorRef
}
