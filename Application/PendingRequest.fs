module Application.PendingRequest

open Akkling.ActorRefs

type RequestId = System.Guid

let newRequestId () = System.Guid.NewGuid()

type PendingRequest<'C> = {
    Id: System.Guid 
    Command: 'C
    Requester: IActorRef<obj>
}

type WithRequestId<'T> = RequestId * 'T
type WithRequestId<'T1, 'T2> = RequestId * 'T1 * 'T2
type WithRequestId<'T1, 'T2, 'T3> = RequestId * 'T1 * 'T2 * 'T3
type WithRequestId<'T1, 'T2, 'T3, 'T4> = RequestId * 'T1 * 'T2 * 'T3 * 'T4

type RequestMap<'C> = Map<RequestId, PendingRequest<'C>>

let registerRequest<'C> id (command : 'C) requester requests : RequestMap<'C> =
    requests |> Map.add id { Id = id; Command = command; Requester = requester }

let getRequest id requests =
    requests |> Map.tryFind id

let unregisterRequest id requests =
    requests |> Map.remove id

let getAndUnregisterRequest id requests = 
    let request = requests |> getRequest id
    request, unregisterRequest id requests
