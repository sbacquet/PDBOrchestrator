module Application.UserPendingRequest

open Application.PendingRequest

type PendingUserRequest<'C> = {
    Id: RequestId 
    Command: 'C
    User: string
}

type WithUser<'T> = string * 'T
type WithUser<'T1, 'T2> = string * 'T1 * 'T2
type WithUser<'T1, 'T2, 'T3> = string * 'T1 * 'T2 * 'T3
type WithUser<'T1, 'T2, 'T3, 'T4> = string * 'T1 * 'T2 * 'T3 * 'T4

type PendingUserRequestMap<'C> = Map<RequestId, PendingUserRequest<'C>>

let registerUserRequest<'C> id (command : 'C) user requests : PendingUserRequestMap<'C> =
    requests |> Map.add id { Id = id; Command = command; User = user }

type CompletedUserRequest<'S> = {
    Id: RequestId 
    Status: 'S
    User: string
}

type CompletedUserRequestMap<'R> = Map<RequestId, CompletedUserRequest<'R>>

let completeUserRequest<'C, 'S> (pendingRequest:PendingUserRequest<'C>) (status : 'S) (pendingRequests : PendingUserRequestMap<'C>) (completedRequests : CompletedUserRequestMap<'S>) =
    let newPendingRequests = pendingRequests |> unregisterRequest pendingRequest.Id
    let newCompletedRequests = completedRequests |> Map.add pendingRequest.Id { Id = pendingRequest.Id; Status = status; User = pendingRequest.User }
    (newPendingRequests, newCompletedRequests)

