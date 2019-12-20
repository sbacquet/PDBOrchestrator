module Application.UserPendingRequest

open Application.PendingRequest

type PendingUserRequest<'C> = {
    Id: RequestId 
    Command: 'C
    User: string
    StartTime: System.DateTime
}

type WithUser<'T> = string * 'T
type WithUser<'T1, 'T2> = string * 'T1 * 'T2
type WithUser<'T1, 'T2, 'T3> = string * 'T1 * 'T2 * 'T3
type WithUser<'T1, 'T2, 'T3, 'T4> = string * 'T1 * 'T2 * 'T3 * 'T4
type WithUser<'T1, 'T2, 'T3, 'T4, 'T5> = string * 'T1 * 'T2 * 'T3 * 'T4 * 'T5
type WithUser<'T1, 'T2, 'T3, 'T4, 'T5, 'T6> = string * 'T1 * 'T2 * 'T3 * 'T4 * 'T5 * 'T6
type WithUser<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7> = string * 'T1 * 'T2 * 'T3 * 'T4 * 'T5 * 'T6 * 'T7

type PendingUserRequestMap<'C> = Map<RequestId, PendingUserRequest<'C>>

let registerUserRequest<'C> log id (command : 'C) user requests : PendingUserRequestMap<'C> =
    log id command
    requests 
    |> Map.add id { 
        Id = id
        Command = command
        User = user
        StartTime = System.DateTime.Now 
       }

type CompletedUserRequest<'S> = {
    Id: RequestId 
    Status: 'S
    User: string
    Duration: System.TimeSpan
}

type CompletedUserRequestMap<'R> = Map<RequestId, CompletedUserRequest<'R>>

let completeUserRequest<'C, 'S> log (pendingRequests : PendingUserRequestMap<'C>) (completedRequests : CompletedUserRequestMap<'S>) (pendingRequest:PendingUserRequest<'C>) (status : 'S) =
    let newPendingRequests = pendingRequests |> unregisterRequest pendingRequest.Id
    let completedRequest = { 
        Id = pendingRequest.Id
        Status = status
        User = pendingRequest.User
        Duration = System.DateTime.Now - pendingRequest.StartTime 
    }
    log pendingRequest.Id pendingRequest.Command completedRequest
    let newCompletedRequests = completedRequests |> Map.add pendingRequest.Id completedRequest
    (newPendingRequests, newCompletedRequests)

