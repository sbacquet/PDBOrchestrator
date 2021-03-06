﻿module Application.UserPendingRequest

open Application.PendingRequest

type User = Application.UserRights.User

type PendingUserRequest<'C> = {
    Id: RequestId 
    Command: 'C
    User: User
    StartTime: System.DateTime
    Deleted: bool
}

let newPendingUserRequest id command user =
    { 
        Id = id
        Command = command
        User = user
        StartTime = System.DateTime.Now
        Deleted = false
    }

type WithUser<'T> = User * 'T
type WithUser<'T1, 'T2> = User * 'T1 * 'T2
type WithUser<'T1, 'T2, 'T3> = User * 'T1 * 'T2 * 'T3
type WithUser<'T1, 'T2, 'T3, 'T4> = User * 'T1 * 'T2 * 'T3 * 'T4
type WithUser<'T1, 'T2, 'T3, 'T4, 'T5> = User * 'T1 * 'T2 * 'T3 * 'T4 * 'T5
type WithUser<'T1, 'T2, 'T3, 'T4, 'T5, 'T6> = User * 'T1 * 'T2 * 'T3 * 'T4 * 'T5 * 'T6
type WithUser<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7> = User * 'T1 * 'T2 * 'T3 * 'T4 * 'T5 * 'T6 * 'T7

type PendingUserRequestMap<'C> = Map<RequestId, PendingUserRequest<'C>>

let alivePendingRequests (pendingRequests : PendingUserRequestMap<'C>) =
    pendingRequests |> Map.toSeq |> Seq.map snd |> Seq.filter (fun req -> not req.Deleted)

let registerUserRequest<'C> log id (command : 'C) user requests : PendingUserRequestMap<'C> =
    log id command user
    requests |> Map.add id (newPendingUserRequest id command user)

type CompletedUserRequest<'S> = {
    Id: RequestId 
    Status: 'S
    User: User
    CompletionTime: System.DateTime
    Duration: System.TimeSpan
    RetrievedByClient: bool
}

type CompletedUserRequestMap<'R> = Map<RequestId, CompletedUserRequest<'R>>

let completeUserRequest<'C, 'S> log (pendingRequests : PendingUserRequestMap<'C>) (completedRequests : CompletedUserRequestMap<'S>) (pendingRequest:PendingUserRequest<'C>) (status : 'S) =
    pendingRequests |> unregisterRequest pendingRequest.Id,
    if pendingRequest.Deleted then
        completedRequests
    else
        let now = System.DateTime.Now
        let completedRequest = { 
            Id = pendingRequest.Id
            Status = status
            User = pendingRequest.User
            CompletionTime = now
            Duration = now - pendingRequest.StartTime
            RetrievedByClient = false
        }
        log pendingRequest.Id pendingRequest.Command completedRequest
        completedRequests |> Map.add pendingRequest.Id completedRequest

let deletePendingRequest<'C, 'S> log (pendingRequests : PendingUserRequestMap<'C>) (completedRequests : CompletedUserRequestMap<'S>) (requestId:RequestId) : PendingUserRequestMap<'C>*CompletedUserRequestMap<'S> =
    let completedRequest, newCompletedRequests = completedRequests |> getAndUnregisterRequest requestId
    match completedRequest with
    | Some _ -> pendingRequests, newCompletedRequests
    | None -> 
        let pendingRequest = pendingRequests |> Map.tryFind requestId
        match pendingRequest with
        | Some pendingRequest ->
            log pendingRequest.Id pendingRequest.Command
            pendingRequests |> Map.add pendingRequest.Id { pendingRequest with Deleted = true }, newCompletedRequests
        | None -> pendingRequests, newCompletedRequests
    
let isTimedOut timeout (completedRequest:CompletedUserRequest<'S>) =
    System.DateTime.Now - completedRequest.CompletionTime > timeout

let markAsRetrieved (completedRequests : CompletedUserRequestMap<'S>) (completedRequest:CompletedUserRequest<'S>) =
    completedRequests |> Map.add completedRequest.Id { completedRequest with RetrievedByClient = true }

let notRetrievedCompletedRequests (completedRequests : seq<CompletedUserRequest<'S>>) =
    completedRequests |> Seq.filter (fun request -> not request.RetrievedByClient)

let cleanTimedOutCompletedRequests timeout (completedRequests : CompletedUserRequestMap<'S>) =
    let timedOut, notTimedOut = completedRequests |> Map.toList |> List.map snd |> List.partition (isTimedOut timeout)
    if timedOut |> List.isEmpty then
        None
    else
        Some (notTimedOut |> List.map (fun req -> req.Id, req) |> Map.ofList, timedOut)
