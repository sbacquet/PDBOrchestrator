module Infrastructure.Compensable

open Microsoft.Extensions.Logging

type Action<'T, 'E> = 'T -> Result<'T, 'E>
type AsyncAction<'T, 'E> = 'T -> Async<Result<'T, 'E>>
type Compensation<'T> = 'T -> unit
type AsyncCompensation<'T> = 'T -> Async<unit>
type CompensableAction<'T, 'E> = Action<'T, 'E> * Compensation<'T>
type CompensableAsyncAction<'T, 'E> = AsyncAction<'T, 'E> * AsyncCompensation<'T>

let compensable action compensation : CompensableAction<'T, 'E> =
    action, compensation >> ignore

let compensableAsync (action:AsyncAction<'T, 'E>) (compensation:AsyncAction<'T, 'E>) : CompensableAsyncAction<'T, 'E> =
    let comp t = async {
        let! _ = compensation t
        return ()
    }
    action, comp

let notCompensable action : CompensableAction<'T, 'E> = action, fun _ -> ()

let notCompensableAsync (action:AsyncAction<'T, 'E>) : CompensableAsyncAction<'T, 'E> = action, fun _ -> async { return () }

let compose (logger:ILogger) (actions : CompensableAction<'T, 'E> list) input =
    let f (oldResult : Result<'T,'E> * (Compensation<'T> list)) (compAction : CompensableAction<'T, 'E>) : Result<'T,'E> * (Compensation<'T> list) =
        match oldResult with
        | Ok oldValue, comps ->
            let action, comp = compAction
            let result = action oldValue
            match result with
            | Ok r -> (Ok r, comp :: comps)
            | Error e -> 
                logger.LogWarning("Error encountered, compensating")
                comps |> List.iter (fun comp -> try comp oldValue with _ -> ())
                logger.LogWarning("Compensation done")
                (Error e, [])
        | error -> error
    let result, _ = actions |> List.fold f (Ok input, [])
    result

let composeAsync (logger:ILogger) (actions : CompensableAsyncAction<'T, 'E> list) input = async {
    let folder (oldResultAsync : Async<Result<'T,'E> * (AsyncCompensation<'T> list)>) (compAction : CompensableAsyncAction<'T, 'E>) : Async<Result<'T,'E> * (AsyncCompensation<'T> list)> = async {
        let! oldResult = oldResultAsync
        match (oldResult) with
        | Ok oldValue, comps ->
            let action, comp = compAction
            let! result = action oldValue
            match result with
            | Ok r -> return (Ok r, comp :: comps)
            | Error e -> 
                logger.LogWarning("Error encountered, compensating")
                let runCompensation comp = async { try do! comp oldValue with _ -> (); return () }
                let! _ = comps |> List.map runCompensation |> Async.Sequential
                logger.LogWarning("Compensation done")
                return (Error e, [])
        | error -> return error
    }
    let! result, _ = actions |> List.fold folder (async { return (Ok input, []) })
    return result
}
