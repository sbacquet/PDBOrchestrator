module Compensable

type Action<'T, 'E> = 'T -> Result<'T, 'E>
type Compensation<'T> = 'T -> unit
type CompensableAction<'T, 'E> = Action<'T, 'E> * Compensation<'T>

let compensable action compensation : CompensableAction<'T, 'E> =
    action, (compensation >> ignore)

let notCompensable action : CompensableAction<'T, 'E> = action, fun _ -> ()

let compose (actions : CompensableAction<'T, 'E> list) = fun input ->
    let f (oldResult : Result<'T,'E> * (Compensation<'T> list)) (compAction : CompensableAction<'T, 'E>) : Result<'T,'E> * (Compensation<'T> list) =
        match oldResult with
        | Ok oldValue, comps ->
            let action, comp = compAction
            let result = action oldValue
            match result with
            | Ok r -> (Ok r, comp :: comps)
            | Error e -> 
                printfn "Error encountered, compensating"
                comps |> List.iter (fun comp -> try comp oldValue with _ -> ())
                printfn "Compensation done"
                (Error e, [])
        | error -> error
    let result, _ = actions |> List.fold f (Ok input, [])
    result
