module Domain.Common

let ofOption error = function Some s -> Ok s | None -> Error error

let toOption = function Ok s -> Some s | Error _ -> None

let toErrorOption = function Ok _ -> None | Error error -> Some error

module Result =
    type ResultBuilder() =
        member __.Return(x) = Ok x

        member __.ReturnFrom(m: Result<_, _>) = m

        member __.Bind(m, f) = Result.bind f m
        member __.Bind((m, error): (Option<'T> * 'E), f) = m |> ofOption error |> Result.bind f

        member __.Zero() = None

        member __.Combine(m, f) = Result.bind f m

        member __.Delay(f: unit -> _) = f

        member __.Run(f) = f()

        member __.TryWith(m, h) =
            try __.ReturnFrom(m)
            with e -> h e

        member __.TryFinally(m, compensation) =
            try __.ReturnFrom(m)
            finally compensation()

        member __.Using(res:#System.IDisposable, body) =
            __.TryFinally(body res, fun () -> match res with null -> () | disp -> disp.Dispose())

        member __.While(guard, f) =
            if not (guard()) then Ok () else
            do f() |> ignore
            __.While(guard, f)

        member __.For(sequence:seq<_>, body) =
            __.Using(sequence.GetEnumerator(), fun enum -> __.While(enum.MoveNext, __.Delay(fun () -> body enum.Current)))

    let result = new ResultBuilder()

    let apply f x = f |> Result.bind (fun g -> Ok (g x))

    type Exceptional<'R> = Result<'R, exn>

module Validation =
    type Validation<'T, 'E> =
    | Valid of 'T
    | Invalid of 'E list

    let apply (vf: Validation<'T -> 'U, 'E>) (vt:Validation<'T, 'E>) : Validation<'U, 'E> =
        match vf, vt with
        | Valid f, Valid t -> Valid (f t)
        | Invalid errors1, Invalid errors2 -> Invalid (errors1 @ errors2)
        | Valid _, Invalid errors -> Invalid errors
        | Invalid errors, Valid _ -> Invalid errors

    let retn = Valid

    let (<*>) = apply

module Async =
    let map f xAsync = async {
        // get the contents of xAsync 
        let! x = xAsync 
        // apply the function and lift the result
        return f x
    }

    let retn x = async {
        // lift x to an Async
        return x
    }

    let apply fAsync xAsync = async {
        // start the two asyncs in parallel
        let! fChild = Async.StartChild fAsync
        let! xChild = Async.StartChild xAsync

        // wait for the results
        let! f = fChild
        let! x = xChild 

        // apply the function to the results
        return f x 
    }

    let bind f xAsync = async {
        // get the contents of xAsync 
        let! x = xAsync 
        // apply the function but don't lift the result
        // as f will return an Async
        return! f x
    }

module AsyncResult =
    let map f =  f |> Result.map |> Async.map 

    let retn x = Ok x |> Async.retn

    let apply fAsyncResult xAsyncResult = 
        fAsyncResult |> Async.bind (fun fResult -> 
        xAsyncResult |> Async.map (fun xResult -> 
        Result.apply fResult xResult))

    let bind f xAsyncResult = async {
        let! xResult = xAsyncResult 
        match xResult with
        | Ok x -> return! f x
        | Error err -> return (Error err)
    }

    type AsyncResultBuilder() =
        member __.Return(x) = retn x
        member __.ReturnFrom(m: Async<Result<_, _>>) = m
        member __.Bind(m, f) = bind f m
        member __.Zero() = async { return None }

        member __.Combine(m, f) = bind f m

        member __.Delay(f: unit -> _) = f

        member __.Run(f) = f()

        member __.TryWith(m, h) =
            try __.ReturnFrom(m)
            with e -> h e

        member __.TryFinally(m, compensation) =
            try __.ReturnFrom(m)
            finally compensation()

        member __.Using(res:#System.IDisposable, body) =
            __.TryFinally(body res, fun () -> match res with null -> () | disp -> disp.Dispose())

        member __.While(guard, f) =
            if not (guard()) then async { return Ok () } else
            do f() |> ignore
            __.While(guard, f)

        member __.For(sequence:seq<_>, body) =
            __.Using(sequence.GetEnumerator(), fun enum -> __.While(enum.MoveNext, __.Delay(fun () -> body enum.Current)))

let asyncresult = new AsyncResult.AsyncResultBuilder()
