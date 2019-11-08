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
        member __.Delay(f) = f
        member __.Run(f) = f()
        member __.TryWith(body, handler) =
            try 
                __.ReturnFrom(body())
            with 
            | e -> handler e
        member __.TryFinally(body, compensation) =
            try 
                __.ReturnFrom(body())
            finally 
                compensation() 
        member __.Using(disposable:#System.IDisposable, body) =
            let body' = fun () -> body disposable
            __.TryFinally(body', fun () -> 
                match disposable with 
                    | null -> () 
                    | disp -> disp.Dispose())

    let apply f x = f |> Result.bind (fun g -> x |> Result.map (fun y -> g y))

    let rec traverse f list =
        let (<*>) = apply
        let cons head tail = head :: tail
        let folder head tail = Ok cons <*> (f head) <*> tail
        Ok [] |> List.foldBack folder list

    let sequence x = traverse id x

let result = new Result.ResultBuilder()

module Exceptional =
    type Exceptional<'R> = Result<'R, exn>

    let ofException ex : Exceptional<_> = ex |> Error

    let ofString message : Exceptional<_> = message |> exn |> ofException

module Validation =
    type Validation<'T, 'E> =
    | Valid of 'T
    | Invalid of 'E list

    let bind (f:'a -> Validation<'b,'e>) (vt:Validation<'a,'e>) : Validation<'b,'e> =
        match vt with
        | Valid t -> f t
        | Invalid errors -> Invalid errors

    let map f vt =
        match vt with
        | Valid t -> Valid (f t)
        | Invalid errors -> Invalid errors

    let mapError f vt =
        match vt with
        | Valid t -> Valid t
        | Invalid errors -> Invalid (errors |> List.map f)

    let mapErrors f vt =
        match vt with
        | Valid t -> Valid t
        | Invalid errors -> Invalid [ f errors ]

    let apply (vf: Validation<'T -> 'U, 'E>) (vt:Validation<'T, 'E>) : Validation<'U, 'E> =
        match vf, vt with
        | Valid f, Valid t -> Valid (f t)
        | Invalid errors1, Invalid errors2 -> Invalid (errors1 @ errors2)
        | Valid _, Invalid errors -> Invalid errors
        | Invalid errors, Valid _ -> Invalid errors

    let retn = Valid

    let (<*>) = apply

    let (>>=) x f = bind f x

    let rec traverse f list =
        let cons head tail = head :: tail
        let folder head tail = retn cons <*> (f head) <*> tail
        retn [] |> List.foldBack folder list

    let sequence x = traverse id x

    let ofResult result = 
        match result with
        | Ok r -> Valid r
        | Error error -> Invalid [ error ]

    let toResult validation concatErrors = 
        match validation with
        | Valid r -> Ok r
        | Invalid errors -> Error (concatErrors errors)

module Async =
    let map f xAsync = async {
        let! x = xAsync 
        return f x
    }

    // Sequential traverse/sequence
#if !Async_Sequential_bug_fixed
    let retn x = async {
        return x
    }

    let bind f xAsync = async {
        let! x = xAsync 
        return! f x
    }

    let apply (f:Async<'a->'b>) (xAsync:Async<'a>) : Async<'b> =
        f |> bind (fun g -> xAsync |> map g)

    let traverseS f list =
        let (<*>) = apply
        let cons head tail = head :: tail
        let folder head tail = retn cons <*> (f head) <*> tail
        List.foldBack folder list (retn []) 

    let sequenceS list = traverseS id list
#else
    let traverseS f list =
        list 
        |> List.map f 
        |> Async.Sequential
        |> map Array.toList 

    let sequenceS (list:List<Async<_>>)  = list |> Async.Sequential |> map Array.toList
#endif

    // Parallel traverse/sequence
    let traverseP f list =
        list 
        |> List.map f 
        |> Async.Parallel
        |> map Array.toList 

    let sequenceP (list:List<Async<_>>)  = list |> Async.Parallel |> map Array.toList

module AsyncResult =

    let map f =  f |> Result.map |> Async.map

    let mapError f =  f |> Result.mapError |> Async.map 

    let retn x = async { return Ok x }

    let bind f xAsyncResult = async {
        let! xResult = xAsyncResult 
        match xResult with
        | Ok x -> return! f x
        | Error err -> return (Error err)
    }

    // Monadic traverse
    let traverseM f list =
        let (>>=) x f = bind f x
        let cons head tail = head :: tail
        let folder head tail = 
            f head >>= (fun h -> 
            tail >>= (fun t ->
            retn (cons h t) ))
        List.foldBack folder list (retn []) 

    let sequenceM list = traverseM id list

    // Sequential traverse
    let traverseS f list =
        list |> Async.traverseS f |> Async.map Result.sequence

    let sequenceS list = 
        list |> Async.sequenceS |> Async.map Result.sequence

    // Parallel traverse
    let traverseP f list =
        list |> Async.traverseP f |> Async.map Result.sequence

    let sequenceP list = 
        list |> Async.sequenceP |> Async.map Result.sequence

    type AsyncResultBuilder() =
        member __.Return(x) = retn x
        member __.ReturnFrom(m: Async<Result<_, _>>) = m
        member __.ReturnFrom(m: Result<_, _>) = async { return m }
        member __.Bind(m:Async<Result<'a,'e>>, f:('a -> Async<Result<'b,'e>>)) : Async<Result<'b,'e>> = bind f m
        member __.Bind(m:Result<'a,'e>, f:('a -> Async<Result<'b,'e>>)) : Async<Result<'b,'e>> = bind f (async { return m })
        member __.Bind(_:unit, f:(unit -> Async<Result<'b,'e>>)) : Async<Result<'b,'e>> = bind f (async { return Ok () })
        member __.Zero() = async { return Ok () }
        member __.Delay(f) = f
        member __.Run(f) = f()
        member __.TryWith(body : unit -> Async<Result<'a, 'b>>, handler: exn -> Async<Result<'a, 'b>>) : Async<Result<'a, 'b>> =
            try 
                __.ReturnFrom(body())
            with 
            | e -> handler e
        member __.TryFinally(body : unit -> Async<Result<'a, 'b>>, compensation: unit -> unit) : Async<Result<'a, 'b>> =
            try 
                __.ReturnFrom(body())
            finally compensation() 
        member __.Using(disposable:#System.IDisposable, body:#System.IDisposable -> Async<Result<_, _>>) =
            let body' = fun () -> body disposable
            __.TryFinally(body', fun () -> 
                match disposable with 
                    | null -> () 
                    | disp -> disp.Dispose())

let asyncResult = new AsyncResult.AsyncResultBuilder()

module AsyncValidation =
    open Validation

    let map f = f |> Validation.map |> Async.map 

    let mapError f =  f |> Validation.mapError |> Async.map 

    let mapErrors f =  f |> Validation.mapErrors |> Async.map 

    let retn x = async { return Valid x }

    let bind f xAsyncValidation = async {
        let! xResult = xAsyncValidation 
        match xResult with
        | Valid x -> return! f x
        | Invalid errors -> return (Invalid errors)
    }

    let ofAsyncResult x = Async.map ofResult x

    let toAsyncResult x = Async.map toResult x

    // Monadic traverse
    let traverseM f list =
        let (>>=) x f = bind f x
        let cons head tail = head :: tail
        let folder head tail = 
            f head >>= (fun h -> 
            tail >>= (fun t ->
            retn (cons h t) ))
        List.foldBack folder list (retn []) 

    let sequenceM list = traverseM id list

    // Sequential traverse
    let traverseS f list =
        list |> Async.traverseS f |> Async.map Validation.sequence

    let sequenceS list = 
        list |> Async.sequenceS |> Async.map Validation.sequence

    // Parallel traverse
    let traverseP f list =
        list |> Async.traverseP f |> Async.map Validation.sequence

    let sequenceP list = 
        list |> Async.sequenceP |> Async.map Validation.sequence

    type AsyncValidationBuilder() =
        member __.Return(x) = retn x
        member __.ReturnFrom(m: Async<Validation<_, _>>) = m
        member __.Bind(m:Async<Validation<'a,'e>>, f:('a -> Async<Validation<'b,'e>>)) : Async<Validation<'b,'e>> = bind f m
        member __.Bind(m:Validation<'a,'e>, f:('a -> Async<Validation<'b,'e>>)) : Async<Validation<'b,'e>> = bind f (async { return m })
        member __.Bind(m:Async<Result<'a,'e>>, f:('a -> Async<Validation<'b,'e>>)) : Async<Validation<'b,'e>> = bind f (m |> ofAsyncResult)
        member __.Bind(m:Result<'a,'e>, f:('a -> Async<Validation<'b,'e>>)) : Async<Validation<'b,'e>> = bind f (async { return m |> Validation.ofResult })
        member __.Bind(_:unit, f:(unit -> Async<Validation<'b,'e>>)) : Async<Validation<'b,'e>> = bind f (async { return Validation.retn () })
        member __.Zero() = async { return Valid () }
        member __.Delay(f) = f
        member __.Run(f) = f()
        member __.TryWith(body : unit -> Async<Validation<'a, 'b>>, handler: exn -> Async<Validation<'a, 'b>>) : Async<Validation<'a, 'b>> =
            try 
                __.ReturnFrom(body())
            with 
            | e -> handler e
        member __.TryFinally(body : unit -> Async<Validation<'a, 'b>>, compensation: unit -> unit) : Async<Validation<'a, 'b>> =
            try 
                __.ReturnFrom(body())
            finally 
                compensation() 
        member __.Using(disposable:#System.IDisposable, body:#System.IDisposable -> Async<Validation<_, _>>) =
            let body' = fun () -> body disposable
            __.TryFinally(body', fun () -> 
                match disposable with 
                    | null -> () 
                    | disp -> disp.Dispose())

let asyncValidation = new AsyncValidation.AsyncValidationBuilder()

