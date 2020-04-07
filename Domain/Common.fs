module Domain.Common

open System.Threading.Tasks
open System

let (=~) s1 s2 =
     System.String.Equals(s1, s2, System.StringComparison.CurrentCultureIgnoreCase)

let ofOption error = function Some s -> Ok s | None -> Error error

let toOption = function Ok s -> Some s | Error _ -> None

let toErrorOption = function Ok _ -> None | Error error -> Some error

module Result =
    type ResultBuilder() =
        member __.Return (value: 'T) : Result<'T, 'TError> =
          Ok value

        member __.ReturnFrom (result: Result<'T, 'TError>) : Result<'T, 'TError> =
          result

        member this.Zero () : Result<unit, 'TError> =
          this.Return ()

        member __.Bind
            (result: Result<'T, 'TError>, binder: 'T -> Result<'U, 'TError>)
            : Result<'U, 'TError> =
          Result.bind binder result

        member __.Delay
            (generator: unit -> Result<'T, 'TError>)
            : unit -> Result<'T, 'TError> =
          generator

        member __.Run
            (generator: unit -> Result<'T, 'TError>)
            : Result<'T, 'TError> =
          generator ()

        member this.Combine
            (result: Result<unit, 'TError>, binder: unit -> Result<'T, 'TError>)
            : Result<'T, 'TError> =
          this.Bind(result, binder)

        member this.TryWith
            (generator: unit -> Result<'T, 'TError>,
             handler: exn -> Result<'T, 'TError>)
            : Result<'T, 'TError> =
          try this.Run generator with | e -> handler e

        member this.TryFinally
            (generator: unit -> Result<'T, 'TError>, compensation: unit -> unit)
            : Result<'T, 'TError> =
          try this.Run generator finally compensation ()

        member this.Using
            (resource: 'T when 'T :> IDisposable, binder: 'T -> Result<'U, 'TError>)
            : Result<'U, 'TError> =
          this.TryFinally (
            (fun () -> binder resource),
            (fun () -> if not <| obj.ReferenceEquals(resource, null) then resource.Dispose ())
          )

        member this.While
            (guard: unit -> bool, generator: unit -> Result<unit, 'TError>)
            : Result<unit, 'TError> =
          if not <| guard () then this.Zero ()
          else this.Bind(this.Run generator, fun () -> this.While (guard, generator))

        member this.For
            (sequence: #seq<'T>, binder: 'T -> Result<unit, 'TError>)
            : Result<unit, 'TError> =
          this.Using(sequence.GetEnumerator (), fun enum ->
            this.While(enum.MoveNext,
              this.Delay(fun () -> binder enum.Current)))

    let apply f x = f |> Result.bind (fun g -> x |> Result.map g)

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

    let toResult concatErrors validation = 
        match validation with
        | Valid r -> Ok r
        | Invalid errors -> Error (concatErrors errors)

module Async =
    let map f xAsync = async {
        let! x = xAsync 
        return f x
    }

    // Sequential traverse/sequence
    let traverseS f list =
        list 
        |> List.map f 
        |> Async.Sequential
        |> map Array.toList 

    let sequenceS (list:List<Async<_>>)  = list |> Async.Sequential |> map Array.toList

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

        member __.Return (value: 'T) : Async<Result<'T, 'TError>> =
            async.Return <| result.Return value

        member __.ReturnFrom
            (asyncResult: Async<Result<'T, 'TError>>)
            : Async<Result<'T, 'TError>> =
            asyncResult

        member __.ReturnFrom
            (taskResult: Task<Result<'T, 'TError>>)
            : Async<Result<'T, 'TError>> =
            Async.AwaitTask taskResult

        member __.ReturnFrom
            (result: Result<'T, 'TError>)
            : Async<Result<'T, 'TError>> =
            async.Return result

        member __.Zero () : Async<Result<unit, 'TError>> =
            async.Return <| result.Zero ()

        member __.Bind
            (asyncResult: Async<Result<'T, 'TError>>,
                binder: 'T -> Async<Result<'U, 'TError>>)
            : Async<Result<'U, 'TError>> =
            async {
                let! result = asyncResult
                match result with
                | Ok x -> return! binder x
                | Error x -> return Error x
            }

        member this.Bind
            (taskResult: Task<Result<'T, 'TError>>,
                binder: 'T -> Async<Result<'U, 'TError>>)
            : Async<Result<'U, 'TError>> =
            this.Bind(Async.AwaitTask taskResult, binder)

        member this.Bind
            (result: Result<'T, 'TError>, binder: 'T -> Async<Result<'U, 'TError>>)
            : Async<Result<'U, 'TError>> =
            this.Bind(this.ReturnFrom result, binder)

        member __.Bind(_:unit, f:(unit -> Async<Result<'b,'e>>)) : Async<Result<'b,'e>> = bind f (async { return Ok () })

        member __.Delay
            (generator: unit -> Async<Result<'T, 'TError>>)
            : Async<Result<'T, 'TError>> =
            async.Delay generator

        member this.Combine
            (computation1: Async<Result<unit, 'TError>>,
                computation2: Async<Result<'U, 'TError>>)
            : Async<Result<'U, 'TError>> =
            this.Bind(computation1, fun () -> computation2)

        member __.TryWith
            (computation: Async<Result<'T, 'TError>>,
                handler: System.Exception -> Async<Result<'T, 'TError>>)
            : Async<Result<'T, 'TError>> =
            async.TryWith(computation, handler)

        member __.TryFinally
            (computation: Async<Result<'T, 'TError>>,
                compensation: unit -> unit)
            : Async<Result<'T, 'TError>> =
            async.TryFinally(computation, compensation)

        member __.Using
            (resource: 'T when 'T :> IDisposable,
                binder: 'T -> Async<Result<'U, 'TError>>)
            : Async<Result<'U, 'TError>> =
            async.Using(resource, binder)

        member this.While
            (guard: unit -> bool, computation: Async<Result<unit, 'TError>>)
            : Async<Result<unit, 'TError>> =
            if not <| guard () then this.Zero ()
            else this.Bind(computation, fun () -> this.While (guard, computation))

        member this.For
            (sequence: #seq<'T>, binder: 'T -> Async<Result<unit, 'TError>>)
            : Async<Result<unit, 'TError>> =
            this.Using(sequence.GetEnumerator (), fun enum ->
            this.While(enum.MoveNext,
                this.Delay(fun () -> binder enum.Current)))

[<AutoOpen>]
module AsyncResultExtensions =
    // Having Async<_> members as extensions gives them lower priority in
    // overload resolution between Async<_> and Async<Result<_,_>>.
    type AsyncResult.AsyncResultBuilder with

        member __.ReturnFrom (async': Async<'T>) : Async<Result<'T, 'TError>> =
            async {
                let! x = async'
                return Ok x
            }

        member __.ReturnFrom (task: Task<'T>) : Async<Result<'T, 'TError>> =
            async {
                let! x = Async.AwaitTask task
                return Ok x
            }

        member __.ReturnFrom (task: Task) : Async<Result<unit, 'TError>> =
            async {
                do! Async.AwaitTask task
                return result.Zero ()
            }

        member this.Bind
            (async': Async<'T>, binder: 'T -> Async<Result<'U, 'TError>>)
            : Async<Result<'U, 'TError>> =
            let asyncResult = async {
                let! x = async'
                return Ok x
            }
            this.Bind(asyncResult, binder)


        member this.Bind
            (task: Task<'T>, binder: 'T -> Async<Result<'U, 'TError>>)
            : Async<Result<'U, 'TError>> =
            this.Bind(Async.AwaitTask task, binder)

        member this.Bind
            (task: Task, binder: unit -> Async<Result<'T, 'TError>>)
            : Async<Result<'T, 'TError>> =
            this.Bind(Async.AwaitTask task, binder)

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

    let toAsyncResult concatErrors x = Async.map (toResult concatErrors) x

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

         member __.Return (value: 'T) : Async<Validation.Validation<'T, 'TError>> =
             async.Return <| Validation.retn value

         member __.ReturnFrom
             (asyncValidation: Async<Validation.Validation<'T, 'TError>>)
             : Async<Validation.Validation<'T, 'TError>> =
             asyncValidation

         member __.ReturnFrom
             (taskValidation: Task<Validation.Validation<'T, 'TError>>)
             : Async<Validation.Validation<'T, 'TError>> =
             Async.AwaitTask taskValidation

         member __.ReturnFrom
             (validation: Validation.Validation<'T, 'TError>)
             : Async<Validation.Validation<'T, 'TError>> =
             async.Return validation

         member __.Zero () : Async<Validation.Validation<unit, 'TError>> =
             async.Return <| Validation.retn ()

         member __.Bind
             (asyncValidation: Async<Validation.Validation<'T, 'TError>>,
                 binder: 'T -> Async<Validation.Validation<'U, 'TError>>)
             : Async<Validation.Validation<'U, 'TError>> =
             async {
                 let! result = asyncValidation
                 match result with
                 | Validation.Valid x -> return! binder x
                 | Validation.Invalid x -> return Validation.Invalid x
             }

         member this.Bind
             (taskResult: Task<Validation.Validation<'T, 'TError>>,
                 binder: 'T -> Async<Validation.Validation<'U, 'TError>>)
             : Async<Validation.Validation<'U, 'TError>> =
             this.Bind(Async.AwaitTask taskResult, binder)

         member this.Bind
             (result: Validation.Validation<'T, 'TError>, binder: 'T -> Async<Validation.Validation<'U, 'TError>>)
             : Async<Validation.Validation<'U, 'TError>> =
             this.Bind(this.ReturnFrom result, binder)

         member __.Bind(_:unit, f:(unit -> Async<Validation.Validation<'b,'e>>)) : Async<Validation.Validation<'b,'e>> = bind f (async { return Validation.Valid () })

         member __.Delay
             (generator: unit -> Async<Validation.Validation<'T, 'TError>>)
             : Async<Validation.Validation<'T, 'TError>> =
             async.Delay generator

         member this.Combine
             (computation1: Async<Validation.Validation<unit, 'TError>>,
                 computation2: Async<Validation.Validation<'U, 'TError>>)
             : Async<Validation.Validation<'U, 'TError>> =
             this.Bind(computation1, fun () -> computation2)

         member __.TryWith
             (computation: Async<Validation.Validation<'T, 'TError>>,
                 handler: System.Exception -> Async<Validation.Validation<'T, 'TError>>)
             : Async<Validation.Validation<'T, 'TError>> =
             async.TryWith(computation, handler)

         member __.TryFinally
             (computation: Async<Validation.Validation<'T, 'TError>>,
                 compensation: unit -> unit)
             : Async<Validation.Validation<'T, 'TError>> =
             async.TryFinally(computation, compensation)

         member __.Using
             (resource: 'T when 'T :> System.IDisposable,
                 binder: 'T -> Async<Validation.Validation<'U, 'TError>>)
             : Async<Validation.Validation<'U, 'TError>> =
             async.Using(resource, binder)

         member this.While
             (guard: unit -> bool, computation: Async<Validation.Validation<unit, 'TError>>)
             : Async<Validation.Validation<unit, 'TError>> =
             if not <| guard () then this.Zero ()
             else this.Bind(computation, fun () -> this.While (guard, computation))

         member this.For
             (sequence: #seq<'T>, binder: 'T -> Async<Validation.Validation<unit, 'TError>>)
             : Async<Validation.Validation<unit, 'TError>> =
             this.Using(sequence.GetEnumerator (), fun enum ->
             this.While(enum.MoveNext,
                 this.Delay(fun () -> binder enum.Current)))

[<AutoOpen>]
module AsyncValidationExtensions =
 // Having Async<_> members as extensions gives them lower priority in
 // overload resolution between Async<_> and Async<Validation<_,_>>.
    type AsyncValidation.AsyncValidationBuilder with

        member __.ReturnFrom (async': Async<'T>) : Async<Validation.Validation<'T, 'TError>> =
             async {
                 let! x = async'
                 return Validation.Valid x
             }

        member __.ReturnFrom (task: Task<'T>) : Async<Validation.Validation<'T, 'TError>> =
             async {
                 let! x = Async.AwaitTask task
                 return Validation.Valid x
             }

        member __.ReturnFrom (task: Task) : Async<Validation.Validation<unit, 'TError>> =
             async {
                 do! Async.AwaitTask task
                 return Validation.retn ()
             }

        member this.Bind
             (async': Async<'T>, binder: 'T -> Async<Validation.Validation<'U, 'TError>>)
             : Async<Validation.Validation<'U, 'TError>> =
             let asyncResult = async {
                 let! x = async'
                 return Validation.Valid x
             }
             this.Bind(asyncResult, binder)


        member this.Bind
             (task: Task<'T>, binder: 'T -> Async<Validation.Validation<'U, 'TError>>)
             : Async<Validation.Validation<'U, 'TError>> =
             this.Bind(Async.AwaitTask task, binder)

        member this.Bind
             (task: Task, binder: unit -> Async<Validation.Validation<'T, 'TError>>)
             : Async<Validation.Validation<'T, 'TError>> =
             this.Bind(Async.AwaitTask task, binder)

        member __.Bind(m:Async<Result<'a,'e>>, f:('a -> Async<Validation.Validation<'b,'e>>)) : Async<Validation.Validation<'b,'e>> = 
            AsyncValidation.bind f (m |> AsyncValidation.ofAsyncResult)

        member __.Bind(m:Result<'a,'e>, f:('a -> Async<Validation.Validation<'b,'e>>)) : Async<Validation.Validation<'b,'e>> =
            AsyncValidation.bind f (async { return m |> Validation.ofResult })

let asyncValidation = new AsyncValidation.AsyncValidationBuilder()

