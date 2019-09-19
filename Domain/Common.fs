﻿module Domain.Common

let ofOption error = function Some s -> Ok s | None -> Error error

let toOption = function Ok s -> Some s | Error _ -> None

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
