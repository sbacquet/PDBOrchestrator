module Domain.Tests.Common

open Xunit
open Domain.Common
open Domain.Common.Validation

[<Fact>]
let ``Async traverseS is sequential`` () =
    let mutable steps : int list = []
    let add1 x = async { 
        let step = x*10
        steps <- step :: steps
        do! Async.Sleep(100)
        steps <- step+1 :: steps
        return x+1 
    }
    let result = [ 1; 2; 3 ] |> Async.traverseS add1 |> Async.RunSynchronously
    Assert.True( [ 2; 3; 4 ] = result)
    steps <- steps |> List.rev
    Assert.True( [ 10; 11; 20; 21; 30; 31 ] = steps)

[<Fact>]
let ``AsyncResult traverseS is sequential`` () =
    let mutable steps : int list = []
    let add1 x = asyncResult {
        do! () // mandatory
        steps <- (x*10) :: steps
        do! Async.Sleep(100) |> Async.map Ok
        steps <- (x*10)+1 :: steps
        return x+1
    }
    let result = [ 1; 2; 3 ] |> AsyncResult.traverseS add1 |> Async.RunSynchronously
    match result with
    | Ok result ->
        Assert.True( [ 2; 3; 4 ] = result)
        steps <- steps |> List.rev
        Assert.True( [ 10; 11; 20; 21; 30; 31 ] = steps)
    | Error error -> raise error

[<Fact>]
let ``AsyncValidation traverseS is sequential`` () =
    let mutable steps : int list = []
    let add1 (x:int) = asyncValidation {
        do! () // mandatory
        steps <- (x*10) :: steps
        do! Async.Sleep(100) |> Async.map Valid
        steps <- (x*10)+1 :: steps
        return x+1
    }
    let result = [ 1; 2; 3 ] |> AsyncValidation.traverseS add1 |> Async.RunSynchronously
    match result with
    | Valid result ->
        Assert.True( [ 2; 3; 4 ] = result)
        steps <- steps |> List.rev
        Assert.True( [ 10; 11; 20; 21; 30; 31 ] = steps)
    | Invalid errors -> failwith (System.String.Join("; ", errors))

[<Fact>]
let ``AsyncValidation sequenceM is monadic`` () =
    let tasks = [
        async { return Invalid [ "1" ] }
        async { return Invalid [ "2" ] }
    ]
    let result = tasks |> AsyncValidation.sequenceM |> Async.RunSynchronously
    match result with
    | Invalid errors -> Assert.True([ "1" ] = errors)
    | Valid _ -> failwith "valid!?"

[<Fact>]
let ``AsyncValidation sequenceS is not monadic`` () =
    let tasks = [
        async { return Invalid [ "1" ] }
        async { return Valid "2" }
        async { return Invalid [ "3" ] }
    ]
    let result = tasks |> AsyncValidation.sequenceS |> Async.RunSynchronously
    match result with
    | Invalid errors -> Assert.True([ "1"; "3" ] = errors)
    | Valid _ -> failwith "valid!?"

[<Fact>]
let ``AsyncValidation sequenceP is not monadic`` () =
    let tasks = [
        async { return Invalid [ "1" ] }
        async { return Invalid [ "2" ] }
    ]
    let result = tasks |> AsyncValidation.sequenceP |> Async.RunSynchronously
    match result with
    | Invalid errors -> Assert.True([ "1"; "2" ] = errors)
    | Valid _ -> failwith "valid!?"
