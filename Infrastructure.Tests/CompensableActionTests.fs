module Infrastructure.Tests.CompensableActionTests

open Xunit
open Infrastructure.Compensable
open Serilog
open Microsoft.Extensions.Logging

Serilog.Log.Logger <- 
    (new LoggerConfiguration()).
        WriteTo.Trace().
        MinimumLevel.Debug().
        CreateLogger()

let loggerFactory = (new Serilog.Extensions.Logging.SerilogLoggerFactory(dispose=true) :> ILoggerFactory)
let logger = loggerFactory.CreateLogger("tests")

[<Fact>]
let ``No compensation triggered on composition`` () =
    let action1 a = Ok (a+1)
    let action2 a = Ok (a+2)
    let mutable actionCompTag = 0
    let actionComp a =
        actionCompTag <- actionCompTag + 1
        Ok a
    let comp1 : CompensableAction<int, exn> = compensable action1 actionComp
    let comp2 : CompensableAction<int, exn> = compensable action2 actionComp
    let result = compose logger [ comp1; comp2 ] 2
    result
    |> Result.map (fun a -> Assert.Equal(5, a); Assert.Equal(0, actionCompTag); a)
    |> Result.mapError raise
    |> ignore

[<Fact>]
let ``Compensation triggered on composition`` () =
    let action1 a = Ok (a+1)
    let action2 a = Ok (a+2)
    let action3 _ = Error <| exn "expected"
    let mutable actionCompTags = []
    let actionComp1 a =
        actionCompTags <- 1 :: actionCompTags
        Ok a
    let actionComp2 a =
        actionCompTags <- 2 :: actionCompTags
        Ok a
    let actionComp3 a =
        actionCompTags <- 3 :: actionCompTags
        Ok a
    let comp1 : CompensableAction<int, exn> = compensable action1 actionComp1
    let comp2 : CompensableAction<int, exn> = compensable action2 actionComp2
    let comp3 : CompensableAction<int, exn> = compensable action3 actionComp3
    let result = compose logger [ comp1; comp2; comp3 ] 2
    result
    |> Result.mapError (fun ex -> Assert.Equal("expected", ex.Message); Assert.True([1;2] = actionCompTags); ex)
    |> Result.map (fun _ -> failwith "not expected")
    |> ignore

[<Fact>]
let ``No compensation triggered on async composition`` () =
    let action1 a = async { return Ok (a+1) }
    let action2 a = async { return Ok (a+2) }
    let mutable actionCompTag = 0
    let actionComp a = async {
        actionCompTag <- actionCompTag + 1
        return Ok a
    }
    let comp1 : CompensableAsyncAction<int, exn> = compensableAsync action1 actionComp
    let comp2 : CompensableAsyncAction<int, exn> = compensableAsync action2 actionComp
    let result = composeAsync logger [ comp1; comp2 ] 2 |> Async.RunSynchronously
    result
    |> Result.map (fun a -> Assert.Equal(5, a); Assert.Equal(0, actionCompTag); a)
    |> Result.mapError raise
    |> ignore

[<Fact>]
let ``Compensation triggered on async composition`` () =
    let action1 a = Ok (a+1)
    let action2 a = Ok (a+2)
    let action3 _ = Error <| exn "expected"
    let mutable actionCompTags = []
    let actionComp1 a =
        actionCompTags <- 1 :: actionCompTags
        Ok a
    let actionComp2 a =
        actionCompTags <- 2 :: actionCompTags
        Ok a
    let actionComp3 a =
        actionCompTags <- 3 :: actionCompTags
        Ok a
    let comp1 : CompensableAction<int, exn> = compensable action1 actionComp1
    let comp2 : CompensableAction<int, exn> = compensable action2 actionComp2
    let comp3 : CompensableAction<int, exn> = compensable action3 actionComp3
    let result = compose logger [ comp1; comp2; comp3 ] 2
    result
    |> Result.mapError (fun ex -> Assert.Equal("expected", ex.Message); Assert.True([1;2] = actionCompTags); ex)
    |> Result.map (fun _ -> failwith "not expected")
    |> ignore
