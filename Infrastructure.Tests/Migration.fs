module Infrastructure.Tests.Migration

open Xunit
open Infrastructure.Migration
open Domain.Common.Validation

[<Fact>]
let ``Migration`` () =
    let x = "intcdb3" |> migrate "fr1psl013820.misys.global.ad" "dba" "dbapass"
    match x with
    | Valid _ -> ()
    | Invalid e -> failwith (sprintf "Errors of migration : %s" (System.String.Join("; ", e)))

