module Infrastructure.Tests.Migration

open Xunit
open Infrastructure.Migration
open Domain.Common.Validation

[<Fact>]
let ``Migration`` () =
    let x = migrate "fr1psl013820.misys.global.ad" "intcdb3"
    match x with
    | Valid rows -> Assert.True(rows.Length > 0)
    | Invalid e -> failwith (sprintf "Errors of migration : %s" (System.String.Join("; ", e)))

