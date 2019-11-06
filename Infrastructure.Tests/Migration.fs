module Infrastructure.Tests.Migration

open Xunit
open Infrastructure.Migration
open Domain.Common.Validation

[<Fact>]
let ``Migration`` () =
    let x = "intcdb" |> migrate "fr1psl015710.misys.global.ad" "dba" "dbapass" "import" "importPass"
    match x with
    | Valid _ -> ()
    | Invalid e -> failwith (sprintf "Errors of migration : %s" (System.String.Join("; ", e)))

