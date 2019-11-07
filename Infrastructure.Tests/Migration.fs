module Infrastructure.Tests.Migration

open Xunit
open Infrastructure.Migration
open Domain.Common.Validation

[<Fact>]
let ``Migration intcdb1`` () =
    let x = "intcdb" |> migrate "fr1psl015710.misys.global.ad" "sys" "syspwd8" "system" "syspwd8" "oraadm" "m15y5db" "ssh-ed25519 256 nh/AeczEiGHb03dscipCKRR9FtVu6zQmu/zRBbN9Lnc=" false
    match x with
    | Valid _ -> ()
    | Invalid e -> e |> String.concat "; " |> failwith

[<Fact>]
let ``Migration intcdb2`` () =
    let x = "intcdb2" |> migrate "fr1psl010716.misys.global.ad" "sys" "syspwd8" "system" "syspwd8" "oracle" "m15y5db" "ssh-ed25519 256 CcNFefba5mM1EW9RGjJrbxBmyyeVGIMHOCamkpgQJa8=" true
    match x with
    | Valid _ -> ()
    | Invalid e -> e |> String.concat "; " |> failwith

