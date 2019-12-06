module Infrastructure.Tests.Migration

open Xunit
open Infrastructure.Migration
open Domain.Common.Validation

[<Fact>]
let ``Migration intcdb1`` () =
    let x = 
        "intcdb" 
        |> migrate 
            "fr1psl015710.misys.global.ad" 
            "sys" "syspwd8" 
            "system" "syspwd8" 
            "oraadm" "m15y5db" 
            "ssh-ed25519 256 nh/AeczEiGHb03dscipCKRR9FtVu6zQmu/zRBbN9Lnc=" 
            "ssh-ed25519 256 d8:6e:e3:4b:bd:f4:c8:f6:ee:76:29:1e:b5:f9:e8:6b"
            false
    match x with
    | Valid _ -> ()
    | Invalid e -> e |> String.concat "; " |> failwith

[<Fact>]
let ``Migration intcdb2`` () =
    let x = 
        "intcdb2" 
        |> migrate 
            "fr1psl010716.misys.global.ad" 
            "sys" "syspwd8" 
            "system" "syspwd8" 
            "oracle" "m15y5db" 
            "ssh-ed25519 256 CcNFefba5mM1EW9RGjJrbxBmyyeVGIMHOCamkpgQJa8=" 
            "ssh-ed25519 256 24:c1:4c:4d:5a:b3:78:9c:87:c2:1d:c0:9f:89:eb:88"
            true
    match x with
    | Valid _ -> ()
    | Invalid e -> e |> String.concat "; " |> failwith

[<Fact>]
let ``Migration intcdb3`` () =
    let x = 
        "intcdb3" 
        |> migrate 
            "fr1psl013820.misys.global.ad" 
            "sys" "Bright!20" 
            "system" "Shaft!20" 
            "oracle" "m15y5db" 
            "ssh-ed25519 256 OmV8VsyS4023mhMbBDh04Pn8eK/aF9SBbPmAJgK4uu0=" 
            "ssh-ed25519 256 cf:5b:2a:53:72:ff:2a:f8:c1:e3:47:83:fb:b7:ef:80"
            true
    match x with
    | Valid _ -> ()
    | Invalid e -> e |> String.concat "; " |> failwith

