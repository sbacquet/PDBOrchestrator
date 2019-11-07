module Infrastructure.Tests.FileTransfer

open Xunit
open Domain.Common
open Infrastructure

[<Fact>]
let ``Upload file`` () =
    let result = 
        FileTransfer.uploadFile 
            (System.TimeSpan.FromMinutes(1.) |> Some)
            @"C:\Windows\explorer.exe" 
            "fr1psl010716.misys.global.ad" 
            "/u01/app/oracle/admin/INTCDB2/dpdump/" 
            "oracle" "m15y5db" 
            "ssh-ed25519 256 CcNFefba5mM1EW9RGjJrbxBmyyeVGIMHOCamkpgQJa8="
    result |> Result.mapError raise |> ignore
