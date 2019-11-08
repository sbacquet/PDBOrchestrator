module Infrastructure.Tests.FileTransfer

open Xunit
open Domain.Common
open Infrastructure

[<Fact>]
let ``Upload file`` () =
    let result = result {
        use! session = 
            FileTransfer.newSession 
                (System.TimeSpan.FromMinutes(1.) |> Some)
                "fr1psl010716.misys.global.ad" 
                "oracle" "m15y5db" 
                "ssh-ed25519 256 CcNFefba5mM1EW9RGjJrbxBmyyeVGIMHOCamkpgQJa8="
        return! 
            FileTransfer.uploadFile 
                session
                @"C:\Windows\explorer.exe" 
                "/u01/app/oracle/admin/INTCDB2/dpdump/" 
    }
    result |> Result.mapError raise |> ignore
