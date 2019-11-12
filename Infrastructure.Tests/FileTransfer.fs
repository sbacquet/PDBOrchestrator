module Infrastructure.Tests.FileTransfer

open Xunit
open Domain.Common
open Infrastructure

[<Fact>]
let ``Upload and download file`` () =
    Assert.True(System.IO.File.Exists(@"C:\Windows\explorer.exe"))
    Assert.False(System.IO.File.Exists("explorer.exe"))
    result {
        use! session = 
            FileTransfer.newSession 
                (System.TimeSpan.FromMinutes(1.) |> Some)
                "fr1psl010716.misys.global.ad" 
                "oracle" "m15y5db" 
                "ssh-ed25519 256 CcNFefba5mM1EW9RGjJrbxBmyyeVGIMHOCamkpgQJa8="
        let res = result {
            let! _ =
                FileTransfer.uploadFile 
                    session
                    @"C:\Windows\explorer.exe" 
                    "/u01/app/oracle/admin/INTCDB2/dpdump/" 
            let! _ =
                FileTransfer.downloadFile 
                    session
                    "/u01/app/oracle/admin/INTCDB2/dpdump/explorer.exe"
                    (sprintf @"%s\" (System.IO.Directory.GetCurrentDirectory()))
            Assert.True(System.IO.File.Exists("explorer.exe"))
            System.IO.File.Delete("explorer.exe")
            return Ok ""
        }
        let! _ = FileTransfer.deleteRemoteFile session "/u01/app/oracle/admin/INTCDB2/dpdump/explorer.exe"
        res |> Result.mapError raise |> ignore
        let! exists = FileTransfer.fileExists session "/u01/app/oracle/admin/INTCDB2/dpdump/explorer.exe"
        Assert.False(exists)
        return Ok ""
    } |> ignore
