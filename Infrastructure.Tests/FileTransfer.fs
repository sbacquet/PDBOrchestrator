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

let instance1 = 
    Domain.OracleInstance.newOracleInstance
        "instance1" 
        "fr1psl015710.misys.global.ad" None
        "sys" "syspwd8" 
        "system" "syspwd8" 
        "oraadm" "m15y5db" 
        "ssh-ed25519 256 nh/AeczEiGHb03dscipCKRR9FtVu6zQmu/zRBbN9Lnc=" 
        "ssh-ed25519 256 d8:6e:e3:4b:bd:f4:c8:f6:ee:76:29:1e:b5:f9:e8:6b"
        "/u01/app/oraadm/oradata/MasterDBs" "/u01/app/oraadm/oradata/MasterDBs" "/raid0/oracle/oradata/WorkingCopies" ""
        "DP_DIR" "/u01/app/intcdb_dumps"
        false
let instance2 = 
    Domain.OracleInstance.newOracleInstance
        "instance2" 
        "fr1psl010716.misys.global.ad" None
        "sys" "syspwd8" 
        "system" "syspwd8" 
        "oracle" "m15y5db" 
        "ssh-ed25519 256 CcNFefba5mM1EW9RGjJrbxBmyyeVGIMHOCamkpgQJa8=" 
        "ssh-ed25519 256 24:c1:4c:4d:5a:b3:78:9c:87:c2:1d:c0:9f:89:eb:88"
        "/u01/app/oracle/oradata/MasterDBs" "/u01/app/oracle/oradata/MasterDBs" "/raid0/oracle/oracle/WorkingCopies" ""
        "DP_DIR" "/u01/app/intcdb_dumps"
        true

[<Fact>]
let ``Copy file between instances`` () =
    let res = 
        FileTransfer.copyFileBetweenInstances
            (System.TimeSpan.FromMinutes(1.) |> Some)
            instance1 "/u01/app/oraadm/oradata/MasterDBs/GOLDEN_V001.XML"
            instance2 "/u01/app/oracle/oradata/tmp/"
        |> Async.RunSynchronously
    res |> Result.mapError raise |> ignore

[<Fact>]
let ``Copy files between instances`` () =
    let fromFiles = [ 73..108 ] |> List.map (sprintf "/u01/app/oraadm/oradata/MasterDBs/GOLDEN_V%03d.XML")
    let dest = [ 73..108 ] |> List.map (sprintf "/u01/app/oracle/oradata/tmp/GOLDEN_V%03d_.XML")
    let res = 
        FileTransfer.copyFilesBetweenInstances
            (System.TimeSpan.FromMinutes(1.) |> Some)
            instance1 fromFiles
            instance2 dest
        |> Async.RunSynchronously
    res |> Result.mapError raise |> ignore
