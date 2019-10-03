module Domain.Tests.MasterPDB

open Xunit
open Domain.MasterPDB
open Domain.MasterPDBVersion

[<Fact>]
let ``Next version available`` () =
    let pdb = newMasterPDB "test1" [ { User = "invest"; Password = ""; Type = "Invest" } ] "me" (System.DateTime.Now) "comment version 1"
    let nextVersion = pdb |> getNextAvailableVersion
    Assert.Equal(2, nextVersion)

[<Fact>]
let ``Next version available with deleted`` () =
    let versions = [
        newPDBVersion "me" System.DateTime.Now "version 1"
        consPDBVersion 2 true "me" System.DateTime.Now "version 2"
        consPDBVersion 3 true "me" System.DateTime.Now "version 3"
    ]
    let pdb = consMasterPDB "test1" (masterPDBManifest "test1" 1) [ { User = "invest"; Password = ""; Type = "Invest" } ] versions None
    let nextVersion = pdb |> getNextAvailableVersion
    Assert.Equal(4, nextVersion)

[<Fact>]
let ``Previous version with deleted`` () =
    let versions = [
        newPDBVersion "me" System.DateTime.Now "version 1"
        consPDBVersion 2 false "me" System.DateTime.Now "version 2"
        consPDBVersion 3 true "me" System.DateTime.Now "version 3"
        consPDBVersion 4 true "me" System.DateTime.Now "version 4"
    ]
    let pdb = consMasterPDB "test1" (masterPDBManifest "test1" 1) [ { User = "invest"; Password = ""; Type = "Invest" } ] versions None
    let version = pdb |> getLatestAvailableVersion
    Assert.Equal(2, version.Number)

[<Fact>]
let ``Delete existing version`` () =
    let versions = [
        newPDBVersion "me" System.DateTime.Now "version 1"
        consPDBVersion 2 false "me" System.DateTime.Now "version 2"
        consPDBVersion 3 false "me" System.DateTime.Now "version 3"
        consPDBVersion 4 false "me" System.DateTime.Now "version 4"
    ]
    let pdb = consMasterPDB "test1" (masterPDBManifest "test1" 1) [ { User = "invest"; Password = ""; Type = "Invest" } ] versions None
    let version = pdb |> getLatestAvailableVersion
    Assert.Equal(4, version.Number)
    let result = pdb |> deleteVersion 4
    match result with
    | Ok pdb -> 
        Assert.True(pdb.Versions.[4].Deleted)
        let version = pdb |> getLatestAvailableVersion
        Assert.Equal(3, version.Number)
        let nextVersion = pdb |> getNextAvailableVersion
        Assert.Equal(5, nextVersion)
    | Error e -> failwith e

[<Fact>]
let ``Cannot delete non existing version`` () =
    let versions = [
        newPDBVersion "me" System.DateTime.Now "version 1"
        consPDBVersion 2 false "me" System.DateTime.Now "version 2"
        consPDBVersion 3 false "me" System.DateTime.Now "version 3"
        consPDBVersion 4 false "me" System.DateTime.Now "version 4"
    ]
    let pdb = consMasterPDB "test1" (masterPDBManifest "test1" 1) [ { User = "invest"; Password = ""; Type = "Invest" } ] versions None
    let result = pdb |> deleteVersion 5
    match result with
    | Ok _ -> failwith "version 5 does not exist!"
    | Error e -> ()

[<Fact>]
let ``Cannot delete deleted version`` () =
    let versions = [
        newPDBVersion "me" System.DateTime.Now "version 1"
        consPDBVersion 2 false "me" System.DateTime.Now "version 2"
        consPDBVersion 3 true "me" System.DateTime.Now "version 3"
        consPDBVersion 4 false "me" System.DateTime.Now "version 4"
    ]
    let pdb = consMasterPDB "test1" (masterPDBManifest "test1" 1) [ { User = "invest"; Password = ""; Type = "Invest" } ] versions None
    let result = pdb |> deleteVersion 3
    match result with
    | Ok _ -> failwith "version 3 already deleted!"
    | Error e -> ()

[<Fact>]
let ``Cannot delete version 1`` () =
    let pdb = newMasterPDB "test1" [ { User = "invest"; Password = ""; Type = "Invest" } ] "me" (System.DateTime.Now) "comment version 1"
    let result = pdb |> deleteVersion 1
    match result with
    | Ok _ -> failwith "version 1 cannot be deleted!"
    | Error e -> ()

