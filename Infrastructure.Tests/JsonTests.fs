module Infrastructure.Tests.Json

open System
open Xunit
open Domain.MasterPDB
open Infrastructure
open Chiron
open Application.DTO.MasterPDB

[<Fact>]
let ``Serialize and deserialize master PDB`` () =
    let password = "toto"
    let date = DateTime.Now
    let pdb = newMasterPDB "test1" [ consSchema "toto" password "Invest" ] "me" date "comment1"
    let json = pdb |> toDTO |> MasterPDBJson.DTOtoJson
    let pdb2 = json |> MasterPDBJson.jsonToDTO
    match pdb2 with
    | JPass p -> 
        let schema = Assert.Single(p.Schemas)
        Assert.Equal(password, schema.Password)
        let version = Assert.Single(p.Versions)
        Assert.Equal(date, version.CreationDate)
    | JFail e -> failwith (e.ToString())
