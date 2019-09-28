module Infrastructure.Tests.Json

open System
open Xunit
open Microsoft.Extensions.Logging.Abstractions
open Domain.MasterPDB
open Infrastructure
open Chiron

[<Fact>]
let ``Serialize Master PDB`` () =
    let password = "toto"
    let pdb = newMasterPDB "test1" [ consSchema "toto" password "Invest" ] "me" DateTime.Now "comment1"
    let json = pdb |> MasterPDBJson.DTOtoJson
    let pdb2 = json |> MasterPDBJson.jsonToDTO
    match pdb2 with
    | JPass p -> 
        let schema = Assert.Single(p.Schemas)
        Assert.Equal(password, schema.Password)
    | JFail e -> failwith (e.ToString())
