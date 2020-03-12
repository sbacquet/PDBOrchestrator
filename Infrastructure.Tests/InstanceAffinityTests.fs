module Infrastructure.Tests.InstanceAffinityTests

open Xunit
open Infrastructure.OracleInstanceAffinity
open System.Security.Claims

[<Fact>]
let ``Compute affinity`` () =

    let (affinities:PartialOracleInstanceAffinities) = [
        [           ("B", 4)           ]
        [ ("A", 2); ("B", 3)           ]
        [ ("A", 2)          ; ("C", 1) ]
        [ ("A", 1); ("B", 2); ("C", 3); ("E", 77) ]
    ]

    let aff = [ "A"; "B"; "C"; "D" ] |> getAffinity affinities
    Assert.True([ "C"; "A"; "B"; "D" ] = aff)

[<Fact>]
let ``User affinity`` () =

    let user = 
        [
            "CDB_affinity_9", "A:2;B:3"
            "CDB_affinity",   "B:4"
            "CDB_affinity_7", "A:1;B:2;C:3;E:77"
            "CDB_affinity_8", "A:2;C:1"
        ]
        |> List.map Claim |> ClaimsIdentity

    let aff = [ "A"; "B"; "C"; "D" ] |> userAffinity user
    Assert.True([ "C"; "A"; "B"; "D" ] = aff)
