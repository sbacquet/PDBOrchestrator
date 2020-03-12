module Infrastructure.OracleInstanceAffinity

type OracleInstanceAffinity = string list

type PartialOracleInstanceAffinity = (string * int) list

type PartialOracleInstanceAffinities = PartialOracleInstanceAffinity list

let getAffinity (affinities:PartialOracleInstanceAffinities) (defaultAffinity:OracleInstanceAffinity) : OracleInstanceAffinity =
    let instanceAffinity instance =
        affinities
        |> List.fold (fun value aff -> if value |> Option.isSome then value else aff |> Map.ofList |> Map.tryFind instance) None
        |> Option.defaultValue 99
    defaultAffinity
    |> List.map (fun i -> (i, instanceAffinity i))
    |> List.sortBy snd
    |> List.map fst

open System.Security.Claims

let userAffinity (user:ClaimsIdentity) (defaultAffinity:OracleInstanceAffinity) =

    let partialAffinityFromAttribute (attribute:string) : PartialOracleInstanceAffinity =
        let affinityItemFromString (affinityValueString:string) =
            match affinityValueString.Split(':') with
            | [| instance; aff |] -> 
                let ok, aff = System.Int32.TryParse aff
                if ok then Some (instance, aff) else None
            | _ -> None
        attribute.Split(";")
        |> Seq.map affinityItemFromString
        |> Seq.choose id
        |> List.ofSeq

    let affinities : PartialOracleInstanceAffinities =
        user.Claims
        |> Seq.filter (fun claim -> claim.Type.StartsWith("CDB_affinity_"))
        |> Seq.map (fun claim -> (claim.Type, claim.Value))
        |> Seq.sortByDescending fst
        |> Seq.map (snd >> partialAffinityFromAttribute)
        |> List.ofSeq

    let strongestAffinity =
        user.Claims
        |> Seq.tryFind (fun claim -> claim.Type = "CDB_affinity")
        |> Option.map (fun claim -> partialAffinityFromAttribute claim.Value)
        |> Option.defaultValue []

    getAffinity (strongestAffinity :: affinities) defaultAffinity
