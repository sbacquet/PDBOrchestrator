module Domain.MasterPDBWorkingCopy

type Source = 
| SpecificVersion of int
| Edition

let isSpecificVersion = function
| SpecificVersion _ -> true
| _ -> false

let sourceText = function
| SpecificVersion version -> sprintf "specific version %d" version
| Edition -> "edition"

type Lifetime =
| Temporary of System.DateTime
| Durable

module Lifetime =
    let isDurable = function
    | Durable -> true
    | _ -> false

    let isTemporary = function
    | Temporary _ -> true
    | _ -> false

    let text isDurable =
        match isDurable with
        | true -> "durable"
        | false -> "temporary"

    let isDurableText = isDurable >> text

type MasterPDBWorkingCopy = {
    Name: string
    MasterPDBName: string
    CreationDate: System.DateTime
    CreatedBy: string
    Source: Source
    Lifetime: Lifetime
}

let consWorkingCopy (date:System.DateTime) lifetime createdBy source (masterPDBName:string) (name:string) =
    {
        Name = name.ToUpper()
        MasterPDBName = masterPDBName.ToUpper()
        CreationDate = date.ToUniversalTime()
        CreatedBy = createdBy
        Source = source
        Lifetime = lifetime
    }

let computeWorkingCopyExpiry (from:System.DateTime) (delay:System.TimeSpan) = (from + delay).ToUniversalTime()

let newTempWorkingCopy expiryDelay = 
    let now = System.DateTime.UtcNow
    consWorkingCopy now (computeWorkingCopyExpiry now expiryDelay |> Temporary)

let newDurableWorkingCopy = consWorkingCopy System.DateTime.Now Durable

let extendWorkingCopy (temporaryWorkingCopyLifetime:System.TimeSpan) workingCopy = 
    match workingCopy.Lifetime with
    | Temporary _ -> { workingCopy with Lifetime = Temporary (System.DateTime.UtcNow + temporaryWorkingCopyLifetime) }
    | Durable -> { workingCopy with CreationDate = System.DateTime.UtcNow }

let isExpired wc =
    let now = System.DateTime.UtcNow
    match wc.Lifetime with 
    | Temporary expiry -> expiry <= now
    | _ -> false

let isDurable workingCopy = workingCopy.Lifetime |> Lifetime.isDurable

let isTemporary workingCopy = workingCopy.Lifetime |> Lifetime.isTemporary
