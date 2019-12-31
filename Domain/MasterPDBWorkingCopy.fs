module Domain.MasterPDBWorkingCopy

type Source = 
| SpecificVersion of int
| Edition

type Lifetime =
| Temporary of System.DateTime
| Durable

type MasterPDBWorkingCopy = {
    Name: string
    CreationDate: System.DateTime
    CreatedBy: string
    Source: Source
    Lifetime: Lifetime
}

let consWorkingCopy (date:System.DateTime) lifetime createdBy source name =
    {
        Name = name
        CreationDate = date.ToUniversalTime()
        CreatedBy = createdBy
        Source = source
        Lifetime = lifetime
    }

let computeWorkingCopyeExpiry (from:System.DateTime) (delay:System.TimeSpan) = (from + delay).ToUniversalTime()

let newTempWorkingCopy expiryDelay = 
    let now = System.DateTime.Now
    consWorkingCopy now (computeWorkingCopyeExpiry now expiryDelay |> Temporary)

let newDurableWorkingCopy = consWorkingCopy System.DateTime.Now Durable
