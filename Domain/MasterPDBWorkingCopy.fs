module Domain.MasterPDBWorkingCopy

type Source = 
| SpecificVersion of int
| Edition

type Lifetime =
| Temporary of System.DateTime
| Durable

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
    let now = System.DateTime.Now
    consWorkingCopy now (computeWorkingCopyExpiry now expiryDelay |> Temporary)

let newDurableWorkingCopy = consWorkingCopy System.DateTime.Now Durable
