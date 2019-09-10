module Application.DTO

open Domain

type MasterPDBVersion = {
    Version: int
    CreatedBy: string
    Comment: string
}

type Schema = {
    User: string
    Password: string
    Type: string
}

type MasterPDB = {
    Name: string
    Schemas: Schema list
    Versions: MasterPDBVersion list
    Locked: bool
    Locker: string
    LockDate: System.DateTime
}

type State = {
    MasterPDBs: MasterPDB list
}

let stateToDTO (state : Domain.State.State) : State =
    let mapMasterPDB 
        (versions:Domain.PDB.MasterPDBVersion list) 
        (locks:Map<string, Domain.PDB.LockInfo>)
        (pdb:Domain.PDB.MasterPDB) 
        : MasterPDB =
            let lock = Map.tryFind pdb.Name locks
            let locked, locker, lockDate = 
                match lock with
                | Some lockinfo -> true, lockinfo.Locker, lockinfo.Date
                | None -> false, "", System.DateTime.MinValue
            { 
                Name = pdb.Name
                Schemas = pdb.Schemas |> List.map (fun schema ->
                    { User = schema.User; Password = schema.Password; Type = schema.Type }
                )
                Versions = 
                    versions 
                    |> List.filter (fun version -> version.Name = pdb.Name)
                    |> List.map (fun version -> {
                        Version = version.Version
                        Comment = version.Comment
                        CreatedBy = version.CreatedBy
                    })
                Locked = locked
                Locker = locker
                LockDate = lockDate
            }
    { 
        MasterPDBs = 
            state.MasterPDBs 
            |> List.map (mapMasterPDB state.MasterPDBVersions state.LockedMasterPDBs)
    }

let DTOtoState (state : State) : Domain.State.State =
    {
        MasterPDBs = 
            state.MasterPDBs 
            |> List.map (fun pdb -> 
                PDB.newMasterPDB 
                    pdb.Name 
                    (pdb.Schemas |> List.map (fun schema -> PDB.newSchema schema.User schema.Password schema.Type)))
        MasterPDBVersions = 
            state.MasterPDBs 
            |> List.collect (fun pdb -> 
                pdb.Versions 
                |> List.map (fun version -> PDB.newPDBVersion pdb.Name version.Version version.CreatedBy version.Comment))
        LockedMasterPDBs = 
            state.MasterPDBs 
            |> List.filter (fun pdb -> pdb.Locked) 
            |> List.map (fun pdb -> pdb.Name, PDB.newLockInfo pdb.Locker pdb.LockDate)
            |> Map.ofList
    }
