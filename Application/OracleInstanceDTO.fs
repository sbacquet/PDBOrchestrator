module Application.DTO.OracleInstance

open Domain
open Domain.Common

type MasterPDBVersion = {
    Version: int
    CreatedBy: string
    Comment: string
    Unused: bool
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
    ActiveVersion: MasterPDBVersion option
    Locked: bool
    Locker: string
    LockDate: System.DateTime
}

type State = {
    MasterPDBs: MasterPDB list
}

//let domainStateToDTO (state : Domain.OracleInstance.OracleInstanceState) : State =
//    let mapMasterPDB 
//        (versions:Domain.PDB.MasterPDBVersion list) 
//        (locks:Map<string, Domain.PDB.LockInfo>)
//        (pdb:Domain.PDB.MasterPDB) 
//        : MasterPDB =
//            let lock = Map.tryFind pdb.Name locks
//            let locked, locker, lockDate = 
//                match lock with
//                | Some lockinfo -> true, lockinfo.Locker, lockinfo.Date
//                | None -> false, "", System.DateTime.MinValue
//            let versionsDTO = 
//                versions 
//                |> List.filter (fun version -> version.Name = pdb.Name)
//                |> List.map (fun version -> {
//                    Version = version.Version
//                    Comment = version.Comment
//                    CreatedBy = version.CreatedBy
//                    Unused = Domain.OracleInstance.isPDBVersionUnused pdb.Name version.Version state
//                })
//            let versionToDTO unused (version : Domain.PDB.MasterPDBVersion) =
//                { Version = version.Version; CreatedBy = version.CreatedBy; Comment = version.Comment; Unused = unused }
//            let activeVersion = 
//                Domain.OracleInstance.getLatestUsedMasterPDBVersion pdb.Name state
//                |> toOption
//                |> Option.map (versionToDTO false)
//            { 
//                Name = pdb.Name
//                Schemas = pdb.Schemas |> List.map (fun schema ->
//                    { User = schema.User; Password = schema.Password; Type = schema.Type }
//                )
//                Versions = versionsDTO
//                ActiveVersion = activeVersion
//                Locked = locked
//                Locker = locker
//                LockDate = lockDate
//            }
//    { 
//        MasterPDBs = 
//            state.MasterPDBs 
//            |> List.map (mapMasterPDB state.MasterPDBVersions state.LockedMasterPDBs)
//    }

//let DTOtoState (state : State) : Domain.OracleInstance.OracleInstanceState =
//    {
//        MasterPDBs = 
//            state.MasterPDBs 
//            |> List.map (fun pdb -> 
//                PDB.newMasterPDB 
//                    pdb.Name 
//                    (pdb.Schemas |> List.map (fun schema -> PDB.newSchema schema.User schema.Password schema.Type)))
//        MasterPDBVersions = 
//            state.MasterPDBs 
//            |> List.collect (fun pdb -> 
//                pdb.Versions 
//                |> List.map (fun version -> PDB.newPDBVersion pdb.Name version.Version version.CreatedBy version.Comment))
//        LockedMasterPDBs = 
//            state.MasterPDBs 
//            |> List.filter (fun pdb -> pdb.Locked) 
//            |> List.map (fun pdb -> pdb.Name, PDB.newLockInfo pdb.Locker pdb.LockDate)
//            |> Map.ofList
//        UnusedMasterPDBVersions =
//            state.MasterPDBs
//            |> List.collect (fun pdb -> 
//                pdb.Versions 
//                |> List.filter (fun version -> version.Unused)
//                |> List.map (fun version -> (pdb.Name, version.Version))
//               ) 
//            |> Set.ofList
//    }
