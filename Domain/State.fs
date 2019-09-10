module Domain.State

open Domain

type State = {
    MasterPDBs: Domain.PDB.MasterPDB list
    MasterPDBVersions: Domain.PDB.MasterPDBVersion list
    LockedMasterPDBs: Map<string, Domain.PDB.LockInfo>
}

let newState () = { MasterPDBs = []; MasterPDBVersions = []; LockedMasterPDBs = Map.empty }

// IO
type SetState = State -> unit

// IO
type GetState = unit -> State

let addMasterPDB name schemas user comment state = {
    state with 
        MasterPDBs = (PDB.newMasterPDB name schemas) :: state.MasterPDBs; 
        MasterPDBVersions = (PDB.newPDBVersion name 1 user comment) :: state.MasterPDBVersions
}
