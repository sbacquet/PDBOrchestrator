module Domain.OracleInstanceState

open Domain
open Domain.Common

type OracleInstanceState = {
    MasterPDBs: Domain.PDB.MasterPDB list
    MasterPDBVersions: Domain.PDB.MasterPDBVersion list
    UnusedMasterPDBVersions: Set<Domain.PDB.VersionKey>
    LockedMasterPDBs: Map<string, Domain.PDB.LockInfo>
}

let newState () = { MasterPDBs = []; MasterPDBVersions = []; LockedMasterPDBs = Map.empty; UnusedMasterPDBVersions = Set.empty }

type OracleInstanceStateError =
| MasterPDBWithSameNameAlreadyExists of Domain.PDB.MasterPDB
| MasterPDBDoesNotExist of string
| MasterPDBVersionAlreadyExists of Domain.PDB.MasterPDBVersion
| MasterPDBVersionDoesNotExist of string * int
| MasterPDBVersionMustIncrement of string * int

let buildStateErrorMessage = function
| MasterPDBWithSameNameAlreadyExists pdb -> sprintf "master PDB already exists with name %s" pdb.Name
| MasterPDBDoesNotExist pdb -> sprintf "no master PDB found with name %s" pdb
| MasterPDBVersionAlreadyExists version -> sprintf "version %d of master PDB %s already exists" version.Version version.Name 
| MasterPDBVersionDoesNotExist (pdb, version) -> sprintf "version %d of master PDB %s does not exist" version pdb
| MasterPDBVersionMustIncrement (pdb, version) -> sprintf "version %d of master PDB %s is less than or equal to highest version" version pdb

let findMasterPDB name state =
    let pdbMaybe = state.MasterPDBs |> List.tryFind (fun pdb -> pdb.Name = name)
    match pdbMaybe with
    | None -> MasterPDBDoesNotExist name |> Error
    | Some pdb -> Ok pdb

let addMasterPDBToState name schemas user comment state = 
    let exists = state |> findMasterPDB name
    match exists with
    | Ok pdb -> MasterPDBWithSameNameAlreadyExists pdb |> Error
    | Error _ -> 
        {
            state with 
                MasterPDBs = (PDB.newMasterPDB name schemas) :: state.MasterPDBs; 
                MasterPDBVersions = (PDB.newPDBVersion name 1 user comment) :: state.MasterPDBVersions
        } |> Ok

let getMasterPDBVersions name state =
    state
    |> findMasterPDB name
    |> Result.map (fun _ -> state.MasterPDBVersions |> List.filter (fun pdbVersion -> pdbVersion.Name = name))

let findMasterPDBVersion name version state =
    state
    |> findMasterPDB name
    |> Result.bind (fun _ -> 
        state.MasterPDBVersions 
        |> List.tryFind (fun pdbVersion -> pdbVersion.Name = name && pdbVersion.Version = version)
        |> ofOption (MasterPDBVersionDoesNotExist (name, version))
       )

// prerequisite : versions must not be empty
let getHighestVersion (versions : Domain.PDB.MasterPDBVersion list) =
    versions |> List.maxBy (fun version -> version.Version)

let addMasterPDBVersionToState name version createdBy comment state =
    result {
        let! masterPDBVersions = getMasterPDBVersions name state
        let highestVersion = masterPDBVersions |> getHighestVersion
        if (version <= highestVersion.Version) 
        then return! Error (MasterPDBVersionMustIncrement (name, version))
        else return { state with MasterPDBVersions = (PDB.newPDBVersion name (highestVersion.Version+1) createdBy comment) :: state.MasterPDBVersions }
    }

let isPDBVersionUnused name version state =
    state.UnusedMasterPDBVersions |> Set.contains (name, version)

let getLatestUsedVersion (unusedVersions : Set<Domain.PDB.VersionKey>) (versions : Domain.PDB.MasterPDBVersion list) =
    versions 
    |> List.filter (fun version -> not (unusedVersions |> Set.contains (version.Name, version.Version))) 
    |> getHighestVersion

let getLatestUsedMasterPDBVersion name state =
    result {
        let! versions = getMasterPDBVersions name state
        return versions |> getLatestUsedVersion state.UnusedMasterPDBVersions
    }
