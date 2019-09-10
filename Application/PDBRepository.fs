module Application.PDBRepository

open Domain

type Error =
| PDBDoesNotExist of string
| PDBVersionDoesNotExist of string * Domain.PDB.Version
| PDBAlreadyExists of string

let errorToMessage = function
| PDBDoesNotExist pdb -> sprintf "PDB %s does not exist" pdb
| PDBVersionDoesNotExist (pdb, version) -> sprintf "version %d of PDB %s does not exist" version pdb
| PDBAlreadyExists pdb -> sprintf "PDB %s already exists" pdb

type PDBResult<'A> = Result<'A, Error>

type CleanMasterPDBRepo = unit -> unit

type GetAllMasterPDBs = unit -> Domain.PDB.MasterPDB list

type GetMasterPDB = string -> PDBResult<Domain.PDB.MasterPDB>

type GetPDBVersion = string -> Domain.PDB.Version -> PDBResult<Domain.PDB.MasterPDBVersion>

type GetPDBVersions = string -> PDBResult<Domain.PDB.MasterPDBVersion list>

type RegisterMasterPDB = Domain.PDB.MasterPDB -> PDBResult<unit>
