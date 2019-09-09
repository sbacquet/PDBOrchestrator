module Application.PDBRepository

type Error =
| PDBDoesNotExist of string
| PDBVersionDoesNotExist of string * Domain.PDB.Version

let errorToMessage = function
| PDBDoesNotExist pdb -> sprintf "PDB %s does not exist" pdb
| PDBVersionDoesNotExist (pdb, version) -> sprintf "version %d of PDB %s does not exist" version pdb

type PDBResult<'A> = Result<'A, Error>

type GetAllMasterPDBs = unit -> Domain.PDB.MasterPDBs

type GetMasterPDB = string -> PDBResult<Domain.PDB.MasterPDB>

type GetPDBVersion = string -> Domain.PDB.Version -> PDBResult<Domain.PDB.PDBVersion>

type GetPDBVersions = string -> PDBResult<Domain.PDB.PDBVersion list>

type RegisterMasterPDB = Domain.PDB.MasterPDB -> PDBResult<unit>
