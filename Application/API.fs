module Application.API

open Domain.PDB

type GetCurrentState = unit -> DTO.State


type AddMasterPDB = DTO.MasterPDB -> AddMasterPDBError option
and MasterPDBAdded = string
and AddMasterPDBError =
| PDBWithSameNameAlreadyExists of string
| Other of Domain.Common.Exceptional

type GetMasterPDB = string -> Result<DTO.MasterPDB, GetMasterPDBErrors>
and GetMasterPDBErrors =
| PDBDoesNotExist of string
| Other of Domain.Common.Exceptional

