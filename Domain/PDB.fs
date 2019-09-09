namespace Domain

module PDB =

    type MasterPDB = {
        Name: string
        Schemas: string list
        LatestVersion: int
        Lock: string option
    }

    let newMasterPDB name schemas = { Name = name; Schemas = schemas; LatestVersion = 1; Lock = Option.None }

    type MasterPDBs = MasterPDB list

    type Version = int

    type PDBVersion = {
        Name: string
        Version: Version
        CreatedBy: string
        Comments: string
    }