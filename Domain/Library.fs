namespace Domain

module PDBDatabase =
    type MasterPDB = {
        Name: string
        Schemas: string list
        LatestVersion: int
    }