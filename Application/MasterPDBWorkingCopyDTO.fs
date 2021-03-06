﻿module Application.DTO.MasterPDBWorkingCopy

open Akkling
open Application.DTO.MasterPDBVersion
open Domain.MasterPDBWorkingCopy

type MasterPDBWorkingCopyDTO = {
    Name: string
    MasterPDBName: string
    CreationDate: System.DateTime
    CreatedBy: string
    Source: Source
    Lifetime: Lifetime
    Schemas: SchemaDTO list
    IsSnapshot: bool
}

let toWorkingCopyDTO (pdbService:string) (schemas:SchemaDTO list) (wc:MasterPDBWorkingCopy) : MasterPDBWorkingCopyDTO =
    {
        Name = wc.Name
        MasterPDBName = wc.MasterPDBName
        CreationDate = wc.CreationDate
        CreatedBy = wc.CreatedBy
        Source = wc.Source
        Lifetime = wc.Lifetime
        Schemas = schemas |> List.map (fun schema -> { schema with ConnectionString = sprintf "%s/%s@%s" schema.User schema.Password pdbService |> Some })
        IsSnapshot = wc.IsSnapshot
    }

