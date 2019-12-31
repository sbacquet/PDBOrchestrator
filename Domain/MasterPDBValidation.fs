module Domain.Validation.MasterPDB

open Domain.Common.Validation
open Domain.Validation.MasterPDBVersion
open Domain.MasterPDB
open Domain.MasterPDBWorkingCopy

let validateName name : Validation<string, string> =
    if System.String.IsNullOrEmpty(name) then
        Invalid [ "a row has an empty name" ]
    else
        Valid name

let validateSchemaUser pdb schemaUser =
    if System.String.IsNullOrEmpty(schemaUser) then
        Invalid [ sprintf "PDB %s has an empty main schema" pdb ]
    else
        Valid schemaUser

let validateSchemaPassword pdb schemaUser schemaPassword =
    if System.String.IsNullOrEmpty(schemaPassword) then
        Invalid [ sprintf "PDB %s has an empty password for schema %s" pdb schemaUser ]
    else
        Valid schemaPassword

let consValidSchema pdb user password t =
    retn
        consSchema <*>
            validateSchemaUser pdb user <*>
            validateSchemaPassword pdb user password <*>
            Valid t

let validateSchema pdb (schema:Schema) =
    consValidSchema pdb schema.User schema.Password schema.Type

let validateSchemas pdb (schemas:Schema list) =
    schemas |> traverse (validateSchema pdb)

let validateLock pdb (lockinfo:EditionInfo option) =
    match lockinfo with
    | Some lock ->
        if System.String.IsNullOrEmpty(lock.Editor) then
            Invalid [ sprintf "PDB %s has an empty locker" pdb ]
        else
            Valid lockinfo
    | None -> Valid lockinfo

let validateVersions pdb versions =
    versions |> traverse (validateVersion pdb)

let validateProperties pdb properties =
    if properties |> Map.forall (fun key _ -> not (System.String.IsNullOrEmpty(key))) then
        Valid properties
    else
        Invalid [ sprintf "PDB %s has an empty property key" pdb ]

let validateWorkingCopy (workingCopy:MasterPDBWorkingCopy) =
    Valid workingCopy // TODO

let validateWorkingCopies (workingCopies:MasterPDBWorkingCopy list) =
    workingCopies |> traverse validateWorkingCopy
    
let consValidMasterPDB name schemas versions lockState editionDisabled properties workingCopies =
    retn
        consMasterPDB <*> 
        validateName name <*> 
        validateSchemas name schemas <*>
        validateVersions name versions <*>
        validateLock name lockState <*>
        Valid editionDisabled <*>
        validateProperties name properties <*>
        validateWorkingCopies workingCopies
