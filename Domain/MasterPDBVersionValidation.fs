module Domain.Validation.MasterPDBVersion

open Domain.Common.Validation
open Domain.MasterPDBVersion

let validateNumber pdb number =
    if number <= 0 then
        Invalid [ sprintf "PDB %s must have a positive version number" pdb ]
    else
        Valid number
 
let validateCreator pdb version creator =
    if System.String.IsNullOrEmpty(creator) then
        Invalid [ sprintf "PDB %s version %d has an empty creator" pdb version ]
    else
        Valid creator

let validateCreationDate pdb version date =
    Valid date

let validateComment pdb version comment =
    if System.String.IsNullOrEmpty(comment) then
        Invalid [ sprintf "PDB %s version %d has an empty comment" pdb version ]
    else
        Valid comment

let validateProperties pdb version properties =
    if properties |> Map.forall (fun key _ -> not (System.String.IsNullOrEmpty(key))) then
        Valid properties
    else
        Invalid [ sprintf "PDB %s version %d has an empty property key" pdb version ]

let consValidPDBVersion pdb version deleted createdBy creationDate comment properties =
    retn 
        consPDBVersion <*>
            validateNumber pdb version <*>
            Valid deleted <*>
            validateCreator pdb version createdBy <*>
            validateCreationDate pdb version creationDate <*>
            validateComment pdb version comment <*>
            validateProperties pdb version properties

let validateVersion pdb (version:MasterPDBVersion) =
    consValidPDBVersion pdb version.VersionNumber version.Deleted version.CreatedBy version.CreationDate version.Comment version.Properties
