module Infrastructure.Migration

open Infrastructure.Oracle
open Domain.Common
open Domain.Validation.MasterPDB
open Domain.Validation.MasterPDBVersion
open Domain
open Domain

type MasterPDBRow = {
    Name: string
    Schema: string option
    Srm_Schema: string option
    Locked: int
    LockDate: System.DateTime option
    LockUser: string option
    Updatable: int
}

type MasterPDBVersionRow = {
    Name: string
    Version: decimal
    RevisionDate: System.DateTime
    OSUser: string
    Reason: string option
    Erased: int
}

let getMasterPDBRows conn = async {
    try
        use! result = Sql.asyncExecReader conn "select Name, Schema, Srm_Schema, locked, lockdate, lockuser, updatable from master_test_databases" [] 
        return result |> Sql.map (Sql.asRecord<MasterPDBRow> "") |> List.ofSeq |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let getMasterPDBVersionRows conn = async {
    try
        use! result = Sql.asyncExecReader conn "select Name, Version, RevisionDate, OSUser, Reason, Erased from master_test_database_ver" [] 
        return result |> Sql.map (Sql.asRecord<MasterPDBVersionRow> "") |> List.ofSeq |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let rowToValidMasterPDB getVersions (row:MasterPDBRow) =
    let schemaType i = match i with | 0 -> "Invest" | 1 -> "SRM" | _ -> failwith "schema index > 1 !"
    let convertSchema i (schema:string option) =
        schema |> Option.map(fun schema -> 
            let schemaAndPass = schema.Split('/')
            MasterPDB.consSchema schemaAndPass.[0] schemaAndPass.[1] (schemaType i)
        )
    let schemas = 
        [ row.Schema; row.Srm_Schema ] 
        |> List.mapi convertSchema
        |> List.choose id
    let lock, editionDisabled = 
        if row.Locked = 0 then 
            None, false
        elif row.LockUser.IsNone || row.LockDate.IsNone then
            None, true
        else
            Some (MasterPDB.consEditionInfo row.LockUser.Value row.LockDate.Value), false
    let versions = getVersions row.Name |> Option.defaultValue []
    consValidMasterPDB row.Name schemas versions lock editionDisabled

let rowToValidMasterPDBVersion (row:MasterPDBVersionRow) =
    consValidPDBVersion row.Name (int row.Version) (row.Erased <> 0) row.OSUser row.RevisionDate (row.Reason |> Option.defaultValue "?")
    |> Validation.map (fun version -> row.Name, version)

let migrate fromServer dbaUser dbaPassword instanceName = 
    let conn = Sql.withNewConnection (openConn fromServer 1521 "orclpdb" "c##pdba" "pass" false)
    getMasterPDBVersionRows conn
    |> Async.RunSynchronously
    |> Result.mapError (fun ex -> ex.Message)
    |> Validation.ofResult
    |> Validation.bind (Validation.traverse rowToValidMasterPDBVersion)
    |> Validation.bind (fun pdbVersions ->
        let versionsPerName = 
            pdbVersions 
            |> List.groupBy fst 
            |> List.map (fun (key, values) -> (key, values |> List.map snd |> List.sortBy (fun v -> v.Number)))
            |> Map.ofList
        getMasterPDBRows conn 
        |> Async.RunSynchronously 
        |> Result.mapError (fun ex -> ex.Message)
        |> Validation.ofResult
        |> Validation.bind (Validation.traverse (rowToValidMasterPDB (fun pdb -> versionsPerName |> Map.tryFind pdb)))
       )
    |> Validation.bind (fun pdbs ->
        pdbs |> List.iter (fun pdb ->
            let repo = Infrastructure.MasterPDBRepository.NewMasterPDBRepository(instanceName, pdb) :> Application.Common.IMasterPDBRepository
            repo.Put pdb |> ignore
        )
        let instance = 
            OracleInstance.consOracleInstance 
                (pdbs |> List.map (fun pdb -> pdb.Name)) 
                instanceName 
                fromServer
                None
                dbaUser dbaPassword 
                "" "" "" "" "" // paths to edit manually in the instance JSON file
        let repo = Infrastructure.OracleInstanceRepository.NewOracleInstanceRepository(".", instance) :> Application.Common.IOracleInstanceRepository
        repo.Put instance |> ignore
        sprintf "%s imported properly from %s" instanceName fromServer |> Validation.Valid
       )
