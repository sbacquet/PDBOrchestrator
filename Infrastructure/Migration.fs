module Infrastructure.Migration

open Infrastructure.Oracle
open Domain.Common
open Domain.Validation.MasterPDB
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

let getMasterPDBs conn = async {
    try
        use! result = Sql.asyncExecReader conn "select Name, Schema, Srm_Schema, locked, lockdate, lockuser, updatable from master_test_databases" [] 
        return result |> Sql.map (Sql.asRecord<MasterPDBRow> "") |> List.ofSeq |> Ok
    with :? Oracle.ManagedDataAccess.Client.OracleException as ex -> 
        return Error ex
}

let rowToMasterPDB (row:MasterPDBRow) =
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
    let lock = 
        if row.Locked = 0 then 
            None 
        elif row.LockUser.IsNone || row.LockDate.IsNone then
            None
        else
            Some (MasterPDB.consLockInfo row.LockUser.Value row.LockDate.Value)
    consValidMasterPDB row.Name schemas [] lock

let migrate fromServer instance = 
    let conn = Sql.withNewConnection (openConn fromServer 1521 "orclpdb" "c##pdba" "pass" false)
    let rows = 
        getMasterPDBs conn 
        |> Async.RunSynchronously 
        |> Result.mapError (fun ex -> ex.Message)
        |> Validation.ofResult
    rows |> Validation.bind (fun rows -> rows |> List.map rowToMasterPDB |> Validation.sequence)
