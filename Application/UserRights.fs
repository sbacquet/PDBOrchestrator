module Application.UserRights

[<CustomEquality; NoComparison>]
type User =
    {
        Name: string
        Roles: string list
        getOracleInstanceAffinity: string list -> string list
    }
    override x.Equals(yobj) =
        match yobj with
        | :? User as u -> u.Name = x.Name && u.Roles = x.Roles
        | _ -> false

    override __.GetHashCode() = 0

let consUser getOracleInstanceAffinity roles name = { Name = name; Roles = roles; getOracleInstanceAffinity = getOracleInstanceAffinity }
let consUserWithDefaults = consUser id

let [<Literal>]rolePrefix = "pdb_"
let [<Literal>]userRole = "user"
let [<Literal>]adminRole = "admin"
let [<Literal>]unlockerRole = "unlocker"
let [<Literal>]forceInstance= "force_instance"

let [<Literal>]anonymousUserName = "anonymous"

let hasRole role (user:User) = 
     user.Roles |> List.contains (rolePrefix + role)

let isAdmin = hasRole adminRole

let isUnlocker = hasRole unlockerRole

let canLockPDB (pdb:Domain.MasterPDB.MasterPDB) (user:User) =
    pdb.EditionRole |> Option.map (fun role -> user |> hasRole role) |> Option.defaultValue true

let canUnlockPDB (lockInfo:Domain.MasterPDB.EditionInfo) user =
    lockInfo.Editor = user.Name || lockInfo.Editor = anonymousUserName || isUnlocker user
