module Application.UserRights

type User = {
    Name: string
    Roles: string list
}

let consUser roles name = { Name = name; Roles = roles }

let [<Literal>]rolePrefix = "pdb_"
let [<Literal>]userRole = "user"
let [<Literal>]adminRole = "admin"
let [<Literal>]unlockerRole = "unlocker"

let [<Literal>]anonymousUserName = "anonymous"

let hasRole role (user:User) = 
     user.Roles |> List.contains (rolePrefix + role)

let isAdmin = hasRole adminRole

let isUnlocker = hasRole unlockerRole

let canLockPDB (pdb:Domain.MasterPDB.MasterPDB) (user:User) =
    pdb.EditionRole |> Option.map (fun role -> user |> hasRole role) |> Option.defaultValue true

let canUnlockPDB (lockInfo:Domain.MasterPDB.EditionInfo) user =
    lockInfo.Editor = user.Name || lockInfo.Editor = anonymousUserName || isUnlocker user
