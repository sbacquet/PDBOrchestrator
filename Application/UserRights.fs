module Application.UserRights

type User = {
    Name: string
    Roles: string list
}

let consUser roles name = { Name = name; Roles = roles }

let [<Literal>]rolePrefix = "pdb_"
let [<Literal>]adminRole = "admin"
let [<Literal>]unlockerRole = "unlocker"

let allRoles = [ adminRole; unlockerRole ] |> List.map ((+)rolePrefix)

let [<Literal>]anonymousUserName = "anonymous"

let hasRole role (user:User) = 
     user.Roles |> List.contains (rolePrefix + role)

let isAdmin = hasRole adminRole

let isUnlocker = hasRole unlockerRole

let canLockPDB (_:Domain.MasterPDB.MasterPDB) _ = true

let canUnlockPDB (lockInfo:Domain.MasterPDB.EditionInfo) user =
    lockInfo.Editor = user.Name || lockInfo.Editor = anonymousUserName || isUnlocker user
