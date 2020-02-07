module Application.UserRights

type User = {
    Name: string
    Roles: string list
}

let consUser roles name = { Name = name; Roles = roles }

let [<Literal>]adminRole = "admin"
let [<Literal>]anonymousUserName = "anonymous"

let isAdmin (user:User) = 
    user.Roles |> List.contains adminRole

let canLockPDB (_:Domain.MasterPDB.MasterPDB) _ = true

let canUnlockPDB (lockInfo:Domain.MasterPDB.EditionInfo) user =
    isAdmin user || lockInfo.Editor = user.Name || lockInfo.Editor = anonymousUserName
