module Application.UserRights

type User = {
    Name: string
    Roles: string list
}

let normalUser name = { Name = name; Roles = [] }

let adminUser = { Name = "admin"; Roles = [ "admin" ] }

let isAdmin (user:User) = 
    user.Name = "admin" // TODO
    //user.Roles |> List.contains "admin"

let canLockPDB (_:Domain.MasterPDB.MasterPDB) _ = true

let canUnlockPDB (lockInfo:Domain.MasterPDB.EditionInfo) user =
    isAdmin user || lockInfo.Editor = user.Name
