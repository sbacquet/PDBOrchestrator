module Application.UserRights

type User = {
    Name: string
    Scopes: string list
}

let normalUser name = { Name = name; Scopes = [] }

let adminUser = { Name = "admin"; Scopes = [ "admin" ] }

let isAdmin (user:User) = 
    user.Name = "admin" // TODO
    //user.Scopes |> List.contains "admin"

let canLockPDB (_:Domain.MasterPDB.MasterPDB) _ = true

let canUnlockPDB (lockInfo:Domain.MasterPDB.LockInfo) user =
    isAdmin user || lockInfo.Locker = user.Name
