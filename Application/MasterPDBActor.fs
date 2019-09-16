module Application.MasterPDBActor

type Command =
| PrepareForModification
| Commit of string * string
| Rollback

type Event = 
| MasterPDBCreated of string * (string * string * string) list * string * string
| MasterPDBVersionCreated of string * int * string * string
