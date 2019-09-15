module Application.MasterPDBActor

type MasterPDBMessage =
| PrepareForModification
| Commit of string * string
| Rollback
