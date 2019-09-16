module Application.OracleDiskIntensiveActor

type Command =
| ImportPDB of string * string * string

type Event =
| PDBImported of string
