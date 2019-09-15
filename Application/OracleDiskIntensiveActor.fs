module Application.OracleDiskIntensiveActor

type OracleDiskIntensiveMessage =
| InstanciateMasterPDB of string (* manifest *)
| InstanciateTestPDB of string (* manifest *)
| ExportPDB of string * string
