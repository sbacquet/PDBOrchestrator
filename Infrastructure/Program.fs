open Infrastructure
open Application.Common
open Microsoft.Extensions.Logging
open Serilog.Extensions.Logging
open Domain.OracleInstance
open Application
open Application.OrchestratorActor
open Serilog
open Akka.Actor
open Akkling
open Domain.Common.Validation

[<EntryPoint>]
let main args =
    Serilog.Log.Logger <- LoggerConfiguration().WriteTo.Console().CreateLogger()
    let loggerFactory = new SerilogLoggerFactory(dispose=true) :> ILoggerFactory
    let akkaConfig = 
        Akkling.Configuration.parse @"
    akka { 
        loggers=[""Akka.Logger.Serilog.SerilogLogger, Akka.Logger.Serilog""] 
    }"

    let rootFolder = System.Environment.CurrentDirectory
    let orchestratorName = "orchestrator"
    let orchestratorPath = sprintf "%s\%s" rootFolder orchestratorName
    let orchestratorRepo = OrchestratorRepository.OrchestratorRepository orchestratorPath :> IOrchestratorRepository
    let oracleInstanceRepo = OracleInstanceRepository.OracleInstanceRepository orchestratorPath
    let getMasterPDBRepo (instance:OracleInstance) = 
        MasterPDBRepository.loadMasterPDBRepository (OracleInstanceRepository.instanceFolder orchestratorPath instance.Name) instance.MasterPDBs
    let getOracleAPI (instance:OracleInstance) = Oracle.OracleAPI(loggerFactory, Oracle.connAsDBAFromInstance instance, Oracle.connAsDBAInFromInstance instance)
    let orchestrator = orchestratorRepo.Get orchestratorName

    use system = Akkling.System.create "pdb-orchestrator-system" akkaConfig
    let orchestratorActor = system |> OrchestratorActor.spawn getOracleAPI oracleInstanceRepo getMasterPDBRepo orchestrator
    let ctx = API.consAPIContext system orchestratorActor loggerFactory

    System.Console.WriteLine "Ready"

    // TEST --------------------------------------------------------
    let state = API.getState ctx
    printfn "State:\n%A" state

    let res : RequestValidation = API.snapshotMasterPDBVersion ctx "me" "instance1" "test1" 1 "toto"
    match res with
    | Valid req -> 
        printfn "Request id = %s" (req.ToString())
        System.Threading.Thread.Sleep 5000
        printfn "Request for %s = %A" (req.ToString()) (API.getRequestStatus ctx req)
    | Invalid errors -> printfn "Cannot snapshot : %A" errors
    // TEST --------------------------------------------------------

    System.Console.ReadKey() |> ignore

    System.Console.WriteLine "Exiting..."
    system.Stop(untyped orchestratorActor)
    system.Terminate().Wait()
    0
