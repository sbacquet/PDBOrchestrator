namespace Application

open Application.OracleDiskIntensiveActor

type public OracleDiskIntensiveTaskExecutorMailbox(settings, config) =
    inherit Akka.Dispatch.UnboundedStablePriorityMailbox(settings, config)
    override __.PriorityGenerator(message) =
        match message with
        | :? Command as command ->
            match command with
            | DeletePDB _ -> 0
            | _ -> 1
        | _ -> 1

