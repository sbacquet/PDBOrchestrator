module Application.Parameters

type Parameters = {
    ServerInstanceName : string
    ShortTimeout : System.TimeSpan option
    LongTimeout : System.TimeSpan option
    VeryLongTimeout : System.TimeSpan option
    NumberOfOracleShortTaskExecutors : int
    NumberOfOracleLongTaskExecutors : int
    NumberOfOracleDiskIntensiveTaskExecutors : int
    TemporaryWorkingCopyLifetime : System.TimeSpan
    NumberOfWorkingCopyWorkers : int
    CompletedRequestRetrievalTimeout : System.TimeSpan
}

let consParameters
    serverInstanceName
    shortTimeout
    longTimeout
    veryLongTimeout
    numberOfOracleShortTaskExecutors
    numberOfOracleLongTaskExecutors
    numberOfOracleDiskIntensiveTaskExecutors
    temporaryWorkingCopyLifetime
    numberOfWorkingCopyWorkers
    completedRequestRetrievalTimeout = 
    {
        ServerInstanceName = serverInstanceName
        ShortTimeout = shortTimeout
        LongTimeout = longTimeout
        VeryLongTimeout = veryLongTimeout
        NumberOfOracleShortTaskExecutors = numberOfOracleShortTaskExecutors
        NumberOfOracleLongTaskExecutors = numberOfOracleLongTaskExecutors
        NumberOfOracleDiskIntensiveTaskExecutors = numberOfOracleDiskIntensiveTaskExecutors
        TemporaryWorkingCopyLifetime = temporaryWorkingCopyLifetime
        NumberOfWorkingCopyWorkers = numberOfWorkingCopyWorkers
        CompletedRequestRetrievalTimeout = completedRequestRetrievalTimeout
    }