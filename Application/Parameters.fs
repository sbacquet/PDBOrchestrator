module Application.Parameters

type Parameters = {
    ServerInstanceName : string
    ShortTimeout : System.TimeSpan option
    LongTimeout : System.TimeSpan option
    VeryLongTimeout : System.TimeSpan option
    NumberOfOracleShortTaskExecutors : int
    NumberOfOracleLongTaskExecutors : int
    NumberOfOracleDiskIntensiveTaskExecutors : int
    GarbageCollectionDelay : System.TimeSpan
}

let consParameters 
    serverInstanceName 
    shortTimeout 
    longTimeout 
    veryLongTimeout 
    numberOfOracleShortTaskExecutors 
    numberOfOracleLongTaskExecutors 
    numberOfOracleDiskIntensiveTaskExecutors 
    garbageCollectionDelay = 
    {
        ServerInstanceName = serverInstanceName
        ShortTimeout = shortTimeout
        LongTimeout = longTimeout
        VeryLongTimeout = veryLongTimeout
        NumberOfOracleShortTaskExecutors = numberOfOracleShortTaskExecutors
        NumberOfOracleLongTaskExecutors = numberOfOracleLongTaskExecutors
        NumberOfOracleDiskIntensiveTaskExecutors = numberOfOracleDiskIntensiveTaskExecutors
        GarbageCollectionDelay = garbageCollectionDelay
    }