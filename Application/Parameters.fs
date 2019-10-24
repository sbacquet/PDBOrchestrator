module Application.Parameters

type Parameters = {
    ServerInstanceName : string
    ShortTimeout : System.TimeSpan option
    LongTimeout : System.TimeSpan option
    VeryLongTimeout : System.TimeSpan option
    NumberOfOracleLongTaskExecutors : int
    NumberOfOracleDiskIntensiveTaskExecutors : int
    GarbageCollectionDelay : System.TimeSpan
}

let consParameters 
    serverInstanceName 
    shortTimeout 
    longTimeout 
    veryLongTimeout 
    numberOfOracleLongTaskExecutors 
    numberOfOracleDiskIntensiveTaskExecutors 
    garbageCollectionDelay = 
    {
        ServerInstanceName = serverInstanceName
        ShortTimeout = shortTimeout
        LongTimeout = longTimeout
        VeryLongTimeout = veryLongTimeout
        NumberOfOracleLongTaskExecutors = numberOfOracleLongTaskExecutors
        NumberOfOracleDiskIntensiveTaskExecutors = numberOfOracleDiskIntensiveTaskExecutors
        GarbageCollectionDelay = garbageCollectionDelay
    }