module Infrastructure.Configuration

open Microsoft.Extensions.Configuration
open Domain.Common.Validation

let validateServerInstanceName (config:IConfigurationRoot) =
    let configEntry = "serverInstanceName"
    let name = config.GetValue(configEntry, "A")
    if (System.String.IsNullOrWhiteSpace(name))
    then Invalid [ sprintf "config entry %s must not be empty" configEntry ]
    else Valid name

let validateShortTimeout (config:IConfigurationRoot) =
    let configEntry = "shortTimeoutInSeconds"
    try
        let timeout = config.GetValue(configEntry, 5)
        if (timeout > 0)
        then System.TimeSpan.FromSeconds((float)timeout) |> Some |> Valid
        else 
#if DEBUG
            if timeout = -1 then Valid None
            else
#endif
            Invalid [ sprintf "config entry %s must be > 0" configEntry ]
    with _ -> Invalid [ sprintf "config entry %s is not a valid integer" configEntry ] 

let validateLongTimeout (config:IConfigurationRoot) =
    let configEntry = "longTimeoutInMinutes"
    try
        let timeout = config.GetValue(configEntry, 2)
        if (timeout > 0)
        then System.TimeSpan.FromMinutes((float)timeout) |> Some |> Valid
        else 
#if DEBUG
            if timeout = -1 then Valid None
            else
#endif
            Invalid [ sprintf "config entry %s must be > 0" configEntry ]
    with _ -> Invalid [ sprintf "config entry %s is not a valid integer" configEntry ] 

let validateVeryLongTimeout (config:IConfigurationRoot) =
    let configEntry = "veryLongTimeoutInMinutes"
    try
        let timeout = config.GetValue(configEntry, 20)
        if (timeout > 0)
        then System.TimeSpan.FromMinutes((float)timeout) |> Some |> Valid
        else 
#if DEBUG
            if timeout = -1 then Valid None
            else
#endif
            Invalid [ sprintf "config entry %s must be > 0" configEntry ]
    with _ -> Invalid [ sprintf "config entry %s is not a valid integer" configEntry ] 

let validateNumberOfOracleLongTaskExecutors (config:IConfigurationRoot) =
    let configEntry = "numberOfOracleLongTaskExecutors"
    try
        let number = config.GetValue(configEntry, 3)
        if (number > 0)
        then number |> Valid
        else Invalid [ sprintf "config entry %s must be > 0" configEntry ]
    with _ -> Invalid [ sprintf "config entry %s is not a valid integer" configEntry ] 

let validateNumberOfOracleDiskIntensiveTaskExecutors (config:IConfigurationRoot) =
    let configEntry = "numberOfOracleDiskIntensiveTaskExecutors"
    try
        let number = config.GetValue(configEntry, 1)
        if (number > 0)
        then number |> Valid
        else Invalid [ sprintf "config entry %s must be > 0" configEntry ]
    with _ -> Invalid [ sprintf "config entry %s is not a valid integer" configEntry ] 

let validateGarbageCollectionDelay (config:IConfigurationRoot) =
    let configEntry = "garbageCollectionDelayInHours"
    try
        let delayInHours = config.GetValue(configEntry, 12.)
        if (delayInHours >= 0.)
        then System.TimeSpan.FromHours(delayInHours) |> Valid
        else Invalid [ sprintf "config entry %s must be >= 0" configEntry ]
    with _ -> Invalid [ sprintf "config entry %s is not a valid floating point number" configEntry ] 

let configToGlobalParameters (config:IConfigurationRoot) = 
    let serverInstanceName = validateServerInstanceName config
    let shortTimeout = validateShortTimeout config
    let longTimeout = validateLongTimeout config
    let veryLongTimeout = validateVeryLongTimeout config
    let numberOfOracleLongTaskExecutors = validateNumberOfOracleLongTaskExecutors config
    let numberOfOracleDiskIntensiveTaskExecutors = validateNumberOfOracleDiskIntensiveTaskExecutors config
    let garbageCollectionDelay = validateGarbageCollectionDelay config
    retn Application.GlobalParameters.consGlobalParameters 
        <*> serverInstanceName
        <*> shortTimeout
        <*> longTimeout
        <*> veryLongTimeout
        <*> numberOfOracleLongTaskExecutors
        <*> numberOfOracleDiskIntensiveTaskExecutors
        <*> garbageCollectionDelay
