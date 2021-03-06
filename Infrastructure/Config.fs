﻿module Infrastructure.Configuration

open Microsoft.Extensions.Configuration
open Domain.Common.Validation

// Application parameters

let validateServerInstanceName (config:IConfigurationRoot) =
    let configEntry = "UniqueName"
    let name = config.GetValue configEntry
    if (System.String.IsNullOrWhiteSpace(name))
    then Invalid [ sprintf "config entry %s must not be empty, and must be unique for every server instance" configEntry ]
    else Valid name

let validateShortTimeout (config:IConfigurationRoot) =
    let configEntry = "ShortTimeoutInSeconds"
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
    let configEntry = "LongTimeoutInMinutes"
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
    let configEntry = "VeryLongTimeoutInMinutes"
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

let validateNumberOfOracleShortTaskExecutors (config:IConfigurationRoot) =
    let configEntry = "NumberOfOracleShortTaskExecutors"
    try
        let number = config.GetValue(configEntry, 10)
        if (number > 0)
        then number |> Valid
        else Invalid [ sprintf "config entry %s must be > 0" configEntry ]
    with _ -> Invalid [ sprintf "config entry %s is not a valid integer" configEntry ] 

let validateNumberOfOracleLongTaskExecutors (config:IConfigurationRoot) =
    let configEntry = "NumberOfOracleLongTaskExecutors"
    try
        let number = config.GetValue(configEntry, 3)
        if (number > 0)
        then number |> Valid
        else Invalid [ sprintf "config entry %s must be > 0" configEntry ]
    with _ -> Invalid [ sprintf "config entry %s is not a valid integer" configEntry ] 

let validateNumberOfOracleDiskIntensiveTaskExecutors (config:IConfigurationRoot) =
    let configEntry = "NumberOfOracleDiskIntensiveTaskExecutors"
    try
        let number = config.GetValue(configEntry, 1)
        if (number > 0)
        then number |> Valid
        else Invalid [ sprintf "config entry %s must be > 0" configEntry ]
    with _ -> Invalid [ sprintf "config entry %s is not a valid integer" configEntry ] 

let validateNumberOfWorkingCopyWorkers (config:IConfigurationRoot) =
    let configEntry = "NumberOfWorkingCopyWorkers"
    try
        let number = config.GetValue(configEntry, 10)
        if (number > 0)
        then number |> Valid
        else Invalid [ sprintf "config entry %s must be > 0" configEntry ]
    with _ -> Invalid [ sprintf "config entry %s is not a valid integer" configEntry ] 

let validateTemporaryWorkingCopyLifetime (config:IConfigurationRoot) =
    let configEntry = "TemporaryWorkingCopyLifetimeInHours"
    try
        let delayInHours = config.GetValue(configEntry, 12.)
        if (delayInHours >= 0.)
        then System.TimeSpan.FromHours(delayInHours) |> Valid
        else Invalid [ sprintf "config entry %s must be >= 0" configEntry ]
    with _ -> Invalid [ sprintf "config entry %s is not a valid floating point number" configEntry ] 

let validateCompletedRequestRetrievalTimeout (config:IConfigurationRoot) =
    let configEntry = "CompletedRequestRetrievalTimeoutInMinutes"
    try
        let delayInMinutes = config.GetValue(configEntry, 5.)
        if (delayInMinutes >= 0.)
        then System.TimeSpan.FromMinutes(delayInMinutes) |> Valid
        else Invalid [ sprintf "config entry %s must be >= 0" configEntry ]
    with _ -> Invalid [ sprintf "config entry %s is not a valid floating point number" configEntry ] 

let configToApplicationParameters (config:IConfigurationRoot) = 
    let serverInstanceName = validateServerInstanceName config
    let shortTimeout = validateShortTimeout config
    let longTimeout = validateLongTimeout config
    let veryLongTimeout = validateVeryLongTimeout config
    let numberOfOracleShortTaskExecutors = validateNumberOfOracleShortTaskExecutors config
    let numberOfOracleLongTaskExecutors = validateNumberOfOracleLongTaskExecutors config
    let numberOfOracleDiskIntensiveTaskExecutors = validateNumberOfOracleDiskIntensiveTaskExecutors config
    let temporaryWorkingCopyLifetime = validateTemporaryWorkingCopyLifetime config
    let numberOfWorkingCopyWorkers = validateNumberOfWorkingCopyWorkers config
    let completedRequestRetrievalTimeout = validateCompletedRequestRetrievalTimeout config
    retn Application.Parameters.consParameters 
        <*> serverInstanceName
        <*> shortTimeout
        <*> longTimeout
        <*> veryLongTimeout
        <*> numberOfOracleShortTaskExecutors
        <*> numberOfOracleLongTaskExecutors
        <*> numberOfOracleDiskIntensiveTaskExecutors
        <*> temporaryWorkingCopyLifetime
        <*> numberOfWorkingCopyWorkers
        <*> completedRequestRetrievalTimeout

// Infrastructure parameters

let validateRoot (config:IConfigurationRoot) = 
    let configEntry = "Root"
    try
        let root = config.GetValue configEntry
        if (System.String.IsNullOrEmpty(root))
        then Invalid [ sprintf "config entry %s must be a non empty string" configEntry ]
        elif (not (System.IO.Directory.Exists(root)))
        then Invalid [ sprintf "config entry %s must be a valid path" configEntry ]
        else Valid (System.IO.Path.GetFullPath root)
    with _ -> Invalid [ sprintf "config entry %s is not a valid string" configEntry ] 

let validatePort (config:IConfigurationRoot) = 
    let configEntry = "Port"
    try
        let port = config.GetValue(configEntry, 61200)
        if (port > 0 && port <= 65535)
        then Valid port
        else Invalid [ sprintf "config entry %s must be > 0 and <= 65535" configEntry ]
    with _ -> Invalid [ sprintf "config entry %s is not a valid integer" configEntry ] 

let validateDNSName (config:IConfigurationRoot) = 
    let configEntry = "DNSName"
    try
        let dnsName = config.GetValue configEntry
        let dnsName, builtFromEnv =
            if (System.String.IsNullOrEmpty(dnsName)) then
                sprintf "%s.%s" (System.Environment.GetEnvironmentVariable("COMPUTERNAME")) (System.Environment.GetEnvironmentVariable("USERDNSDOMAIN")),
                true
            else
                dnsName, false
        if (System.Uri.CheckHostName(dnsName)) = System.UriHostNameType.Unknown
        then
            if builtFromEnv then
                Invalid [ sprintf "DNS name = \"%s\" built from environment is not a valid host name, please specify a valid DNS name in config (%s entry)" dnsName configEntry ]
            else 
                Invalid [ sprintf "config entry %s must be a valid DNS name" configEntry ]
        else 
            Valid dnsName
    with _ -> 
        Invalid [ sprintf "config entry %s is not a valid string" configEntry ] 

let validateOpenIdConnectUrl (config:IConfigurationRoot) =
    let configEntry = "OpenIdConnectUrl"
    try
        let url = config.GetValue(configEntry, "")
        if url = "" then
            Invalid [ sprintf "config entry %s must be a valid URL to OpenIdConnect server" configEntry ]
        else
            Valid url
    with _ -> Invalid [ sprintf "config entry %s is not a valid string" configEntry ] 

let validateCertificatePath rootFolder (config:IConfigurationRoot) =
    let configEntry = "CertificatePath"
    try
        let path = config.GetValue(configEntry, "")
        if path = "" then
            Invalid [ sprintf "config entry %s must not be empty" configEntry ]
        else
            let path =
                if System.IO.Path.IsPathRooted path then path
                else System.IO.Path.Combine(rootFolder, path)
            let path = System.IO.Path.GetFullPath path
            if System.IO.File.Exists path then
                Valid path
            else
                Invalid [ sprintf "config entry %s must contain a valid file path, either absolute or relative to root folder" path ]
    with _ -> Invalid [ sprintf "config entry %s is not a valid string" configEntry ] 

let validateDomain (config:IConfigurationRoot) =
    let configEntry = "Domain"
    try
        let domain = config.GetValue(configEntry, "dev")
        if domain = "" then
            Invalid [ sprintf "config entry %s must not be empty" configEntry ]
        else
            Valid domain
    with _ -> Invalid [ sprintf "config entry %s is not a valid string" configEntry ]

let configToInfrastuctureParameters (config:IConfigurationRoot) = 
    let root = validateRoot config
    let port = validatePort config
    let dnsName = validateDNSName config
    let domain = validateDomain config
    let openIdConnectUrl = validateOpenIdConnectUrl config
    let certificatePath = root |> Domain.Common.Validation.bind (fun root -> validateCertificatePath root config)
    retn Infrastructure.Parameters.consParameters 
        <*> root
        <*> port
        <*> dnsName
        <*> domain
        <*> openIdConnectUrl
        <*> certificatePath

let mapConfigValues f (mapping:(string * string) list) (config:IConfigurationRoot) =
    config.AsEnumerable() 
    |> Seq.map (fun kvp -> System.Collections.Generic.KeyValuePair(kvp.Key, mapping |> List.fold (fun (value:string) (keyword, keyValue) -> if (value = null) then null else value.Replace(keyword, f keyValue)) kvp.Value))

let mapConfigValuesToKeys (mapping:(string * string) list) (config:IConfigurationRoot) =
    mapConfigValues (fun keywordKey -> config.[keywordKey]) mapping config

let mapConfigValuesToValues (mapping:(string * string) list) (config:IConfigurationRoot) =
    mapConfigValues id mapping config
