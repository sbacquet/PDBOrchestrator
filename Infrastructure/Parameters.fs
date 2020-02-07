module Infrastructure.Parameters

type Parameters = {
    Root: string
    Port: int
    DNSName: string
    UseGit: bool
    OpenIdConnectUrl: string
    AuthenticationIsMandatory: bool
}

let consParameters 
    root
    port
    dnsName
    useGit
    openIdConnectUrl
    authenticationMandatory
    =
    {
        Root = root
        Port = port
        DNSName = dnsName
        UseGit = useGit
        OpenIdConnectUrl = openIdConnectUrl
        AuthenticationIsMandatory = authenticationMandatory
    }
