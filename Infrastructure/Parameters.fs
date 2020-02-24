module Infrastructure.Parameters

type Parameters = {
    Root: string
    Port: int
    DNSName: string
    Domain: string
    UseGit: bool
    OpenIdConnectUrl: string
    AuthenticationIsMandatory: bool
    EnforceHTTPS: bool
    CertificatePath: string
}

let consParameters 
    root
    port
    dnsName
    domain
    useGit
    openIdConnectUrl
    authenticationMandatory
    enforceHTTPS
    certificatePath
    =
    {
        Root = root
        Port = port
        DNSName = dnsName
        Domain = domain
        UseGit = useGit
        OpenIdConnectUrl = openIdConnectUrl
        AuthenticationIsMandatory = authenticationMandatory
        EnforceHTTPS = enforceHTTPS
        CertificatePath = certificatePath
    }
