module Infrastructure.Parameters

type Parameters = {
    Root: string
    Port: int
    DNSName: string
    Domain: string
    UseGit: bool
    OpenIdConnectUrl: string
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
        EnforceHTTPS = enforceHTTPS
        CertificatePath = certificatePath
    }
