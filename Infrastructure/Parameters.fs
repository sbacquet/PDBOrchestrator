module Infrastructure.Parameters

type Parameters = {
    Root: string
    Port: int
    DNSName: string
    Domain: string
    OpenIdConnectUrl: string
    CertificatePath: string
}

let consParameters 
    root
    port
    dnsName
    domain
    openIdConnectUrl
    certificatePath
    =
    {
        Root = root
        Port = port
        DNSName = dnsName
        Domain = domain
        OpenIdConnectUrl = openIdConnectUrl
        CertificatePath = certificatePath
    }
