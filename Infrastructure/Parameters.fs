module Infrastructure.Parameters

type Parameters = {
    Root: string
    Port: int
    DNSName: string
}

let consParameters 
    root
    port
    dnsName =
    {
        Root = root
        Port = port
        DNSName = dnsName
    }
