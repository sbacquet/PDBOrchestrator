module Infrastructure.Parameters

type Parameters = {
    Root: string
    Port: int
    DNSName: string
    UseGit: bool
}

let consParameters 
    root
    port
    dnsName
    useGit
    =
    {
        Root = root
        Port = port
        DNSName = dnsName
        UseGit = useGit
    }
