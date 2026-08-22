# Security Policy

## Supported Versions

Kurd is currently in beta.

Security fixes are provided for the latest minor release line.

| Version | Supported |
|---|---|
| 0.4.x | Yes |
| 0.3.x | No |
| < 0.3 | No |

## Reporting a Vulnerability

Do not disclose suspected security vulnerabilities in a public GitHub issue.

Please report security issues privately to:

**sn391@yahoo.com**

Include, when possible:

- affected Kurd version
- operating system and Python version
- reproduction steps
- expected behavior
- actual behavior
- impact assessment
- proof-of-concept details
- suggested mitigation, if known

Please avoid including secrets, access tokens, private keys, or unrelated personal data.

## Current Security Controls

Kurd includes the following gateway security controls:

- optional bearer-token authentication
- constant-time bearer-token comparison
- JSON content-type validation
- 1 MiB MCP request body limit
- upstream URL scheme validation
- rejection of embedded URL credentials
- rejection of URL fragments
- optional blocking of loopback/private upstream targets
- configurable upstream request timeout
- sanitized upstream transport errors
- global concurrency backpressure
- per-upstream concurrency backpressure
- Python callback concurrency backpressure
- graceful HTTP shutdown
- request ID propagation for operational correlation

## Deployment Guidance

For deployments reachable outside a trusted local network:

- terminate TLS at a reverse proxy, ingress, or trusted load balancer
- enable bearer authentication or stronger external authentication
- restrict network access to `/mcp`
- disable private upstream access unless it is explicitly required
- use conservative concurrency limits
- configure upstream timeouts
- monitor `/status` and structured request logs
- rotate authentication credentials periodically
- avoid exposing debugging output to untrusted clients

Kurd does not currently provide built-in TLS termination, role-based authorization, OAuth, or a secret-management system. These controls should be provided by the deployment environment.

## Security Response

Confirmed vulnerabilities may result in:

1. a private fix
2. regression tests
3. a patch or minor release
4. release notes describing the impact and mitigation
5. coordinated public disclosure when appropriate
