# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Aether, please report it responsibly.

**Do NOT file a public GitHub issue for security vulnerabilities.**

Instead, email **open-source-team@scitrera.com** with:

- A description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

## Response Timeline

- **Acknowledgment**: Within 48 hours of your report
- **Assessment**: We will confirm the vulnerability and its severity within 5 business days
- **Fix**: Critical vulnerabilities targeted for resolution within 90 days

We will coordinate disclosure with you and credit your contribution (unless you prefer anonymity).

## Supported Versions

| Version | Supported |
|---------|-----------|
| Latest release | Yes |
| Older releases | No |

Only the latest release receives security fixes. We recommend always running the latest version.

## Scope

The following are in scope for security reports:

- Server code (`server/`)
- Client SDKs (`sdk/`)
- Docker images and Dockerfiles
- Deployment manifests and Helm charts
- Authentication and authorization logic
- Data handling and encryption

Out of scope:

- Third-party dependencies (report upstream, but let us know if we use a vulnerable version)
- Social engineering
- Denial of service via resource exhaustion in development configurations

## Known Issues

The following Docker Engine advisories are tracked for the published Go SDK (`github.com/scitrera/aether/sdk/go`) because it imports the legacy `github.com/docker/docker` client module. Aether uses the client packages, not the affected Engine plugin implementation, but the Go vulnerability records do not provide symbol-level data or a fixed version for this legacy module path, so `govulncheck` conservatively reports them as reachable:

| Advisory | Affected | Status |
|---|---|---|
| [GO-2026-4887](https://pkg.go.dev/vuln/GO-2026-4887) | Docker Engine < 29.3.1; legacy Go module has no fixed release | Engine AuthZ-plugin bypass; Aether imports only the Docker API client. Tracking migration to `github.com/moby/moby/client`. |
| [GO-2026-4883](https://pkg.go.dev/vuln/GO-2026-4883) | Docker Engine < 29.3.1; legacy Go module has no fixed release | Engine plugin privilege-validation issue; Aether imports only the Docker API client. Tracking migration to `github.com/moby/moby/client`. |

Mitigation: callers that don't need the Docker orchestrator can build their applications without importing `sdk/go/orchestrators/docker`. We will migrate to the separately versioned Moby client module once compatibility is validated.

## Security Best Practices

When deploying Aether in production:

- Enable mTLS for all gRPC connections
- Use the secrets generation tool (`init-secrets`) to create unique credentials
- Never use `--dev` or `--insecure-admin` flags in production
- Configure PostgreSQL with TLS and strong credentials
- Review ACL rules and workspace isolation settings
