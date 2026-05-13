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

The following vulnerabilities are tracked but unresolved at the time of the current release because no upstream fix is yet available. They are reachable from the published Go SDK (`github.com/scitrera/aether/sdk/go`) via the Docker-based orchestrator (`sdk/go/orchestrators/docker`):

| Advisory | Affected | Status |
|---|---|---|
| [GO-2026-4887](https://pkg.go.dev/vuln/GO-2026-4887) | `github.com/docker/docker` ≤ v28.5.2 | No upstream fix released. Tracking. |
| [GO-2026-4883](https://pkg.go.dev/vuln/GO-2026-4883) | `github.com/docker/docker` ≤ v28.5.2 | No upstream fix released. Tracking. |

Mitigation: callers that don't need the Docker orchestrator can build their applications without importing `sdk/go/orchestrators/docker`. We will bump the dependency immediately when upstream ships fixed releases.

## Security Best Practices

When deploying Aether in production:

- Enable mTLS for all gRPC connections
- Use the secrets generation tool (`init-secrets`) to create unique credentials
- Never use `--dev` or `--insecure-admin` flags in production
- Configure PostgreSQL with TLS and strong credentials
- Review ACL rules and workspace isolation settings
