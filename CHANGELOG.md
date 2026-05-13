# Changelog

All notable changes to Aether are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Aether uses [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

### Added

### Changed

### Fixed

### Security

---

## [0.1.59] - 2026-05-12

### Security
- Bumped Go toolchain to **1.25.10**, clearing 9 standard-library vulnerabilities affecting `net/http`, `crypto/tls`, `html/template`, and related packages (HTTP/2 SETTINGS_MAX_FRAME_SIZE DoS, TLS 1.3 KeyUpdate DoS, html/template XSS, and others).
- Bumped `golang.org/x/net` to **v0.54.0** (HTTP/2 transport DoS in `golang.org/x/net/internal/http2`).
- Bumped `github.com/docker/docker` (Go SDK dependency) to **v28.5.2** (latest available). Two upstream vulnerabilities tracked as known issues with no fix yet available — see `SECURITY.md`.

### Changed
- Docker images now publish **multi-architecture manifests** (`linux/amd64` and `linux/arm64`).
- Server Dockerfile uses **Go cross-compilation** (`$BUILDPLATFORM` + `TARGETOS`/`TARGETARCH`) for native-speed arm64 builds without QEMU emulation.
- Bumped `google.golang.org/grpc` to **v1.80.0** across the `api`, `sdk/go`, and server modules for ecosystem consistency.

### Fixed
- Numerous internal code-quality fixes surfaced by `golangci-lint`: deprecated API usages replaced (`grpc.Dial` → `grpc.NewClient`, AMQP `QueueInspect` → `QueueDeclarePassive`), error-string casing normalized, dead assignments removed, dead code pruned, ineffectual assignments eliminated. No public API changes.

---

## [0.1.58] - 2026-05-12

Initial public OSS release of the Aether gateway, SDKs (Go, Python, TypeScript), and API definitions.

### Added

#### Gateway & runtime1
- gRPC bidirectional streaming gateway supporting eight principal types: Agent, Task, User, Service, Orchestrator, WorkflowEngine, MetricsBridge, and Bridge.
- RabbitMQ Streams-backed message routing with per-topic producer pools and shared consumer fan-out.
- Redis-backed distributed session registry (SetNX locks with 30 s TTL, 10 s refresh).
- Hierarchical KV store with global, workspace, user, and user-workspace scopes.
- Checkpoint store for persistent agent/task state.
- PostgreSQL-backed task lifecycle management, orchestration profiles, ACL rules, API token storage, and audit log.
- Horizontal scaling via stateless gateway instances sharing Redis and PostgreSQL.
- REST admin API with embedded UI and Prometheus metrics endpoint.
- Kubernetes and Docker Compose deployment manifests.
- Embedded schema migrations (auto-run on startup).
- Badger-backed token and session stores as alternatives to Redis for embedded deployments.

#### Standalone binaries
- `aetherlite` (`cmd/aetherlite`): lightweight single-binary deployment target embedding SQLite + Badger; configurable via `AETHERLITE_*` environment variables or CLI flags.
- `msgbridge` (`cmd/msgbridge`): messaging bridge server bridging Discord, Teams, and Email to Aether via the `Bridge` principal type.
- `auth-proxy` (`cmd/auth-proxy`): authentication/authorization gateway for external services (e.g., MemoryLayer), backed by the same PostgreSQL schema as the gateway.

#### Fan-in subscription architecture (Workflow Engine & Metrics Bridge)
- `server/pkg/sharding` package with `ShardForWorkspace` stub (always returns 0 today; future fnv64 hash) and `ReceiverTopic` helpers.
- Workflow Engine subscribes as a singleton to `event::receiver0` with offset-tracked exclusive consumption; supports replay-on-reconnect. Sender API unchanged — senders still write `event.{workspace}`, gateway rewrites to the receiver topic.
- Metrics Bridge subscribes to `metric::receiver0` with offset-tracked exclusive consumption (replay-on-reconnect supported).
- Cross-workspace event/metric broadcasts gated by `capability/event_broadcast` / `capability/metric_broadcast` ACL permissions; implicit grant for the sender's native workspace.
- `IncomingMessage.workspace` proto field — gateway-populated mirror of `SendMessage.app_workspace`, surfacing the effective declared workspace for any message with workspace context.

#### Foreign audit logging
- `SubmitAuditEvent` RPC lets connected principals publish their own activity into the gateway audit pipeline. Identity is stamped by the gateway (non-forgeable provenance), metadata is unconditionally sanitized for credential patterns, event types are whitelisted (`message`, `kv`, `task`, `custom`), and submissions are gated by the `capability/audit_submit` ACL permission (implicit grant for native workspace; explicit grant for cross-workspace). Default rate limit 100 events/sec/principal, configurable via `AETHER_AUDIT_FOREIGN_RATE_PER_SEC`.
- `AuditEntry.source` proto field and corresponding `source` column on `comprehensive_audit_log` distinguishing gateway-emitted (`gateway`) vs principal-submitted (`principal`) events.
- `EventTypeCustom` audit event category for application-defined events.

#### Configuration
- All aether-specific environment variables use a strict `AETHER_*` prefix (gateway runtime: `AETHER_GATEWAY_ID`, `AETHER_ADMIN_PORT`, `AETHER_ADMIN_ENABLED`, `AETHER_ADMIN_API_KEY`, `AETHER_ADMIN_TLS_CERT_FILE`, `AETHER_ADMIN_TLS_KEY_FILE`, `AETHER_ACL_REQUIRED`, `AETHER_AUTH_MODES`, `AETHER_LOG_LEVEL`, `AETHER_AUDIT_*`, etc.). Cloud-platform conventions (`PORT`, `POSTGRES_*`, `REDIS_*`, `STREAM_URL`, `AMQP_URL`, `DATABASE_URL`), `OTEL_*`, and service-scoped names (`MSGBRIDGE_*`, `WORKFLOW_*`, `AUTH_PROXY_*`, `PROXY_SIDECAR_*`, `TEAMS_*`, `DISCORD_*`, `SMTP_*`) are preserved for PaaS portability.
- AetherLite-specific overrides via `AETHERLITE_*` env vars (`AETHERLITE_PORT`, `AETHERLITE_ADMIN_PORT`, `AETHERLITE_DATA_DIR`, etc.) — each CLI flag has a matching env var; precedence is CLI flag > env > compiled-in default.
- `docs/environment.md` — comprehensive reference covering all 87 environment variables across every binary.

#### Client SDKs
- **Go SDK** (`sdk/go/`) with all eight principal types; KV operations across all scopes; checkpoint store; auto-reconnect with configurable backoff; TLS / mTLS; Docker-based orchestrator; progress reporting; foreign audit submission; `AdminClient` for token, ACL, workspace, agent, and session management.
- **Python SDK** (`sdk/python-client/`, `scitrera-aether-client` on PyPI) with both sync and async clients; multiprocess orchestrator; `ServiceClient` / `AsyncServiceClient` for trusted backend intermediaries with on-behalf-of authority via `AuthorizationContext`; `authority_cache` module for caching authority grants; `proxy_http` / `proxy_http_async` helpers; foreign audit submission; `AdminClient` / `AsyncAdminClient`.
- **TypeScript SDK** (`sdk/typescript/`, `@scitrera/aether-client` on npm) with Agent, Task, User, Orchestrator, WorkflowEngine, MetricsBridge, and Bridge clients; auto-reconnect; foreign audit submission; `AdminClient`; runnable examples under `examples/`.

### Notes
- All SDK packages, the Go modules (gateway, api, sdk/go), the Python wheel, and the npm package are versioned `0.1.58` and tagged in lockstep via `scripts/tag-release.sh`.
- The internal v0.1.57 prepared state was not published publicly; v0.1.58 is the first version on GitHub releases, PyPI, npm, and Go module proxies.

---

[Unreleased]: https://github.com/scitrera/aether/compare/v0.1.59...HEAD
[0.1.59]: https://github.com/scitrera/aether/compare/v0.1.58...v0.1.59
[0.1.58]: https://github.com/scitrera/aether/releases/tag/v0.1.58
