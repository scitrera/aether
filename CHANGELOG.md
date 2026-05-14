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

## [0.1.60] - 2026-05-14

### Added
- **Storage interface layer (`server/internal/storage/`).** New per-domain packages — `audit`, `registry`, `acl`, `tasks`, `workflow` — each defining a `Store` interface alongside a postgres-backed wrapper sub-package (`<domain>/postgres`) and a factory-driven conformance test suite that runs against both PostgreSQL and SQLite. Stage 1 of the storage refactor: decouples gateway code from the legacy concrete types (`internal/audit.AuditLogger`, `pkg/tasks.TaskStore`, `internal/acl.Service`, etc.) without changing behavior. Compile-time `var _ Store = (*Store)(nil)` asserts in every wrapper catch interface drift at build time.
- **In-process gRPC for AetherLite's embedded workflow engine** via `google.golang.org/grpc/test/bufconn`. AetherLite now registers the gateway service on a second `grpc.Server` bound to an in-memory listener; the embedded workflow engine connects through that listener with a pre-dialed `*grpc.ClientConn`. New `internal/gateway/in_process.go` provides unary + stream interceptors that mark bufconn-originated contexts as transport-trust-only, mirroring the existing anonymous-mTLS code path — `InitConnection` still authenticates, but no transport cert is required because the trust boundary is process-local. Reconnect-on-`MaxConnectionAge` cycle re-uses the same conn (gRPC opens a fresh stream, no redial).
- **Go SDK: `aether.NewWorkflowEngineClientWithConn(conn, opts, ownsConn)`** constructor accepting a pre-dialed `*grpc.ClientConn`. Used by embedded callers; full-mode dialers continue to use `NewWorkflowEngineClient(address, ...)`.
- **`ErrorResponse.request_id` proto field** (`api/proto/aether.proto`). Threaded through the KV-op error paths in `internal/gateway/routing.go` via a new `withRequestID(id string) errorOpt`. Lets client SDKs correlate gateway errors to their originating in-flight request futures instead of routing them through the connection-scoped error callback.
- **Python SDK error correlation.** `scitrera_aether_client/exceptions.py` adds `error_response_to_aether_error()` mapping `ErrorResponse.code` strings to typed `AetherError` subclasses (`PermissionDeniedError`, `AuthenticationError`, `NotFoundError`, `InvalidArgumentError`). The async client's `_listen_loop` and the sync client's matching path now reject the pending request future with the typed error when `request_id` is set; un-correlated errors continue through the existing global handler.
- **`QueueStatus` Go constants** (`internal/orchestration/queue_status.go`) for `orchestrated_task_queue.status` values (`pending`, `claimed`, `completed`, `failed`). Replaces hard-coded SQL literals throughout `notify_dispatcher.go` and `polling_dispatcher.go`.
- **SQLite migrations 002–012** as native delta migrations (audit partitioning, authority grants, task authority lineage, parent_task_id, target_specifier, task_class, task_disconnect_grace, drop_delegation_chains, kv_new_scope_fallbacks, permission_namespace_refactor, audit_log_source). Plus **`013_seed_kv_scope_fallbacks.sql`** that backfills the four kv_scope fallback policies (`user_kv_scope`, `agent_kv_scope`, `task_kv_scope`, `service_kv_scope`) that postgres `003_acl_schema.sql:88-99` seeds but the forked SQLite monolith omitted.
- **`migrations/sqlite_audit/` tree** with `001_audit_schema.sql` + `embed.go`, supporting AetherLite's dedicated `audit.db` split. The split moves the audit-log write surface to its own SQLite file + WAL writer goroutine, eliminating SQLITE_BUSY contention with the task-read hot path.

### Changed
- **Truth-in-naming renames** in `internal/orchestration/`. The old type names were misleading: none of these were AMQP-backed, and the "memory" dispatcher polls SQL rather than holding state in memory.
  - `OrchestratorTaskDispatcher` → **`NotifyTaskDispatcher`** (uses PostgreSQL `LISTEN/NOTIFY` via `pq.Listener`, with polling as fallback). File: `dispatcher.go` → `notify_dispatcher.go`.
  - `MemoryTaskDispatcher` → **`PollingTaskDispatcher`** (polls SQL exclusively; durable state lives in postgres or SQLite). File: `memory_dispatcher.go` → `polling_dispatcher.go`.
  - `MemoryQueueCloser` → **`NoopQueueCloser`** (`Close() returns nil`; placeholder for the lite path where there's no upstream AMQP queue to drain). File: `memory_queue.go` → `noop_queue_closer.go`.
- **Gateway server, orchestration services, admin provider, and consumer structs migrated to interface field types.** `GatewayServer.taskStore` (`*tasks.TaskStore` → `tasks.Store`), `.auditLogger` (`*audit.AuditLogger` → `audit.Store`), `.acl` (`*acl.Service` → `acl.Store`); `TaskAssignmentService.{taskStore, agentRegistry}`; `GatewayStateProvider.{taskStore, agentRegistry, profileMgr, aclService}`; `AuthHandler.{acl, auditLogger}`; `KVHandler.auditLogger`; `DisconnectReaper.taskStore`; `cleanup.Service.taskStore`; both dispatcher impls' `taskStore`; `TimeoutHandler.taskStore`. Construction sites at `cmd/{gateway,aetherlite}/main.go` switched to the new wrapper constructors.
- **`OrchestrationServices` consolidation.** The two separate fields `AgentRegistry *registry.AgentRegistry` and `ProfileManager *registry.OrchestratorProfileManager` are now a single required `Registry registry.Store` (which bundles both surfaces via the new postgres wrapper). Legacy `if profileMgr != nil` guards became dead code and were dropped.
- **`GatewayStateProvider.kvStore` widened** from `*kv.Store` (Redis-only concrete) to `gateway.KVReadWriter` interface. `*kv.BadgerKVStore` already satisfied it (compile-time assert in `internal/kv/badger_iface_test.go`). AetherLite's admin paths (CreateWorkspace, etc.) now have a real KV handle instead of the previous typed-nil that masqueraded as "no Redis KV store in lite mode" — fixing silent `"kv store not available"` failures on lite admin operations.
- **`pkg/dbcompat/rewriter.go`: `$N` placeholders now rewrite to `?N`** (SQLite's `?NNN` positional syntax) instead of bare `?`. Preserves PostgreSQL's positional-reuse semantics so queries like `IN ($1, $2) ... WHERE owner = $1` (e.g., `AgentRegistry.GetLaunchParams`) bind the same Go argument to every `$1` reference instead of producing N distinct `?` placeholders that exceed the args count.
- **ACL audit logging centralized** through the shared `audit.AuditLogger`. `internal/acl/audit.go` is now a thin adapter that translates ACL decisions into `audit.AuditEvent`s and forwards them through the single writer goroutine. The previously-separate batched ACL audit goroutine is gone — removing a SQLITE_BUSY contention class against the task-read hot path under lite mode.
- **AetherLite startup ordering** in `cmd/aetherlite/main.go`: now calls `crypto.InitTokenHMAC` after loading secrets (parity with `cmd/gateway/main.go:232-235`), passes the Badger KV store through to `NewGatewayStateProvider` (was `nil`), and stands up the bufconn-backed in-process gRPC server with `gateway.InProcessUnaryInterceptor`/`InProcessStreamInterceptor` before kicking off the embedded workflow engine with the pre-dialed conn.
- **`workflow.Config.AetherConfig`** gains a non-yaml `InProcessConn *grpc.ClientConn` field. When set, `workflow.Server.initAetherClient` constructs its aether client via `aether.NewWorkflowEngineClientWithConn(InProcessConn, ..., ownsConn=false)` instead of dialing by address. External (full-mode) workflow process behavior unchanged.
- **Dependency bumps:** `google.golang.org/grpc` → **v1.81.1**, `go.opentelemetry.io/otel` → **v1.43.0**, plus protobuf-related deps across `server/`, `api/`, `sdk/go/`. Python SDK + TypeScript SDK manifests updated alongside.

### Fixed
- **AetherLite missing `crypto.InitTokenHMAC` call.** Token-mint paths (orchestrated task auth tokens, per-sandbox sidecar tokens) failed silently with `"crypto: HMAC key not initialized"`, leaving spawned agents running with `task_token=None`. The downstream consequence cascaded into apparent ACL-denial symptoms because orchestrator-spawned agents couldn't establish proper authority context. Full gateway always did this at `cmd/gateway/main.go:232-235`; lite was missing the equivalent.
- **AetherLite embedded workflow engine in TLS handshake loop.** The engine attempted plaintext gRPC against the mTLS gateway and reconnected every backoff cycle with `"error reading server preface: EOF"`. Now connects via in-process bufconn with the new transport-trust-only interceptor — no certs needed because the trust boundary is process-local. The admin console's "active connections" panel now shows the workflow engine session.
- **`pkg/dbcompat` lost PostgreSQL positional-reuse semantics.** `IN ($1, $2) ... WHERE x = $1` became `IN (?, ?) ... WHERE x = ?` — three placeholders, two args — causing `AgentRegistry.GetLaunchParams` to fail under SQLite with `"missing argument with index 3"`. Now rewrites `$N → ?N` and preserves the identity of `N` across reuses; SQLite binds the same arg to every same-indexed placeholder.
- **SQLite migration parity gap on KV-scope fallback policies.** `migrations/sqlite/001_full_schema.sql` seeded only seven `acl_fallback_policies` rows; `migrations/003_acl_schema.sql` (postgres) seeds eleven, including the four `*_kv_scope` categories. Lite mode therefore had no `service_kv_scope: READWRITE` fallback, and the wildcard default-deny from migration 010 (sqlite) / 019 (postgres) on `user-workspace-shared` denied service principals trying to write keys like `chat_active_task` — while full-mode postgres allowed the same operation. New `013_seed_kv_scope_fallbacks.sql` backfills the four missing rows idempotently (`ON CONFLICT (rule_category) DO NOTHING`).
- **Python SDK silent `resp=None` failure shape.** Gateway-side KV op errors (and other op-correlated `KV_ERROR` / `ERR_QUOTA_*` / `ERR_PERMISSION_DENIED` codes) arrived via the connection-scoped `_on_error` callback uncorrelated to in-flight request futures, so `kv_put` / `kv_get` / `kv_delete` callers waited until their await timed out and observed `resp=None`. Now correlated via `ErrorResponse.request_id` and surfaced to the caller as typed `AetherError` subclasses. Backward compatible: when `request_id` is empty (older gateways), the path falls through to the existing global handler unchanged.
- **`migrations/002_task_schema.sql`** inline comment listing task status values now includes `starting` — the Go constant `TaskStatusStarting` was always defined and used; only the schema doc lagged.

### Removed
- **Deprecated `OrchestratorTaskDispatcher` integration tests** removed alongside the rename to `NotifyTaskDispatcher`. The new tests live under `notify_dispatcher_test.go` and `notify_dispatcher_integration_test.go`. `stale_claims_test.go.disabled` updated to reference the new type name (still disabled).

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
