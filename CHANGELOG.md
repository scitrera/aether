# Changelog

All notable changes to Aether are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Aether uses [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

Internal builds carry a **v0.2.1** gateway/SDK version stamp; no v0.2.1 tag is published yet.

### Added

- **[AG2] `scitrera-aether-ag2` adapter package.** Bridges AG2 / AutoGen agent runtimes to the Aether wire protocol with three layers: `AetherAgentHost` (server-side hosting of AG2 agents), `AetherRemoteAgent` (client-side stub that participates in AG2 group chats), and `AetherTransport` (typed wire transport). Ships a typed error hierarchy, example scripts for two-agent and group-chat scenarios, an `OPENAI_API_KEY`-driven `groupchat_with_remote_llm.py` example demonstrating `GroupChatManager` with LLM-based speaker selection, and e2e tests covering checkpoint persistence, streaming, and factory-based remote-agent setup.
- **[AG2] `AetherAg2Orchestrator.shutdown()`** — idempotent teardown of tracked processes and client connections for the AG2 adapter. Tests cover process-termination resilience and client-closure robustness.
- **[GO SDK / GATEWAY / PROXY SIDECAR] End-to-end CoDel-backed prioritization.** Priority-aware Controlled-Delay (CoDel) admission control now spans the send path (Go SDK), the delivery path (gateway per-client semaphore), and the sandbox relay path (proxy sidecar per-session queue). **Go SDK** — new `SendWithPriority` API classifies sends across five priority levels (control / chunked / best-effort tiers); `BackpressureError` surfaces sheds to callers; client config gains `BackpressureCapacity`, `BackpressureTarget`, and `BackpressureInterval` tuning knobs. **Gateway** — per-client `deliverySem` CoDel semaphore guards `Deliver*` callers; new `WithDeliveryBackpressure(capacity, target, interval)` server option (defaults: 16 / 50ms / 100ms); on shed or staging-buffer overflow the session emits a `BACKPRESSURE` `DownstreamMessage_Error` notice. **Proxy sidecar** — every send path (`runner.go`, `terminator.go`, `tunnel_transport.go`) routes through `SendWithPriority` so tunnel and proxy envelopes carry consistent priorities; per-session admission queue mirrors the SDK's; synthesized BACKPRESSURE notices on Acquire shed; terminator receive-loop throughput improved by offloading slow response paths and tunneling handshakes to bounded goroutines. Control envelopes are protected first under saturation across all three layers.
- **[PROXY SIDECAR] `MaxSessions` config** to cap concurrent sandbox relay sessions (default `8`) for memory and table-tracking efficiency. Sessions over the cap surface as explicit rejections rather than slow-degrade. Loaded-config logging extended in both dev and prod paths for visibility.
- **[AG2] `conversation_id` support across telemetry, proxy, and tool-loop layers.** Groups multiple `RequestEnvelope` correlation IDs into a single logical session. Telemetry hooks (`on_request_received`, `on_response_chunk`, `on_request_completed`, etc.) accept an optional `conversation_id`; propagation threads through the `proxy` and `tool_loop` modules. Backward-compatible — unit tests cover legacy payloads and schema migration scenarios.
- **[PROXY SIDECAR] End-to-end sidecar integration test suite.** Covers streaming throughput/latency under fanout, chunked uploads under contention, mixed tunnel traffic, and CoDel priority-shedding behavior under real workloads. Adds structured SDK tests for failure and throughput scenarios.
- **[TESTING] Shared aetherlite subprocess for e2e tests.** New `aetherlite_proc` package manages a single aetherlite gateway subprocess across the integration suite; harnesses attach sidecars with isolated per-test specs. Replaces the previous `fake-gateway` harness path for higher test fidelity. Lifecycle and startup-readiness checks run through `TestMain`.
- **[GATEWAY] Clustering integration test for the chat-task workflow** (`TestClusterIntegration_ChatTaskWorkflow_EndToEnd`). Exercises the full production chat-task path on a single-node embedded NATS cluster: service-principal setup, agent orchestration, sandbox cache, agent-sandbox communication, cold-start replay, OBO grant propagation, task dispatching, and KV/cache interactions. Runs under `-short` without external dependencies.
- **[PROXY SIDECAR] `HeaderMode` e2e coverage** expanded to query parsing plus `HEAD`, `PATCH`, and `OPTIONS` verbs.
- **[AG2] Streaming buffer for empty final-message content** in `remote/proxy.py`. Continuation passes now reset cleanly; new unit tests cover buffer correctness and reset behavior.
- **[AG2] Tool-loop error synthesis and lifecycle improvements.** Synthesizes errors for missing executors or raised exceptions. `_mirror_into_oai_messages` checkpoints agent conversation state. `RequestEnvelope.deadline_ms` is now enforced in `AetherAgentHost` with matching tests. `on_max_continuations` config handles proxy continuation limits gracefully. Telemetry counters track dropped events and metrics.
- **[GO SDK] `QueryAuditLog` API** (`sdk/go/aether/audit_query_ops.go`). Synchronous wrapper around the gateway's `AuditQuery` / `AuditQueryResponse` round-trip. System principals (`OrchestratorClient`, `WorkflowEngineClient`) bypass ACL; agent/user principals require `admin_operations` or workspace read access. Filters cover operation, event type, workspace, actor ID, time range, and pagination (default limit 100, max 500). Correlation mirrors `SubmitAuditEvent` — per-call `request_id` registered in `pendingAuditQueryRequests` and resolved by `handleAuditQueryResponse`.
- **[PROXY SIDECAR / GATEWAY / TESTING] mTLS and audit-event/metrics e2e suites.** `mtls_test.go` exercises the full SDK ↔ sidecar ↔ aetherlite mTLS path under three scenarios (happy-path round-trip with identity verification, missing client cert rejected at handshake, wrong-CA cert rejected). Each mTLS test owns its own aetherlite subprocess on a distinct port so the shared insecure suite is undisturbed. `audit_metrics_test.go` asserts that the gateway emits the expected audit events (`proxy_http_routed`, `proxy_http_failed`, `tunnel_opened`, `tunnel_closed`) for normal proxy/tunnel operations and that Prometheus counters (`aether_proxy_local_bypass_total{envelope_type=...}`) increment correctly — scraped from `aetherliteProc.opsAddr` and parsed from the text format.
- **[CI / TESTING] `scripts/run-ci.sh` — unified local-and-CI test executor** (324 lines). Single entry point that mirrors the GitHub Actions matrix locally with per-job logs and failure summaries. Honors `LOG_DIR` (override default log destination) and `RUN_CI_VERBOSE` (stream sub-process output live instead of buffering to log files). The CI workflow (`.github/workflows/test.yml`) gains a dedicated `e2e` job that scopes to `server/internal/proxysidecar/integration_e2e/` with the `e2e` build tag, isolating long-running integration runs from the unit-test job.

### Changed

- **[GO SDK] Tunnel inflight registries scoped per `BaseClient`** instead of process-global, preventing key collisions when multiple clients share a process. `registerTunnelInflight` / `deleteTunnelInflight` now operate on the owning `BaseClient` instance. New `newTunnelState` constructs tunnel state testably without touching registries, improving test isolation across the tunnel unit-test suite.
- **[PROXY SIDECAR] Concurrency and buffering refactor.** Replaced `aether.Async` in `OnProxyHttpRequest` and `OnTunnelOpen` with custom handlers that more tightly manage the receive-loop concurrency. New `pendingTunnel` buffer preserves frame order before tunnel registration, eliminating silent drops in registration-race scenarios. Logging and error handling extended for buffer-overflow and duplicate-tunnel-id paths. Affected e2e tests disable `t.Parallel()` to reduce contention and timeout risk under concurrent execution.
- **[GATEWAY] Async-writer audit test replaced poll-based visibility check with deterministic drain via `Close()`.** Faster and stable on slow CI runners; aligns the test with the contract that events are queryable after drain.
- **[VERSIONING] Internal version stamp bumped to v0.2.1** across the gateway and SDKs; log levels for target-clamp drops adjusted so routine backpressure no longer surfaces as warnings.
- **[CI] GitHub Actions workflows:** `pull_request` triggers expanded; dev-image builds disable `latest` auto-injection (`flavor.latest=false`) so production and `dev-latest` tags stay distinct.
- **[CONFIG / DEPLOYMENT] Environment-variable prefix unification (breaking for operators).** Env vars that configure functionality *shared* between `aetherlite` and the full `gateway` binary have been renamed from `AETHERLITE_*` to `AETHER_*` so the two binaries read the same names for the same knobs. Lite-only settings keep the `AETHERLITE_*` prefix. Docker Compose manifests (`cluster.yaml`, `cluster-ha.yaml`, `cluster-single.yaml`), the deployment README, `docs/environment.md`, and the AetherLite clustering guide are all updated. **Operators upgrading deployments must update env files** — there is no compatibility shim.
- **[GATEWAY] JetStream stream-creation timeouts switched from a shared 30s budget to per-stream 30s timeouts.** With `Replicas>1`, each `CreateOrUpdateStream` call triggers a Raft leader election for that stream's own Raft group; on a freshly-formed cluster a single slow early election could consume the whole shared budget and starve the remaining streams. Each stream now gets its own deadline.
- **[DEPS] Dependency cleanup.** Removed `github.com/bwmarrin/discordgo` and `github.com/stretchr/testify` (both unused after msgbridge removal — discordgo backed the Discord platform adapter; testify migrated to standard-library asserts). Added **`github.com/scitrera/go-backpressure v0.1.0`** as a new external dependency — the CoDel/admission-queue implementation that powers the priority/backpressure work has been extracted into a reusable upstream package. Added `go.work` and `go.work.sum` to `.gitignore` for local multi-module workspaces.
- **[GATEWAY / PROXY SIDECAR] Legacy dead-code purge.** Removed unused `activate`, `followPin`, `encodeRequestPin`, and related routing-pin helpers in `routing_proxy.go` and `tunnel_manager.go`. Variable initialization in `proxy_inflight_tracker` tightened for clarity.

### Fixed

- **[GATEWAY / PROXY SIDECAR / GO SDK] Mid-stream disconnect cascades (H2/H3 e2e gaps).** New `proxyInflightTracker` in the gateway (`server/internal/gateway/proxy_inflight_tracker.go`) tracks `(caller, service)` topic pairs with in-flight wire IDs per session. On session cleanup, the gateway synthesizes `ProxyError{SIDECAR_UNAVAILABLE}` notices to the *surviving* peer of every in-flight `ProxyHttpRequest` involving the departing session: **H2** — a service disconnecting mid-stream previously left the caller's body reader hanging forever; the synthesized fin-chunk now unblocks the SDK's streaming-body path. **H3** — a caller disconnecting mid-stream previously left the service-side backend handler running because nothing told the service to stop; the synthesized error now propagates via `SendProxyHttpResponse` failure into the terminator's dispatch ctx, which cancels the `http.Client` request context. Tracker state is per-session and in-memory (matches the one-gateway-per-node deployment shape). SDK (`client.go`, `proxy.go`) and sidecar (`runner.go`, `terminator.go`) updated in lockstep; regression-protected e2e tests under `resilience_test.go` cover gateway crashes and peer disconnects.
- **[PROXY SIDECAR] Tunnel routing edge cases — peer misroutes and loops.** Tunnel pin lookups now enforce sender-peer integrity, with explicit checks for malformed or expired pins and disconnected peers. Unknown senders are silently dropped to avoid spurious reset cascades. Logging extended for routing edge cases to aid debugging.

### Removed

- Removed experimental msgbridge; a new replacement will be coming in a future release

### Security

---

## [0.2.0] - 2026-05-17

This release lands the **Agentic-Fabric Protocol Update** (Phases 1–6) — a coherent expansion of the task lifecycle, authority model, and connection-time negotiation surface — alongside a cluster-mode NATS/JetStream substrate, full elimination of the `dbcompat` translation layer in favor of per-domain native SQLite stores, and a topic/address format breaking change. SDK versions are bumped to **0.2.0** across Go/Python/TypeScript.

### Added

#### Agentic-Fabric Protocol (Phases 1–6)
- **Phase 1 — Paused-state lifecycle and A2A-aligned `TaskStatus` states.** New `TaskStatus` values `WAITING_INPUT`, `HIBERNATED`, `REJECTED` aligned with A2A semantics. Task pause/resume APIs land in the gateway and SDKs; `WaitSpec` + `WaitReason` capture *why* a task is paused (dependency, input, schedule, hibernation). SQLite schema gains `wait_spec`, `paused_at`, `depends_on` columns (`migrations/sqlite_tasks/002_paused_states.sql`). TypeScript and Python SDKs gain protobuf-mirror types.
- **Phase 2 — Authority-request lifecycle (sudo).** New `acl_authority_requests` table backs the request → approval → resolution flow with states `PENDING`, `APPROVED`, `DENIED`, `EXPIRED`, `CANCELED` and integrity constraints on resolution. Adds a request sweeper, approver-routing indices, and Go methods for `Create`, `Resolve`, `Expire`, `Cancel`. New protos: `AuthorityRequest`, `AuthorityRequestOperation`, `AuthorityRequestEvent`. Lifecycle actions are audit-logged.
- **Phase 3 — Hibernation lifecycle.** Tasks can now suspend with `WaitSpec.HibernationDescriptor` (checkpoint keys, session IDs, escalation policy) and rehydrate via `TaskHibernated` handoff. Orchestrator gains `applyHibernationHandoffToAssignment`. DisconnectReaper guards against reaping hibernated tasks. Validations cover descriptor preconditions and JSON round-tripping.
- **Phase 4 — Cursor pagination and descendant walks.** `ListTasksPage` returns cursor-encoded pages with recursive descendant traversal via SQLite CTEs. `TaskFilter` gains `CreatorActorID`, `StatusTimestampAfterUnixMs`, and related filters. New cursor encoding/decoding helpers in `server/pkg/tasks/cursor.go`. Comprehensive pagination conformance tests across postgres and SQLite backends.
- **Phase 5 — Resource schema enforcement and audit attribution.** `AgentRegistration` carries a `resource_type_prefix` whose uniqueness is enforced across active registrations. Audit logs gain `owning_agent` and `owning_agent_prefix` metadata so downstream consumers can attribute writes back to the owning agent. ACL service, audit logger, and prefix-based routing all updated in lockstep.
- **Phase 6 — Connection-time extension negotiation.** `InitConnection.extensions` lets clients declare extensions with required vs. optional semantics. The gateway negotiates and activates, returns `ConnectionAck.negotiated_extensions`, and rejects connections whose required extensions are unsupported. Negotiation activity is audit-logged.

#### Cluster-mode NATS / JetStream substrate
- **JetStream-based audit emitter** for cluster deployments. Audit events mirror into the JetStream `audit` stream; single-node deployments keep using the in-process SQLite audit store. Cluster startup gains guardrails for JetStream-based ACL rule propagation and task dispatching; integration tests cover KV propagation and fallback paths.
- **Cross-gateway agent registry sync via NATS KV.** Best-effort KV propagation for `AgentRegistry.Register` and `Delete` keeps registries consistent across cluster members. New `KVSetter` interface; `AgentRegistry` and the SQLite registry store thread a KV bucket handle through. Backup logic now degrades gracefully when KV/stream domains are unprovisioned. Tests cover propagation, nil-KV no-ops, and cluster-mode registry wiring.
- **NATS compatibility hardening.** Escaping logic for invalid NATS consumer/KV names (e.g., `:`, `@`) prevents subject-character errors. Regression tests cover JetStream durable consumers and KV round-trips with special characters. AWS SDK, NATS, and HashiCorp dependencies bumped.
- **Multi-node cluster deployment manifests.** `cluster.yaml` and `cluster-ha.yaml` Docker Compose configs ship 2-node async and 3-node quorum topologies with JetStream, S3 backups, and optional load-balancer/debug profiles. Phase 4 integration tests exercise full cluster backup/restore cycles and HA deployments.

#### Lifecycle and orchestration
- **`TaskWaker`** periodically evaluates waiting tasks and triggers wake transitions on dependency completion, timeout-based failure, and scheduled wakes. Dependency-reconciliation logic moves into an event-driven state machine.
- **Python SDK lifecycle helpers** — `pause_task`, `resume_task`, and matching pieces for the paused-state flow.
- **`Agentic-Fabric Protocol Guide`** — long-form documentation covering Phases 1–6: state diagrams, code snippets, SDK usage, and migration guidance.

#### Storage and migrations
- **Native SQLite migration trees per domain** under `server/migrations/sqlite_{acl,audit_native,registry,tasks,tokens,workflow}/`. Each tree uses SQLite-native idioms (`AUTOINCREMENT`, `TEXT` timestamps, triggers, partial indices) and is embedded via a per-tree `embed.go`.

### Changed

- **Topic and address format standardized on `::` separator.** All task, agent, and progress topic/address strings now use `::` as the field separator across the protobuf models, server routing/validation, and the TypeScript, Python, and Go SDKs. **This is a wire-incompatible change with v0.1.x** — older clients cannot interoperate with v0.2.0 gateways, which is why SDKs are bumped to 0.2.0 in lockstep.
- **Lite-mode services migrated to per-domain native SQLite stores.** ACL, tasks, registry, workflow, audit, and tokens each get their own SQLite database file (`acl.db`, `tasks.db`, `registry.db`, etc.) and dedicated handle, using the bare `modernc.org/sqlite` driver. Domain isolation simplifies lifecycle management and removes a class of cross-domain contention. Conformance tests run unchanged against the new backends.
- **`WorkflowStore` interface in `internal/workflow`** decouples engine logic from `internal/storage/workflow.Store` to avoid an import cycle. Go's structural typing keeps the two interfaces interchangeable; compile-time assertions enforce method parity. Tests cover both the injected-store and legacy paths.
- **`TaskAssignmentService` constructor cleanup.** Removed the unused `db` parameter from all initialization sites — the service has consumed its `taskStore` interface for some time and the raw `*sql.DB` was vestigial.
- **gRPC HTTP/2 handshake timeout** added to the server configuration. Bounds shutdown latency and stabilizes CI by ensuring mid-handshake streams cannot stall server stop.
- **Cert/key paths use a filesystem-safe `cnFilename`** helper to handle CNs containing characters disallowed in filenames (slashes, etc.) without manual sanitization at every call site.
- **SDK version bumps to 0.2.0** — Go SDK, Python client (`scitrera_aether_client`), TypeScript SDK, and the gateway/aetherlite binaries. `versions.yaml` updated accordingly.

### Removed

- **`server/pkg/dbcompat/` removed in its entirety** — `dialect.go`, `driver.go`, `rewriter.go`, and their test files. The translation layer that previously rewrote PostgreSQL DDL/DML for SQLite is no longer in the source tree; each domain owns its native SQLite schema.
- **Legacy lite-mode (`internal/aetherlite/lite.go`) and workflow SQLite migrations** under the old layout. Responsibilities consolidated into the per-domain native SQLite stores. Affected tests deleted or moved to the new layout.
- **Unused helpers and dead code** pruned across packages: `collect`, `taskStatusToWaitReason`, `parseTimePtr`, and other identifiers with no remaining callers. Deferred `Stop` invocations replaced with error-checking closures (`defer func() { _ = watcher.Stop() }()`) for clearer resource management. `//nolint` markers added at the few deprecated-dependency call sites where migration is planned separately.

### Fixed

- **`proxysidecar/relay.go` indefinite-hang on shutdown.** `GracefulStop` is now bounded with a 3-second grace period; if the deadline passes (mid-handshake or stuck streams), the relay falls back to `Stop()` and logs a warning. Prior behavior could hang shutdown indefinitely under specific stream timing.

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
- All aether-specific environment variables use a strict `AETHER_*` prefix (gateway runtime: `AETHER_GATEWAY_ID`, `AETHER_ADMIN_PORT`, `AETHER_ADMIN_ENABLED`, `AETHER_ADMIN_API_KEY`, `AETHER_ADMIN_TLS_CERT_FILE`, `AETHER_ADMIN_TLS_KEY_FILE`, `AETHER_ACL_REQUIRED`, `AETHER_AUTH_MODES`, `AETHER_LOG_LEVEL`, `AETHER_AUDIT_*`, etc.). Cloud-platform conventions (`PORT`, `POSTGRES_*`, `REDIS_*`, `STREAM_URL`, `AMQP_URL`, `DATABASE_URL`), `OTEL_*`, and service-scoped names (`WORKFLOW_*`, `AUTH_PROXY_*`, `PROXY_SIDECAR_*`) are preserved for PaaS portability.
- AetherLite-specific overrides via `AETHERLITE_*` env vars (`AETHER_PORT`, `AETHER_ADMIN_PORT`, `AETHERLITE_DATA_DIR`, etc.) — each CLI flag has a matching env var; precedence is CLI flag > env > compiled-in default.
- `docs/environment.md` — comprehensive reference covering all 87 environment variables across every binary.

#### Client SDKs
- **Go SDK** (`sdk/go/`) with all eight principal types; KV operations across all scopes; checkpoint store; auto-reconnect with configurable backoff; TLS / mTLS; Docker-based orchestrator; progress reporting; foreign audit submission; `AdminClient` for token, ACL, workspace, agent, and session management.
- **Python SDK** (`sdk/python-client/`, `scitrera-aether-client` on PyPI) with both sync and async clients; multiprocess orchestrator; `ServiceClient` / `AsyncServiceClient` for trusted backend intermediaries with on-behalf-of authority via `AuthorizationContext`; `authority_cache` module for caching authority grants; `proxy_http` / `proxy_http_async` helpers; foreign audit submission; `AdminClient` / `AsyncAdminClient`.
- **TypeScript SDK** (`sdk/typescript/`, `@scitrera/aether-client` on npm) with Agent, Task, User, Orchestrator, WorkflowEngine, MetricsBridge, and Bridge clients; auto-reconnect; foreign audit submission; `AdminClient`; runnable examples under `examples/`.

### Notes
- All SDK packages, the Go modules (gateway, api, sdk/go), the Python wheel, and the npm package are versioned `0.1.58` and tagged in lockstep via `scripts/tag-release.sh`.
- The internal v0.1.57 prepared state was not published publicly; v0.1.58 is the first version on GitHub releases, PyPI, npm, and Go module proxies.

---

[Unreleased]: https://github.com/scitrera/aether/compare/v0.1.60...HEAD
[0.1.60]: https://github.com/scitrera/aether/compare/v0.1.59...v0.1.60
[0.1.59]: https://github.com/scitrera/aether/compare/v0.1.58...v0.1.59
[0.1.58]: https://github.com/scitrera/aether/releases/tag/v0.1.58
