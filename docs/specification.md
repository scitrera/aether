# Aether System Specification v4.0

> **Status:** This specification reflects the implemented system as of v0.1.0.
> Previous versions described pre-implementation design intent. v4.0 is derived
> from the running codebase and supersedes all prior spec documents.

## 0. Purpose & Scope

Aether is a distributed control plane for routing structured messages, tracking tasks, and managing connection lifecycles. **Aether does not execute user code or internal workflows.** It coordinates external agents, tasks, and engines that perform work.

**Defining property:** The active gRPC connection itself functions simultaneously as the distributed lock for an identity and as its liveness proof. When the TCP stream closes, the lock releases and the identity becomes available for reconnection. This eliminates heartbeat polling, simplifies failure detection, and makes connection state the single source of truth.

---

## 1. Core Concepts

### 1.1 Principal Types

Every connection authenticates as exactly one of eight principal types:

| Type | Identity Fields | Uniqueness | Purpose |
|---|---|---|---|
| **Agent** | `workspace` + `implementation` + `specifier` | Exactly one connection per identity | Long-running service |
| **Task (Unique)** | `workspace` + `implementation` + `unique_specifier` | Exactly one connection per identity | Named finite unit of work (e.g., "nightly-backup") |
| **Task (Non-Unique)** | `workspace` + `implementation` (server assigns ID) | Multiple connections allowed | Workers competing on a shared broadcast topic |
| **User** | `user_id` + `window_id` | One connection per window | Human operator; multiple browser tabs allowed via distinct window IDs |
| **Workflow Engine** | `wfe::shard{N}` (server-assigned) | One active connection per shard (today: exactly one, `wfe::shard0`) | Sole subscriber to the assigned `event::receiver{shard}` fan-in topic; processes broadcast events. Currently a singleton; N-shard parallelism planned for a future release. |
| **Metrics Bridge** | `metric::receiver{N}` (server-assigned) | One active connection per shard (today: exactly one) | Sole subscriber to the assigned `metric::receiver{shard}` fan-in topic; receive-only with offset-tracked replay on reconnect. |
| **Orchestrator** | `implementation` + `specifier` | One per specifier | Receives task assignments to spin up compute on demand |
| **Service** | `implementation` + `specifier` | One per specifier | Cross-workspace HTTP-over-Aether service proxy; addressable via `sv.*` topics |
| **Bridge** | `implementation` + `specifier` | One per specifier | Cross-workspace messaging integration (Discord, Teams, email, etc.); sends to any workspace subject to per-message ACL |

### 1.2 Connection = Lock = Heartbeat

- **Implicit Locking:** An active gRPC stream connection represents the distributed lock for that identity. No separate lock API exists.
- **Exclusivity:** Two clients cannot hold the same identity simultaneously. A second connection attempt for an occupied identity is rejected with `DuplicateIdentityError`.
- **Liveness:** The TCP/gRPC connection state serves as the heartbeat. The gateway refreshes a Redis lock TTL (30s) every 10 seconds while the connection is alive. If the gateway crashes, the lock auto-expires.
- **Session Resume:** A reconnecting client may provide a `resume_session_id` to atomically take over an existing lock (e.g., after a brief network partition). This uses a Lua script for atomic check-and-swap.

### 1.3 Message Types

Messages carry a `message_type` enum:

| Type | Description |
|---|---|
| `OPAQUE` | **Default for SDK generic send helpers.** Sender and receiver own the payload schema; the gateway forwards verbatim, performs no payload decoding, and skips text-based audit content extraction. Use this for any structured app payload (JSON, JSX, RPC envelopes, binary blobs). |
| `CHAT` | True conversational text between participants. Pass explicitly when the payload is human-readable chat — bare generic sends now default to `OPAQUE`. |
| `CONTROL` | System control signals (start, stop, configure) |
| `TOOL_CALL` | Tool invocation requests and responses |
| `EVENT` | Broadcast events routed to the Workflow Engine |
| `METRIC` | Telemetry data routed to the Metrics Bridge |

### 1.4 Message Envelope

All messages are wrapped in a server-stamped `MessageEnvelope` before publishing to RabbitMQ Streams:

```protobuf
message MessageEnvelope {
  string source = 1;          // Server-verified sender topic (cannot be spoofed)
  bytes payload = 2;
  MessageType message_type = 3;
  int64 timestamp_ms = 4;     // Server-assigned timestamp
}
```

---

## 2. System Architecture

### 2.1 Server Components

| Component | Location | Responsibility |
|---|---|---|
| **Gateway Server** | `internal/gateway/` | gRPC bidirectional stream handling, authentication, connection lifecycle, message routing, KV/checkpoint operations |
| **Router** | `internal/router/` | Topic-to-RabbitMQ-stream mapping, producer pool management with idle eviction, shared consumer fan-out with offset tracking |
| **Session Registry** | `internal/state/session.go` | Redis `SetNX`-based distributed locks with TTL, session metadata, stale lock cleanup |
| **KV Store** | `internal/kv/` | Hierarchical configuration store backed by Redis with namespace-scoped access control |
| **Checkpoint Store** | `internal/checkpoint/` | Redis-backed persistent state checkpointing for agents and tasks |
| **Task Store** | `pkg/tasks/` | PostgreSQL-backed task lifecycle management (create, assign, complete, fail, retry, purge) |
| **ACL Service** | `internal/acl/` | Role-based access control for workspace access |
| **Audit Logger** | `internal/audit/` | Batched, configurable event capture with retention policies |
| **Orchestration** | `internal/orchestration/` | Task dispatch via AMQP, claim-based delivery, orchestrator profile management |
| **Admin Server** | `internal/admin/` | REST API + embedded UI for operations; separate ops server for health probes and Prometheus metrics |
| **Auth Proxy** | `cmd/auth-proxy/` | Standalone authentication/authorization gateway for external services (e.g., MemoryLayer) |

### 2.2 External Dependencies

> **Note:** This section describes the full deployment mode. AetherLite mode replaces all external services with embedded equivalents — see [Section 2.3a](#23a-aetherlite-mode) below.

| Service | Purpose | Required (full mode) |
|---|---|---|
| **Redis** (or Valkey) | Session locks, KV store, checkpoint store, task tokens, quota counters | Yes |
| **RabbitMQ** (Streams plugin) | Persistent message routing with consumer offset tracking | Yes |
| **PostgreSQL** | Task persistence, orchestration profiles, ACL rules, API tokens, audit log | Optional (graceful degradation: task/orchestration/ACL/audit features disabled) |

### 2.3a AetherLite Mode

AetherLite is an alternative deployment mode that eliminates all external service dependencies by substituting in-process backends. It is activated via the `--lite` flag on any binary, or via `mode: lite` in the YAML config. The `cmd/aetherlite` binary always runs in lite mode.

**Backend substitutions:**

| Full Mode | AetherLite Equivalent |
|---|---|
| Redis (sessions, KV, checkpoints, tokens) | Badger embedded KV database |
| RabbitMQ Streams (message routing) | Badger-backed persistent log with consumer offset tracking |
| PostgreSQL | SQLite (via `sqlite_compat` driver; PG syntax is rewritten at the driver layer) |
| AMQP task dispatcher | In-memory polling dispatcher |
| Redis quota counters | In-memory quota manager (ephemeral — resets on restart) |

**Scope and limitations:**
- Single-node only. Badger and SQLite are local files; they cannot be shared across gateway instances.
- Quota counters are in-memory and reset on process restart.
- Message replay is supported: Badger stores the full message log with per-topic consumer offsets.
- The gRPC protocol, topic schema, principal types, and all client SDKs are identical to full mode.
- Horizontal scaling requires migrating to full mode. See [`docs/aetherlite.md`](aetherlite.md).

### 2.3 Messaging Backbone

RabbitMQ **Streams** (not classic queues) provide:
- Persistent, replayable message logs
- Consumer offset tracking for at-least-once delivery
- Per-topic producer pools with health checks and idle eviction
- Shared consumers with local fan-out to reduce RabbitMQ connections

The Router automatically declares streams on first publish or subscribe.

---

## 3. Connection & Authentication

### 3.1 Connection Flow

```
Client                         Gateway                        Redis / RabbitMQ
  |                               |                                |
  |-- InitConnection ------------>|                                |
  |                               |-- Authenticate (mTLS/OAuth) -->|
  |                               |-- AcquireLock (SetNX+TTL) --->|
  |                               |<- Lock granted ---------------|
  |                               |-- ACL Check ----------------->|
  |                               |<- Access granted -------------|
  |                               |-- Quota Check + Increment --->|
  |                               |-- Register Session ---------->|
  |                               |-- Subscribe to topic(s) ----->|
  |<-- ConnectionAck (sessionID) -|                                |
  |<-- ConfigSnapshot (KV) -------|                                |
  |                               |                                |
  |<======= message loop ========>|<====== stream I/O ============>|
  |                               |                                |
  |-- (disconnect) -------------->|                                |
  |                               |-- Unsubscribe --------------->|
  |                               |-- ReleaseLock --------------->|
  |                               |-- UnregisterSession --------->|
```

### 3.2 Authentication Methods

Authentication methods are configured via `auth.modes` in the YAML config. Multiple methods can be active simultaneously.

| Method | Mechanism | Identity Source |
|---|---|---|
| **mTLS (strict)** | Client certificate CN encodes full identity | Certificate |
| **mTLS (relaxed)** | Certificate confirms principal type only; details from `InitConnection` | Certificate + `InitConnection` |
| **Task Token** | Short-lived Redis-backed token issued by orchestration system | Token validates target identity |
| **API Key** | Long-lived PostgreSQL-stored token with workspace pattern matching | `InitConnection` (token authorizes workspace access) |
| **OAuth/JWT** | JWT validated against JWKS endpoint with claims mapping | JWT claims |
| **None** | No authentication (mTLS not required, no credentials) | `InitConnection` only |

**Precedence:** mTLS identity > Task token validation > API key / OAuth credential authentication.

### 3.3 Session Lifecycle

1. **Lock Acquisition:** Atomic `SetNX` in Redis with 30-second TTL. Supports resume via Lua script.
2. **ACL Check:** Workspace access verified before the session becomes discoverable. System principals (Orchestrator, WorkflowEngine, MetricsBridge) skip workspace ACL checks.
3. **Quota Check:** Per-workspace connection count atomically checked and incremented via Lua script.
4. **Session Registration:** Session metadata stored in Redis with matching TTL.
5. **Lock Refresh:** Background goroutine refreshes lock and session TTL every 10 seconds. If refresh fails (lock stolen), the session is disconnected.
6. **Cleanup on Disconnect:** Unsubscribe topics, release lock, decrement quota, update orchestration task state, audit log.

---

## 4. Messaging Topology & Topics

### 4.1 Topic Schema

| Prefix | Target | Format | Description |
|---|---|---|---|
| `ag` | Agent | `ag::{workspace}::{impl}::{spec}` | Specific long-running agent instance |
| `tu` | Unique Task | `tu::{workspace}::{impl}::{unique_spec}` | Named task instance |
| `ta` | Assigned Task | `ta::{workspace}::{impl}::{task_id}` | Server-assigned non-unique task instance |
| `tb` | Task Broadcast | `tb::{workspace}::{impl}` | Load-balancing topic; workers compete/round-robin |
| `us` | User (Window) | `us::{user_id}::{window_id}` | Specific browser window |
| `uw` | User (Workspace) | `uw::{user_id}::{workspace}` | User scoped to a workspace |
| `ga` | Global Agents | `ga::{workspace}` | Broadcast to all agents in a workspace |
| `gu` | Global Users | `gu::{workspace}` | Broadcast to all users in a workspace |
| `pg` | Progress | `pg::{workspace}` | Progress updates with server-side recipient filtering |
| `event.*` | Workflow Engine | **Write (senders):** `event.{workspace}` or `event::*` — gateway rewrites to `event::receiver{shard}`. **Subscribe (WE):** `event::receiver{shard}` (today: always `event::receiver0`). Workspace attribution surfaced via `IncomingMessage.workspace`. | Fan-in event routing; Workflow Engine is the sole subscriber per shard. |
| `metric.*` | Metrics Bridge | **Write (senders):** `metric.{workspace}` or `metric::*` — gateway rewrites to `metric::receiver{shard}`. **Subscribe (MB):** `metric::receiver{shard}` (today: always `metric::receiver0`). Workspace attribution surfaced via `IncomingMessage.workspace`. | Fan-in metric routing; Metrics Bridge is the sole subscriber per shard. |
| `sv` | Service | `sv::{impl}::{spec}` | Cross-workspace service proxy endpoint (no workspace component) |
| `br` | Bridge | `br::{impl}::{spec}` | Cross-workspace messaging bridge endpoint (no workspace component) |

Topic names are validated against allowed prefixes and must be 1-256 characters.

### 4.2 Subscription Model

Each principal type automatically subscribes to specific topics on connection:

| Principal | Exclusive (offset-tracked) | Shared (no offset) |
|---|---|---|
| **Agent** | `ag::{ws}::{impl}::{spec}` | `ga::{ws}`, `pg::{ws}` |
| **Task (Unique)** | `tu::{ws}::{impl}::{spec}` | — |
| **Task (Non-Unique)** | `ta::{ws}::{impl}::{id}` | `tb::{ws}::{impl}` |
| **User** | `us::{uid}::{wid}` | `gu::{ws}`, `uw::{uid}::{ws}`, `pg::{ws}` |
| **Workflow Engine** | `event::receiver{shard}` (today: `event::receiver0`) | — |
| **Metrics Bridge** | `metric::receiver{shard}` (today: `metric::receiver0`) | — |
| **Orchestrator** | — | — (receives tasks via direct gRPC stream) |

**Exclusive subscriptions** use RabbitMQ consumer offset tracking so messages are replayed from the last committed position on reconnection. **Shared subscriptions** fan out locally from a single RabbitMQ consumer.

### 4.3 Permission Matrix

| Sender | Can Send To | Cannot Send To |
|---|---|---|
| **Agent** | Agents, Tasks, Users, Events, Metrics | Orchestrators, Progress |
| **Task** | Agents, Tasks, Users, Events, Metrics | Orchestrators, Progress |
| **User** | Agents, Tasks, Users | Events, Metrics, Progress |
| **Workflow Engine** | Agents, Tasks, Users, Events, Metrics | — |
| **Metrics Bridge** | *None (receive only)* | All |
| **Orchestrator** | Agent/Task topics only (status updates) | Events, Metrics |
| **Service** | Any topic (cross-workspace); per-message ACL checked against target workspace | — |
| **Bridge** | Any topic in any workspace; per-message ACL checked against target workspace | — |

**Cross-workspace enforcement:** Workspace-scoped principals cannot send to topics in other workspaces. The workspace component of the target topic must match the sender's workspace.

**Cross-workspace event/metric broadcast:** Sending to `event.*` or `metric.*` topics in a workspace other than the sender's own native workspace requires the `capability/event_broadcast` or `capability/metric_broadcast` ACL permission respectively. Sending to the sender's own workspace is implicitly permitted.

**Metric payload enforcement:** Messages with type `METRIC` sent to `metric.*` topics must carry a valid `Metric` proto as their payload (see [Section 4.5](#45-metric-payload-schema)). The gateway validates and rejects malformed metric messages before they reach the Metrics Bridge.

### 4.4 Progress Reports

Agents and tasks can send `ProgressReport` messages upstream. The gateway:

1. Validates the sender (only agents/tasks can report progress).
2. Publishes a `ProgressUpdate` to `pg::{workspace}` via RabbitMQ Streams.
3. Updates the associated task heartbeat in PostgreSQL (side-effect).

Subscribers receive progress updates through a per-client filtering handler that:
- Suppresses self-echo (sender doesn't receive their own progress).
- Applies server-side recipient filtering (optional `recipient` field).

### 4.5 Metric Payload Schema

All `METRIC` messages must carry a `Metric` proto as their `payload` bytes. The gateway rejects messages that do not conform.

```protobuf
message MetricEntry {
  string name  = 1;  // Non-empty metric name (e.g. "tokens_in", "time_seconds")
  string kind  = 2;  // Free-form sub-classifier (e.g. "modelA", "" for none)
  double qty   = 3;  // Additive delta; must be finite (not NaN/Inf)
}

message Metric {
  string                trace_id           = 1;  // Optional correlation ID
  repeated MetricEntry  entries            = 2;  // At least one entry required
  map<string, string>   metadata           = 3;  // Arbitrary key-value tags
  int64                 client_timestamp_ms = 4; // Client-side epoch ms (advisory)
}
```

**Invariants enforced by the gateway:**

| Rule | Error Code |
|---|---|
| Payload must unmarshal as `Metric` | `ERR_METRIC_INVALID` |
| `entries` must be non-empty (and ≤ 1024) | `ERR_METRIC_EMPTY` |
| `metadata` must have ≤ 64 keys | `ERR_METRIC_INVALID` |
| Each entry: `name` non-empty, `qty` finite (not NaN/Inf) | `ERR_METRIC_INVALID_ENTRY` |
| Any negative `qty` requires ACL permission `capability/metric_credit` (checked at `AccessAdmin`) | `ERR_METRIC_NEGATIVE_FORBIDDEN` |

**All entries are additive deltas.** The Metrics Bridge accumulates them; the gateway does not aggregate.

**Mixed-sign batches are allowed** for grant-holders: a single `Metric` can contain both positive and negative entries — the credit gate fires whenever any entry is negative.

**Lifecycle signaling:** To annotate a metric batch with lifecycle context (e.g. startup or shutdown), set `metadata["lifecycle"]` to a well-known value such as `"startup"` or `"shutdown"`. This replaces the former flags-based convention.

**`client_timestamp_ms` is advisory:** the server stamps its own authoritative `MessageEnvelope.timestamp_ms`. Downstream consumers must prefer the envelope timestamp for ordering; `client_timestamp_ms` is only a client-side hint that may help reconstruct intent in batched/replayed scenarios.

**On-behalf-of (OBO) credit grants:** when a sender invokes with an `AuthorizationContext` (on_behalf_of mode), the negative-delta gate evaluates the **subject's** `capability/metric_credit` rather than the sender's. This lets a trusted task or service mint credits on behalf of a holder without itself being granted the capability.

**Example:**

```json
{
  "trace_id": "req-abc123",
  "entries": [
    {"name": "tokens_in",  "kind": "gpt-4o", "qty": 512},
    {"name": "latency_ms", "kind": "",       "qty": 34.7}
  ],
  "metadata": {"source.region": "us-east-1", "lifecycle": "shutdown"},
  "client_timestamp_ms": 1713500000000
}
```

### 4.6 Fan-in Sharding (Event & Metric Topics)

Events and metrics are routed through a **fan-in** layer that decouples workspace-scoped senders from the singleton (or eventually sharded) consumer.

#### Design

- **Sender write API is unchanged.** Agents, tasks, and workflow engines send to `event.{workspace}` or `metric.{workspace}` exactly as before. The gateway rewrites the destination to the appropriate receiver shard before publishing to RabbitMQ Streams.
- **Receiver topics** follow the pattern `event::receiver{N}` and `metric::receiver{N}`. Today only shard 0 exists.
- **Shard assignment** (`ShardForWorkspace`) is a stub that always returns 0. A future release will use fnv64 hashing to distribute workspaces across N shards, enabling N parallel Workflow Engine and Metrics Bridge instances.
- **Exclusive offset-tracked subscriptions** are used for all receiver topics. Both the Workflow Engine and Metrics Bridge reconnect to the last committed offset, providing replay-on-reconnect and at-least-once delivery semantics.

#### `IncomingMessage.workspace`

Because all workspaces fan in to a single receiver stream, the gateway populates the `IncomingMessage.workspace` field with the effective declared workspace from `SendMessage.app_workspace`. Consumers **must** read this field to determine which workspace an event or metric originated from; the receiver topic itself carries no workspace information.

#### Current limits (shard stub)

| Parameter | Today | Future |
|---|---|---|
| Shard count | 1 (constant) | Controlled by `AETHER_FANIN_SHARDS` env var |
| WE identity | `wfe::shard0` (enforced singleton) | `wfe::shard{N}` |
| MB identity | `metric::receiver0` (enforced singleton) | `metric::receiver{N}` |

A second Workflow Engine or Metrics Bridge connection attempt fails with `DuplicateIdentityError` until sharding goes live.

---

## 5. Orchestration & Lazy Loading

### 5.1 Trigger

When the gateway receives a message targeting an `ag.*` or `tu.*` topic and the target is not currently connected:

1. **Local check:** `identityIndex` (O(1) sync.Map lookup) for locally connected clients.
2. **Distributed check:** Redis `EXISTS` on the identity's lock key.
3. **If offline:** Create an orchestration task and dispatch to a matching orchestrator.

Messages are still published to the RabbitMQ stream regardless of target availability. Since streams are persistent and exclusive subscriptions resume from the last committed offset, the target will receive all queued messages when it reconnects.

### 5.2 Task Assignment Flow

1. Gateway creates a task in PostgreSQL with the target identity and orchestrator profile.
2. The orchestration dispatcher publishes a notification to the AMQP orchestration queue.
3. One of the connected gateways claims the task atomically (distributed claim via PostgreSQL).
4. The claiming gateway looks up a matching orchestrator by profile and workspace.
5. A `TaskAssignment` message is sent to the orchestrator via the gRPC stream, including:
   - Task ID, target identity, workspace, launch parameters
   - A short-lived authentication token (stored in Redis, 24h TTL)
6. The orchestrator spins up compute (container, VM, etc.) with the token.
7. The agent connects with the token as its credential.
8. The gateway validates the token, marks the task as "running", and delivers baseline config.

### 5.3 Orchestrator Profile Management

Orchestrators register their supported profiles on connection. The gateway stores this in PostgreSQL and uses heartbeat updates (piggy-backed on lock refresh) to track orchestrator liveness.

### 5.4 Task Lifecycle

Tasks transition through these states:

```
pending → assigned → starting → running → completed
                              ↘ failed → (retry) → pending
                              ↘ cancelled
```

Background cleanup jobs purge old tasks based on configurable retention policies.

### 5.5 Task Status Notifications

When a task transitions state (running, completed, failed, cancelled, pending via retry), the gateway publishes a synthetic `ProgressUpdate` to `pg::{workspace}` with the `recipient` field set to the parent agent's topic (the `ParentAgentID` stored on the task). This ensures:

- Only the spawning agent receives the notification (server-side recipient filtering).
- The notification uses existing progress infrastructure — no additional proto types needed.
- Delivery is persistent (RabbitMQ Streams) and survives brief disconnections.
- Notifications are best-effort: failures are logged but do not block the state transition.

The `ProgressUpdate` fields are populated as follows:

| Field | Value |
|---|---|
| `source` | Gateway ID (system-generated, not a client topic) |
| `task_id` | The task that changed state |
| `state` | New status: `running`, `completed`, `failed`, `cancelled`, `pending` |
| `summary` | Human-readable description (includes error message for failures) |
| `recipient` | Parent agent's topic (e.g., `ag.default.orchestrator.main`) |
| `workspace` | Task's workspace |

Notifications are sent at these transition points:

| Transition | Trigger |
|---|---|
| → `running` | Orchestrated agent connects and validates its task token |
| → `completed` | Agent disconnects gracefully (clean EOF) |
| → `failed` | Agent disconnects unexpectedly, or task is explicitly failed |
| → `cancelled` | Client sends `TaskOperation_CANCEL` |
| → `pending` | Client sends `TaskOperation_RETRY` |

---

## 6. KV Store

### 6.1 Scopes

The KV store provides hierarchical, namespace-scoped configuration:

| Scope | Redis Key Pattern | Write Access | Purpose |
|---|---|---|---|
| **Global** | `kv:agent:{impl}.{spec}:global` | Owning agent/task | Cross-workspace agent state |
| **Workspace** | `kv:agent:{impl}.{spec}:ws:{workspace}` | Platform only (read-only for agents) | Workspace configuration pushed on connect |
| **User** | `kv:agent:{impl}.{spec}:user:{user_id}` | Owning agent/task | Per-user agent state |
| **User-Workspace** | `kv:agent:{impl}.{spec}:user:{user_id}:ws:{workspace}` | Owning agent/task | Per-user, per-workspace state |

Namespace components are sanitized to prevent traversal via `:` injection.

### 6.2 Operations

| Operation | Description |
|---|---|
| `GET` | Retrieve a value by key |
| `PUT` | Set a value (with optional TTL) |
| `DELETE` | Remove a key |
| `LIST` | List all keys/values in a namespace (with pagination support) |
| `INCREMENT` | Atomic counter increment (with optional TTL on first increment) |
| `DECREMENT` | Atomic counter decrement |

### 6.3 Baseline Config

On connection, agents and tasks receive a `ConfigSnapshot` containing:
- **Workspace KV:** All keys in the workspace scope for their implementation.
- **Global KV:** All keys in the global scope for their implementation.
- **Task Context:** If an active orchestration task exists, its metadata and launch parameters are included.

---

## 7. Checkpoint Store

Checkpoints provide persistent state storage for agents and tasks that survives restarts. This is separate from RabbitMQ offset tracking (which handles message stream position).

| Operation | Description |
|---|---|
| `SAVE` | Store arbitrary bytes with optional TTL (-1 = server default, 0 = no expiration, >0 = specific seconds) |
| `LOAD` | Retrieve checkpoint data and metadata |
| `DELETE` | Remove a checkpoint |
| `LIST` | List all checkpoint keys for the identity |

Redis key format: `checkpoint:{identity}:{key}`

---

## 8. Access Control

### 8.1 ACL Model

- **Design:** Role-Based Access Control (RBAC) backed by PostgreSQL.
- **Default:** ACL enforcement is workspace-aware but defaults to allow-all when no explicit rules are configured.
- **Scope:** ACL checks are performed on connection (workspace access) and on message send.

### 8.2 On-Behalf-Of Authority Grants

When a principal (e.g., a user) spawns a task or agent via orchestration, authority lineage is tracked through OBO (On-Behalf-Of) authority grants stored in `acl_authority_grants`. The derived principal inherits a scoped, capped access level from the root principal, with explicit hop limits and optional expiry. Delegation chains were removed in favor of OBO authority grants.

### 8.3 System Principals

Orchestrators, Workflow Engines, and Metrics Bridges operate at the system level and skip workspace-based ACL checks. In production, these should be authenticated via mTLS with dedicated certificates.

---

## 9. Quotas & Rate Limiting

### 9.1 Per-Client Rate Limiting

Each client connection has a `rate.Limiter` (token bucket) configured at gateway level. Messages exceeding the rate receive `ERR_RATE_LIMITED` with a `retry_after_ms` hint.

### 9.2 Per-Workspace Quotas

When quotas are enabled, the following limits are enforced per workspace:

| Quota | Default | Enforcement |
|---|---|---|
| Max connections per workspace | 1000 | Atomic Lua check-and-increment on connect |
| Max message rate per identity | 100/s | Redis sliding window counter |
| Max KV keys per namespace | 10000 | Count check on PUT |
| Max KV value size | 1MB | Size check on PUT |

Workspace-specific overrides can be set via the admin API and are cached in-memory.

### 9.3 Circuit Breakers

Redis and RabbitMQ operations are protected by circuit breakers:
- **Redis breaker:** Protects lock refresh calls. When open, lock refreshes are skipped (the lock TTL provides a grace period).
- **RabbitMQ publish breaker:** Protects message publishing. When open, clients receive `ERR_CIRCUIT_OPEN`.

---

## 10. Observability

### 10.1 Metrics (Prometheus)

| Metric | Type | Labels |
|---|---|---|
| `aether_messages_routed_total` | Counter | `workspace`, `message_type` |
| `aether_message_errors_total` | Counter | `workspace`, `error_type` |
| `aether_active_connections` | Gauge | `workspace`, `principal_type` |
| `aether_connection_duration_seconds` | Histogram | `workspace`, `principal_type` |
| `aether_message_routing_latency_seconds` | Histogram | `workspace` |
| `aether_kv_operation_latency_seconds` | Histogram | `operation`, `scope` |
| `aether_kv_operations_total` | Counter | `operation`, `scope`, `status` |
| `aether_connection_attempts_total` | Counter | `workspace`, `principal_type`, `status` |
| `aether_orchestration_triggers_total` | Counter | `workspace` |
| `aether_topic_subscriptions_active` | Gauge | — |

### 10.2 Tracing

OpenTelemetry tracing is integrated via `otelgrpc`. Trace spans cover:
- `gateway.Connect`, `gateway.RouteMessage`, `gateway.KVOperation`
- `gateway.AcquireSessionLock`, `gateway.AuthenticateMTLS`, `gateway.ResolveIdentity`
- `gateway.CleanupSession`

### 10.3 Audit Logging

Configurable audit events are batched and written to PostgreSQL:

| Event Type | Operations Captured |
|---|---|
| `connection` | Lock acquired/rejected, session registered, connection closed |
| `auth` | mTLS success/failure, token validation, credential auth, identity resolution |
| `message` | Message received, routed, route failed |
| `kv` | GET, PUT, DELETE, LIST, INCREMENT, DECREMENT |
| `admin` | Admin API operations |
| `acl` | ACL check results |

Verbosity levels control what metadata is included per event:

| Level | Fields included |
|---|---|
| `low` (default) | Routing metadata only (actor, target topic, timestamp) |
| `medium` | Adds message type and payload size |
| `high` | Adds truncated message content (first 1 KB); credential-shaped metadata keys are automatically redacted |

**Metadata redaction at high verbosity.** When `verbosity: high` is configured, the audit logger applies key-pattern redaction to the `metadata` map before it is batched for storage. Keys whose names contain (case-insensitively) any of the following substrings have their values replaced with `"[REDACTED]"`: `password`, `passwd`, `api_key`, `apikey`, `secret`, `token`, `bearer`, `authorization`, `credential`, `private_key`, `privatekey`. Nested maps and slices are walked recursively. The original key name is preserved so audit readers can see the field existed.

**Residual risk.** Redaction is best-effort pattern matching on key names. Callers that embed sensitive values under non-credential-shaped keys (e.g., `"data"`, `"payload"`, `"value"`) will not have those values redacted. Operators should not rely solely on this mechanism for sensitive payloads; instead, avoid passing credential material in audit metadata at all.

### 10.4 Health Probes

| Endpoint | Port | Purpose |
|---|---|---|
| `/health/live` | Ops (9090) | Liveness probe — always returns 200 |
| `/health/ready` | Ops (9090) | Readiness probe |
| `/health/startup` | Ops (9090) | Startup probe |
| `/metrics` | Ops (9090) | Prometheus metrics endpoint |
| gRPC Health | Gateway (50051) | Standard gRPC health check service |

---

## 11. API Surface

### 11.1 gRPC Connection Stream

```protobuf
service AetherGateway {
  rpc Connect (stream UpstreamMessage) returns (stream DownstreamMessage);
}
```

### 11.2 Upstream Messages (Client → Server)

| Message | Purpose |
|---|---|
| `InitConnection` | First message; declares principal type, identity, and credentials |
| `SendMessage` | Route a message to a target topic |
| `SwitchWorkspace` | User only: change workspace subscription bindings |
| `KVOperation` | GET, PUT, DELETE, LIST, INCREMENT, DECREMENT on KV store |
| `CheckpointOperation` | SAVE, LOAD, DELETE, LIST on checkpoint store |
| `CreateTaskRequest` | Create an orchestration task |
| `ProgressReport` | Report progress on a running task |
| `TaskQuery` | Query task status (GET, LIST with filters) |
| `TaskOperation` | Task lifecycle operations (CANCEL, RETRY) |

### 11.3 Downstream Messages (Server → Client)

| Message | Purpose |
|---|---|
| `ConnectionAck` | Session ID for reconnection; `resumed` flag |
| `ConfigSnapshot` | Workspace KV + global KV + task context on connect |
| `IncomingMessage` | Routed message with server-verified source topic |
| `KVResponse` | KV operation result with request ID correlation |
| `CheckpointResponse` | Checkpoint operation result |
| `TaskAssignment` | Orchestrator: task to execute with launch params and auth token |
| `TaskQueryResponse` | Task query results |
| `TaskOperationResponse` | Task operation result |
| `ProgressUpdate` | Progress report from another agent/task |
| `Signal` | Server signals: `FORCE_DISCONNECT` (shutdown, admin) |
| `ErrorResponse` | Structured error with code, message, retryable flag, retry_after_ms |

### 11.4 Admin REST API

The admin server provides a full REST API (when enabled) for:
- Session management (list, disconnect, inspect)
- KV browsing and manipulation
- Token management (create, list, revoke, delete)
- ACL rule management
- Workspace management
- Agent registry management
- Orchestrator profile management
- WebSocket-based live topic monitoring

See `docs/admin-ui.md` for the complete endpoint reference.

---

## 12. Horizontal Scaling

### 12.1 Architecture

- **Stateless gateways:** All state lives in Redis and PostgreSQL. Gateway instances share nothing.
- **Distributed locking:** Redis `SetNX` ensures identity uniqueness across all gateway instances.
- **Session affinity:** Load balancer uses ClientIP (Kubernetes) or cookies (nginx) to route reconnecting clients to the same instance when possible.
- **Automatic failover:** If a gateway crashes, lock TTLs expire (30s), clients reconnect to a different instance, and RabbitMQ Streams preserve message offsets.

### 12.2 Multi-Gateway Orchestration

Task claims use PostgreSQL-backed atomic operations. When the orchestration dispatcher publishes a task notification:
1. All gateway instances receive the notification via AMQP.
2. Each checks for a locally connected orchestrator matching the profile.
3. Only one gateway successfully claims the task (distributed claim).
4. The winning gateway delivers the task to its local orchestrator.

### 12.3 Deployment Options

- **Kubernetes:** Deployment, Service (ClientIP affinity), Ingress (cookie affinity), ConfigMap, cert-manager integration.
- **Docker Compose:** Multi-instance setup with nginx load balancer.
- **Dockerfile:** Multi-stage build, non-root user, health check, exposes ports 50051 (gRPC), 9090 (ops), 31880 (admin).

---

## 13. Configuration

Configuration is loaded from a YAML file with environment variable overrides and CLI flag overrides.

### 13.1 Key Configuration Sections

| Section | Purpose |
|---|---|
| `gateway` | Port, gateway ID, message rate limit, circuit breaker |
| `admin` | Admin UI enable/port, API key, TLS, CORS, rate limiting |
| `auth` | Authentication modes, mTLS config, OAuth providers, token HMAC key |
| `postgres` | Host, port, database, credentials, connection pool |
| `redis` | Cluster addresses, mode (auto/single/cluster), pool tuning |
| `rabbitmq` | Stream URL, AMQP URL, stream capacity |
| `audit` | Enable, event types, verbosity, batch size, retention |
| `cleanup` | Task purge interval, retention periods, reconciliation |
| `checkpoint` | Default TTL |
| `kv` | Default TTL, max TTL |
| `shutdown` | Graceful timeout |
| `quotas` | Enable, per-workspace connection/rate/KV limits |

### 13.2 Graceful Shutdown

1. Gateway receives SIGINT/SIGTERM.
2. Sends `FORCE_DISCONNECT` signal to all connected clients.
3. Waits for active connections to drain (configurable timeout, default 30s).
4. Calls `grpcServer.GracefulStop()` with timeout fallback to `Stop()`.
5. Closes admin server, ops server, cleanup runner, ACL service, router.

---

## 14. Client SDKs

### 14.1 Go SDK (`sdk/go/`)

Full-featured SDK with typed clients for all six principal types. Features:
- `AgentClient`, `TaskClient`, `UserClient`, `OrchestratorClient`, `WorkflowEngineClient`, `MetricsBridgeClient`
- Synchronous and asynchronous KV and checkpoint operations
- Auto-reconnection with exponential backoff and jitter
- Task creation with targeted, broadcast, and auto assignment modes
- Full TLS/mTLS support
- Typed error hierarchy with `IsRecoverable()`, `IsConnectionError()`, `IsTimeoutError()`
- Reference orchestrator implementations: Docker (`sdk/go/orchestrators/docker/`) and Echo (`sdk/go/orchestrators/echo/`)

### 14.2 Python SDK (`sdk/python-client/`)

Sync (`AgentClient`) and async (`AsyncAgentClient`) variants with feature parity:
- All principal types, messaging, KV, checkpointing, task creation
- Multiprocess orchestrator reference implementation
- Async context manager support

### 14.3 TypeScript SDK (`sdk/typescript/`)

Agent and User clients with gRPC transport:
- Auto-reconnect, KV operations, TLS, auth
- Typed error hierarchy and topic helpers

---

## 15. Scenarios

### 15.1 Cold Start Agent

1. User sends message to `ag.default.gpu-worker.01`.
2. Gateway checks `identityIndex` — not locally connected.
3. Gateway checks Redis — no active lock.
4. Gateway publishes message to `ag.default.gpu-worker.01` stream (persisted).
5. Gateway creates orchestration task for `gpu-worker` implementation.
6. Dispatcher publishes task notification to AMQP queue.
7. A gateway with a connected orchestrator claims the task.
8. `TaskAssignment` + auth token sent to orchestrator via gRPC stream.
9. Orchestrator spins up a container with the token.
10. Container connects as `ag.default.gpu-worker.01` with the token.
11. Gateway validates token, acquires lock, subscribes to topic.
12. Agent receives the persisted message from RabbitMQ stream (offset replay).

### 15.2 User Workspace Switch

1. Alice is in `Finance` workspace, receiving on `gu.finance`, `uw.alice.finance`, `pg.finance`.
2. Alice clicks "HR" in the UI.
3. Client sends `SwitchWorkspace("HR")`.
4. Gateway checks ACL for Alice's access to workspace "HR".
5. If denied, sends `ERR_PERMISSION_DENIED` to client.
6. If allowed, unsubscribes from `gu.finance`, `uw.alice.finance`, `pg.finance`.
7. Updates Alice's identity workspace under write lock.
8. Subscribes to `gu.hr`, `uw.alice.hr`, `pg.hr`.
9. Alice's `us.alice.browser1` subscription remains active (window-scoped, independent of workspace).

### 15.3 Workflow Trigger

1. `ag.finance.payroll.01` finishes processing.
2. Agent sends a message with type `EVENT` to `event.finance`.
3. Gateway verifies the sender is allowed to publish events (agents can).
4. Message is published to the `event.finance` RabbitMQ stream.
5. The Workflow Engine (sole subscriber to `event.finance`) receives it.
6. Workflow Engine evaluates rules and sends a command to `ag.default.email-sender.01`.

---

## 16. Error Codes

Errors are returned as `ErrorResponse` messages with structured codes:

| Code | gRPC Status | Retryable | Description |
|---|---|---|---|
| `ERR_INVALID_TOPIC` | InvalidArgument | No | Invalid topic prefix or length |
| `ERR_PERMISSION_DENIED` | PermissionDenied | No | Permission matrix or ACL violation |
| `ERR_RATE_LIMITED` | ResourceExhausted | Yes | Per-client message rate limit exceeded |
| `ERR_QUOTA_001` | ResourceExhausted | Yes | Per-workspace quota exceeded |
| `ERR_CIRCUIT_OPEN` | Unavailable | Yes | Message broker temporarily unavailable |
| `ERR_PUBLISH_FAILED` | Internal | Yes | Message delivery to RabbitMQ failed |
| `ERR_INVALID_PRINCIPAL` | InvalidArgument | No | Operation not allowed for principal type |
| `ERR_WORKSPACE_SWITCH_FAILED` | Internal | No | Workspace switch failed (ACL or subscription error) |
| `ERR_NOT_IMPLEMENTED` | Unimplemented | No | Operation not supported over streaming API |
| `DuplicateIdentityError` | AlreadyExists | No | Identity already has an active connection |
| `ERR_METRIC_INVALID` | InvalidArgument | No | METRIC payload did not unmarshal as a `Metric` proto |
| `ERR_METRIC_EMPTY` | InvalidArgument | No | `Metric.entries` is empty |
| `ERR_METRIC_INVALID_ENTRY` | InvalidArgument | No | An entry has an empty name or a non-finite `qty` (NaN/Inf) |
| `ERR_METRIC_NEGATIVE_FORBIDDEN` | PermissionDenied | No | Negative `qty` entry requires ACL permission `capability/metric_credit` |
| `ERR_INVALID_IDENTIFIER` | InvalidArgument | No | Identifier token violates charset policy (see Section 17) |

See `docs/error-codes.md` for the complete catalog.

---

## 17. Identifier Charset Policy

### 17.1 Rationale

Aether identifier tokens (workspace names, agent implementations, specifiers, user IDs, window IDs, etc.) are embedded directly into NATS subject strings after translation by the `natscodec` package. Certain characters cause irreversible ambiguity:

- `*` and `>` are NATS wildcard tokens; a literal `*` in an agent workspace would match every topic in every namespace.
- The substring `::` is aether's own segment separator; if it appeared inside a token the resulting topic string would be structurally ambiguous (extra apparent segments).
- ASCII whitespace and control characters (bytes `< 0x20` or `== 0x7F`) are never safe in subject tokens and indicate malformed or adversarially-crafted input.

The `natscodec` package *can* escape these characters (it does so for the internal translation layer), but allowing them at registration time would create identities that behave differently depending on whether they are used via NATS or via the RabbitMQ/PostgreSQL paths. Rejecting them at the ingestion boundary keeps all paths consistent.

### 17.2 Allowed Characters

| Character class | Allowed? | Notes |
|---|---|---|
| ASCII letters (`A-Z`, `a-z`) | Yes | |
| ASCII digits (`0-9`) | Yes | |
| Hyphen (`-`) | Yes | |
| Underscore (`_`) | Yes | |
| Period (`.`) | Yes | Reverse-DNS impl names like `com.example.chat-agent` are explicitly supported; `natscodec` escapes `.` when translating to NATS subjects |
| `*` | **No** | NATS wildcard |
| `>` | **No** | NATS wildcard |
| Space or any whitespace | **No** | |
| Control chars `< 0x20` or `== 0x7F` | **No** | |
| Substring `::` | **No** | Aether segment separator |

Maximum token length: **128 bytes**.

### 17.3 Enforcement Points

Validation runs at every ingestion boundary — any point where a new identifier is first accepted from external input — before any persistent write:

| Boundary | Location | Fields validated |
|---|---|---|
| `InitConnection` (gRPC) | `internal/gateway/auth_handler.go` → `resolveIdentity` | `workspace`, `implementation`, `specifier`, `id` |
| `CreateTaskRequest` (gRPC) | `internal/gateway/orchestration_integration.go` → `handleCreateTask` | `workspace`, `target_implementation` |
| Admin `POST /workspaces` (HTTP) | `internal/admin/workspace_handler.go` → `handleCreateWorkspace` | `workspace_id` |

Validation does **not** run in hot paths (per-message routing, per-lookup ACL) — only at the boundary where an identifier is first persisted.

### 17.4 Configuration

Set `AETHER_STRICT_IDENTIFIER_CHARSET=false` to disable charset validation globally. This opt-out is intended for operators migrating from deployments that pre-date this policy and have existing identifiers that would fail the new rules. The default is `true` (strict validation enabled).

Implementation: `internal/identval/identval.go`.

See `docs/error-codes.md` for the complete catalog.
