# Aether (Agent Fabric)

**Aether is the connective tissue for multi-agent systems.** It is the substrate that long-running agents, finite
tasks, human users, workflow engines, and orchestrators all plug into with typed identity, enforced routing
permissions, durable task lifecycles, and on-demand compute, all with a clear audit trail and served over a single bidirectional gRPC stream per
participant.

It is *not* a general-purpose message bus, a workflow engine, or an RPC framework. It is what those tools don't give
you on their own: a fabric where every participant is a known, addressable, authorized identity, where messages to
offline agents lazily spin up the right compute, and where a task can pause, hibernate, request elevated authority
("sudo"), or be reclaimed by another worker without the surrounding application code knowing any of those mechanics
exist. (Ok but then yes... it is also all those things)

### Deployment tiers

A single protocol surface, served by three different runtime shapes — pick the one that matches your scale and
durability needs:

1. **AetherLite (local mode)** — single binary, embedded SQLite + Badger, no external dependencies. Dev, edge,
   single-tenant self-hosted.
2. **AetherLite (clustered, NATS JetStream)** — same binary, embedded NATS JetStream replaces in-process state and
   adds cross-node messaging. Scales from 1-node test through 2-node async/sync replication to 3+ node quorum HA via
   config only — no code changes.
3. **Aether (full distributed stack)** — separate `gateway` binary backed by PostgreSQL (tasks, audit, ACL,
   registry), Redis (session locks, KV, checkpoints), and RabbitMQ Streams (messaging backbone). Stateless gateway
   replicas behind a load balancer; the original production deployment shape.

All three tiers serve the same wire protocol and SDKs (Go, Python, TypeScript).

Built by [scitrera.ai](https://scitrera.ai).

## Key Features

- **Connection = Lock = Heartbeat** — An active gRPC stream serves simultaneously as the distributed lock for an
  identity and its liveness proof. No separate heartbeat API is needed.
- **Bidirectional Streaming** — A single `rpc Connect` stream multiplexes all client-server communication: messages, KV
  operations, config snapshots, and signals.
- **Distributed Session Management** — Exclusive identity locks with automatic expiry via the configured state backend
  ensure only one connection per identity at any time.
- **Hierarchical KV Store** — Namespace-scoped configuration with global, workspace, user, and user-workspace scopes.
  Config is pushed to connecting clients as a baseline snapshot.
- **First Class Access Control** — Access control is fundamentally baked into every layer which (1) enforces security
    even preventing message sending/routing/receipt based on ACL rules as well as throughout KV, orchestration, etc and
    (2) benefits applications and agent design by handling one of the more critical and error-prone parts of enterprise
    development for you.
- **Workspaces** — Built-in support for "workspaces" which are logical namespaces for agent identities, messages, and KV. 
  By default, workspaces are isolated but ACL rules allow cross-workspace messaging and KV if configured. 
  Applications can use workspaces to organize data and enforce access control.
- **Intent-Based Message Routing** — SDKs provide helpers for topics to enable developers to focus on intent (e.g. send 
  message to some other agent, etc.) rather than the mechanics of topics and fan-in/out targeting. The SDKs and gateway 
  handle logistics for prefix-driven routing with per-principal permission enforcement.
- **Orchestration / Lazy Loading** — When a message targets an offline agent or task, the gateway enqueues the message
  and signals the responsible orchestrator to spin up compute.
- **Built-in Workflow Engine** — Both **DAG-based** workflows (declarative graphs with typed inputs/outputs,
  expression-driven edges, and conditional branching) and **event-driven** workflows (react to messages, task
  lifecycle transitions, and KV changes) are first-class. Single-leader execution per workflow ensures consistency
  without sacrificing the gateway's stateless scaling story; persistent state survives gateway restarts.
- **Horizontal Scaling** — Stateless gateway instances share state through the configured state and messaging
  backends, enabling multi-node deployments either on the JetStream-clustered AetherLite path or on the full
  Redis + RabbitMQ + PostgreSQL stack.
- **Progress Updates** — A built-in topic schema and message type for handling progress updates on tasks.
- **Audit Logging** — Configurable event capture (connection, auth, message, KV, admin, ACL) with batched writes and
  retention policies. By default, everything is captured in audit logs, but you control it.

## Architecture

Aether is intentionally narrow in scope: it does **not** execute user code or internal workflows. It routes messages
between external participants and manages durable state including task execution.

### Core Components

The table below describes each component's responsibility. The backing implementation varies by deployment tier
(in-process / Badger for Lite, JetStream KV+Stream for clustered AetherLite, Redis+RabbitMQ+PostgreSQL for the full
stack).

| Component            | Location                       | Responsibility                                                                       |
|----------------------|--------------------------------|--------------------------------------------------------------------------------------|
| **Gateway Server**   | `internal/gateway/server.go`   | gRPC stream handling, auth, connection lifecycle, message routing, KV/checkpoint ops |
| **Router**           | `internal/router/router.go`    | Topic-to-stream mapping, producer pool management, shared consumer fan-out           |
| **Session Registry** | `internal/state/session.go`    | Distributed identity locks with TTL, active session tracking                         |
| **KV Store**         | `internal/kv/store.go`         | Hierarchical config store (global/workspace/user/user-workspace scopes)              |
| **Checkpoint Store** | `internal/checkpoint/store.go` | Persistent state checkpointing for agents/tasks                                      |
| **Task Store**       | `pkg/tasks/store.go`           | Task lifecycle management (pause/resume/hibernate/wake, dependencies, deadlines)     |
| **ACL Service**      | `internal/acl/service.go`      | RBAC with delegation chains, authority requests, and workspace access enforcement    |
| **Orchestration**    | `internal/orchestration/`      | Task dispatch and claim-based delivery; lazy-loaded compute via Orchestrators        |
| **Workflow Engine**  | `internal/workflow/`           | DAG-based and event-driven workflows: scheduler, state machine, single-leader executor, expression evaluation |
| **Identity Model**   | `pkg/models/identity.go`       | Eight principal types, topic address derivation via `ToTopic()`                      |

### Connection Flow

```
Client                         Gateway                        Redis / RabbitMQ
  |                               |                                |
  |-- InitConnection ------------>|                                |
  |                               |-- Authenticate (mTLS/OAuth) -->|
  |                               |-- AcquireLock (SetNX+TTL) --->|
  |                               |<- Lock granted ---------------|
  |                               |-- ACL Check ----------------->|
  |                               |-- Quota Check + Increment --->|
  |                               |-- Subscribe to topic(s) ----->|
  |<-- ConnectionAck (sessionID) -|                                |
  |<-- ConfigSnapshot (KV) -------|                                |
  |                               |                                |
  |<======= message loop ========>|<====== stream I/O ============>|
  |                               |                                |
  |-- (disconnect) -------------->|                                |
  |                               |-- Unsubscribe --------------->|
  |                               |-- ReleaseLock --------------->|
  |                               |-- Decrement quota ----------->|
```

## Quick Start

### Option A: AetherLite — local mode (no external dependencies)

AetherLite bundles the gateway and workflow server into a single binary backed by embedded SQLite and Badger.
No Redis, RabbitMQ, PostgreSQL, or NATS required.

```bash
cd server
go build -o aetherlite ./cmd/aetherlite
AETHER_ALLOW_DEV_MODE=true ./aetherlite --dev --insecure-admin
```

State is persisted in `./aether-lite-data/`. gRPC on `:50051`, admin UI on `:31880`. See
[`./docs/aetherlite.md`](./docs/aetherlite.md) for details.

Badger-backed messages and consumer offsets are retention-capped (env `AETHER_MESSAGE_RETENTION_TTL`,
default `24h`; `AETHER_OFFSET_RETENTION_TTL`, default `168h`) so the embedded log does not grow without bound.

> Production-ready for single-node deployments. No horizontal scaling, no cross-node messaging — data loss on
> hardware failure unless S3 backups are configured.

### Option B: AetherLite — clustered mode (embedded NATS JetStream)

Same `aetherlite` binary as Option A, but with `AETHERLITE_CLUSTER_MODE=true`. Embedded NATS server replaces the
in-process state surfaces (locks, pins, KV, session registry, checkpoints, message routing, audit stream) with
JetStream-backed equivalents. The same code paths are used at every scale — go from a 1-node test instance to a
3-node quorum cluster purely by changing config.

```bash
# Single-node cluster (topology A2 — useful for testing cluster-mode features)
AETHERLITE_CLUSTER_MODE=true ./aetherlite --dev --insecure-admin

# 2-node async (topology B1) — primary + hot mirror, accepts 1–5s RPO on failover
AETHERLITE_CLUSTER_MODE=true \
AETHERLITE_CLUSTER_PEERS=nats://replica:6222 \
AETHERLITE_HA_MODE=async \
./aetherlite

# 3+ node quorum (topology C) — full HA, zero data loss
AETHERLITE_CLUSTER_MODE=true \
AETHERLITE_CLUSTER_PEERS=nats://node2:6222,nats://node3:6222 \
./aetherlite
```

Docker Compose manifests for each topology live under
[`server/deployments/docker-compose/`](server/deployments/docker-compose/)
(`cluster-single.yaml`, `cluster.yaml`, `cluster-ha.yaml`).

See [`server/docs/aetherlite-clustering.md`](server/docs/aetherlite-clustering.md) for the full topology matrix:
which backend stores each concern at each scale (identity locks, KV, audit log, task queue, registry, etc.),
RPO/RTO targets, and S3 backup behavior.

> **Why use B over A?** Cross-node messaging, HA failover, and live cluster-mode feature surfaces (JetStream
> Watch-driven `PrefixIndex`, authority lifecycle events, replicated audit stream) — all without operating
> Redis, RabbitMQ, or PostgreSQL.

### Option C: Full Aether (Redis + RabbitMQ + PostgreSQL)

The original distributed deployment: a separate `gateway` binary, stateless replicas behind a load balancer,
state in Redis (session locks, KV, checkpoints), task lifecycle / ACL / audit in PostgreSQL, and messages on
RabbitMQ Streams.

#### Prerequisites

- Go 1.25+
- Redis 7+ (or Valkey) — session registry and KV store
- RabbitMQ 3.13+ with the Streams plugin — messaging backbone
- PostgreSQL 16+ — task registry, orchestration profiles, audit log

#### Start Development Dependencies

```bash
# RabbitMQ Streams (ports 55552 stream, 55672 AMQP, management UI on 15672)
./scripts/docker_rmq_test.sh

# Redis / Valkey
./scripts/docker_valkey_test.sh
```

#### Build and Run

```bash
# Build
go build -o gateway ./cmd/gateway

# Run with the default dev config
./gateway --config configs/dev.yaml

# Or run directly
go run ./cmd/gateway
```

#### Run Tests

```bash
go test ./...                         # all packages
go test -v ./internal/gateway/...     # specific package, verbose
```

## Configuration

The gateway is configured via a YAML file. CLI flags override config-file values.

```yaml
# configs/dev.yaml (abbreviated)
gateway:
  port: 50051
  gateway_id: "gateway-dev-1"

admin:
  enabled: true
  port: 31880

auth:
  modes: [ mtls, task_token, api_key ]
  mtls:
    required: false
    mode: relaxed
  oauth:
    verify_signature: false   # disable JWT sig verification in dev

postgres:
  host: "localhost"
  port: 55432
  database: "aether"
  user: "aether"
  password: "aether_dev"

redis:
  cluster:
    - "localhost:56379"
    - "localhost:56380"
    - "localhost:56381"

rabbitmq:
  stream_url: "rabbitmq-stream://guest:guest@localhost:55552"
  amqp_url: "amqp://guest:guest@localhost:55672/"

audit:
  enabled: true
  event_types: [ connection, auth, message, kv, admin, acl ]
  retention_days: 90

log_level: "info"
```

### CLI Flags

| Flag                                                                | Description                                            |
|---------------------------------------------------------------------|--------------------------------------------------------|
| `--config <path>`                                                   | Path to YAML config file (default: `configs/dev.yaml`) |
| `--port <n>`                                                        | gRPC server port (overrides config)                    |
| `--tls`                                                             | Enable mTLS                                            |
| `--cert-file`, `--key-file`, `--ca-file`                            | mTLS certificate paths                                 |
| `--db-host`, `--db-port`, `--db-user`, `--db-password`, `--db-name` | PostgreSQL overrides                                   |
| `--redis <host:port>`                                               | Redis address override                                 |
| `--stream-url`                                                      | RabbitMQ Stream URL override                           |
| `--amqp-url`                                                        | RabbitMQ AMQP URL override                             |
| `--admin-port <n>`                                                  | Admin UI port override                                 |

## Principal Types

Every connection authenticates as exactly one of eight principal types.

| Type                | Uniqueness                  | Identity Fields                                     | Notes                                                                        |
|---------------------|-----------------------------|-----------------------------------------------------|------------------------------------------------------------------------------|
| **Agent**           | One connection per identity | `workspace` + `implementation` + `specifier`        | Long-running service                                                         |
| **Unique Task**     | One connection per identity | `workspace` + `implementation` + `unique_specifier` | Named finite unit of work                                                    |
| **Non-Unique Task** | Many connections allowed    | `workspace` + `implementation` (server assigns ID)  | Workers competing for tasks on a shared broadcast topic                      |
| **User**            | One connection per window   | `user_id` + `window_id`                             | Multiple browser tabs allowed                                                |
| **Workflow Engine** | One active connection       | N/A (Future: sharding)                              | Sole subscriber to `event::` topics                                          |
| **Metrics Bridge**  | One active connection       | N/A (Future: sharding)                              | Sole subscriber to `metric::` topics; receive-only                           |
| **Orchestrator**    | One per specifier           | `implementation` + `specifier`                      | Receives `TaskAssignment` messages to spin up compute                        |
| **Service**         | One per specifier           | `implementation` + `specifier`                      | Cross-workspace HTTP-over-Aether proxy; addressable via `sv::{impl}::{spec}`   |
| **Bridge**          | One per specifier           | `implementation` + `specifier`                      | Cross-workspace messaging integration; sends to any workspace subject to ACL |

## Topic Schema

Messages are routed by a structured topic prefix.

| Prefix     | Target           | Format                                | Description                                           |
|------------|------------------|---------------------------------------|-------------------------------------------------------|
| `ag`       | Agent            | `ag::{workspace}::{impl}::{spec}`        | Specific agent instance                               |
| `tu`       | Unique Task      | `tu::{workspace}::{impl}::{unique_spec}` | Named task instance                                   |
| `ta`       | Assigned Task    | `ta::{workspace}::{impl}::{task_id}`     | Server-assigned non-unique task instance              |
| `tb`       | Task Broadcast   | `tb::{workspace}::{impl}`               | Load-balancing topic; all workers of a type compete   |
| `us`       | User (Window)    | `us::{user_id}::{window_id}`            | Specific browser window                               |
| `uw`       | User (Workspace) | `uw::{user_id}::{workspace}`            | User scoped to a workspace                            |
| `uu`       | User (Broadcast) | `uu::{user_id}`                        | Reaches all of a user's windows regardless of active workspace; platform-principal senders only |
| `ga`       | Global Agents    | `ga::{workspace}`                      | Broadcast to all agents in a workspace                |
| `gu`       | Global Users     | `gu::{workspace}`                      | Broadcast to all users in a workspace                 |
| `pg`       | Progress         | `pg::{workspace}`                      | Progress updates with server-side recipient filtering |
| `event::`  | Workflow Engine  | `event::{workspace}`                  | Workflow Engine is the sole subscriber                |
| `metric::` | Metrics Bridge   | `metric::{workspace}`                 | Metrics Bridge is the sole subscriber                 |
| `sv`       | Service          | `sv::{impl}::{spec}`                    | Cross-workspace service proxy endpoint                |
| `br`       | Bridge           | `br::{impl}::{spec}`                    | Cross-workspace messaging bridge endpoint             |

### Permission Matrix

| Sender          | Can Send To                                           |
|-----------------|-------------------------------------------------------|
| Agent           | Agents, Tasks, Users, Broadcast Events, Metrics       |
| Task            | Agents, Tasks, Users, Broadcast Events, Metrics       |
| User            | Agents, Tasks, Users                                  |
| Workflow Engine | Agents, Tasks, Users, Broadcast Events, Metrics       |
| Metrics Bridge  | None (receive only)                                   |
| Orchestrator    | Task/Agent topics only (status updates)               |
| Service         | Any topic (cross-workspace); per-message ACL enforced |
| Bridge          | Any topic in any workspace; per-message ACL enforced  |

## gRPC API

The full API is defined in [`api/proto/aether.proto`](api/proto/aether.proto).

```protobuf
service AetherGateway {
  // Bidirectional stream. First message must be InitConnection.
  rpc Connect (stream UpstreamMessage) returns (stream DownstreamMessage);
}
```

**Upstream (client to server):** `InitConnection`, `SendMessage`, `SwitchWorkspace`, `KVOperation`,
`CheckpointOperation`, `CreateTaskRequest`, `ProgressReport`, `TaskQuery`, `TaskOperation`

**Downstream (server to client):** `ConnectionAck`, `IncomingMessage`, `ConfigSnapshot`, `Signal`, `ErrorResponse`,
`KVResponse`, `CheckpointResponse`, `TaskAssignment`, `TaskQueryResponse`, `TaskOperationResponse`, `ProgressUpdate`

**Message types:** `CHAT`, `CONTROL`, `TOOL_CALL`, `EVENT`, `METRIC`

Regenerate Go bindings after proto changes:

```bash
./scripts/compilo_protos.sh
```

## Client SDKs

- **Go SDK**: [`sdk/go/`](sdk/go) — full-featured SDK with typed clients for all eight principal types, sync/async KV &
  checkpoint helpers, reconnection with backoff, and TLS/mTLS support. See [`sdk/go/README.md`](sdk/go/README.md).
- **Python SDK**: [`sdk/python-client/`](sdk/python-client) — sync and async clients with feature parity, including
  orchestrator and multiprocess orchestrator implementations. See [
  `sdk/python-client/README.md`](sdk/python-client/README.md).
- **TypeScript SDK**: [`sdk/typescript/`](sdk/typescript) — Agent and User clients with gRPC transport, auto-reconnect,
  KV operations, and typed error hierarchy. See [`sdk/typescript/README.md`](sdk/typescript/README.md).

### Horizontal Scaling Notes

**Option C (full Aether):**
- All mutable state lives in Redis and PostgreSQL — gateway instances are stateless.
- Redis `SetNX` locks guarantee identity uniqueness across all replicas.
- RabbitMQ Streams preserve consumer offsets; clients reconnecting to a different instance experience no message loss
  (at-least-once delivery).
- Locks are TTL-backed; a crashed gateway's locks expire automatically so clients can reconnect to another instance.

**Option B (AetherLite clustered):**
- Identity locks, pins, sessions, KV, checkpoints, and registry live in JetStream KV with configurable replica counts
  (R=1 standalone, R=2 quorum, R=3+ HA).
- Topic message routing flows through JetStream Streams with consumer offset tracking — same at-least-once guarantee as
  RabbitMQ Streams in Option C.
- Task lifecycle, ACL, and audit have a hybrid model: SQLite per-node for fast reads, JetStream KV / CDC stream for
  cross-node coordination.
- Scale-up path is config-only: add `AETHERLITE_CLUSTER_PEERS`, restart, and JetStream forms a quorum across the new
  node set.
- See [`server/docs/aetherlite-clustering.md`](server/docs/aetherlite-clustering.md) for the per-topology backend matrix
  and failure-mode analysis.

## License

Copyright 2025+ scitrera.ai

Licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.
