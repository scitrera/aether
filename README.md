# Aether Gateway

A distributed control plane for routing structured messages, tracking tasks, and managing connection lifecycles. Aether
coordinates external agents, tasks, and engines through a gRPC gateway backed by RabbitMQ Streams for messaging and
Redis for state management.

Built by [scitrera.ai](https://scitrera.ai).

## Key Features

- **Connection = Lock = Heartbeat** — An active gRPC stream serves simultaneously as the distributed lock for an
  identity and its liveness proof. No separate heartbeat API is needed.
- **Bidirectional Streaming** — A single `rpc Connect` stream multiplexes all client-server communication: messages, KV
  operations, config snapshots, and signals.
- **Distributed Session Management** — Redis-backed exclusive locks with automatic expiry ensure only one connection per
  identity at any time.
- **Hierarchical KV Store** — Namespace-scoped configuration with global, workspace, user, and user-workspace scopes.
  Workspace config is pushed to connecting clients as a baseline snapshot.
- **Topic-Based Message Routing** — Prefix-driven routing through RabbitMQ Streams with per-principal permission
  enforcement.
- **Orchestration / Lazy Loading** — When a message targets an offline agent or task, the gateway enqueues the message
  and signals the responsible orchestrator to spin up compute.
- **Horizontal Scaling** — Stateless gateway instances share state through Redis and RabbitMQ, enabling multiple
  replicas behind a load balancer.
- **Audit Logging** — Configurable event capture (connection, auth, message, KV, admin, ACL) with batched writes and
  retention policies.

## Architecture

Aether is intentionally narrow in scope: it does **not** execute user code or internal workflows. It routes messages
between external participants and manages durable state including task execution.

### Core Components

| Component            | Location                       | Responsibility                                                                          |
|----------------------|--------------------------------|-----------------------------------------------------------------------------------------|
| **Gateway Server**   | `internal/gateway/server.go`   | gRPC stream handling, auth, connection lifecycle, message routing, KV/checkpoint ops    |
| **Router**           | `internal/router/router.go`    | Topic-to-stream mapping, producer pool management, shared consumer fan-out              |
| **Session Registry** | `internal/state/session.go`    | Redis `SetNX`-based distributed locks with TTL, active session tracking                 |
| **KV Store**         | `internal/kv/store.go`         | Hierarchical config store backed by Redis (global/workspace/user/user-workspace scopes) |
| **Checkpoint Store** | `internal/checkpoint/store.go` | Persistent state checkpointing for agents/tasks (Redis-backed)                          |
| **Task Store**       | `pkg/tasks/store.go`           | PostgreSQL-backed task lifecycle management                                             |
| **ACL Service**      | `internal/acl/service.go`      | RBAC with delegation chains for workspace access                                        |
| **Orchestration**    | `internal/orchestration/`      | Task dispatch via AMQP, claim-based delivery, profile management                        |
| **Identity Model**   | `pkg/models/identity.go`       | Eight principal types, topic address derivation via `ToTopic()`                         |

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

### Option A: AetherLite (no external dependencies)

AetherLite bundles the gateway, workflow server, and messaging bridge into a single binary backed by embedded SQLite and
Badger. No Redis, RabbitMQ, or PostgreSQL required.

```bash
cd server
go build -o aetherlite ./cmd/aetherlite
AETHER_ALLOW_DEV_MODE=true ./aetherlite --dev --insecure-admin
```

State is persisted in `./aether-lite-data/`. The gRPC gateway listens on `:50051` and the admin UI on `:31880`. See [
`./docs/aetherlite.md`](./docs/aetherlite.md) for details.

> AetherLite is production-ready for single-node deployments. It does not support horizontal scaling.

### Option B: Full Stack (Redis + RabbitMQ + PostgreSQL)

### Prerequisites

- Go 1.25+
- Redis 7+ (or Valkey) — session registry and KV store
- RabbitMQ 3.13+ with the Streams plugin — messaging backbone
- PostgreSQL 16+ — task registry, orchestration profiles, audit log

### Start Development Dependencies

```bash
# RabbitMQ Streams (ports 55552 stream, 55672 AMQP, management UI on 15672)
./scripts/docker_rmq_test.sh

# Redis / Valkey
./scripts/docker_valkey_test.sh
```

### Build and Run

```bash
# Build
go build -o gateway ./cmd/gateway

# Run with the default dev config
./gateway --config configs/dev.yaml

# Or run directly
go run ./cmd/gateway
```

### Run Tests

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
| **Workflow Engine** | One active connection       | N/A (Future: sharding)                              | Sole subscriber to `event.*` topics                                          |
| **Metrics Bridge**  | One active connection       | N/A (Future: sharding)                              | Sole subscriber to `metric.*` topics; receive-only                           |
| **Orchestrator**    | One per specifier           | `implementation` + `specifier`                      | Receives `TaskAssignment` messages to spin up compute                        |
| **Service**         | One per specifier           | `implementation` + `specifier`                      | Cross-workspace HTTP-over-Aether proxy; addressable via `sv.{impl}.{spec}`   |
| **Bridge**          | One per specifier           | `implementation` + `specifier`                      | Cross-workspace messaging integration; sends to any workspace subject to ACL |

## Topic Schema

Messages are routed by a structured topic prefix.

| Prefix     | Target           | Format                                | Description                                           |
|------------|------------------|---------------------------------------|-------------------------------------------------------|
| `ag`       | Agent            | `ag.{workspace}.{impl}.{spec}`        | Specific agent instance                               |
| `tu`       | Unique Task      | `tu.{workspace}.{impl}.{unique_spec}` | Named task instance                                   |
| `ta`       | Assigned Task    | `ta.{workspace}.{impl}.{task_id}`     | Server-assigned non-unique task instance              |
| `tb`       | Task Broadcast   | `tb.{workspace}.{impl}`               | Load-balancing topic; all workers of a type compete   |
| `us`       | User (Window)    | `us.{user_id}.{window_id}`            | Specific browser window                               |
| `uw`       | User (Workspace) | `uw.{user_id}.{workspace}`            | User scoped to a workspace                            |
| `ga`       | Global Agents    | `ga.{workspace}`                      | Broadcast to all agents in a workspace                |
| `gu`       | Global Users     | `gu.{workspace}`                      | Broadcast to all users in a workspace                 |
| `pg`       | Progress         | `pg.{workspace}`                      | Progress updates with server-side recipient filtering |
| `event.*`  | Workflow Engine  | `event.{workspace}`                   | Workflow Engine is the sole subscriber                |
| `metric.*` | Metrics Bridge   | `metric.{workspace}`                  | Metrics Bridge is the sole subscriber                 |
| `sv`       | Service          | `sv.{impl}.{spec}`                    | Cross-workspace service proxy endpoint                |
| `br`       | Bridge           | `br.{impl}.{spec}`                    | Cross-workspace messaging bridge endpoint             |

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

- All mutable state lives in Redis and PostgreSQL — gateway instances are stateless.
- Redis `SetNX` locks guarantee identity uniqueness across all replicas.
- RabbitMQ Streams preserve consumer offsets; clients reconnecting to a different instance experience no message loss (
  at-least-once delivery).
- Locks are TTL-backed; a crashed gateway's locks expire automatically so clients can reconnect to another instance.

## License

Copyright 2025+ scitrera.ai

Licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.
