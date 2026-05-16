# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Aether is a distributed control plane for routing structured messages, tracking tasks, and managing connection lifecycles. It coordinates external agents, tasks, and engines through a gRPC gateway backed by RabbitMQ Streams for messaging and Redis for state management.

This project is made by scitrera.ai. Therefore, the naming involves "scitrera"; NEVER "scittera" or any other similar typographical error variant.

**Key architectural principle:** The connection itself IS the distributed lock AND the heartbeat. No separate heartbeat API exists - connection liveness determines entity availability.

## Directory Structure (Monorepo)
- api/ - Protobuf definitions and generated code (own Go module: `github.com/scitrera/aether/api`)
- sdk/ - Client SDKs (Go, Python, TypeScript)
- server/ - Go server (module: `github.com/scitrera/aether`)
  - server/cmd/ - Main entry points (gateway, auth-proxy, migrate, cleanup, loadtest)
  - server/configs/ - YAML configuration files (e.g., dev.yaml)
  - server/deployments/ - Kubernetes manifests and Docker Compose files
  - server/docs/ - Server documentation (quickstart, scaling, monitoring, admin API, error codes, etc.)
  - server/internal/ - Core implementation packages
  - server/migrations/ - PostgreSQL schema migrations (embedded, auto-run on startup)
  - server/pkg/ - Shared models and utilities
  - server/scripts/ - Server dev scripts (infra, test, certs, load test)
  - server/specification.md - Full system specification (v4.0, reflects actual implementation)
  - server/go.mod, server/go.sum - Go module files
  - server/Dockerfile - Multi-stage build for gateway, cleanup, and migrate binaries
  - server/Makefile - Build/test/run targets (run from server/ directory)
- scripts/ - Repo-wide scripts (compile_protos.sh)
- refs/ - Reference materials (external open source code; exclude from general scans)
- .claude - Claude configuration files
- .slop - Directory to store random markdown files and notes that may be relevant but not necessarily
- CLAUDE.md - This file

## Build and Run Commands

# NOTE: go executable path: /home/drew/sdk/go1.25.5/bin/go
# NOTE: env: GOPATH=/home/drew/go
# NOTE: All go commands run from the server/ directory

### Build
```bash
cd server
go build -o gateway ./cmd/gateway
```

### Run Gateway
```bash
cd server

# With config file:
./gateway --config configs/dev.yaml

# With dev defaults (no config file required):
AETHER_ALLOW_DEV_MODE=true ./gateway --dev --insecure-admin

# Or run directly:
go run ./cmd/gateway
```

### Run Tests
```bash
cd server
go test ./...                    # Run all tests
go test -short ./...             # Skip integration tests
go test -v ./internal/gateway    # Run specific package tests with verbose output
```

### Generate Protobuf Code
# may require: activate a Python virtualenv with grpcio-tools / mypy-protobuf installed,
# ensure protoc-gen-go and protoc-gen-go-grpc are on PATH, then run scripts/compile_protos.sh
```bash
./scripts/compile_protos.sh
```

### Docker Build
```bash
# Build context is the repo root
docker build -f server/Dockerfile -t scitrera/aether-gateway .
```

### Development Dependencies

```bash
cd server

# RabbitMQ with Streams plugin (ports 55552 stream, 55672 AMQP, 15672 management)
./scripts/docker_rmq_test.sh

# Redis / Valkey (ports 56379-56381 cluster)
./scripts/docker_valkey_test.sh

# PostgreSQL (optional — task/orchestration/ACL/audit features disabled without it)
# Use docker or a local instance; gateway connects via config postgres.* settings
```

## Core Architecture

### System Components

| Component | Location | Responsibility |
|---|---|---|
| **Gateway Server** | `server/internal/gateway/server.go` | gRPC stream handling, auth, connection lifecycle, message routing, KV/checkpoint ops |
| **Router** | `server/internal/router/router.go` | Topic-to-RabbitMQ-stream mapping, producer pool management, shared consumer fan-out |
| **Session Registry** | `server/internal/state/session.go` | Redis SetNX-based distributed locks with TTL, session metadata |
| **KV Store** | `server/internal/kv/store.go` | Hierarchical config store (global/workspace/user/user-workspace scopes) |
| **Checkpoint Store** | `server/internal/checkpoint/store.go` | Persistent state checkpointing for agents/tasks (Redis-backed) |
| **Task Store** | `server/pkg/tasks/store.go` | PostgreSQL-backed task lifecycle management |
| **ACL Service** | `server/internal/acl/service.go` | RBAC with delegation chains for workspace access |
| **Audit Logger** | `server/internal/audit/` | Batched, configurable event capture (connection, auth, message, KV, admin, ACL) |
| **Orchestration** | `server/internal/orchestration/` | Task dispatch via AMQP, claim-based delivery, profile management |
| **Admin Server** | `server/internal/admin/server.go` | REST API + embedded UI; ops server for health probes + Prometheus metrics |
| **Auth Proxy** | `server/cmd/auth-proxy/` + `server/internal/authproxy/` | Standalone auth gateway for external services (e.g., MemoryLayer) |
| **Identity Model** | `server/pkg/models/identity.go` | Seven principal types (Agent, Task, User, Orchestrator, WorkflowEngine, MetricsBridge, Bridge), topic address derivation via `ToTopic()` |
| **Messaging Bridge** | `server/cmd/msgbridge/` + `server/internal/msgbridge/` | Standalone server bridging Discord/Teams/Email ↔ Aether via `PrincipalBridge` type |

### Topic Schema and Routing

| Prefix | Format | Description |
|---|---|---|
| `ag` | `ag::{workspace}::{impl}::{spec}` | Specific agent instance |
| `tu` | `tu::{workspace}::{impl}::{spec}` | Unique task (named) |
| `ta` | `ta::{workspace}::{impl}::{id}` | Non-unique task instance (server-assigned ID) |
| `tb` | `tb::{workspace}::{impl}` | Task broadcast (load-balancing) |
| `us` | `us::{user_id}::{window_id}` | User window-specific |
| `uw` | `uw::{user_id}::{workspace}` | User workspace-scoped |
| `ga` | `ga::{workspace}` | Global agent broadcast |
| `gu` | `gu::{workspace}` | Global user broadcast |
| `pg` | `pg::{workspace}` | Progress updates (server-side recipient filtering) |
| `event.*` | Write: `event.{workspace}` (gateway rewrites to `event::receiver{shard}`); Subscribe (WE): `event::receiver0` | Workflow Engine fan-in; today 1 shard (`event::receiver0`) |
| `metric.*` | Write: `metric.{workspace}` (gateway rewrites to `metric::receiver{shard}`); Subscribe (MB): `metric::receiver0` | Metrics Bridge fan-in; today 1 shard (`metric::receiver0`) |
| `br` | `br::{impl}::{spec}` | Bridge (cross-workspace messaging integration) |

### Connection Flow

1. Client opens gRPC stream and sends `InitConnection` with principal type, identity, and credentials
2. Gateway authenticates (mTLS / task token / API key / OAuth)
3. Gateway acquires distributed lock in Redis via `SetNX` with 30s TTL
4. If lock occupied → reject with `DuplicateIdentityError`
5. ACL check verifies workspace access (before session becomes discoverable)
6. Quota check atomically increments workspace connection count
7. Session registered, client added to `activeStreams` and `identityIndex`
8. Lock refresh goroutine starts (every 10s)
9. Subscribe to appropriate topics based on principal type
10. Send `ConnectionAck` (with session ID for reconnection) + `ConfigSnapshot` (KV)
11. Enter main message loop until disconnect
12. On disconnect: unsubscribe, release lock, decrement quota, update task state, audit

### Orchestration Pattern

When a message targets an offline agent (`ag.*`) or unique task (`tu.*`):
1. Gateway checks `identityIndex` (local O(1)) then Redis lock (distributed)
2. If offline: message is published to RabbitMQ stream (persisted) AND orchestration task created
3. Dispatcher publishes task notification to AMQP queue
4. One gateway claims the task atomically and sends `TaskAssignment` to a connected orchestrator
5. Orchestrator spins up compute with a short-lived auth token
6. Target connects, validates token, receives persisted messages via offset replay

### Protocol Details

All communication happens over a single bidirectional gRPC stream defined in `api/proto/aether.proto`:
- **Upstream (client → server):** InitConnection, SendMessage, SwitchWorkspace, KVOperation, CheckpointOperation, CreateTaskRequest, ProgressReport, TaskQuery, TaskOperation
- **Downstream (server → client):** ConnectionAck, IncomingMessage, ConfigSnapshot, Signal, ErrorResponse, KVResponse, CheckpointResponse, TaskAssignment, TaskQueryResponse, TaskOperationResponse, ProgressUpdate

Message types: CHAT, CONTROL, TOOL_CALL, EVENT, METRIC

## Important Implementation Notes

### Connection = Lock = Heartbeat Paradigm
An active gRPC stream connection represents both the distributed lock for that identity AND its liveness proof. The lock has a 30-second TTL refreshed every 10 seconds. If the gateway crashes, locks auto-expire. Session resume is supported via atomic Lua script for lock takeover.

### Identity Uniqueness
- **Agents and Unique Tasks:** Globally unique. Two clients cannot connect with the same identity.
- **Non-unique Tasks:** Multiple connections allowed. Each gets a server-generated ID and subscribes to both `ta.*` (direct) and `tb.*` (broadcast) topics.
- **Users:** Unique per window (`us::{user_id}::{window_id}`), allowing multiple browser tabs.

### Message Routing Permissions
| Sender | Can Send To | Cannot Send To |
|---|---|---|
| Agent/Task | Agents, Tasks, Users, Events, Metrics | Orchestrators, Progress |
| User | Agents, Tasks, Users | Events, Metrics, Progress |
| Workflow Engine | Everything | — |
| Metrics Bridge | Nothing (receive-only) | All |
| Orchestrator | Agent/Task topics only | Events, Metrics |
| Bridge | Everything (any workspace) | — |

Cross-workspace sends are blocked: workspace-scoped principals cannot target topics in other workspaces. Bridges are cross-workspace by design (no workspace component) and check ACL per-message against the target workspace.

**Cross-workspace event/metric broadcast:** Sending to `event.*` or `metric.*` in another workspace requires `capability/event_broadcast` or `capability/metric_broadcast` ACL permission. Sending to the sender's own native workspace is implicitly permitted.

**Metric payloads are structured:** `METRIC` messages must carry a `Metric` proto payload (fields: `trace_id`, `entries` [{`name`, `kind`, `qty`}], `metadata`, `client_timestamp_ms`). All entries are additive deltas; negative `qty` requires the `capability/metric_credit` ACL permission. See spec Section 4.5 for details and error codes.

### KV Store Scopes
Four scopes with Redis namespace isolation:
- **Global** (`kv:agent:{impl}.{spec}:global`) — cross-workspace agent state
- **Workspace** (`kv:agent:{impl}.{spec}:ws:{workspace}`) — read-only for agents, managed by platform
- **User** (`kv:agent:{impl}.{spec}:user:{user_id}`) — per-user agent state
- **User-Workspace** (`kv:agent:{impl}.{spec}:user:{user_id}:ws:{workspace}`) — per-user per-workspace

### Redis Usage
Redis serves multiple purposes via a shared `UniversalClient`:
1. **Session Registry:** Distributed locks and session metadata (30s TTL)
2. **KV Store:** Namespace-scoped configuration with optional TTL
3. **Checkpoint Store:** Persistent agent/task state
4. **Task Tokens:** Short-lived orchestration auth tokens (24h TTL)
5. **Quota Counters:** Per-workspace connection and message rate tracking

Supports single-node, cluster, and auto-detect modes.

### RabbitMQ Streams vs Traditional Queues
The system uses RabbitMQ **Streams**, not classic queues. Streams provide:
- Persistent, replayable message logs
- Consumer offset tracking for at-least-once delivery
- Per-topic producer pools with health checks and idle eviction (5min)
- Shared consumers with local fan-out to reduce RabbitMQ connections

### PostgreSQL (Optional)
PostgreSQL is used for persistent features that gracefully degrade when unavailable:
- Task lifecycle management (create, assign, complete, fail, retry, purge)
- Orchestration profiles and queue management
- ACL rules and delegation chains
- API token storage (HMAC-SHA256 hashed)
- Audit log (batched writes with retention)
- Agent registry

Schema is managed by embedded migrations in `server/migrations/` that auto-run on startup.

## Horizontal Scaling

- **Stateless Design:** All state in Redis and PostgreSQL. Gateway instances share nothing.
- **Distributed Locking:** Redis SetNX ensures identity uniqueness across all instances.
- **Session Affinity:** Load balancer uses ClientIP (K8s) or cookies (nginx) for reconnection.
- **Multi-Gateway Orchestration:** Task claims use PostgreSQL-backed atomic operations; only one gateway delivers each task.
- **Graceful Failover:** Lock TTLs expire (30s), clients reconnect, RabbitMQ preserves offsets.

### Deployment Options
- **Kubernetes:** `server/deployments/k8s/gateway/` — Deployment, Service, Ingress, ConfigMap, cert-manager
- **Docker Compose:** `server/deployments/docker-compose/multi-instance.yaml` — 3 instances + nginx
- **Dockerfile:** `server/Dockerfile` — Multi-stage build, non-root user, ports 50051/9090/31880 (build from repo root: `docker build -f server/Dockerfile .`)

## Client SDKs

- **Go SDK** (`sdk/go/`) — Full-featured: all 6 principal types, KV, checkpoints, reconnection, TLS, Docker orchestrator
- **Python SDK** (`sdk/python-client/`) — Sync + async clients, multiprocess orchestrator
- **TypeScript SDK** (`sdk/typescript/`) — Agent/User clients, gRPC transport, auto-reconnect

## Specification Reference

The complete system specification is in `server/specification.md` (v4.0, derived from the running codebase). Key sections:
- Section 1: Core Concepts (Principal Types, Connection=Lock=Heartbeat)
- Section 4: Messaging Topology & Topics (topic schema and permission matrix)
- Section 5: Orchestration & Lazy Loading (task assignment flow)
- Section 6-7: KV Store and Checkpoint Store
- Section 9: Quotas & Rate Limiting
- Section 12: Horizontal Scaling

## Deployment Notes
- Each aether server or "server group" represents a single tenant. Multi-tenancy is achieved by deploying separate server groups with their own PostgreSQL and Redis databases and leveraging RabbitMQ virtual hosts.
- Workspaces are logical namespaces within a tenant/server group, not separate deployments — ACL-based isolation is sufficient at the workspace level.
