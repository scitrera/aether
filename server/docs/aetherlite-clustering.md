# AetherLite Clustering Guide

**Strategic design rationale:** See `/home/drew/.claude/plans/review-home-drew-oss-rqlite-and-home-dre-distributed-biscuit.md` for the complete architectural reasoning behind topology selection, backup tiers, and JetStream integration.

---

## TL;DR: Topology Selection

| Aspect | Lite (A1) | Cluster (A2) | B1 (2-node async) | B2 (2-node sync) | C (3+ node) |
|---|---|---|---|---|---|
| **NATS running?** | No | Yes (standalone) | Yes (clustered) | Yes (clustered) | Yes (clustered) |
| **Cluster mode env** | false | true | true | true | true |
| **Peer list** | — | empty | 1 peer | 1 peer | ≥2 peers |
| **HA mode** | — | — | `async` | `sync` | auto or unset |
| **When to use** | Dev, testing, edge | Dev+test cluster behavior on 1 machine | 2-node HA, accept write loss | 2-node strict consistency | Production HA, zero data loss |
| **Replicas** | R=1 (Badger) | R=1 (JetStream) | R=1 (primary) + mirror | R=2 (quorum) | R=3 (quorum) |
| **Write availability** | Full | Full | Full | Blocked if 1 down | Full (tolerate 1 node down) |
| **Data loss on failure** | Total | Total | 1–5s (in-flight) | None (quorum) | None (quorum) |

**Key insight:** A1 (lite) and A2 (cluster) are both single-node deployments but fundamentally different: A1 uses Badger only and NATS never starts; A2 starts NATS R=1 (standalone, no Raft clustering). **Why run A2?** Because it uses the same backend code paths as B/C, so adding a second node later is purely a config change. Useful for testing cluster-mode features on a single machine.

---

## Storage Backends by Concern and Topology

The tables below are the operator's primary reference: they show **exactly which backend stores each concern** at each topology. All tables use the same canonical row set for easy comparison.

### Canonical Concerns (All Topologies)

| # | Concern |
|---|---|
| 1 | Identity locks |
| 2 | Tunnel/request pins |
| 3 | Session registry |
| 4 | Topic message log / routing |
| 5 | KV store |
| 6 | Checkpoint store (small ≤256 KB) |
| 7 | Checkpoint store (large >256 KB) |
| 8 | Task work queue |
| 9 | ACL rules |
| 10 | Authority requests |
| 11 | Authority grants |
| 12 | Registry / PrefixIndex |
| 13 | Audit log |
| 14 | Tasks (lifecycle state) |
| 15 | Workflow state |
| 16 | Quota counters |
| 17 | S3 backup |

### Topology A1: Single-Node Lite (No NATS)

**Config:** `AETHERLITE_CLUSTER_MODE=false` (default)

| # | Concern | Backend | Notes |
|---|---|---|---|
| 1 | Identity locks | Badger (in-process mutex) | Not distributed; session lock via `sync.Mutex` |
| 2 | Tunnel/request pins | Badger | In-process map |
| 3 | Session registry | Badger | In-process map, cleared on restart |
| 4 | Topic message log / routing | Badger | Local-only log, no cross-node delivery |
| 5 | KV store | Badger | `kv.BadgerKVStore` (Badger-backed) |
| 6 | Checkpoint store (small) | Badger | Small payloads only |
| 7 | Checkpoint store (large) | Badger (single file) | No size-gated Object Store |
| 8 | Task work queue | SQLite `tasks.db` | Polling dispatcher reads `orchestrated_task_queue` table directly |
| 9 | ACL rules | SQLite `acl.db` | Native SQLite store, no clustering |
| 10 | Authority requests | SQLite `acl.db` | Stored as ACL domain rows |
| 11 | Authority grants | SQLite `acl.db` | Stored as ACL domain rows |
| 12 | Registry / PrefixIndex | SQLite `registry.db` | Static in-memory PrefixIndex, rebuilt on startup or manual trigger |
| 13 | Audit log | SQLite `audit.db` | Batched async writer (single WAL lock per domain) |
| 14 | Tasks (lifecycle) | SQLite `tasks.db` | Tasks domain; migrations in `migrations/sqlite_tasks/` |
| 15 | Workflow state | SQLite `workflow.db` | Embedded workflow server uses `workflow.db` |
| 16 | Quota counters | In-memory | `quota.MemoryQuotaManager`; lost on restart |
| 17 | S3 backup | Optional (if `AETHERLITE_S3_BUCKET` set) | Local filesystem fallback: `{data_dir}/backups/` |

**NATS?** No. Embedded NATS server does not start. Badger is the sole durability layer (except S3).

**Durability story:** No off-node replication. Data loss on hardware failure unless S3 backups are configured. Acceptable for dev, edge, and single-tenant self-hosted with external backup discipline.

---

### Topology A2: Single-Node Cluster (Embedded NATS, R=1 Standalone)

**Config:** `AETHERLITE_CLUSTER_MODE=true`, `AETHERLITE_CLUSTER_PEERS=""` (empty)

| # | Concern | Backend | NATS Resource | Notes |
|---|---|---|---|---|
| 1 | Identity locks | JetStream KV | `aether_locks` (R=1) | Same code path as B/C; no Raft because `ClusterName=""` |
| 2 | Tunnel/request pins | JetStream KV | `aether_pins_tunnel`, `aether_pins_request` (R=1) | Pin refresh via KV Put |
| 3 | Session registry | JetStream KV | `aether_sessions` (R=1) | Single-node, but uses same interface as cluster |
| 4 | Topic message log / routing | JetStream Stream | `ag`, `tu`, `ta`, `tb`, `us`, `uw`, `ga`, `gu`, `pg`, `br`, `event`, `metric` (all R=1) | Subject-routed via NATS codec; local file store |
| 5 | KV store | JetStream KV | `aether_kv` (R=1) | Single bucket, all scopes; `.` as separator (NATS-safe) |
| 6 | Checkpoint store (small) | JetStream KV | `aether_checkpoints_kv` (R=1) | Payloads ≤256 KB |
| 7 | Checkpoint store (large) | JetStream Object Store | `aether_checkpoints_obj` (R=1) | Payloads >256 KB; sidecar index in `aether_checkpoints_idx` KV |
| 8 | Task work queue | JetStream Stream | `tasks.queue` (R=1, AckPolicy=Explicit) + SQLite `tasks.db` | JetStream dispatcher consumes; SQLite holds state. No clustering, but same interface as B/C. |
| 9 | ACL rules | JetStream KV + SQLite | `aether_acl_rules` KV (R=1) + `acl.db` | KV is source of truth for cluster awareness; SQLite is local persistence |
| 10 | Authority requests | JetStream KV + Stream | `aether_authority_requests` KV (R=1) + `authreq` stream (R=1) | CAS on KV; events on stream for lifecycle |
| 11 | Authority grants | JetStream KV | `aether_authority_grants` (R=1) | TTL'd entries; revocation = delete |
| 12 | Registry / PrefixIndex | JetStream KV | `aether_registry` (R=1) | Watch-driven PrefixIndex; real-time updates from KV (no polling) |
| 13 | Audit log | JetStream Stream + SQLite | `audit` stream (R=1) + `audit.db` | Append-only on stream; SQLite for fast reads |
| 14 | Tasks (lifecycle) | SQLite | `tasks.db` | Per-node only; no JetStream sync yet |
| 15 | Workflow state | SQLite | `workflow.db` | Per-node only; coordination via JetStream task events |
| 16 | Quota counters | In-memory | — | `quota.MemoryQuotaManager`; lost on restart |
| 17 | S3 backup | Optional (if `AETHERLITE_S3_BUCKET` set) | Leader-elected per-domain | Coordinator runs even with no peers; local filesystem fallback |

**NATS?** Yes. Embedded NATS server starts with `ClusterName=""` (from code line 344-346). JetStream runs standalone (R=1 file store), no Raft clustering (no peer routes). Same code path as B/C, so scale-up is config-only (add peers, restart).

**Why use A2 instead of A1?** Because you want to test cluster-mode behavior (JetStream Watch, cross-gateway PrefixIndex, authority lifecycle events) on a single machine. Once you add `AETHERLITE_CLUSTER_PEERS=nats://node2:6222` and restart, you're in B1 topology — no code changes, just config.

**Durability story:** No off-node replication (R=1). S3 backups optional; same as A1. Useful for dev/staging of cluster features.

---

### Topology B1: Dual-Node Async (Primary + Async Replica)

**Config:** `AETHERLITE_CLUSTER_MODE=true`, `AETHERLITE_CLUSTER_PEERS=nats://replica:6222`, `AETHERLITE_HA_MODE=async` (or omitted)

| # | Concern | Primary | Replica | Notes |
|---|---|---|---|---|
| 1 | Identity locks | JetStream KV `aether_locks` (R=1) | Source/mirror (async) | Primary keeps R=1; replica mirrors with lag |
| 2 | Tunnel/request pins | JetStream KV (R=1) | Source/mirror (async) | Pin updates replicate asynchronously |
| 3 | Session registry | JetStream KV `aether_sessions` (R=1) | Source/mirror (async) | Replica sees new sessions within 1–2s |
| 4 | Topic message log / routing | JetStream Stream (R=1) | Source consumer (async) | Primary produces; replica tails via consumer offset |
| 5 | KV store | JetStream KV `aether_kv` (R=1) | Source/mirror (async) | Replica cache-hot for reads |
| 6 | Checkpoint store (small) | JetStream KV `aether_checkpoints_kv` (R=1) | Mirror (async) | KV payloads replicate via mirror |
| 7 | Checkpoint store (large) | JetStream Object Store `aether_checkpoints_obj` (R=1) | Mirror (async) | Object store replication also asynchronous |
| 8 | Task work queue | JetStream Stream `tasks.queue` (R=1) + SQLite `tasks.db` (primary) | SQLite `tasks.db` (replica, via CDC) | CDC stream `cdc.tasks.*` applies writes to replica |
| 9 | ACL rules | JetStream KV `aether_acl_rules` (R=1) + SQLite `acl.db` (primary) | SQLite `acl.db` (replica, via CDC) | CDC stream `cdc.acl.*` applies changes; KV provides cross-gateway watch |
| 10 | Authority requests | JetStream KV `aether_authority_requests` (R=1) + stream `authreq` (R=1) | Mirror (async) | Lifecycle events asynchronously replicate |
| 11 | Authority grants | JetStream KV `aether_authority_grants` (R=1) | Mirror (async) | Replicated asynchronously |
| 12 | Registry / PrefixIndex | JetStream KV `aether_registry` (R=1) + SQLite `registry.db` (primary) | SQLite `registry.db` (replica, via CDC) | Watch on primary KV drives PrefixIndex in real-time; CDC syncs SQLite |
| 13 | Audit log | JetStream Stream `audit` (R=1) + SQLite `audit.db` (primary) | SQLite `audit.db` (replica, via CDC) | Append-only stream + per-node SQLite; CDC stream `cdc.audit.*` applies |
| 14 | Tasks (lifecycle) | SQLite `tasks.db` (primary) | SQLite `tasks.db` (replica, via CDC) | CDC stream syncs task state changes |
| 15 | Workflow state | SQLite `workflow.db` (primary) | SQLite `workflow.db` (replica, via CDC) | CDC stream `cdc.workflow.*` applies workflow state |
| 16 | Quota counters | In-memory | In-memory (may diverge) | Not replicated; both nodes track locally |
| 17 | S3 backup | Periodic per-domain (leader-elected) | Skips (primary is leader) | Both nodes can read from S3; leader writes snapshots |

**Replication lag:** Sub-second under nominal load; up to several seconds during primary failure window.

**RPO (Recovery Point Objective):** 1–5 seconds. Writes on primary within the replication lag before failure are lost.

**RTO (Recovery Time Objective):** 30s–2min. Operator detects primary down (LB health check, monitoring) and promotes replica via manual action or automation.

**Key difference from B2:** Writes proceed locally on primary with R=1. Replica is a **hot, readable mirror** (not cold standby). If primary dies mid-write before source/mirror sync completes, those writes are lost.

---

### Topology B2: Dual-Node Sync (Quorum)

**Config:** `AETHERLITE_CLUSTER_MODE=true`, `AETHERLITE_CLUSTER_PEERS=nats://peer2:6222`, `AETHERLITE_HA_MODE=sync`

| # | Concern | Backend | Replicas | Notes |
|---|---|---|---|---|
| 1 | Identity locks | JetStream KV `aether_locks` | R=2 (quorum) | Both nodes must ack before write returns |
| 2 | Tunnel/request pins | JetStream KV (pins_tunnel, pins_request) | R=2 (quorum) | Pin updates block on both-node ack |
| 3 | Session registry | JetStream KV `aether_sessions` | R=2 (quorum) | New sessions sync before client sees ack |
| 4 | Topic message log / routing | JetStream Stream (all topics) | R=2 (quorum) | Messages persist on both nodes before ack |
| 5 | KV store | JetStream KV `aether_kv` | R=2 (quorum) | All KV ops block on quorum |
| 6 | Checkpoint store (small) | JetStream KV `aether_checkpoints_kv` | R=2 (quorum) | Small payloads sync to both nodes |
| 7 | Checkpoint store (large) | JetStream Object Store `aether_checkpoints_obj` | R=2 (quorum) | Large payloads replicate to both nodes |
| 8 | Task work queue | JetStream Stream `tasks.queue` + SQLite `tasks.db` | R=2 (quorum) on stream; per-node SQLite | Work-queue claims replicated to both nodes |
| 9 | ACL rules | JetStream KV `aether_acl_rules` + SQLite `acl.db` | R=2 (quorum) on KV; per-node SQLite | Rule mutations block on both-node ack |
| 10 | Authority requests | JetStream KV `aether_authority_requests` + stream `authreq` | R=2 (quorum) | Lifecycle mutations block on both nodes |
| 11 | Authority grants | JetStream KV `aether_authority_grants` | R=2 (quorum) | Grant mutations replicate to both nodes |
| 12 | Registry / PrefixIndex | JetStream KV `aether_registry` + SQLite `registry.db` | R=2 (quorum) on KV; per-node SQLite | Registry mutations block on both-node ack |
| 13 | Audit log | JetStream Stream `audit` + SQLite `audit.db` | R=2 (quorum) on stream; per-node SQLite | Audit events replicate to both nodes before ack |
| 14 | Tasks (lifecycle) | SQLite `tasks.db` | Per-node only | No JetStream replication for task state |
| 15 | Workflow state | SQLite `workflow.db` | Per-node only | No JetStream replication |
| 16 | Quota counters | In-memory | Not replicated | Both nodes track independently |
| 17 | S3 backup | Periodic per-domain (leader-elected) | — | One node elected via KV CAS; uploads snapshots |

**Trade-off:** Strict consistency (quorum writes) at cost of write blocking when either node is down. **Not recommended** for most deployments — B1's explicit data loss window is more predictable than B2's "writes just hang" during partial failure.

**Automatic recovery:** When failed node rejoins, cluster automatically syncs it from the survivor (no manual promotion needed).

---

### Topology C: 3+ Node (Full HA)

**Config:** `AETHERLITE_CLUSTER_MODE=true`, `AETHERLITE_CLUSTER_PEERS=nats://node1:6222,nats://node2:6222,nats://node3:6222` (or more)

| # | Concern | Backend | Replicas | Notes |
|---|---|---|---|---|
| 1 | Identity locks | JetStream KV `aether_locks` | R=3 (quorum=2) | Survives 1 node down; writes proceed |
| 2 | Tunnel/request pins | JetStream KV (pins_tunnel, pins_request) | R=3 (quorum=2) | Pin updates require 2 of 3 acks |
| 3 | Session registry | JetStream KV `aether_sessions` | R=3 (quorum=2) | All nodes see new sessions immediately (quorum write) |
| 4 | Topic message log / routing | JetStream Stream (all topics) | R=3 (quorum=2) | At-least-once delivery; offset tracking per consumer |
| 5 | KV store | JetStream KV `aether_kv` | R=3 (quorum=2) | Revision-CAS for atomic updates; quorum-safe |
| 6 | Checkpoint store (small) | JetStream KV `aether_checkpoints_kv` | R=3 (quorum=2) | Payloads ≤256 KB synced to quorum |
| 7 | Checkpoint store (large) | JetStream Object Store `aether_checkpoints_obj` | R=3 (quorum=2) | Payloads >256 KB synced to quorum; sidecar index in KV |
| 8 | Task work queue | JetStream Stream `tasks.queue` (work-queue policy) + SQLite `tasks.db` | R=3 (quorum=2) on stream; per-node SQLite | Exactly-one claim via JetStream AckPolicy=Explicit |
| 9 | ACL rules | JetStream KV `aether_acl_rules` + SQLite `acl.db` | R=3 (quorum=2) on KV; per-node SQLite | Rule mutations replicated to quorum immediately |
| 10 | Authority requests | JetStream KV `aether_authority_requests` + stream `authreq` | R=3 (quorum=2) | Lifecycle CAS + events; quorum-safe |
| 11 | Authority grants | JetStream KV `aether_authority_grants` | R=3 (quorum=2) | TTL'd entries; revocation safe across cluster |
| 12 | Registry / PrefixIndex | JetStream KV `aether_registry` + SQLite `registry.db` | R=3 (quorum=2) on KV; per-node SQLite | Watch-driven PrefixIndex; real-time cross-gateway visibility (ms latency) |
| 13 | Audit log | JetStream Stream `audit` + SQLite `audit.db` | R=3 (quorum=2) on stream; per-node SQLite | High-volume append-only; persisted to quorum |
| 14 | Tasks (lifecycle) | SQLite `tasks.db` (per-node) | Per-node only | No cluster replication; coordination via JetStream events |
| 15 | Workflow state | SQLite `workflow.db` (per-node) | Per-node only | No cluster replication; coordination via JetStream task events |
| 16 | Quota counters | In-memory | Not replicated | Each node tracks independently |
| 17 | S3 backup | Periodic per-domain via leader-elected coordinator | — | One node elected; uploads per-domain snapshots to S3 per `defaultBackupPolicies()` |

**Write latency:** Quorum commits (2 of 3 nodes ack) before returning to client. Typical: 5–50ms for small writes.

**Failure tolerance:** Survives 1 node failure with zero data loss or write disruption. Quorum = 2 of 3, so writes proceed even if 1 node is down.

**Topology marker:** When 3 or more peers in `AETHERLITE_CLUSTER_PEERS`, startup logs show `topology=C (3+ node, R=3)`.

---

## Why Use Each Topology?

| Scenario | Topology | Reason |
|---|---|---|
| **Laptop dev** | A1 (lite) | No NATS overhead; Badger is fast. Restart wipes data (acceptable). |
| **Test cluster code on 1 machine** | A2 (cluster) | Same JetStream code path as B/C; S3 restore works; easy to add `AETHERLITE_CLUSTER_PEERS` and graduate to B1. |
| **2-node HA, accept ~5s data loss** | B1 (async) | Write availability during failures; simple failover (promote replica). Best for "if one dies, I need most of it." |
| **2-node HA, strict consistency** | B2 (sync) | All writes to quorum; no data loss. Trade-off: writes block if 1 node down. Automatic failover (no promotion). |
| **3+ node HA, production** | C (3+ node) | Survives single-node failure with zero data loss; writes proceed. Quorum-safe. Designed for scaling (workspace sharding in v2). |

---

## Environment Variables and Configuration

All clustering behavior is controlled by environment variables parsed in `cluster_wiring.go`. Cluster mode is **opt-in**; the default is single-node Badger (A1 lite).

### Master Switch and Peer Discovery

```bash
# Master switch: enable cluster mode (default: false)
# false → Topology A1 (lite, Badger-only, NATS not started)
# true  → Topology A2/B/C (embedded NATS + JetStream)
AETHERLITE_CLUSTER_MODE=true|false

# NATS peer list (NATS route URLs); empty = single-node (default: "")
# Comma-separated; spaces trimmed. Routes peer discovery.
# Empty + CLUSTER_MODE=true  → A2 (standalone NATS, R=1)
# 1 peer                       → B1 or B2 (depending on HA_MODE)
# 2+ peers                     → C (3+ node cluster)
AETHERLITE_CLUSTER_PEERS=nats://node1:6222,nats://node2:6222,nats://node3:6222

# HA mode for 2-node deployments (default: "auto")
# auto   = derived from peer count (R=1 for 0 peers, R=2 for 1 peer, R=3 for ≥2 peers)
# async  = B1 topology (primary R=1, replica mirrors asynchronously)
# sync   = B2 topology (both nodes R=2 quorum; writes block on partial failure)
AETHERLITE_HA_MODE=auto|async|sync
```

### NATS Embedded Server Ports

```bash
# NATS client port (default: 0 = auto-assign)
# Only relevant when CLUSTER_MODE=true (NATS starts)
AETHERLITE_NATS_CLIENT_PORT=4222

# NATS cluster route port for inter-node communication (default: 6222)
# Used in AETHERLITE_CLUSTER_PEERS URLs (nats://host:6222)
AETHERLITE_NATS_CLUSTER_PORT=6222
```

### Backup Storage

```bash
# S3 bucket for periodic backups; empty = local filesystem (default: "")
# Works in all topologies (A1/A2/B1/B2/C). Local fallback: {data_dir}/backups/
AETHERLITE_S3_BUCKET=my-backup-bucket

# S3 key prefix (default: "aetherlite/")
AETHERLITE_S3_PREFIX=backups/aetherlite/

# AWS region (default: "us-east-1")
AETHERLITE_S3_REGION=us-west-2

# Optional S3 endpoint for MinIO / R2 / other S3-compatible services
AETHERLITE_S3_ENDPOINT=https://s3.example.com

# Static credentials (optional; prefer IAM roles)
AETHERLITE_S3_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
AETHERLITE_S3_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

# Force path-style URLs for MinIO / non-AWS S3 (default: false)
AETHERLITE_S3_FORCE_PATH_STYLE=true

# Cold restore on startup from S3 (default: false; destructive if set)
# When true: before joining cluster, restore per-domain snapshots from S3
AETHERLITE_RESTORE_FROM_S3=true
```

### Task Dispatcher

```bash
# Dispatcher for orchestrated task assignment (default: polling for A1, jetstream for A2/B/C)
# polling   = legacy polling dispatcher; reads SQLite tasks_queue directly (single-node safe)
# jetstream = JetStream work-queue stream with exclusive consumer (cluster-safe)
AETHER_DISPATCHER=jetstream|polling
```

### Identifier Validation

```bash
# Strict validation of workspace/user/spec identifiers (default: true)
# When true: rejects *, >, whitespace, control chars, :: substrings
# Set to false only for legacy tenants with non-conformant identifiers
AETHER_STRICT_IDENTIFIER_CHARSET=true|false
```

---

## Per-Domain Backup Policies

From `defaultBackupPolicies()` in `cluster_wiring.go`. These are the cold-tier cadences when S3 is configured. Applies to topologies A2, B1, B2, and C (A1 lite has no backup coordinator).

| Domain | Bucket/Stream Type | Min Interval | S3 Prefix | Notes |
|---|---|---|---|---|
| `aether_registry` | KV | 1 minute | `registry/` | Agent metadata; critical for orchestration |
| `aether_acl_rules` | KV | 30 seconds | `acl/` | Security-critical; low write volume |
| `aether_authority_grants` | KV | 30 seconds | `auth_grants/` | Security-critical; revocations must persist |
| `aether_authority_requests` | KV | 30 seconds | `auth_requests/` | Authority lifecycle; security-critical |
| `audit` | Stream | 5 minutes | `audit/` | High-volume append-only; batch writes acceptable |
| `aether_kv` | KV | 5 minutes | `kv/` | Application-level state; rebuildable in worst case |
| `aether_checkpoints_idx` | KV | 5 minutes | `checkpoints/` | Index only (large blobs not backed up) |
| (Message topics) | Stream | **Never** | — | Transient by design; use JetStream `MaxAge` for retention |
| (Session locks / pins) | KV | **Never** | — | Ephemeral by definition; expire on TTL |

**Leader election:** The backup coordinator uses KV CAS on an internal `_aetherlite_backup_index` bucket to elect one node per domain. That node uploads snapshots; others skip.

---

## JetStream Resource Names (for ops/debugging)

When a cluster is running (A2/B/C), these NATS resources exist. Inspect with `nats` CLI:

```bash
nats kv ls              # List all KV buckets
nats stream ls          # List all streams
nats kv watch aether_locks  # Watch a bucket for changes
nats consumer ls msg    # View stream consumer groups
```

### Key-Value Buckets (All Topologies A2/B/C)

| Bucket Name | Purpose | Replica Count |
|---|---|---|
| `aether_locks` | Identity locks (session exclusivity) | R=1 (A2), R=1 (B1), R=2 (B2), R=3 (C) |
| `aether_sessions` | Session registry and metadata | R=1 (A2), R=1 (B1), R=2 (B2), R=3 (C) |
| `aether_pins_tunnel` | Proxy tunnel pin management | R=1 (A2), R=1 (B1), R=2 (B2), R=3 (C) |
| `aether_pins_request` | HTTP request pin management | R=1 (A2), R=1 (B1), R=2 (B2), R=3 (C) |
| `aether_kv` | Application-level KV store (all scopes) | R=1 (A2), R=1 (B1), R=2 (B2), R=3 (C) |
| `aether_registry` | Agent registry + PrefixIndex (watch-driven) | R=1 (A2), R=1 (B1), R=2 (B2), R=3 (C) |
| `aether_acl_rules` | ACL rules and fallback policies | R=1 (A2), R=1 (B1), R=2 (B2), R=3 (C) |
| `aether_authority_requests` | Authority request lifecycle (CAS + status) | R=1 (A2), R=1 (B1), R=2 (B2), R=3 (C) |
| `aether_authority_grants` | Issued authority grants (TTL'd) | R=1 (A2), R=1 (B1), R=2 (B2), R=3 (C) |
| `aether_checkpoints_kv` | Small checkpoints (≤256 KB) | R=1 (A2), R=1 (B1), R=2 (B2), R=3 (C) |
| `aether_checkpoints_idx` | Checkpoint index (location metadata) | R=1 (A2), R=1 (B1), R=2 (B2), R=3 (C) |

### JetStream Streams (All Topologies A2/B/C)

| Stream Subject Pattern | Retention | Purpose |
|---|---|---|
| `ag.>`, `tu.>`, `ta.>`, `tb.>` | MaxAge=24h | Agent/task topic message log |
| `us.>`, `uw.>` | MaxAge=24h | User window and workspace message log |
| `ga.>`, `gu.>` | MaxAge=24h | Global agent/user broadcast topics |
| `pg.>` | MaxAge=24h | Progress updates (server-side filtering) |
| `br.>` | MaxAge=24h | Bridge cross-workspace messages |
| `event.>` (→ `event::receiver{shard}`) | MaxAge=7d | Workflow engine event fan-in |
| `metric.>` (→ `metric::receiver{shard}`) | MaxAge=7d | Metrics bridge fan-in |
| `tk.>` | MaxAge=24h | Task event stream (Phase 4, recursive subscriptions) |
| `authreq.{ws}.events` | MaxAge=7d | Authority request lifecycle events |
| `audit` | MaxAge=7d | Append-only audit log |
| `tasks.queue` | AckPolicy=Explicit | Work-queue stream for orchestrated task assignment (C topology only in B1/B2) |
| `cdc.acl.*`, `cdc.tasks.*`, `cdc.audit.*`, `cdc.workflow.*`, `cdc.registry.*` | MaxAge=24h | Change-data-capture streams for per-node SQLite sync (B topologies) |

**Note:** Subject names shown above use NATS codec translation (`::`→`.` and escape sequences). In aether-level API calls, use canonical `::` form; the router translates automatically.

### Object Stores (A2/B/C)

| Bucket Name | Purpose |
|---|---|
| `aether_checkpoints_obj` | Large checkpoints (>256 KB) |

---

## Operator Runbook

### Starting Topology A1: Single-Node Lite (Default)

**Fastest startup, development/edge:**

```bash
export AETHERLITE_DATA_DIR=./aether-lite-data
export AETHERLITE_DEV=true
export AETHER_ALLOW_DEV_MODE=true

./aetherlite \
  --dev \
  --insecure-admin \
  --data-dir $AETHERLITE_DATA_DIR
```

**With optional S3 backup:**

```bash
export AETHERLITE_DATA_DIR=./aether-lite-data
export AETHERLITE_S3_BUCKET=my-backup-bucket
export AETHERLITE_S3_REGION=us-east-1
export AWS_PROFILE=aetherlite-admin

./aetherlite --dev --data-dir $AETHERLITE_DATA_DIR
```

**Verify:** Logs show `AetherLite is ready` and **no mention of NATS** (not started in A1).

---

### Starting Topology A2: Single-Node Cluster (Test Cluster Code)

**Same machine, but with JetStream and cluster-mode code paths:**

```bash
export AETHERLITE_CLUSTER_MODE=true
export AETHERLITE_CLUSTER_PEERS=""  # empty = standalone NATS
export AETHERLITE_DATA_DIR=./aether-lite-data
export AETHERLITE_S3_BUCKET=my-backup-bucket
export AETHER_ALLOW_DEV_MODE=true

./aetherlite --dev --data-dir $AETHERLITE_DATA_DIR
```

**Verify:** Logs show `cluster mode enabled; topology=A (single-node, R=1)` and `embedded NATS server ready; JetStream available (replicas: 1)`.

**Why this matters:** You now use the same backend code (JetStream KV, Streams) as B/C. Next step: add a second node via `AETHERLITE_CLUSTER_PEERS=nats://node2:6222` → B1 (no code change).

---

### Adding a Second Node (A2 or B1 → B1)

Assume you have a running node at `node1.example.com:6222` (NATS cluster port).

**On the replica node:**

```bash
export AETHERLITE_CLUSTER_MODE=true
export AETHERLITE_CLUSTER_PEERS=nats://node1.example.com:6222
export AETHERLITE_HA_MODE=async  # or omitted; async is the default for 1 peer
export AETHERLITE_DATA_DIR=./aether-lite-data
export AETHERLITE_S3_BUCKET=my-backup-bucket

./aetherlite --data-dir $AETHERLITE_DATA_DIR
```

**On the primary node (update to include replica):**

```bash
export AETHERLITE_CLUSTER_MODE=true
export AETHERLITE_CLUSTER_PEERS=nats://replica.example.com:6222
export AETHERLITE_HA_MODE=async
export AETHERLITE_DATA_DIR=./aether-lite-data

./aetherlite --data-dir $AETHERLITE_DATA_DIR
```

**Verify replication lag:** Watch KV on primary, observe changes on replica:

```bash
# Primary terminal
nats kv watch aether_sessions

# Replica terminal (separate machine)
nats kv watch aether_sessions
```

Changes should appear on replica within 1–2 seconds.

**Sticky load balancing:** Use nginx or K8s Service with `sessionAffinity: ClientIP` to pin clients to the same node.

---

### Promoting Replica to Primary (B1 Failover)

**Scenario:** Primary crashes. Replica has ~95% of in-flight writes (1–5s window lost).

**Operator action:**

1. **Detect primary failure** (LB health check, monitoring, alert).
2. **Update LB / DNS** to point to replica address.
3. **Restart replica as new primary:**
   ```bash
   export AETHERLITE_CLUSTER_MODE=true
   export AETHERLITE_CLUSTER_PEERS=""  # empty = run as standalone
   export AETHERLITE_DATA_DIR=./aether-lite-data
   
   ./aetherlite --data-dir $AETHERLITE_DATA_DIR
   ```
4. **Clients reconnect** to the new primary (via updated LB).
5. **Optional:** Restore lost primary from S3 backup if needed.

**Data loss:** ~1–5 seconds of in-flight writes. This is the B1 trade-off for write availability.

---

### Deploying Topology C (3+ Nodes)

**Production HA: all nodes join cluster automatically.**

On each of 3 nodes:

```bash
export AETHERLITE_CLUSTER_MODE=true
export AETHERLITE_CLUSTER_PEERS=nats://node1:6222,nats://node2:6222,nats://node3:6222
export AETHERLITE_DATA_DIR=/var/lib/aetherlite
export AETHERLITE_S3_BUCKET=aether-backups

./aetherlite --data-dir $AETHERLITE_DATA_DIR
```

All three nodes join the `aetherlite` cluster automatically. Watch for:

```
cluster mode enabled; topology=C (3+ node, R=3)
embedded NATS server ready; JetStream available (replicas: 3)
```

**Verify quorum health:**

```bash
nats server info
# Look for: Cluster: yes, Cluster Size: 3, Healthy: true
```

---

### Restoring from S3 Backup

**Cold start scenario:** New deployment or node recovery.

```bash
export AETHERLITE_RESTORE_FROM_S3=true
export AETHERLITE_S3_BUCKET=aether-backups
export AETHERLITE_S3_REGION=us-east-1
export AETHERLITE_DATA_DIR=/var/lib/aetherlite

./aetherlite --data-dir $AETHERLITE_DATA_DIR
```

**Flow:**
1. Embedded NATS starts.
2. Before joining cluster, restore reads S3 manifests per domain.
3. Per-domain snapshots streamed into local JetStream.
4. Node joins cluster; syncs remaining changes from peers.

**Restore order:** Registry → ACL → authority grants/requests → audit → KV → checkpoints (per code). If any fails, skipped with warning (non-fatal).

---

### Removing a Failed Node (C Topology)

**Scenario:** One of 3 nodes dies and can't be recovered immediately.

**On surviving nodes:** No manual action needed. Cluster remains operable at quorum (2 of 3).

**When ready to replace the node:**

1. Restart with updated peer list (omitting dead node) — **optional** (cluster tolerates absent peer).
2. Deploy new node with same `AETHERLITE_CLUSTER_PEERS` (includes all 3 addresses).
3. New node restores from S3:
   ```bash
   export AETHERLITE_RESTORE_FROM_S3=true
   ./aetherlite --data-dir /var/lib/aetherlite
   ```

---

## Failure Modes and Recovery

### Topology A1 (Single-Node Lite)

| Failure | Symptom | Recovery |
|---|---|---|
| **Disk full** | Write errors | Free disk space; restart |
| **Process crash** | All clients disconnected | Restart binary; clients reconnect |
| **Hardware failure** | Total data loss (unless S3 configured) | Restore from S3 (if configured) |
| **Badger corruption** | `panic: invalid header` on startup | Delete `badger/` dir; lose all data; restore from S3 |

---

### Topology A2 (Single-Node Cluster)

| Failure | Symptom | Recovery |
|---|---|---|
| **Disk full** | gRPC server rejects writes | Free disk space; restart |
| **Process crash** | All clients disconnected | Restart binary; clients reconnect |
| **NATS file store corruption** | JetStream streams unreachable | Delete `nats/` subdir; lose all data; restore from S3 |
| **Hardware failure** | Total data loss (unless S3 configured) | Restore from S3 with `AETHERLITE_RESTORE_FROM_S3=true` |

---

### Topology B1 (Dual-Node Async)

| Failure | Symptom | Recovery |
|---|---|---|
| **Primary down** | New sessions time out (sticky LB) | LB health check fails; switches to replica; clients reconnect |
| **Replica lag >5s** | Replica lags on writes | Monitor lag; resolves when network recovers |
| **Replica disk full** | Mirror consumer blocks; replication increases | Free disk; mirror catches up automatically |
| **Both down** | Total loss of data not on S3 | Restore from S3; one becomes primary, other joins as replica |
| **Primary lost mid-write** | 1–5s of data lost | Promote replica; lost data between primary crash and mirror lag |
| **Network partition** | Primary isolated from replica | When partition heals, source/mirror resync |

---

### Topology B2 (Dual-Node Sync)

| Failure | Symptom | Recovery |
|---|---|---|
| **1 node down** | Quorum = 2 of 2; writes block | Restart failed node; automatic catch-up |
| **Both nodes down** | Total loss (no data on S3) | Restore from S3 |
| **Network partition** | Each node isolated; both read-only | When partition heals, nodes rejoin cluster |

---

### Topology C (3+ Nodes)

| Failure | Symptom | Recovery |
|---|---|---|
| **1 node down** | Quorum = 2 of 3; writes proceed normally | Restart failed node; catches up from S3 + peers |
| **1 node slow** | Write latency increases (slower ack) | If latency >500ms, consider migrating or upgrading |
| **2 nodes down** | Quorum lost; writes fail (reads work) | Restart one node; cluster becomes operational |
| **Network partition** | Isolated nodes go read-only | When partition heals, isolated nodes rejoin |
| **All 3 down** | Total loss (no data on S3) | Restore from S3; restart all 3 with `AETHERLITE_RESTORE_FROM_S3=true` |

---

## Identifier Charset and NATS Codec

The implementation includes a bidirectional codec that translates aether's canonical `::`-separated topics to NATS' `.`-separated subject names, with escaping for reserved chars.

**Example (common case: reverse-DNS impl identifier):**

```
Aether topic:    ag::acme::com.example.chat-agent::v1
                 ↓ (split on ::)
Tokens:          ["ag", "acme", "com.example.chat-agent", "v1"]
                 ↓ (escape . within tokens)
Escaped:         ["ag", "acme", "com_2Eexample_2Echat-agent", "v1"]
                 ↓ (join on .)
NATS subject:    ag.acme.com_2Eexample_2Echat-agent.v1
```

**Charset policy:**
- By default, `AETHER_STRICT_IDENTIFIER_CHARSET=true` — rejects `*`, `>`, whitespace, control chars, and `::` substrings in identifiers.
- Set to `false` only for legacy deployments with non-conformant identifiers.

---

## Related Documentation

- **Full Aether documentation:** `/home/drew/scitrera-aether3-go/oss-repo/server/specification.md` (v4.0).
- **Error codes and signals:** `/home/drew/scitrera-aether3-go/oss-repo/server/docs/error-codes.md`.
- **KV scopes reference:** `/home/drew/scitrera-aether3-go/oss-repo/server/docs/kv-scopes.md`.
- **Admin API:** Built-in at `http://localhost:31880` (A1/A2) or `http://{gateway}:31880` (B/C).

---

## Glossary

| Term | Definition |
|---|---|
| **Badger** | Embedded in-process key-value store; single-node only. Topology A1 uses it for locks, routing, KV, checkpoints. |
| **JetStream** | NATS' distributed append-only log substrate; provides Streams, KV, Object Store. Topologies A2/B/C use it. |
| **KV (Key-Value bucket)** | NATS JetStream primitive; atomic CAS, revision tracking, watch. Used for locks, sessions, checkpoints, ACL. |
| **Stream** | NATS JetStream primitive; append-only log, offset tracking, consumers, retention. Used for topics, audit, task queue. |
| **Object Store** | NATS JetStream primitive; blob storage for large (>256 KB) checkpoints. |
| **Replicas (R)** | Number of nodes holding a copy of JetStream data. R=1 = one node. R=2 = two nodes. R=3 = three nodes. |
| **Quorum** | Minimum nodes that must ack a write before it succeeds. For R=N, quorum = (N/2)+1. R=2→quorum=2. R=3→quorum=2. |
| **Source/Mirror** | NATS feature: one stream (source) replicates asynchronously to another (mirror) on different node. One-way. |
| **CDC** | Change-Data-Capture; JetStream streams capturing per-node SQLite writes, propagating to other nodes (B topologies). |
| **RPO** | Recovery Point Objective; acceptable data loss (time). A1/A2: all (no replication). B1: 1–5s. B2/C: 0 (quorum). |
| **RTO** | Recovery Time Objective; time to restore after failure. A1: n/a (restart). B1: 30s–2min (manual). B2/C: <10s (auto). |
| **ClusterName** | NATS server config. Empty → no Raft (standalone). "aetherlite" → Raft clustering enabled (B/C). |

---

## Appendix: Environment Variable Reference

| Variable | Type | Default | Topology | Purpose |
|---|---|---|---|---|
| `AETHERLITE_CLUSTER_MODE` | bool | false | all | Master switch (false→A1, true→A2/B/C) |
| `AETHERLITE_CLUSTER_PEERS` | CSV | "" | A2/B/C | NATS route URLs (empty→A2, 1→B, 2+→C) |
| `AETHERLITE_HA_MODE` | string | auto | B | Replication mode (auto/async/sync) |
| `AETHERLITE_NATS_CLIENT_PORT` | int | 0 | A2/B/C | NATS client port (0=ephemeral) |
| `AETHERLITE_NATS_CLUSTER_PORT` | int | 6222 | A2/B/C | NATS cluster route port |
| `AETHERLITE_S3_BUCKET` | string | "" | all | S3 bucket (empty→local filesystem) |
| `AETHERLITE_S3_PREFIX` | string | aetherlite/ | all | S3 key prefix |
| `AETHERLITE_S3_REGION` | string | us-east-1 | all | AWS region |
| `AETHERLITE_S3_ENDPOINT` | string | "" | all | Custom S3 endpoint (MinIO, R2) |
| `AETHERLITE_S3_ACCESS_KEY` | string | "" | all | AWS access key (prefer IAM role) |
| `AETHERLITE_S3_SECRET_KEY` | string | "" | all | AWS secret key |
| `AETHERLITE_S3_FORCE_PATH_STYLE` | bool | false | all | Force path-style URLs |
| `AETHERLITE_RESTORE_FROM_S3` | bool | false | all | Restore JetStream from S3 on cold start |
| `AETHER_DISPATCHER` | string | auto | A2/B/C | Task dispatcher (polling/jetstream) |
| `AETHER_STRICT_IDENTIFIER_CHARSET` | bool | true | all | Validate identifiers (reject reserved chars) |
| `AETHERLITE_DATA_DIR` | string | ./aether-lite-data | all | Data directory (SQLite, Badger, NATS files) |
| `AETHERLITE_PORT` | int | 50051 | all | gRPC server port |
| `AETHERLITE_ADMIN_PORT` | int | 31880 | all | Admin UI port |
| `AETHERLITE_DEV` | bool | false | all | Development mode (relaxed security) |
| `AETHER_ALLOW_DEV_MODE` | bool | false | all | Opt-in for relaxed security features |
