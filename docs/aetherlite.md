# AetherLite: Embedded Single-Binary Deployment

AetherLite is a deployment mode for Aether that replaces all external services with embedded in-process alternatives. There is no Redis, no RabbitMQ, and no PostgreSQL to install or manage. Everything runs inside a single process backed by [Badger](https://github.com/dgraph-io/badger) (KV and messaging) and [SQLite](https://sqlite.org) (relational data).

## When to Use AetherLite

| Scenario | AetherLite | Full Aether |
|---|---|---|
| Local development and testing | Recommended | Possible but heavier |
| Single-node production | Supported | Overkill unless you need Redis/RabbitMQ for other reasons |
| Edge or embedded deployments | Recommended | Usually impractical |
| Prototyping / demos | Recommended | — |
| Multi-node horizontal scaling | **Not supported** | Required |
| High-throughput (thousands of msg/s) | Evaluate carefully | Preferred |

AetherLite is production-ready for single-node workloads. It is **not** a development-only toy — data is durable on disk and message replay works correctly.

## How to Run It

### Option 1: Single binary (`cmd/aetherlite`)

The `aetherlite` binary combines the gateway, workflow server, and messaging bridge in one process and always runs in lite mode. No flag is needed.

```bash
cd server
go build -o aetherlite ./cmd/aetherlite

# Development (no auth, CORS wildcard)
AETHER_ALLOW_DEV_MODE=true ./aetherlite --dev --insecure-admin

# With a custom data directory
AETHER_ALLOW_DEV_MODE=true ./aetherlite --data-dir /var/lib/aether-lite --insecure-admin
```

**Flags:**

| Flag | Default | Description |
|---|---|---|
| `--config <path>` | — | Optional YAML config file |
| `--data-dir <path>` | `./aether-lite-data` | Directory for SQLite and Badger storage |
| `--port <n>` | `50051` | gRPC server port |
| `--admin-port <n>` | `31880` | Admin UI port |
| `--dev` | `false` | Development mode (relaxed security) |
| `--insecure-admin` | `false` | Allow admin API without auth key (requires `AETHER_ALLOW_DEV_MODE`) |
| `--workflow` | `true` | Enable embedded workflow server |
| `--workflow-admin-port <n>` | `31881` | Workflow admin API port |
| `--msgbridge` | `false` | Enable embedded messaging bridge |
| `--msgbridge-admin-port <n>` | `31882` | Msgbridge admin API port |
| `--version` | — | Print version and exit |

### Option 2: Individual binaries with `--lite` flag

The `--lite` flag (or `mode: lite` in config) switches any standard binary to lite mode:

```bash
go build -o gateway ./cmd/gateway
./gateway --lite --data-dir ./aether-lite-data --insecure-admin
```

This is useful when you want only the gateway component without workflow or msgbridge.

## Data Directory Layout

AetherLite stores all persistent state under a single directory:

```
aether-lite-data/
  aether.db       — SQLite: tasks, ACL rules, audit log, agent registry, orchestration profiles
  badger/         — Badger: sessions, KV store, checkpoints, tokens, message log, consumer offsets
  workflow.db     — SQLite: workflow rules, definitions, executions, schedules, state machines
  msgbridge.db    — SQLite: messaging bridge state (only present when msgbridge is enabled)
```

The data directory is created automatically on first run. Back it up like any other database directory.

## Configuration

AetherLite uses the same YAML configuration format as the full gateway. When using `./aetherlite`, `mode: lite` is always forced. When using `./gateway --lite`, add `mode: lite` to your YAML.

Minimal production config example:

```yaml
mode: lite

lite:
  data_dir: /var/lib/aether-lite

gateway:
  port: 50051
  gateway_id: "aetherlite-prod-1"

admin:
  enabled: true
  port: 31880
  api_key: "your-secret-api-key-at-least-16-chars"

audit:
  enabled: true
  event_types: [connection, auth, message, kv, admin, acl]

log_level: info
```

### Configuration Keys Specific to Lite Mode

| YAML Key | Default | Description |
|---|---|---|
| `mode` | `"full"` | Set to `"lite"` to activate AetherLite mode |
| `lite.data_dir` | `"./aether-lite-data"` | Directory for all persistent storage |

In lite mode, the `postgres`, `redis`, and `rabbitmq` configuration blocks are ignored. Validation of those fields is skipped.

## Architecture Overview

AetherLite uses the same gateway code as full Aether. The only difference is which backend implementations are wired in at startup.

```
┌───────────────────────────────────────────────┐
│               AetherLite Process               │
│                                                │
│  ┌─────────────┐  ┌──────────┐  ┌──────────┐  │
│  │   Gateway   │  │ Workflow │  │MsgBridge │  │
│  │  gRPC :50051│  │ (opt-in) │  │(opt-in)  │  │
│  └──────┬──────┘  └────┬─────┘  └────┬─────┘  │
│         └──────────────┴──────────────┘         │
│                        │                        │
│           ┌────────────┴────────────┐           │
│           │                         │           │
│   ┌───────▼────────┐   ┌───────────▼────────┐  │
│   │   Badger DB    │   │     SQLite DB      │  │
│   │                │   │                    │  │
│   │ sess:  sessions│   │ Tasks              │  │
│   │ kv:    KV store│   │ ACL rules          │  │
│   │ ckpt:  checkpts│   │ Audit log          │  │
│   │ tok:   tokens  │   │ Orchestration      │  │
│   │ msg:   messages│   │ Agent registry     │  │
│   │ off:   offsets │   │                    │  │
│   └────────────────┘   └────────────────────┘  │
└───────────────────────────────────────────────┘
```

**Backend substitution table:**

| Full Mode Component | AetherLite Component | Interface |
|---|---|---|
| Redis session registry | `BadgerSessionRegistry` | `SessionManager` |
| Redis KV store | `BadgerKVStore` | `KVReadWriter` |
| Redis checkpoint store | `BadgerCheckpointStore` | `CheckpointManager` |
| Redis token store | `BadgerTokenStore` | `state.TokenStore` |
| RabbitMQ Streams router | `BadgerRouter` | `MessageRouter` |
| PostgreSQL task store | SQLite task store | `*tasks.TaskStore` |
| AMQP task dispatcher | `MemoryTaskDispatcher` | `TaskDispatcher` |
| Redis quota counters | `MemoryQuotaManager` | `QuotaChecker` |

The gateway server (`internal/gateway/server.go`) is identical in both modes.

## Trade-offs vs Full Mode

| Property | AetherLite | Full Aether |
|---|---|---|
| External dependencies | None | Redis, RabbitMQ, PostgreSQL |
| Horizontal scaling | Single-node only | Multiple instances supported |
| Message durability | Durable (Badger on disk) | Durable (RabbitMQ Streams) |
| Message replay on reconnect | Supported | Supported |
| Quota persistence across restarts | No (in-memory) | Yes (Redis) |
| Operational complexity | Low (one process, one dir) | Higher (3 external services) |
| Throughput ceiling | Limited by local disk I/O | Higher (distributed) |
| PostgreSQL features | Via SQLite (same schema) | Native PostgreSQL |

## Upgrading from AetherLite to Full Mode

If you start with AetherLite and later need to scale horizontally, you can migrate:

1. **Export task data** from `aether.db` using standard SQLite tools (`sqlite3 aether.db .dump > tasks.sql`). The schema matches the PostgreSQL schema (the `sqlite_compat` driver bridges syntax differences); import the dump into PostgreSQL after adjusting any SQLite-specific syntax.
2. **KV and checkpoint data** in Badger has no direct export path to Redis. For most deployments these are regenerated automatically when agents reconnect. If you require migration, write a small Go program using the Badger reader and Redis writer.
3. **Reconfigure** the gateway with Redis, RabbitMQ, and PostgreSQL connection details and remove `mode: lite`.
4. **Restart** — clients reconnect automatically and their state is re-established.

## Limitations and Known Issues

- **Single-node only.** Badger and SQLite are local files; they cannot be shared across multiple gateway processes.
- **Quota counts reset on restart.** The in-memory quota manager does not persist connection counters across restarts. Workspace connection limits are re-enforced from zero after each restart.
- **Back-pressure on message channels.** Under extreme back-pressure, the live subscriber delivery channel may drop messages. Those messages remain durably stored in Badger and are replayed on the subscriber's next reconnect.
- **SQLite concurrency.** SQLite is configured in WAL mode with a 5-second busy timeout. Very high write concurrency may cause transient `SQLITE_BUSY` errors on the audit log or task store. If you hit this consistently, consider the full PostgreSQL deployment.
- **No PG NOTIFY.** The task dispatcher uses polling rather than PostgreSQL NOTIFY/LISTEN. Task assignment latency may be slightly higher than in full mode (typically under a second).

## See Also

- [quickstart.md](quickstart.md) — getting started with AetherLite in minutes
- [architecture.md](architecture.md) — full architecture with AetherLite backend details
- [deployment.md](deployment.md) — systemd unit, Docker, and config file examples
- [horizontal-scaling.md](horizontal-scaling.md) — full mode multi-instance scaling
