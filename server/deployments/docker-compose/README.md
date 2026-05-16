# Aether Docker Compose Configurations

This directory contains Docker Compose configurations for running Aether locally.
All files are intended for **development and testing only** — not production deployment.

---

## Files at a glance

| File | Description |
|---|---|
| `multi-instance.yaml` | Full Aether Gateway — 3 instances, nginx LB, Redis, RabbitMQ, PostgreSQL |
| `cluster-single.yaml` | AetherLite Topology A — 1 node, cluster mode, MinIO backup |
| `cluster-ha.yaml` | AetherLite Topology B — 2 nodes, async HA, MinIO backup, optional nginx LB |
| `cluster.yaml` | AetherLite Topology C — 3 nodes, R=3 quorum, MinIO backup, optional nginx LB |

---

## AetherLite Cluster Topologies

AetherLite is the embedded single-binary server: gateway + workflow + SQLite + embedded NATS/JetStream.
No external Redis, RabbitMQ, or PostgreSQL is required. All cluster coordination runs over embedded NATS.

### Topology A — Single-node (`cluster-single.yaml`)

One AetherLite node running in cluster mode with an embedded NATS/JetStream server.
No peers — JetStream runs standalone (R=1). Backups go to MinIO.

**Use for:** local development, integration tests, single-machine production (no HA required).

```bash
# From repository root
docker compose -f server/deployments/docker-compose/cluster-single.yaml up

# Detached
docker compose -f server/deployments/docker-compose/cluster-single.yaml up -d

# With debug sidecar (netshoot)
docker compose -f server/deployments/docker-compose/cluster-single.yaml --profile debug up
```

| Service | Port(s) | Purpose |
|---|---|---|
| `aetherlite` | 50051 (gRPC), 31880 (Admin UI) | AetherLite gateway |
| `minio` | 9000 (S3 API), 9001 (Console) | S3-compatible backup store |
| `minio-init` | — | One-shot bucket creator (exits) |
| `netshoot` | — | Debug sidecar (`--profile debug`) |

### Topology B — 2-node HA (`cluster-ha.yaml`)

Two AetherLite nodes forming a NATS cluster with async source+mirror replication (R=2).
`AETHERLITE_HA_MODE=async` — leader acknowledges writes; replica catches up asynchronously.
Tolerates one node failure; brief in-flight message loss is possible during the failover window.

**Use for:** staging, moderate-availability scenarios, 2-machine setups.

```bash
# Nodes only
docker compose -f server/deployments/docker-compose/cluster-ha.yaml up

# With nginx gRPC load balancer (ip_hash sticky sessions) on :50050
docker compose -f server/deployments/docker-compose/cluster-ha.yaml --profile lb up

# With debug sidecar
docker compose -f server/deployments/docker-compose/cluster-ha.yaml --profile debug up
```

| Service | Port(s) | Purpose |
|---|---|---|
| `aetherlite-1` | 50051 (gRPC), 31880 (Admin UI) | Cluster node 1 |
| `aetherlite-2` | 50052 (gRPC), 31881 (Admin UI) | Cluster node 2 |
| `minio` | 9000, 9001 | S3-compatible backup store |
| `minio-init` | — | One-shot bucket creator (exits) |
| `nginx` | 50050 (gRPC LB), 8080 (health) | Load balancer (`--profile lb`) |
| `netshoot` | — | Debug sidecar (`--profile debug`) |

### Topology C — 3-node Full HA (`cluster.yaml`)

Three AetherLite nodes forming a NATS cluster with R=3 quorum replication.
`AETHERLITE_HA_MODE=auto` — the embedded NATS server auto-selects R=3 when three peers join.
Tolerates one node failure with **no data loss** (any two nodes constitute a quorum).

**Use for:** production-like testing, chaos engineering, high-availability validation.

```bash
# Nodes only
docker compose -f server/deployments/docker-compose/cluster.yaml up

# With nginx gRPC load balancer (ip_hash sticky sessions) on :50050
docker compose -f server/deployments/docker-compose/cluster.yaml --profile lb up

# With debug sidecar
docker compose -f server/deployments/docker-compose/cluster.yaml --profile debug up
```

| Service | Port(s) | Purpose |
|---|---|---|
| `aetherlite-1` | 50051 (gRPC), 31880 (Admin UI) | Cluster node 1 |
| `aetherlite-2` | 50052 (gRPC), 31881 (Admin UI) | Cluster node 2 |
| `aetherlite-3` | 50053 (gRPC), 31882 (Admin UI) | Cluster node 3 |
| `minio` | 9000, 9001 | S3-compatible backup store |
| `minio-init` | — | One-shot bucket creator (exits) |
| `nginx` | 50050 (gRPC LB), 8080 (health) | Load balancer (`--profile lb`) |
| `netshoot` | — | Debug sidecar (`--profile debug`) |

---

## Common Operations (AetherLite cluster files)

### View logs

```bash
# All services
docker compose -f server/deployments/docker-compose/cluster.yaml logs -f

# Specific node
docker compose -f server/deployments/docker-compose/cluster.yaml logs -f aetherlite-1
```

### Stop and clean up

```bash
# Stop, keep volumes (data persists for next run)
docker compose -f server/deployments/docker-compose/cluster.yaml down

# Stop and delete all volumes (full reset)
docker compose -f server/deployments/docker-compose/cluster.yaml down -v
```

### Build the image locally

All three files use the same image (`scitrera/aether-gateway:latest` by default).
The `aetherlite` binary is included in the standard Dockerfile build — no separate image needed.

```bash
# From repository root (build context must be the repo root)
docker build -f server/Dockerfile -t scitrera/aether-gateway:latest .

# Override image for a compose file
AETHERLITE_IMAGE=scitrera/aether-gateway:dev \
  docker compose -f server/deployments/docker-compose/cluster.yaml up
```

### Admin UI

After startup, the Admin UI is available at:
- Topology A: http://localhost:31880
- Topology B node 1: http://localhost:31880 / node 2: http://localhost:31881
- Topology C node 1: http://localhost:31880 / node 2: http://localhost:31881 / node 3: http://localhost:31882

The Admin UI is opened with `AETHERLITE_INSECURE_ADMIN=true` + `AETHER_ALLOW_DEV_MODE=true`
so no authentication is required in these dev configurations.

### MinIO console

Browse backup objects and verify snapshot uploads at http://localhost:9001.
Default credentials: `minioadmin` / `minioadmin` (override with `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`).

### Chaos / network testing

Activate the `debug` profile to get a `netshoot` container on the same network.
From inside it you can run `tcpdump`, `iperf3`, `nmap`, `ip route`, etc.:

```bash
docker exec -it aetherlite-cluster-netshoot bash
# or for HA topology:
docker exec -it aetherlite-ha-netshoot bash
```

Simulate a node failure:
```bash
# Kill node 2 (cluster.yaml)
docker stop aetherlite-cluster-2

# Watch the remaining nodes maintain quorum in the logs
docker compose -f server/deployments/docker-compose/cluster.yaml logs -f aetherlite-1

# Bring it back
docker start aetherlite-cluster-2
```

---

## Operator gotchas

### Port conflicts

The three AetherLite compose files use overlapping host ports (9000/9001 for MinIO, 50051/31880 for node 1).
Only one cluster file should be running at a time unless you override the port mappings.

### Volume cleanup

Data volumes (`aetherlite-*-data`) persist between `down` / `up` cycles.
This is intentional — JetStream state survives restarts.
Run `down -v` for a clean slate or to test cold-start restore from MinIO.

### MinIO seeding

The `minio-init` service creates the `aetherlite-backups` bucket on first run and exits.
Docker Compose records it as `service_completed_successfully`; the aetherlite nodes
depend on this condition so they start only after the bucket exists.
If you pre-create the bucket externally, `minio-init` will harmlessly no-op (`--ignore-existing`).

### Cluster formation timing

NATS peer routes are established asynchronously after each node starts.
The 60-second `start_period` in each aetherlite healthcheck accounts for this.
If a node logs `"failed to start embedded NATS server"` with a route-dial error on
first boot, it is retrying peer connections — this is normal and resolves within a
few seconds once all nodes are up.

### Restore from S3

To restore JetStream state from a MinIO snapshot on a fresh cluster:

```bash
AETHERLITE_RESTORE_FROM_S3=true \
  docker compose -f server/deployments/docker-compose/cluster.yaml up
```

Set this only on the **first** node to start; subsequent nodes will sync via NATS replication.

---

## Full Aether Gateway (`multi-instance.yaml`)

The existing multi-instance configuration runs three full Aether Gateway instances
backed by external infrastructure (Redis, RabbitMQ Streams, PostgreSQL).

See the section below for its original documentation.

### Start all services:
```bash
RABBITMQ_PASS=yourpassword POSTGRES_PASSWORD=yourpassword \
  docker compose -f server/deployments/docker-compose/multi-instance.yaml up
```

### Architecture

- **3 Gateway Instances** (`gateway-1`, `gateway-2`, `gateway-3`)
  - Ports: 50051–50053 (gRPC), 8081–8083 (HTTP), 9091–9093 (Prometheus)
- **Nginx Load Balancer** — port 50050 (gRPC), 8080 (health), `ip_hash` session affinity
- **PostgreSQL** — task persistence (port 5432)
- **Redis (Valkey)** — session registry, KV, checkpoints (port 6379)
- **RabbitMQ Streams** — message routing (ports 5552, 5672, 15672)

### View logs:
```bash
docker compose -f server/deployments/docker-compose/multi-instance.yaml logs -f
docker compose -f server/deployments/docker-compose/multi-instance.yaml logs -f gateway-1
```

### Stop:
```bash
docker compose -f server/deployments/docker-compose/multi-instance.yaml down
docker compose -f server/deployments/docker-compose/multi-instance.yaml down -v  # removes volumes
```

### Monitoring

- **Gateway Health**: http://localhost:8081/health/live (gateway-1)
- **Load Balancer**: http://localhost:8080/health
- **RabbitMQ Management**: http://localhost:15672 (guest/guest)
- **Prometheus Metrics**: http://localhost:9091/metrics (gateway-1)
