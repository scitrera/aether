# Why Aether?

## What Aether Is

Aether is a distributed control plane for routing structured messages between typed participants — Agents, Tasks, Users, Workflow Engines, and Orchestrators — over persistent gRPC streams backed by RabbitMQ Streams and Redis. Its defining property is that the active connection itself functions simultaneously as the distributed lock for an identity and as its liveness proof: when the TCP stream closes, the lock releases and the identity becomes available for reconnection. This eliminates heartbeat polling, simplifies failure detection, and makes connection state the single source of truth across every gateway instance in a horizontally scaled cluster.

## Comparison

The table below compares Aether to systems that address overlapping but distinct problem spaces.

| Capability | Aether | NATS / NATS JetStream | Temporal | Kafka | LiveKit |
|---|---|---|---|---|---|
| **Connection = distributed lock** | Yes — the gRPC stream IS the lock. No separate lock API, no TTL polling. | No — NATS has no concept of connection-as-lock. Clients are anonymous. | No — workflow locks are managed by the workflow engine, not connection state. | No — Kafka consumers are identified by group/offset, not connection liveness. | No — room participants are tracked, but not as exclusive distributed locks. |
| **Built-in typed identity model** | Yes — Agent, Task (unique/non-unique), User, Workflow Engine, Metrics Bridge, Orchestrator. Each type has enforced routing permissions. | No — NATS subjects are untyped; identity is application-defined. | Partial — Temporal has workflow and activity types but no connection-level identity enforcement. | No — Kafka has consumer groups but no typed identity model at the protocol level. | Partial — LiveKit has participant roles (host, viewer) but no agent/task distinction. |
| **Orchestration / lazy loading** | Yes — messages to offline agents trigger a `StartupRequest` to the registered Orchestrator, which spins up compute on demand. | No — NATS does not know whether a subscriber is running; messages either deliver or are lost/queued. | Yes — Temporal schedules workflow execution, but the execution engine is internal and tightly coupled to Temporal's runtime model. | No — Kafka holds messages in topics; it has no mechanism to start a consumer on demand. | No — LiveKit does not launch compute based on connection state. |
| **Message persistence** | Yes — RabbitMQ Streams provide a persistent, replayable message log with consumer offset tracking. | Yes (JetStream) — durable subjects with consumer offsets. NATS Core is ephemeral. | Yes — workflow event history is fully durable. | Yes — Kafka is built on a persistent, partitioned log. | No — LiveKit is real-time only; no persistent message log. |
| **Hierarchical KV / config store** | Yes — tenant / workspace / address / chat namespace. Workspace config is pushed to connecting clients as a snapshot on connect. | Partial (JetStream KV) — flat KV buckets, no hierarchical push-on-connect semantics. | No — Temporal has no built-in KV store; configuration is application-managed. | No — Kafka has no KV store. | No — LiveKit has no KV store. |
| **Bidirectional single-stream protocol** | Yes — one gRPC bidirectional stream multiplexes messages, KV ops, config snapshots, and signals. | No — NATS uses separate publish and subscribe connections. | No — Temporal uses a polling model; workers pull tasks from the server. | No — Kafka separates producer and consumer. | Partial — WebRTC data channels are bidirectional but not multiplexed over a single control stream. |
| **Horizontal scaling model** | Stateless gateway instances; all locks and session state in Redis; no coordination between instances. | Cluster-aware — NATS clustering is built into the broker. | Server-managed — Temporal clusters are stateful and require coordination. | Partition-based — Kafka scales by adding partitions and consumer group members. | SFU-based — LiveKit scales via selective forwarding units. |
| **Operational complexity** | Low-medium — Redis + RabbitMQ + PostgreSQL (optional for persistence). Standard infrastructure. | Low (NATS Core) to medium (JetStream cluster) | High — Temporal requires its own server cluster, database, and worker fleet. | High — Kafka requires ZooKeeper or KRaft, careful partition management, and dedicated ops. | Medium — LiveKit requires STUN/TURN infrastructure and SFU deployment. |
| **Primary use case** | Coordination of AI agents, tasks, and human users in multi-agent systems. | General-purpose messaging, microservice pub/sub, lightweight event streaming. | Durable workflow orchestration with retry, compensation, and long-running sagas. | High-throughput event streaming, log aggregation, data pipelines. | Real-time audio/video conferencing with data channel support. |

## When to Use Aether

Aether fits well when:

- You are building a **multi-agent AI system** where agents, tasks, and users need to exchange messages through a central coordination point with enforced identity and access control.
- You need **connection-level exclusivity**: exactly one process should hold a given identity at any time, and you want that invariant enforced by the infrastructure rather than application logic.
- You want **lazy orchestration**: agents or tasks that are not running should be started on demand when a message arrives for them, without the sender needing to know whether the target is online.
- You need **workspace-scoped configuration** delivered automatically to connecting clients without a separate config-fetch step.
- Your system has multiple participant types (long-running services, finite work units, human operators, event processors) that require different routing permissions enforced at the protocol level.
- You want a **single connection** to handle messaging, KV operations, config delivery, and server-sent signals without maintaining separate channels for each concern.

## When Not to Use Aether

Aether is not the right choice when:

- You need **general-purpose pub/sub** with anonymous publishers and subscribers and no connection-level identity. NATS or Redis Pub/Sub are simpler choices.
- You need **high-throughput event streaming** — millions of events per second from many producers to many consumers with replayable partitioned logs. Kafka is purpose-built for this.
- You need **durable workflow orchestration** with built-in retry policies, compensation logic, timeouts, and a visual workflow history. Temporal solves this problem directly.
- Your primary requirement is **real-time audio/video** with selective forwarding. LiveKit is the correct tool.
- You want a **zero-dependency broker** with sub-millisecond latency and no external storage. NATS Core with no JetStream comes close to zero operational overhead; Aether requires Redis and RabbitMQ.
- Your team has no existing familiarity with gRPC or RabbitMQ Streams and the operational cost of learning both is not justified by the use case.

## Summary

Aether occupies a specific niche: coordination infrastructure for multi-agent systems where identity exclusivity, typed routing permissions, lazy orchestration, and a tight connection/lock/heartbeat coupling are first-class requirements. It is not a general messaging bus, a workflow engine, or a streaming platform. It is the control plane that sits between those systems and the agents that use them.
