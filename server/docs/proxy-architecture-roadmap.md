# Proxy / Tunnel Data-Plane Architecture Roadmap

This document captures the long-term architecture for Aether's proxy and tunnel
data plane. It is written for engineers picking up Phase 7 (gateway-to-gateway
unicast forwarding) without prior context.

**Related documents:** [proxy.md](proxy.md) · [proxy-cutover.md](proxy-cutover.md) ·
[proxy-load-test-results.md](proxy-load-test-results.md) · [proxy-quickstart.md](proxy-quickstart.md) ·
[proxy-sandbox.md](proxy-sandbox.md)

---

## 0. Shipped Features (Current State)

These features are fully implemented and available in the current codebase.
They are listed here so engineers reading the roadmap can distinguish "already
done" from "planned work" without digging through commit history.

| Feature | Mode / config | Where to find it |
|---------|--------------|------------------|
| **REST proxy** | `terminator` + `initiator` sidecar modes | `server/internal/proxysidecar/terminator.go`, `initiator.go` |
| **TCP / WebSocket / UDP tunnels** | `terminator` mode, `kind: tcp/ws/udp` backends | `tunnel_tcp.go`, `tunnel_ws.go`, `tunnel_udp.go` |
| **Multi-backend routing** | First-match-by-ACL or explicit `backend_name` | `server/internal/proxysidecar/terminator.go` |
| **`proxy_path` resource scope** | Per-grant HTTP path ACL | `server/pkg/identityheaders/proxypath.go` |
| **`tunnel_target` resource scope** | Per-grant tunnel target ACL | `server/pkg/identityheaders/tunneltarget.go` |
| **`stream_response_indefinitely`** | SSE / long-poll opt-in on `ProxyHttpRequest` | `server/internal/proxysidecar/terminator.go` |
| **SIGHUP reload** | Zero-downtime backend swap on `kill -HUP` | `server/internal/proxysidecar/terminator.go` |
| **Relay mode** (`relay`) | gRPC mitm — sandbox dials UDS, sidecar injects credentials | `server/internal/proxysidecar/relay.go` |
| **Relay+terminator composite** (`relay+terminator`) | Both surfaces in one process, one gateway lock | `server/internal/proxysidecar/composite.go` |
| **Caller identity headers** | `X-Aether-Caller-Topic` / `X-Aether-Caller-Subject` stamped by terminator | `server/internal/proxysidecar/terminator.go` |
| **Hop-depth tracking** | `proxy_chain_depth` clamped by relay to prevent loops | `server/internal/proxysidecar/relay.go` |
| **Local single-node bypass (Phase 6)** | In-process fast path for same-gateway caller+target | `server/internal/gateway/routing_proxy.go` |

**Relay mode** in particular is the foundation of the sandbox deployment
pattern documented in [proxy-sandbox.md](proxy-sandbox.md). A sandbox process
dials `unix:///run/aether.sock` with no credentials; the sidecar enforces an
operation allow-list (`sandbox-default`, `sandbox-tunnels`, `tool-stub-only`
profiles) and a target-topic clamp before forwarding to the real gateway.

---

## 1. Motivation

### Why RMQ is wrong for proxy/tunnel data

Aether's proxy and tunnel features initially route all bytes — control and data
alike — through RabbitMQ Streams. RMQ Streams are designed for fan-out,
durability, and replayable logs. Proxy/tunnel data needs none of that:

- **No fan-out.** Every `TunnelData` or `ProxyHttpBodyChunk` frame has exactly
  one destination — the pinned target sidecar or the caller. Broadcasting is
  wasteful.
- **No durability.** In-flight bytes are ephemeral. If a tunnel dies, the
  session is over; replaying stale frames into a reconnected tunnel produces
  garbage.
- **No replay.** Tunnels and proxy streams have strict ordering and are
  connection-scoped. An RMQ consumer offset replay mid-reconnect delivers bytes
  to the wrong context.

The stored-offset semantics caused a concrete failure (T29 smoke validation):
a sidecar reconnect triggered an offset interaction that skipped frames
published mid-disconnect. This is a structural mismatch, not a tuning problem.

### What Phase 6 solves

Phase 6 (local single-node bypass, implemented in
`server/internal/gateway/routing_proxy.go`) adds an in-process fast path:
when the caller and target sidecar are both connected to the **same** gateway
instance, data-plane bytes are delivered directly between gRPC streams using
each `ClientSession`'s existing `Deliver()` channel. The RMQ publish is
skipped entirely for those frames.

This eliminates the RMQ offset problem for the same-node case and improves
throughput substantially (no serialisation, no network round-trip). The fast
path is controlled by `proxy.local_bypass_enabled` (default `true`).

### Why Phase 7 picks unicast forwarding over alternatives

Cross-gateway routes still ride RMQ after Phase 6. For deployments with
multiple gateway instances, data-plane bytes still incur the RMQ overhead on
any tunnel whose caller and target happen to land on different instances.

The alternatives are:

| Option | Problem |
|--------|---------|
| Raft / Paxos cluster state | Requires consensus layer, leader election, quorum logic — all unnecessary overhead for a routing problem |
| Gossip / SWIM membership | Adds operational complexity; another thing to monitor and tune |
| Service mesh (Istio/Linkerd) | External infrastructure dependency; not portable across deployments |
| Custom transport (QUIC, etc.) | Maintenance burden; gRPC already provides reliable multiplexed streams over HTTP/2 |

The right answer is **unicast forwarding**: each gateway opens a single
bidirectional gRPC stream to each peer gateway it needs to reach. Redis — which
already stores session lock metadata — serves as the directory. No new
consensus layer, no gossip, no mesh.

---

## 2. Control / Data Plane Split

This split is a hard contract. It must not be violated during Phase 6 or Phase 7.

| Envelope | Plane | Substrate (Phase 6) | Substrate (Phase 7) |
|---|---|---|---|
| `TunnelOpen` | Control | RMQ | RMQ |
| `TunnelClose` | Control | RMQ | RMQ |
| `ProxyHttpRequest` (header) | Control | RMQ | RMQ |
| `ProxyHttpResponse` (header) | Control | RMQ | RMQ |
| `ProxyError` | Control | RMQ | RMQ |
| `TunnelData` | Data | Local fast path or RMQ | Local fast path or peer-gateway forward |
| `TunnelAck` | Data | Local fast path or RMQ | Local fast path or peer-gateway forward |
| `ProxyHttpBodyChunk` | Data | Local fast path or RMQ | Local fast path or peer-gateway forward |

**Invariant:** the control plane is what the audit pipeline listens on. Control
envelopes ride RMQ even when caller and target are on the same gateway. This
guarantees that every lifecycle event — `tunnel_opened`, `tunnel_closed`,
`proxy_http_routed`, `proxy_http_stream_closed` — is observed by the audit
pipeline without any special-casing in the fast path.

Per-byte audit is a non-goal. Per-session byte summaries are stamped on the
close event (already implemented), which rides the control plane.

---

## 3. Phase 7: Gateway-to-Gateway Unicast Forwarding

### 3.1 Discovery

Each gateway instance registers its forwarding address in Redis at startup:

```
key:   gateway:{id}:forward_addr
value: host:port  (e.g. "gw-3.internal:50552")
TTL:   refreshed on the same schedule as the existing gateway heartbeat
```

The session metadata for each connected principal already records which
gateway holds that principal's connection. The `lock:{identity}` value remains
the bare sessionID (load-bearing for resume/takeover semantics); the
`gateway_id` is stored as a separate field on the per-session HASH at
`session:{sessionID}`. When an originating gateway needs to forward a
data-plane envelope to a principal on a different gateway, it:

1. Calls `SessionManager.GetSessionGateway(ctx, identity)`, which atomically
   reads the lock value (sessionID) and the `gateway_id` field from the
   session HASH via a Lua script.
2. Looks up `gateway:{peer_id}:forward_addr` to obtain the dial address.
3. Dials (or reuses) the bidirectional forwarding stream to that peer.

A stale or missing `forward_addr` is not catastrophic. A failed dial falls back
to RMQ (or returns `ProxyError{SIDECAR_UNAVAILABLE}` if
`proxy.peer_forward.fallback` is set to `"error"`).

**Status:** the `gateway_id` storage and `GetSessionGateway` lookup primitive
are implemented in `server/internal/state/session.go` (Redis) and
`server/internal/state/badger_session.go` (lite/Badger). The Phase-7 work
remaining is the forwarding RPC, peer connection pooling, and
`gateway:{id}:forward_addr` registration; principal discovery is no longer a
prerequisite.

### 3.2 Connection Topology

**One bidirectional gRPC stream per ordered gateway pair.** Because the stream
is bidirectional, `(gw-3 → gw-7)` and `(gw-7 → gw-3)` share the same
physical stream — each gateway both sends and receives on it.

- Established **lazily** on the first cross-gateway data-plane envelope.
- Idle-evicted after `proxy.peer_forward.idle_timeout` (default `5m`).
- Keyed by `peer_gateway_id` in a per-gateway connection pool.
- Concurrent senders multiplex onto the stream via a per-peer write-channel and
  a dedicated send goroutine (avoids write contention without a mutex hold in
  the routing hot path).
- Multiplexed by `forward_id` (`tunnel_id` or `request_id` from the payload).

A naive per-tunnel peer connection would produce N×M connection blowup across
gateway pairs under load. The shared multiplexed stream keeps peer connections
constant regardless of tunnel count.

### 3.3 Forwarding RPC (Proto Sketch)

New service in a new file `api/proto/gateway_forward.proto` (or appended to
`aether.proto` — the file boundary is an implementation detail):

```proto
service GatewayForward {
  rpc Forward(stream ForwardEnvelope) returns (stream ForwardEnvelope);
}

message ForwardEnvelope {
  oneof payload {
    TunnelData            tunnel_data  = 1;
    TunnelAck             tunnel_ack   = 2;
    ProxyHttpBodyChunk    body_chunk   = 3;
    KeepAlive             keep_alive   = 10;
  }
  string forward_id        = 20;  // tunnel_id or request_id from the payload
  string source_principal  = 21;  // originating principal identity string
  string target_principal  = 22;  // destination principal identity string
  string source_gateway    = 23;  // originating gateway_id (for audit / debug)
}

message KeepAlive {
  int64 sent_at_ms = 1;
}
```

**Receiver dispatch:** the receiving gateway peels the `payload` oneof, looks
up `target_principal` in its local `identityIndex` (`sync.Map` field on
`GatewayServer`, keyed by identity string), and delivers via the existing
`ClientSession.Deliver()` method. If the target is not present in
`identityIndex`, the receiver sends back a typed forwarding error; the
originator surfaces this to the caller as `PEER_RESET`.

### 3.4 mTLS

- Gateways use the same TLS certificate material already deployed for the
  gRPC client listener.
- A dedicated peer CA bundle is configurable at
  `proxy.peer_forward.tls.ca_path`.
- Optional: allowlist specific peer certificate hashes at
  `proxy.peer_forward.tls.peer_cert_hashes`. Fails closed if configured and
  the peer does not match.

### 3.5 Failure Modes

| Failure | Behaviour |
|---------|-----------|
| Peer unreachable (dial fails) | Retry with backoff; after threshold, fall back to RMQ or return `PEER_RESET` (per `proxy.peer_forward.fallback`) |
| Peer alive but stream broken | Reconnect with exponential backoff; in-flight bytes lost (caller sees `PEER_RESET`, same as today's RMQ-partition behaviour) |
| Stale Redis address (peer restarted) | Dial fails → re-resolve principal's `gateway_id` via `GetSessionGateway` (principal may have reconnected to a different gateway) → retry forward to new peer; bound retries to avoid loops |
| Network partition | Both sides continue serving local clients; cross-partition tunnels die with `PEER_RESET` — identical to current behaviour on RMQ partition |

### 3.6 Audit

The audit contract is unchanged from today:

- `OpProxyHttpRouted`, `OpProxyHttpFailed`, `OpProxyHttpStreamClosed`,
  `OpTunnelOpened`, `OpTunnelOpenFailed`, `OpTunnelClosed` all fire on the
  **originating** gateway when the corresponding control-plane envelope passes
  through it. Control envelopes remain on RMQ in Phase 7 and are therefore
  observed by the existing audit pipeline without modification.

A new optional per-session event `OpProxyForwardEdge` may be emitted on session
close, capturing:

```
forward_id, source_gateway, target_gateway, bytes_forwarded, started_at, ended_at
```

This event is optional (it is not a substitute for the existing close-event
byte counts) and is intended to help operators correlate forwarding activity
across gateways. Per-byte audit remains a non-goal.

Prometheus metrics carry byte counts for the forwarding stream; those are not
audit events.

### 3.7 Observability

Metrics exposed by the forwarding subsystem:

| Metric | Labels | Description |
|--------|--------|-------------|
| `aether_proxy_peer_forward_bytes_total` | `direction`, `peer` | Bytes forwarded through peer streams |
| `aether_proxy_peer_forward_streams_active` | `peer` | Currently open peer forwarding streams |
| `aether_proxy_peer_forward_dial_failures_total` | `peer`, `reason` | Failed dial attempts to peer gateways |
| `aether_proxy_peer_forward_reconnects_total` | `peer` | Peer stream reconnection attempts |

Tracing: each `ForwardEnvelope` carries `forward_id`; tracing context
propagates via standard gRPC metadata headers.

Logging: peer-stream lifecycle events (open, close, error) are logged at INFO
or WARN; per-envelope logging is not emitted (too high volume).

### 3.8 Backpressure

`TunnelAck` credit semantics are unchanged. In Phase 7, credits flow through
the forwarding stream instead of RMQ:

- The receiver gateway drains `TunnelAck` envelopes from the peer stream and
  delivers them to the local `ClientSession` via `Deliver()`.
- The peer-stream send buffer is bounded. When it fills, the originating
  gateway stalls the routing goroutine for that tunnel, applying the same
  backpressure the RMQ publish buffer applies today.
- The `BACKPRESSURE` signal sent by `ClientSession.Deliver()` on full buffer
  still applies at the local-delivery stage.

### 3.9 Configuration

Full config block under the `proxy` key:

```yaml
proxy:
  local_bypass_enabled: true        # Phase 6 — in-process fast path

  peer_forward:
    enabled: false                  # Phase 7 — off by default; flip per deployment
    listen_addr: ":50552"           # gateway-to-gateway gRPC listen port
    advertise_addr: ""              # external address published to Redis (inferred from listen_addr if empty)
    idle_timeout: 5m                # evict idle peer streams after this duration
    fallback: rmq                   # "rmq" (fall back to RMQ on dial failure) or "error" (return PEER_RESET)
    tls:
      ca_path:   /etc/aether/peer-ca.crt
      cert_path: /etc/aether/peer.crt
      key_path:  /etc/aether/peer.key
```

Environment override for Phase 6 emergency rollback:
`AETHER_PROXY_LOCAL_BYPASS_DISABLED=1`.

### 3.10 Migration Path (Phase 7 Rollout)

1. **Build under feature flag.** Implement the forwarding service with
   `peer_forward.enabled: false` as default. All existing tests pass
   unmodified because the flag is off.
2. **Stage 1 — Canary.** Enable on one gateway instance. Observe
   `aether_proxy_peer_forward_*` metrics. Verify audit invariants: control-plane
   events still fire; per-session byte summaries match expectations from the
   non-forwarding path.
3. **Stage 2 — Single workspace soak.** Enable globally but restrict to one
   workspace via a workspace-scoped config override. Run for one week minimum.
4. **Stage 3 — Full production.** Enable globally. Keep `fallback: rmq` for
   defence in depth — RMQ remains the fallback for any peer-stream failure.
5. **Stage 4 (optional, far future).** After weeks of production confidence,
   evaluate switching `fallback: error`. This effectively stops using RMQ for
   proxy/tunnel data plane entirely on a per-failure basis. See Phase 8 below.

---

## 4. Phase 8 (Optional, Not Planned): Retire RMQ for Proxy/Tunnel Data Plane

After Phase 6 and Phase 7 are proven in production, the only remaining RMQ
traffic for proxy/tunnel would be:

- Control-plane envelopes (`TunnelOpen`, `TunnelClose`, `ProxyHttpRequest`
  header, `ProxyHttpResponse` header, `ProxyError`) — these always ride RMQ.
- The Phase 7 fallback path when `peer_forward.fallback: rmq`.

Two options:

**Option A — Keep RMQ for control plane indefinitely (recommended).**
Control-plane volume is low (one open + one close per tunnel session), the
audit pipeline is already integrated with RMQ, and the rest of Aether
messaging uses RMQ correctly. Unless RMQ becomes a bottleneck for
control-plane volume specifically, there is no cost to keeping it.

**Option B — Move control plane to the forwarding stream too.**
Eliminates RMQ as a dependency for proxy/tunnel entirely. Substantially larger
change: audit pipeline integration point moves, ordering guarantees must be
re-established in the forwarding layer. Likely overkill.

Phase 8 is **not committed work**. Document but do not plan.

---

## 5. Non-Goals

These will not be built. Documented explicitly to prevent drift.

- **Raft / Paxos / leadership election among gateways.** Redis is the directory
  of record for principal location. There is nothing to elect.
- **Cluster membership protocol (gossip, SWIM, etc.).** Discovery is a Redis
  lookup, not a gossip query. Gateways do not maintain a shared cluster view.
- **Replacing RMQ for general Aether messaging.** Fan-out broadcasts,
  orchestration task dispatch, KV propagation, and audit all use RMQ
  correctly — durable, replayable, fan-out. Those use cases stay on RMQ.
- **Service mesh dependency (Istio, Linkerd, etc.).** Gateways handle their own
  mTLS for peer forwarding. No platform-level mesh is required or assumed.
- **Custom transport stack (QUIC implementation, custom HTTP/2 codec, etc.).**
  gRPC over HTTP/2 is the transport for peer forwarding, used as-shipped. The
  standard library's TLS and HTTP/2 stacks are sufficient.

---

## 6. Open Questions for Phase 7 Implementation

These questions do not block Phase 6. They are flagged for the team picking up
Phase 7.

**Multiplexing approach.** The design specifies a single bidirectional gRPC
stream per gateway pair, multiplexed by `forward_id`. An alternative is one
stream per active tunnel session. Per-session is simpler to implement but
produces O(active-tunnels) peer connections — unacceptable at scale. The
`forward_id` mux design is the chosen approach; document the rationale in the
commit that introduces `GatewayForward.Forward`.

**Keep-alive mechanism.** The proto sketch includes a `KeepAlive` envelope.
An alternative is to rely on gRPC's built-in HTTP/2 PING frames (configured
via `grpc.KeepaliveParams`). The HTTP/2 PING approach is cleaner and avoids
application-layer keep-alive logic. If the `KeepAlive` envelope is retained,
document why (e.g. PING frames were insufficient for a specific NAT/proxy
scenario encountered during testing).

**Cross-cluster scope.** The current design assumes all gateway instances are
reachable over a private network. Cross-cluster or internet-traversing
forwarding raises a different security boundary question (certificate authority
trust, firewall traversal). Document this as out of scope for Phase 7: tunnels
spanning a public internet hop should use application-level encryption between
the caller and the target service, not rely on the gateway forwarding layer.

**Testing without multiple real gateway processes.** The T7/T14 integration
tests use an in-process gateway. Phase 7 testing should use the same pattern:
an in-process two-`GatewayServer` harness where both server structs forward
data-plane envelopes to each other over a loopback listener. This tests the
full forwarding code path without external infrastructure.
