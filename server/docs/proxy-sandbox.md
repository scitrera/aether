# Aether Proxy — Sandbox Deployment Pattern

This document describes how to run the proxy sidecar with both the
**terminator** and **relay** surfaces enabled to create a secure sandbox
environment where an untrusted process (a spawned LLM agent, a tool-runner
container, etc.) can participate in the Aether network without holding any
credentials.

**Related documents:** [proxy.md](proxy.md) · [proxy-cutover.md](proxy-cutover.md) ·
[proxy-architecture-roadmap.md](proxy-architecture-roadmap.md) ·
[proxy-quickstart.md](proxy-quickstart.md)

---

## 1. Problem Statement

A typical orchestrator spawns short-lived compute sandboxes — containers,
sub-processes, or ephemeral VMs — that need to:

1. Send messages to other Aether principals (their orchestrator, users, other
   agents).
2. Optionally call HTTP backends through the proxy.
3. Report progress and update KV state.

Giving each sandbox its own API key has obvious problems: key rotation is
expensive at scale, a compromised sandbox can impersonate any principal, and
short-lived sandboxes often cannot safely receive secrets at spawn time.

The relay sidecar solves this by acting as a **credential proxy**: the sandbox
dials a local socket with no credentials; the sidecar presents its own identity
to the real gateway and enforces a strict operation filter.

---

## 2. Surfaces Involved

| Surface      | What it does                                                                                                              |
|--------------|---------------------------------------------------------------------------------------------------------------------------|
| `terminator` | Receives `ProxyHttpRequest` envelopes from the gateway and forwards them to a local HTTP backend.                         |
| `relay`      | Binds a local gRPC server (UDS or TCP). Sandboxes dial it; the sidecar injects credentials and forwards filtered ops upstream. |

Each surface is gated by an `enabled: true` flag in its YAML section; the two
can run together in one process over a single shared gateway connection (one
Aether identity, one lock). For most sandbox deployments enabling both is the
right choice: the same sidecar that exposes tool-runner HTTP endpoints to the
network also provides the local relay socket the sandbox dials.

---

## 3. UDS Path Convention

Unix Domain Socket (UDS) is strongly preferred over TCP for the relay listen
address. It gates access by filesystem permissions — only processes in the same
pod or container namespace can reach the socket. No network port is exposed.

**Convention:** `/run/aether.sock`

```yaml
relay:
  listen: unix:///run/aether.sock
```

The sidecar creates the socket file at startup and removes it on clean shutdown.
If a stale socket file exists (e.g. after a crash), the sidecar logs an error
and exits — remove the file before restarting.

To use TCP instead (e.g. cross-container in a pod with shared networking):

```yaml
relay:
  listen: tcp://localhost:50099
```

TCP relay is supported but not recommended for untrusted sandboxes: any process
on the host that can reach the port can connect.

---

## 4. Sandbox SDK Configuration

The sandbox process dials the relay socket as if it were a plain Aether
gateway. No credentials are needed — the sidecar injects them.

### Python

```python
from scitrera_aether_client import AetherClient

client = AetherClient(
    gateway_address="unix:///run/aether.sock",
    insecure=True,       # no TLS on the UDS hop
    principal_type="agent",
    implementation="my-sandbox",
    specifier="run-001",
    # No api_key — the sidecar holds it
)
```

### Go

```go
import "github.com/scitrera/aether/sdk/go/aether"

client, err := aether.NewAgentClient(ctx,
    aether.WithAddress("unix:///run/aether.sock"),
    aether.WithInsecure(),
    // No API key or TLS — sidecar holds credentials
)
```

### TypeScript

```typescript
import {AetherClient} from "@scitrera/aether-client";

const client = new AetherClient({
    address: "unix:///run/aether.sock",
    insecure: true,
    principalType: "agent",
    implementation: "my-sandbox",
    specifier: "run-001",
});
```

**Important:** The sandbox's claimed identity (`implementation`, `specifier`,
`principal_type`) is **discarded** by the relay. The relay always presents the
sidecar's configured `service.implementation` / `service.specifier` to the
gateway. The sandbox cannot impersonate a different principal.

---

## 5. Security Model

```
┌──────────────────────────────────────────────────────────────────────┐
│  Pod / container namespace                                           │
│                                                                      │
│  ┌─────────────────┐   plain gRPC   ┌──────────────────────────┐   │
│  │  Sandbox        │  (no creds)    │  Proxy Sidecar           │   │
│  │  process        │◄──────────────►│  terminator + relay      │   │
│  │                 │  /run/aether   │                          │   │
│  │  No API key     │  .sock (UDS)   │  Holds API key + TLS     │   │
│  │  No TLS cert    │                │  Enforces allow-list     │   │
│  └─────────────────┘                │  Enforces topic clamp    │   │
│                                     │  Injects identity        │   │
│                                     └────────────┬─────────────┘   │
└──────────────────────────────────────────────────┼─────────────────┘
                                                   │ mTLS gRPC
                                                   ▼
                                        ┌─────────────────────┐
                                        │   Aether Gateway    │
                                        └─────────────────────┘
```

### What the relay enforces

| Enforcement point       | Mechanism                                                                                              |
|-------------------------|--------------------------------------------------------------------------------------------------------|
| **No credential theft** | Sandbox dials a UDS socket with no credentials. There is nothing to steal.                             |
| **Identity lock**       | `identity_override: enforce` — sandbox's claimed identity is discarded; sidecar's identity is used.   |
| **Operation filter**    | `relay.allowed_ops` — only named operations pass through. Everything else is rejected at the relay.   |
| **Topic clamp**         | `relay.target_topic_clamp` — sandbox can only address topics in the `allowed_targets` glob list.      |
| **Hop-depth tracking**  | `proxy_chain_depth` is clamped upward; a sandbox cannot understate its depth to bypass loop limits.   |
| **UDS permissions**     | Filesystem permissions on `/run/aether.sock` gate access; no network port is exposed.                 |

### Operation profiles

| Profile           | Ops allowed                                                                        | When to use                                                   |
|-------------------|------------------------------------------------------------------------------------|---------------------------------------------------------------|
| `sandbox-default` | `SendMessage`, `ProgressReport`, `KVOperation`                                     | Most sandboxes — messaging and state only.                    |
| `sandbox-tunnels` | Above + `ProxyHttpRequest`, `ProxyHttpBodyChunk`, `ProxyHttpResponse`, `TunnelOpen`, `TunnelData`, `TunnelClose`, `TunnelAck` | Sandboxes that need SDK proxy/tunnel access.   |
| `tool-stub-only`  | `InitConnection` only                                                               | Pure tool-stub sandboxes that only receive, never originate.  |

You may also specify a literal list instead of a profile name:

```yaml
relay:
  allowed_ops:
    - SendMessage
    - ProgressReport
    - KVOperation
    - CheckpointOperation
```

---

## 6. Spawn-Time Grant Scopes

When the orchestrator issues a short-lived authority grant for the sandbox
session, two optional resource scopes further restrict what the grant permits:

### `proxy_path` — HTTP path ACL

Restricts which backend + HTTP method + path combinations the grant holder may
reach through the proxy. Pattern grammar: `<backend_glob>::<method_glob> <path_glob>`.

```
# Grant allows POST to /v1/run on any backend, nothing else:
proxy_path: "*::POST /v1/run"

# Multiple patterns (OR logic — any match is sufficient):
proxy_path:
  - "tool-api::GET /v1/status/*"
  - "tool-api::POST /v1/run"
```

An absent `proxy_path` key means all paths allowed by the upstream ACL are
permitted. See [proxy.md — Grant Resource Scopes](proxy.md#grant-resource-scopes)
for the full grammar reference.

### `tunnel_target` — Tunnel target ACL

Restricts which backend + protocol + remote-hint combinations the grant holder
may open a tunnel to. Pattern grammar: `<backend_glob>::<protocol_glob> <remote_hint_glob>`.

```
# Grant allows TCP tunnels to db-primary only:
tunnel_target: "db::tcp db-primary:5432"

# Wildcard: any TCP tunnel to any prod- prefixed host on port 5432:
tunnel_target: "*::tcp prod-*:5432"
```

An absent `tunnel_target` key means all tunnel targets permitted by the upstream
ACL are allowed.

---

## 7. Complete Example Config

See
[`server/configs/proxy-sidecar.sandbox.example.yaml`](../configs/proxy-sidecar.sandbox.example.yaml)
for a fully annotated config (both surfaces enabled) that:

- Binds the relay surface at `unix:///run/aether.sock`
- Exposes one HTTP backend (`tool-api` at `localhost:7000`)
- Uses `sandbox-default` op profile (messaging + KV only, no proxy/tunnel)
- Clamps outbound topics to the orchestrator and user windows

---

## 8. Target-Topic Clamp Reference

```yaml
relay:
  target_topic_clamp:
    mode: reject               # or rewrite_first_match
    allowed_targets:
      - "ag.prod-workspace.orchestrator.*"
      - "us.*"
```

| Mode                  | Behaviour                                                                                                          |
|-----------------------|--------------------------------------------------------------------------------------------------------------------|
| `reject` (default)    | Any outbound proxy/tunnel envelope whose `target_topic` does not match an `allowed_targets` glob is rejected.     |
| `rewrite_first_match` | If the `target_topic` does not match, rewrite it to the first **concrete** (no-glob) entry in `allowed_targets`. Falls back to reject if no concrete entry exists. |

A sandbox that sends to a disallowed topic receives an `ErrorResponse` from the
relay; the envelope never reaches the gateway.

---

## 9. Caller Identity Headers

When a proxied request arrives at the terminator sidecar, the gateway
server-stamps two headers that identify the originating principal before
forwarding the `ProxyHttpRequest` envelope. These headers are written by
`server/pkg/identityheaders` and are always stripped on inbound to prevent
spoofing — only trusted minting paths can set them.

| Header                     | Value                                                                                     | Trust                        |
|----------------------------|-------------------------------------------------------------------------------------------|------------------------------|
| `X-Aether-Caller-Topic`    | Sender's principal topic (e.g. `ag.ws.myagent.spec`)                                     | Server-stamped — trustworthy |
| `X-Aether-Caller-Subject`  | OBO grant subject's canonical topic when `authority_mode=on_behalf_of`; absent in direct mode | Server-stamped — trustworthy |

**Sandbox use case:** The tool-runner backend receives `X-Aether-Caller-Topic`
and uses it to call back the agent that invoked it — for example, to send a
`ProgressReport` or a follow-up `SendMessage` — without a separate identity
lookup. The backend reads the header from the request and uses it as the target
topic on the outbound Aether call.

Both headers are present on every proxied request regardless of whether the
relay surface is involved. They reflect the identity of whichever principal
sent the `ProxyHttpRequest` upstream — which, in a relay scenario, is always
the sidecar's own identity (since the relay rewrites the init). Backends that
need to distinguish individual sandbox sessions should use additional
application-level session tokens passed in the request body or custom headers,
not `X-Aether-Caller-Topic` alone.

---

## 10. Hop-Depth and Loop Prevention

Every `ProxyHttpRequest` and `TunnelOpen` envelope carries a
`proxy_chain_depth` field that tracks how many proxy hops the request has
traversed. The gateway rejects requests whose depth reaches or exceeds
`proxy.max_chain_depth` (default **8**) with:

```
ProxyError{kind: ACL_DENIED, detail: "proxy_chain_depth_exceeded"}
```

**At the relay:** The relay applies a hybrid-floor clamp — it increases
`proxy_chain_depth` by one for each relay hop, but never decreases it. A
sandbox cannot understate its position in the chain to bypass the loop limit.

**Why this matters for sandbox deployments:** A misbehaving or compromised
sandbox that tries to form a proxy loop (sandbox → sidecar → gateway →
different terminator → relay → sandbox…) is terminated at the gateway once
depth reaches the cap. No unbounded routing loops are possible.

**Tuning:**

```yaml
# gateway config
quotas:
  proxy:
    max_chain_depth: 8   # default; increase only if your architecture
                         # legitimately needs deeper chains
```

The depth cap applies per-request, not per-connection. Simple sandbox
deployments (sandbox → sidecar → gateway → terminator) use depth 1 and are
well within the default cap.

---

## 11. Why `sandbox-default` Forbids Proxy/Tunnel Ops

The `sandbox-default` profile allows only `SendMessage`, `ProgressReport`, and
`KVOperation`. `ProxyHttpRequest` and `TunnelOpen` are intentionally excluded.

**Reason:** Proxy and tunnel calls issued directly by the sandbox SDK would
bypass the `relay.target_topic_clamp` for target-topic routing (the clamp
applies only to the relay's outbound forwarding, not to envelopes the sidecar
issues on its own connection). They would also bypass the `proxy_path` and
`tunnel_target` resource scopes on the grant — those are enforced by the
terminator that *receives* the request, not the relay that forwards the
*initiation*.

**For sandboxes that need HTTP access to backends:** Use the sidecar's HTTP
mitmproxy + initiator path instead. The sandbox sends an ordinary HTTP request
to `localhost:8888` (or whichever port the initiator listens on); the initiator
translates it into a `ProxyHttpRequest` envelope using the sidecar's credentials
and subject to the sidecar's `target.topic` config. This keeps all proxy/tunnel
policy decisions in the operator-controlled sidecar config, not in the sandbox's
allowed-ops profile.

If the sandbox legitimately needs SDK-level proxy or tunnel access (rare), use
the `sandbox-tunnels` profile and configure `relay.target_topic_clamp`
carefully to constrain which targets the sandbox may reach.

---

## 12. Operational Notes

**Socket cleanup on crash:** If the sidecar crashes without removing
`/run/aether.sock`, the next start will fail. Add a pre-start lifecycle hook or
`ExecStartPre=-rm -f /run/aether.sock` in your systemd unit / Kubernetes
init container.

**SIGHUP reload:** The sidecar supports zero-downtime backend reconfiguration
via SIGHUP. The relay listen socket and identity are **not** reloaded on SIGHUP
(they are established at startup). Only the `backends` list and backend-level
fields change on reload. See [proxy.md — Reloading Configuration](proxy.md#reloading-configuration).

**Quota:** Each sandbox session that dials the relay socket creates one upstream
gateway session. Each upstream session holds a distributed lock and increments
the workspace connection quota. Size your quota accordingly.

**Audit:** All ops the relay forwards appear in the gateway audit log under the
sidecar's identity (`sv::toolrunner::default`), not the sandbox's claimed
identity. The sandbox's claimed principal type and name are logged by the relay
at `INFO` level (field `sandbox_claim`) for traceability but are not propagated
to the gateway.
