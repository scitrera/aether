# Aether Proxy

Aether's proxy feature lets any connected principal reach a sibling service's
HTTP API tunneled through the existing gRPC stream. You get encryption,
identity propagation, OBO authority delegation, ACL enforcement, and audit
logging with no extra infrastructure.

## Overview

Two capabilities ship today:

| Feature                       | What it does                                                                                                                                                                              |
|-------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **REST proxy**                | Tunnels a complete HTTP request/response pair over the Aether stream. Good for standard REST APIs (JSON, binary uploads, etc.).                                                           |
| **TCP/WebSocket/UDP tunnels** | Open a raw byte-stream tunnel between two Aether principals. TCP, WebSocket, and UDP backends are all implemented in the terminator sidecar — see [Tunnels](#tunnels). |

**Why not just call the backend directly?** Three reasons:

1. **Zero network exposure.** The backend never needs an outbound route or an
   open port visible to the caller. Both sides are behind the Aether gateway.
2. **Identity and OBO for free.** Every proxied request carries the caller's
   verified identity and any On-Behalf-Of grant. The sidecar mints `X-Auth-*`
   headers from the same library as the auth-proxy, so the backend's auth logic
   is unchanged.
3. **Audit for free.** Every proxied request writes `proxy_http_routed` (or
   `proxy_http_failed`) audit events with full grant lineage, resolved instance,
   and byte counts — no custom instrumentation needed.

## Architecture

```
   ┌─────────────────┐      ┌────────────────────┐      ┌─────────────────────┐
   │  Caller-side    │      │   Aether Gateway   │      │  Service-side       │
   │  initiator      │      │  (router only —    │      │  terminator sidecar │
   │  sidecar        │      │  no HTTP term.)    │      │  sv::memorylayer    │
   │                 │      │                    │      │  ::default          │
   │  localhost:8888 │ gRPC │                    │ gRPC │                     │
   │  HTTP listener  │◄────►│   ProxyHttp* /     │◄────►│  X-Auth-* mint lib  │
   │  (curl, legacy) │      │   Tunnel* routes   │      │  ┌────────────────┐ │
   │                 │      │                    │      │  │ HTTP localhost │ │
   │  X-Auth-* mint  │      └────────────────────┘      │  └──────┬─────────┘ │
   │  lib (optional) │                                   │         │           │
   └────────┬────────┘                                   └─────────┼───────────┘
            │                                                       │
            │       (or in-process via SDK adapter)                 ▼
   ┌────────┴────────┐                                   ┌─────────────────────┐
   │  Caller agent   │      ┌────────────────────┐      │   memorylayer       │
   │  Go/Py/TS SDK   │ gRPC │   Aether Gateway   │      │   (HTTP server)     │
   │  proxy_http()   │◄────►│                    │      └─────────────────────┘
   └─────────────────┘      └────────────────────┘
```

The gateway is a **pure router**. It never terminates HTTP or TCP. All
proxying happens at endpoints — either via a sidecar binary or in-process via
SDK adapters. This is a hard architectural rule; it keeps the gateway's
security boundary simple and audit accounting straightforward.

## Service Principal Addressing

Proxy requests target a **service principal** topic, not an arbitrary agent or
user. Two forms are accepted:

| Form              | Example                    | Meaning                                                                             |
|-------------------|----------------------------|-------------------------------------------------------------------------------------|
| Specific instance | `sv::memorylayer::default` | Route to exactly this connected instance. Returns `SIDECAR_UNAVAILABLE` if offline. |
| Wildcard          | `sv::memorylayer`          | Route to any healthy connected instance of this implementation.                     |

The bare `sv::{impl}` wildcard is **only** accepted inside `ProxyHttpRequest`
and `TunnelOpen`. Plain `SendMessage` to a bare `sv::impl` topic is still
rejected as malformed.

### Wildcard resolution

1. The gateway checks its local `identityIndex` first (O(1), prefers the same
   gateway instance that holds the service connection).
2. Falls back to a cluster-wide Redis lock scan, filtering out instances whose
   lock TTL has decayed below 5 s (considered unhealthy).
3. Picks uniformly at random from the survivor set.

**REST proxy**: resolution is per-request — each `ProxyHttpRequest` may land
on a different instance.

**Tunnels**: `TunnelOpen` resolves once and pins the result in Redis.
Subsequent `TunnelData` / `TunnelClose` frames are always routed to the same
instance (sticky).

## Proxy Sidecar

The `proxy-sidecar` binary is composed of three independent surfaces. Each is
gated by an `enabled: true` flag in its YAML section; any combination can run
together in one process over a single shared gateway connection (one Aether
identity, one lock).

| Surface      | When to use                                                                    |
|--------------|--------------------------------------------------------------------------------|
| `terminator` | Expose a local HTTP/TCP/WS/UDP backend to the Aether network.                  |
| `initiator`  | Forward local HTTP traffic to a remote service topic (no SDK required).        |
| `relay`      | Give a credential-free sandbox process a filtered view of the gateway over UDS.|

To run multiple surfaces in one process, enable each section. Sandboxes
typically enable both `terminator` and `relay` so the same sidecar receives
inbound requests for the local service AND mediates the sandbox's outbound
gRPC traffic. See [proxy-sandbox.md](proxy-sandbox.md) and
`server/configs/proxy-sidecar.sandbox.example.yaml` for the full pattern.

### Terminator surface (next to your backend)

The terminator connects to the Aether gateway **as the service principal**
(`sv::{impl}::{spec}`), receives incoming `ProxyHttpRequest` envelopes, and
forwards them to a local HTTP backend.

Use this when you want to expose an existing HTTP service over Aether without
modifying its code.

```yaml
# proxy-sidecar.yaml — terminator surface
gateway:
  address: gateway.example.com:50051
  insecure: false
  api_key_path: /etc/aether/sidecar.key
  tls:
    cert_file: /etc/aether/tls/cert.pem
    key_file: /etc/aether/tls/key.pem
    ca_file: /etc/aether/tls/ca.pem

# Identity registered with the gateway.
service:
  implementation: memorylayer
  specifier: default

# Tenant ID injected as X-Auth-Tenant-ID (header_mode: strict | both).
tenant_id: prod-tenant

terminator:
  enabled: true
  backends:
    - name: default
      kind: http
      url: http://localhost:61001
      allow_paths:
        - "/v1/*"
        - "/healthz"
      allow_methods:
        - GET
        - POST
        - PUT
        - DELETE
      max_body_bytes: 10485760    # 10 MiB
      idle_timeout_ms: 30000
      header_mode: strict         # strict | passthrough | both

logging:
  level: info
  format: json
```

### Initiator surface (caller-side HTTP listener)

The initiator exposes a local HTTP listener. Requests to that listener are
translated into `ProxyHttpRequest` envelopes sent to the configured target
topic. Unmodified callers (curl, legacy scripts, third-party tools) redirect
by changing only a base URL — no code changes, no Aether SDK required.

Use this when you cannot modify the caller but can change its target hostname.

```yaml
# proxy-sidecar.yaml — initiator surface
gateway:
  address: gateway.example.com:50051
  insecure: false
  api_key_path: /etc/aether/sidecar.key

initiator:
  enabled: true
  listen:
    bind: localhost:8888           # local port your caller points at
  target:
    topic: sv::memorylayer::default  # or wildcard: sv::memorylayer

logging:
  level: info
  format: json
```

### Relay surface (sandbox gateway)

The relay binds a local gRPC server (UDS or TCP) that an untrusted sandbox
process dials with no credentials. The relay injects the sidecar's own API key
and identity before forwarding each envelope to the real gateway, enforcing an
operation allow-list and a target-topic clamp.

Use this when you spawn short-lived agents or tool-runner sandboxes that must
not hold API keys or TLS certificates.

```yaml
# proxy-sidecar.yaml — relay surface (standalone)
gateway:
  address: gateway.example.com:50051
  api_key_path: /etc/aether/sidecar.key

service:
  implementation: toolrunner
  specifier: default

relay:
  enabled: true
  listen: unix:///run/aether.sock   # sandbox dials this socket
  identity_override: enforce        # discard sandbox-claimed identity
  allowed_ops:
    profile: sandbox-default        # SendMessage, ProgressReport, KVOperation
  target_topic_clamp:
    mode: reject
    allowed_targets:
      - "ag.prod-workspace.orchestrator.*"

logging:
  level: info
  format: json
```

Enabling `terminator.enabled: true` alongside `relay.enabled: true` runs both
surfaces over the same gateway connection. See
[proxy-sandbox.md](proxy-sandbox.md) and
`server/configs/proxy-sidecar.sandbox.example.yaml` for a fully annotated
example.

### Backend kinds

The `kind` field in each backend entry controls which protocol the terminator
uses to reach the local service:

| Kind   | Protocol     | Notes                                                                               |
|--------|--------------|-------------------------------------------------------------------------------------|
| `http` | HTTP/1.1     | Default. Supports `allow_paths`, `allow_methods`, `header_mode`.                    |
| `tcp`  | Raw TCP      | `url` is `host:port` or `tcp://host:port`. Default `max_bytes`: 100 MiB per tunnel. |
| `ws`   | WebSocket    | `url` is `ws://` or `wss://`. Same flow-control as TCP.                             |
| `udp`  | UDP datagram | `url` is `host:port` or `udp://host:port`. Default `max_datagram_bytes`: 1400.      |

### `header_mode` reference (terminator)

Controls how the terminator handles `Authorization` / `X-Auth-*` headers
before forwarding to the local backend.

| Value         | Behaviour                                                                                                                                                                                        |
|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `strict`      | Strip `Authorization` and all `X-Auth-*` from the inbound request. Mint fresh `X-Auth-*` headers from the OBO grant in `ProxyHttpRequest.authorization`. Default; recommended for most services. |
| `passthrough` | Forward caller-supplied headers unchanged. No minting. Use only when the backend has its own auth layer and you trust the caller's headers.                                                      |
| `both`        | Mint `X-Auth-*` headers AND preserve caller-supplied headers. Minted values override any collisions.                                                                                             |

## SDK One-Liners

The proxy transport adapters let you redirect an existing HTTP client to Aether
with a single line. The host portion of the URL is ignored — only the path and
query string are forwarded.

### Python — httpx

```python
import httpx
from scitrera_aether_client.httpx_transport import AetherHTTPXTransport

# aether_client is your connected sync AetherClient
transport = AetherHTTPXTransport(aether_client, "sv::memorylayer::default")
with httpx.Client(transport=transport) as http:
    resp = http.get("http://ignored/v1/memories/abc")
```

Async variant:

```python
from scitrera_aether_client.httpx_transport import AetherAsyncHTTPXTransport

transport = AetherAsyncHTTPXTransport(aether_client, "sv::memorylayer::default")
async with httpx.AsyncClient(transport=transport) as http:
    resp = await http.get("http://ignored/v1/memories/abc")
```

### Python — requests

```python
import requests
from scitrera_aether_client.requests_adapter import AetherRequestsAdapter

session = requests.Session()
session.mount("aether+sv://", AetherRequestsAdapter(aether_client))

# Specific instance (impl + specifier separated by --)
session.get("aether+sv://memorylayer--default/v1/memories/abc")

# Wildcard / load-balanced (any healthy memorylayer instance)
session.get("aether+sv://memorylayer/v1/memories/abc")
```

The `--` delimiter in the netloc maps to `::` in the Aether topic
(`memorylayer--default` → `sv::memorylayer::default`). If impl or specifier
contains a literal `--`, URL-encode it as `%2D%2D`. `ProxyError` surfaces as
`requests.exceptions.ConnectionError`.

### Go

```go
import (
"net/http"
"github.com/scitrera/aether/sdk/go/aether"
)

// agentClient is your connected *aether.BaseClient (or any principal client)
rt := &aether.AetherRoundTripper{
Client: agentClient,
Target: "sv::memorylayer::default",
}
httpClient := &http.Client{Transport: rt}
resp, err := httpClient.Get("http://ignored/v1/memories/abc")
```

OBO authorization from a Go `context.Context` is forwarded automatically:

```go
ctx = aether.WithOBOAuthorization(ctx, authContext)
req, _ = http.NewRequestWithContext(ctx, "GET", "http://ignored/v1/memories/abc", nil)
resp, err = httpClient.Do(req)
```

### TypeScript

```typescript
import {AetherFetchTransport} from "@scitrera/aether-client";

// agentClient is your connected AetherClient
const transport = new AetherFetchTransport(agentClient, "sv::memorylayer::default");
const resp = await transport.fetch("/v1/memories/abc");
```

`AetherFetchTransport.fetch()` accepts the same signature as the Web Fetch API
(`string | URL | Request`, optional `RequestInit`). The hostname and protocol
of the URL are ignored.

## ACL and OBO Model

The gateway applies the standard ACL check before routing a proxy envelope.
The caller must have `send` permission for the target service principal's
workspace, just as with `SendMessage`.

**On-Behalf-Of (OBO)** authorization is carried inside
`ProxyHttpRequest.authorization` (`AuthorizationContext`):

| Field                                             | Purpose                                                                           |
|---------------------------------------------------|-----------------------------------------------------------------------------------|
| `authority_mode`                                  | `"direct"` — caller acts as itself; `"obo"` — caller acts on behalf of `subject`. |
| `subject.principal_type` / `subject.principal_id` | The end-user or downstream principal being acted for.                             |
| `grant_id`                                        | ID of a pre-established authority grant.                                          |

The terminator sidecar resolves the OBO context and mints `X-Auth-*` headers
using the same `server/pkg/identityheaders` library used by the auth-proxy.
This is the **single source of truth** for identity header minting — the
backend sees identical headers regardless of whether the request arrived via
the auth-proxy path or the proxy-sidecar path.

## Limits and Quotas

Configured under the `proxy` key in the gateway config (all have sensible
defaults).

| Config field                                 | Default       | Description                                                                                                                                                                                                         |
|----------------------------------------------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `proxy.max_request_body_bytes`               | 8 MiB         | Maximum total body size for a single inline `ProxyHttpRequest` envelope. Bodies larger than this are streamed as `ProxyHttpBodyChunk` frames and bounded only by the per-backend `max_body_bytes` (default 10 MiB). |
| `proxy.max_concurrent_tunnels_per_workspace` | 256           | Gateway-wide live-tunnel ceiling per workspace.                                                                                                                                                                     |
| `proxy.max_tunnel_bytes`                     | 0 (unlimited) | Cumulative byte cap per tunnel session.                                                                                                                                                                             |

Bodies that exceed 256 KB are automatically split into `ProxyHttpBodyChunk`
frames by the SDK adapters, routed through the gateway via a per-request pin,
and reassembled by the terminator before dispatch. The same chunked-streaming
mechanism applies in the response direction. This is transparent to
application code; the effective request-body ceiling is the per-backend
`max_body_bytes`.

## Audit Events

Every proxy operation writes to the audit log.

| Event (`operation`)        | When it fires                                                                                                               |
|----------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `proxy_http_routed`        | Request successfully forwarded to the target sidecar.                                                                       |
| `proxy_http_failed`        | Request could not be delivered (ACL denied, sidecar offline, payload too large, etc.).                                      |
| `proxy_http_stream_closed` | Streaming response finished (clean EOF, idle timeout, byte cap, or caller cancel). Only fires for `stream_response_indefinitely=true` requests. |
| `tunnel_opened`            | `TunnelOpen` successfully established and pinned.                                                                           |
| `tunnel_open_failed`       | `TunnelOpen` rejected (ACL, quota, no healthy instance).                                                                    |
| `tunnel_closed`            | Tunnel torn down (client close, stream disconnect, or pin expiry).                                                          |

Each event includes:

- **Caller identity** — the principal that sent the proxy envelope.
- **Resolved target** — the concrete `sv::{impl}::{spec}` after wildcard
  resolution (never the bare wildcard form).
- **Grant lineage** — `grant_id` and `authority_mode` from `AuthorizationContext`,
  enabling full OBO chain reconstruction.
- **Byte counts** — request and response body sizes (for `proxy_http_routed`);
  cumulative bytes transferred (for `tunnel_closed`).

## Failure Modes

`ProxyError.kind` values returned in `ProxyHttpResponse.error`:

| Kind                  | Meaning                                                                                                       | Retry?              |
|-----------------------|---------------------------------------------------------------------------------------------------------------|---------------------|
| `UNKNOWN`             | Catch-all for unclassified errors.                                                                            | No                  |
| `DIAL_FAILED`         | Sidecar could not connect to the local backend. Check the backend is running and `backends[].url` is correct. | No (check config)   |
| `TIMEOUT`             | Request exceeded `timeout_ms`.                                                                                | Yes, if idempotent  |
| `UPSTREAM_RESET`      | Backend closed the connection mid-response.                                                                   | No                  |
| `ACL_DENIED`          | Caller does not have permission to reach this target.                                                         | No                  |
| `SIDECAR_UNAVAILABLE` | No healthy service instance for the target topic.                                                             | Yes, after backoff  |
| `PAYLOAD_TOO_LARGE`   | Body exceeds `proxy.max_request_body_bytes`.                                                                  | No (reduce payload) |
| `DECODE_FAILED`       | Gateway could not decode the proxy envelope.                                                                  | No                  |

**Retry policy**: the gateway never retries on the caller's behalf. SDK
adapters (`AetherRoundTripper`, `AetherHTTPXTransport`, `AetherFetchTransport`)
do not retry automatically. Callers may retry `TIMEOUT` and
`SIDECAR_UNAVAILABLE` on idempotent requests; other kinds indicate a
configuration or permission issue and should not be retried.

## Tunnels

TCP, WebSocket, and UDP tunnel support is proto-complete and gateway-routed
(`TunnelOpen` / `TunnelData` / `TunnelClose` / `TunnelAck` messages exist in
`api/proto/aether.proto` and are dispatched by `server/internal/gateway/routing_proxy.go`).

**What is shipped:**

- Gateway wildcard resolution and tunnel-pin stickiness (Redis-pinned per `tunnel_id`)
- Sidecar TCP, WebSocket (`ws://` / `wss://`), and UDP backends with `TunnelAck` flow control
- End-to-end TCP echo integration tests (see [proxy-quickstart.md](proxy-quickstart.md))

**Future work:**

- Go SDK `TunnelDial`, TypeScript SDK `tunnelDial`, Python `tunnel_dial` — higher-level SDK surface (raw proto access
  works today)

The `TunnelOpen` wire format includes a `session_token` field reserved for
tunnel resume across stream reconnects (not yet implemented).

Tunnels differ from REST proxy in one key way: they are **sticky**. Once
`TunnelOpen` resolves a wildcard to a concrete instance, all subsequent frames
for that `tunnel_id` go to the same instance for the lifetime of the tunnel.
Tunnels are torn down on stream disconnect.

## Multi-Backend Routing

A single terminator sidecar can serve multiple local HTTP (or tunnel) backends.
The sidecar walks the `backends` list and selects a backend using one of two
strategies.

### Selection rules

**HTTP backends — first-match-by-ACL (implicit)**

When `ProxyHttpRequest.backend_name` is absent the sidecar walks `backends` in
declaration order and picks the **first** entry whose `allow_paths` glob and
`allow_methods` list both match the incoming request. The last backend in the
list is conventionally a catch-all (`allow_paths: ["/*"]`).

**HTTP backends — explicit `backend_name`**

When `ProxyHttpRequest.backend_name` is set the sidecar looks up that backend
by `BackendConfig.Name` directly and skips the glob walk. The named backend's
`allow_paths` / `allow_methods` still apply (defence in depth) — `backend_name`
selects which ACL to consult, it does not bypass it.

`BackendConfig.Name` is operator-visible only (logs, metrics) unless
`backend_name` is set explicitly by the caller.

**Tunnel backends — first-match-by-`remote_hint` (implicit)**

For `TunnelOpen`, the sidecar matches `remote_hint` against each backend's
`allow_remote_hints` glob list in order.  `remote_hint` is a caller-supplied
string that identifies the intended tunnel target (e.g. `"db-primary"`,
`"cache-1"`). The first backend whose glob matches wins.

**Tunnel backends — explicit `backend_name`**

Set `TunnelOpen.backend_name` to bypass the glob walk. The named backend's
`allow_remote_hints` still applies.

### Operator strategy — partition by path prefix

The recommended pattern is to declare backends from most-specific to
least-specific and terminate with a catch-all:

```yaml
backends:
  - name: api-v1
    kind: http
    url: http://localhost:61001
    allow_paths: [ "/v1/*", "/healthz" ]
    allow_methods: [ GET, POST ]

  - name: api-v2
    kind: http
    url: http://localhost:61002
    allow_paths: [ "/v2/*" ]
    allow_methods: [ GET, POST, PUT, PATCH, DELETE ]

  - name: default        # catch-all — matches anything not claimed above
    kind: http
    url: http://localhost:61003
    allow_paths: [ "/*" ]
    allow_methods: [ GET, POST, PUT, PATCH, DELETE ]
```

A full three-backend example with inline comments is in
`server/configs/proxy-sidecar.multi-backend.example.yaml`.

### SDK examples — picking a backend by path

Callers do not need to set `backend_name` to benefit from path-prefix routing;
the first-match-by-ACL rule handles it automatically based on the request path.

**Python — httpx**

```python
import httpx
from scitrera_aether_client.httpx_transport import AetherHTTPXTransport

transport = AetherHTTPXTransport(aether_client, "sv::memorylayer::default")
with httpx.Client(transport=transport) as http:
    # Path "/v1/memories/abc" → sidecar matches api-v1 backend (first match)
    resp_v1 = http.get("http://ignored/v1/memories/abc")

    # Path "/v2/memories/abc" → sidecar matches api-v2 backend
    resp_v2 = http.get("http://ignored/v2/memories/abc")
```

**Go**

```go
rt := &aether.AetherRoundTripper{
Client: agentClient,
Target: "sv::memorylayer::default",
}
httpClient := &http.Client{Transport: rt}

// "/v1/..." → api-v1 backend (first-match-by-ACL, implicit)
resp, err := httpClient.Get("http://ignored/v1/memories/abc")

// "/v2/..." → api-v2 backend
resp, err = httpClient.Get("http://ignored/v2/memories/abc")
```

**TypeScript**

```typescript
const transport = new AetherFetchTransport(agentClient, "sv::memorylayer::default");

// "/v1/..." → api-v1 backend (first-match-by-ACL, implicit)
const respV1 = await transport.fetch("/v1/memories/abc");

// "/v2/..." → api-v2 backend
const respV2 = await transport.fetch("/v2/memories/abc");
```

### Explicit backend selection (Go)

When the caller needs to target a specific backend regardless of path, set
`backend_name` directly on the proto request. This is currently a Go-only
low-level API (the higher-level SDK adapters do not expose `backend_name` yet):

```go
import (
"net/http"
pb "github.com/scitrera/aether/api/proto"
"github.com/scitrera/aether/sdk/go/aether"
)

// Build the ProxyHttpRequest manually to set backend_name.
req := &pb.ProxyHttpRequest{
RequestId:   agentClient.NextRequestID(),
TargetTopic: "sv::memorylayer::default",
Method:      http.MethodGet,
Path:        "/v1/memories/abc",
BackendName: "api-v1", // explicit — skips glob walk on the sidecar
}
```

### Tunnels — `remote_hint` and `backend_name`

`remote_hint` is a caller-supplied string passed in `TunnelOpen` that the
sidecar matches against each backend's `allow_remote_hints` glob list. It lets
a single terminator expose multiple tunnel backends (e.g. distinct databases or
cache nodes) while the operator controls which callers can reach which target
via ACL.

Example sidecar config for two tunnel backends:

```yaml
backends:
  - name: db-primary
    kind: tcp
    url: tcp://db-primary.internal:5432
    allow_remote_hints:
      - "db-primary"
      - "db-*"

  - name: cache
    kind: tcp
    url: tcp://cache.internal:6379
    allow_remote_hints:
      - "cache-*"
```

A caller sets `remote_hint: "db-primary"` in `TunnelOpen`; the sidecar selects
the `db-primary` backend automatically. To override the glob walk, set
`TunnelOpen.backend_name = "db-primary"` explicitly (the named backend's
`allow_remote_hints` still applies).

### `backend_name` — current availability

| Transport         | Implicit (first-match)       | Explicit `backend_name`                           |
|-------------------|------------------------------|---------------------------------------------------|
| REST proxy (HTTP) | All SDKs                     | Go proto API (typed `BackendName` field)          |
| Tunnels           | All SDKs (via `remote_hint`) | Go proto API (typed `BackendName` field)          |

Higher-level SDK surface for `backend_name` (Python `proxy_http`, TS
`AetherFetchTransport`) is planned for a future release.

## Streaming Responses (SSE / Long-Poll)

By default the terminator buffers the entire backend response body before
forwarding it to the caller. For SSE feeds, long-poll endpoints, or any
response where the backend sends data incrementally, set
`stream_response_indefinitely = true` on the `ProxyHttpRequest`.

When enabled:

- The terminator begins forwarding `ProxyHttpBodyChunk` frames to the caller
  as soon as the backend produces bytes — no buffering.
- `timeout_ms` on the request governs **time-to-first-byte only**. After the
  first byte arrives, the connection stays open until one of the streaming
  termination conditions below fires.
- The stream terminates when:
  - The backend sends EOF (clean `fin=true` frame).
  - No bytes flow for `stream_idle_timeout_ms` milliseconds — default **30 s**
    (`streamIdleTimeoutDefault` constant in `terminator.go`).
  - Total response bytes exceed `max_response_body_bytes` — default 0
    (unlimited). Set a non-zero value to cap runaway streams.
  - The caller's gRPC stream disconnects.

Each termination emits a `proxy_http_stream_closed` audit event with the
final byte count and close reason.

### Proto fields (on `ProxyHttpRequest`)

| Field                         | Type    | Default    | Purpose                                                                |
|-------------------------------|---------|------------|------------------------------------------------------------------------|
| `stream_response_indefinitely`| `bool`  | `false`    | Opt-in to the streaming path. If false, the buffered path is used.    |
| `stream_idle_timeout_ms`      | `int64` | 30 000 ms  | Idle-byte deadline. Set 0 to use the default.                          |
| `max_response_body_bytes`     | `int64` | 0 (no cap) | Hard byte cap across the full stream. Triggers `PAYLOAD_TOO_LARGE`.   |

### Example — Python httpx (SSE)

The `AetherHTTPXTransport` does not expose these fields directly today. Use
the low-level `proxy_http` / `proxy_http_async` helpers with a hand-built
`ProxyHttpRequest` to opt in:

```python
import asyncio
from scitrera_aether_client.proxy import proxy_http_async
import aether_pb2 as pb

req = pb.ProxyHttpRequest(
    request_id=client.next_request_id(),
    target_topic="sv::memorylayer::default",
    method="GET",
    path="/v1/events/stream",
    stream_response_indefinitely=True,
    stream_idle_timeout_ms=60_000,   # 60 s idle deadline
)

async for chunk in proxy_http_async(client, req):
    print(chunk)
```

---

## Grant Resource Scopes

Standard ACL grants allow broad `send` permission to a target topic. Two
finer-grained **resource scopes** let operators restrict which HTTP paths or
tunnel targets a given grant may reach.

### `proxy_path` — HTTP path ACL

Set the `resource_scope` key `proxy_path` on an `AuthorityGrant` to restrict
which backend + HTTP method + path combinations the grant holder may invoke.

**Pattern grammar:** `<backend_glob>::<method_glob> <path_glob>`

- `<backend_glob>` — matched against the resolved `BackendConfig.Name` (e.g.
  `api-v1`, `*`, `api-*`).
- `<method_glob>` — matched against the HTTP method (upper-cased). Use `*`
  for any method.
- `<path_glob>` — matched against the request path via `path.Match` semantics.

A literal `"*"` pattern (no `::`) is shorthand for "match anything".

If the `proxy_path` key is absent from the grant's resource scope, all paths
are allowed (no restriction). This is backward-compatible — existing grants
without this key are unaffected.

**Examples:**

| Pattern                         | Allows                                               |
|---------------------------------|------------------------------------------------------|
| `*`                             | Any backend, method, and path                        |
| `api-v1::GET /v1/*`             | GET under `/v1/` on the `api-v1` backend only        |
| `*::GET /healthz`               | GET `/healthz` on any backend                        |
| `api-*::* /v2/*`                | Any method under `/v2/` on any `api-*` backend       |

The `_default` backend name is used when the request arrives via the
auth-proxy path (not the proxy sidecar), keeping grant patterns portable
between the two components.

**Implementation:** `server/pkg/identityheaders.MatchProxyPath` /
`ResourceTypeProxyPath = "proxy_path"`.

---

### `tunnel_target` — Tunnel target ACL

Set the `resource_scope` key `tunnel_target` on an `AuthorityGrant` to
restrict which backend + protocol + remote-hint combinations the grant holder
may open a tunnel to.

**Pattern grammar:** `<backend_glob>::<protocol_glob> <remote_hint_glob>`

- `<backend_glob>` — matched against the resolved `BackendConfig.Name`.
- `<protocol_glob>` — matched against the tunnel protocol (`tcp`, `udp`, `ws`,
  always lower-cased).
- `<remote_hint_glob>` — matched against the caller-supplied `remote_hint`
  string in `TunnelOpen`.

A literal `"*"` pattern is shorthand for "match anything". An absent
`tunnel_target` key means all tunnels are allowed.

**Examples:**

| Pattern                           | Allows                                                    |
|-----------------------------------|-----------------------------------------------------------|
| `*`                               | Any backend, protocol, and remote hint                    |
| `db-primary::tcp db-primary`      | TCP tunnel to `db-primary` hint on the `db-primary` backend |
| `*::tcp prod-*:5432`              | TCP tunnel to any `prod-*:5432` hint on any backend       |
| `cache::tcp cache-*`              | TCP tunnel to any `cache-*` hint on the `cache` backend   |

**Implementation:** `server/pkg/identityheaders.MatchTunnelTarget` /
`ResourceTypeTunnelTarget = "tunnel_target"`.

---

## Reloading Configuration

The terminator sidecar supports zero-downtime backend reconfiguration via
`SIGHUP`. Send the signal to the running process and it will:

1. Re-read the YAML file that was passed via `--config` at startup.
2. Validate the new config (same rules as startup validation).
3. Atomically swap the backend slices under a write lock — in-flight requests
   keep their originally-captured backend reference and finish naturally.
4. Log a `"terminator: config reloaded"` info message with old/new backend
   counts.

```bash
kill -HUP $(pidof proxy-sidecar)
# or, if you know the PID:
kill -HUP <pid>
```

**What you can change on reload:**

- Add, remove, or reconfigure any backend (HTTP, TCP, WS, UDP).
- Change `allow_paths`, `allow_methods`, `header_mode`, `max_body_bytes`, etc.
- Change gateway credentials or TLS paths (takes effect on the *next*
  reconnect, not the current live connection).

**What is rejected on reload:**

| Condition                                 | Behaviour                     |
|-------------------------------------------|-------------------------------|
| Config file not found or unreadable       | Log error, keep old config    |
| Invalid YAML or failed validation         | Log error, keep old config    |
| Mode change (`terminator` → `initiator`)  | Log error, keep old config    |
| Second SIGHUP while reload is in progress | Silently dropped (no queuing) |

**In-flight tunnel safety:** Tunnels that are alive at the time of the swap
hold a direct pointer to their backend struct, which is not freed until all
references drop. New tunnels after the swap will route against the new backend
list; existing tunnels are unaffected.

**Dev-defaults mode:** When the sidecar starts with `--dev` and no config file
is found, `cfgPath` is empty and SIGHUP is a no-op (the error is logged). To
enable live reload, always provide an explicit `--config` path.

## Performance

### Single-node data-plane bypass

When the caller and the target sidecar are connected to the **same gateway
instance**, the gateway can deliver bulk data-plane bytes directly between
the two gRPC streams in-process, skipping the RabbitMQ round-trip. This
materially cuts per-frame latency and load on the message broker for the
common single-gateway / co-located deployments.

**Which envelopes take the bypass**

Only data-plane envelopes — bytes-only payloads that are not audited
per-frame — are eligible:

| Envelope                | Bypass eligible? |
|-------------------------|------------------|
| `TunnelData`            | Yes              |
| `TunnelAck`             | Yes              |
| `ProxyHttpBodyChunk`    | Yes              |
| `TunnelOpen`            | **No** — always RMQ |
| `TunnelClose`           | **No** — always RMQ |
| `ProxyHttpRequest` (header) | **No** — always RMQ |
| `ProxyHttpResponse` (header) | **No** — always RMQ |
| `ProxyError`            | **No** — always RMQ |

**Audit invariant.** Control-plane envelopes carry the audit signal
(`tunnel_opened`, `proxy_http_routed`, etc.). They ALWAYS travel through
RabbitMQ regardless of co-location, so audit emission is preserved exactly
as in the cross-gateway case. The bypass affects byte routing only.

**Backpressure parity.** When the target session's outbound delivery buffer
is full, the bypass falls back to RMQ rather than stalling routing. This
matches the existing slow-reader behavior of the RMQ fan-out path: a slow
sidecar absorbs pressure via the broker's persisted log, not by blocking
the routing goroutine.

**Configuration**

Enabled by default. To disable (e.g. emergency rollback during an
incident):

```yaml
quotas:
  proxy:
    local_bypass_enabled: false
```

Or via environment override (no restart needed for new connections):

```bash
export AETHER_PROXY_LOCAL_BYPASS_DISABLED=1
```

**Observability**

The Prometheus counter `aether_proxy_local_bypass_total{envelope_type, result}`
exposes per-envelope outcomes:

| Result          | Meaning                                                                 |
|-----------------|-------------------------------------------------------------------------|
| `hit`           | Bypass succeeded; bytes delivered locally without touching RMQ.        |
| `rmq_fallback`  | Target not connected to this gateway; routed via RMQ.                  |
| `full_buffer`   | Target's deliveryCh was full; routed via RMQ to avoid stalling.        |
| `disabled`      | Bypass turned off by config or env override.                           |

For the broader roadmap of routing-layer optimizations and the design
rationale behind the single-node bypass, see
[proxy-architecture-roadmap.md](proxy-architecture-roadmap.md).

## Caller Identity Headers

Two additional headers are stamped on every proxied request that reaches a
backend via the terminator sidecar or the auth-proxy direct path. They let
sandboxed backends discover who called them — so they can call back — without
a separate lookup.

| Header                    | Value                                                                             | Trust                              |
|---------------------------|-----------------------------------------------------------------------------------|------------------------------------|
| `X-Aether-Caller-Topic`   | Sender's principal topic (e.g. `ag.ws.myagent.spec` or a user/service ID)        | Server-stamped — trustworthy       |
| `X-Aether-Caller-Subject` | OBO grant subject's ID when `authority_mode=on_behalf_of`; absent in direct mode | Server-stamped — trustworthy       |

**Source of truth:**

- **Terminator sidecar path:** `X-Aether-Caller-Topic` is taken from the
  `x-aether-actor-topic` field that the gateway stamps on every `ProxyHttpRequest`
  envelope before forwarding. `X-Aether-Caller-Subject` is derived from the
  resolved OBO authority (after grant validation).
- **Auth-proxy direct path:** `X-Aether-Caller-Topic` equals the authenticated
  principal's ID. `X-Aether-Caller-Subject` equals the OBO subject's ID when
  a grant was resolved; absent otherwise.

Both components use `server/pkg/identityheaders.MintInto` to stamp these
headers, so the wire format is identical regardless of which path the request
arrived through.

**Spoofing prevention:** Both headers are in the `X-Aether-*` namespace and
are stripped by `identityheaders.StripInbound` on every inbound request before
any trusted minting occurs. Clients cannot inject these headers.

## Sandbox Runtimes

The proxy sidecar can enable both the **terminator** and **relay** surfaces
in one process to create a secure sandbox environment: an untrusted spawned
process (LLM agent, tool runner, ephemeral container) dials a local Unix
Domain Socket with no
credentials. The sidecar injects its own API key and identity, enforces a strict
operation allow-list and target-topic clamp, and forwards permitted envelopes
upstream.

The sandbox holds zero credentials. The UDS path (`/run/aether.sock` by
convention) gates access by filesystem permissions. If the sandbox is
compromised, the attacker gains only what the relay's allow-list and topic clamp
permit — not free access to the gateway.

See [proxy-sandbox.md](proxy-sandbox.md) for the full deployment pattern,
UDS path convention, SDK config snippets, spawn-time grant scopes
(`proxy_path`, `tunnel_target`), and the annotated example YAML at
`server/configs/proxy-sidecar.sandbox.example.yaml`.

## Related Documents

- [proxy-quickstart.md](proxy-quickstart.md) — running the sidecar, integration tests, and auth-proxy regression suite
- [proxy-cutover.md](proxy-cutover.md) — production rollout criteria, SLOs, rollback steps, and runbook
- [proxy-load-test-results.md](proxy-load-test-results.md) — routing-layer benchmark results and scope caveats
- [proxy-architecture-roadmap.md](proxy-architecture-roadmap.md) — phased plan for proxy/tunnel routing-layer evolution
- [proxy-sandbox.md](proxy-sandbox.md) — sandbox deployment pattern (relay+terminator, UDS, grant scopes)
