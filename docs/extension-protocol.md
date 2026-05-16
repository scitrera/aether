# Extension Protocol

> Phase 6 of the Aether enterprise-agentic-fabric upgrade introduces a
> URI-based extension mechanism so customers and future-Aether features
> can negotiate optional protocol surfaces at connect-time without forking
> the proto or branching the gateway. The model is intentionally aligned
> with [A2A's extension model](https://google.github.io/A2A/) so future
> A2A interop is cheap.

## Why

Before Phase 6 there was no formal hook for customer code (or upcoming
Aether features) to declare "I want extension X active on this connection".
The choices were:

1. Add the feature directly to the proto + gateway (no opt-in, hard to ship
   experimental work, breaks lite-mode parity).
2. Ship out-of-band via gRPC metadata headers (no formal schema, no
   negotiation, no rejection semantics for unsupported features).
3. Fork the SDK or the proto (carries every downstream consumer along
   for the ride).

Phase 6 adds a fourth, formal path:

1. **`ExtensionDeclaration`** — a URI-typed handle a participant can pass
   on InitConnection or on a per-message basis. Carries optional version,
   a `required` flag, and an optional JSON Schema string.
2. **Connect-time negotiation** — the gateway returns
   `ConnectionAck.negotiated_extensions` indicating which URIs are
   supported. A `required` URI that the gateway does not support causes
   the connection to be rejected with `ERR_EXTENSION_UNSUPPORTED`.
3. **Per-message activation hook** — `UpstreamMessage.active_extensions`
   and `DownstreamMessage.active_extensions` (a `repeated string` URI
   list) advertise which extensions apply to a specific message. Peers
   that don't support a required URI listed on the envelope MUST reject
   the message.
4. **`Aether-Extensions` gRPC metadata header** — a lighter-weight surface
   for clients that prefer a header to a proto field. Comma-separated URI
   list. Always non-required; the proto field is the authoritative source
   for `required` semantics.
5. **Server-side `KnownExtensions` registry** — a single map in the
   gateway that declares which URIs the gateway natively supports. Adding
   an entry is the only step required to make a server-blessed extension
   first-class.

The wire foundation lands in Phase 6 with **`KnownExtensions` intentionally
empty** — concrete extensions get added as later phases ship them. This
phase exists so customer code can begin layering on extension semantics
without waiting for Aether-blessed implementations of every primitive.

---

## Concepts

### `ExtensionDeclaration`

```proto
message ExtensionDeclaration {
  string uri = 1;          // required
  string version = 2;      // optional; empty = any
  bool required = 3;       // hard-fail when unsupported
  string json_schema = 4;  // informational; not enforced
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `uri` | yes | Globally unique extension URI. Recommended `"https://..."` style. Treated as a free string by the gateway — no schema check beyond non-empty. |
| `version` | no | Pinned version. Empty matches any version the server happens to support. |
| `required` | no | When `true`: the gateway MUST reject the connection if it doesn't support the URI (or, on a per-message declaration, the peer MUST reject the message). When `false`: a missing URI is surfaced in the negotiation result but doesn't fail anything. |
| `json_schema` | no | Optional JSON Schema string describing the shape of extension-specific data. Informational only; Aether does not validate. |

### `NegotiatedExtension`

The server's response side of the handshake, returned in
`ConnectionAck.negotiated_extensions`:

```proto
message NegotiatedExtension {
  string uri = 1;
  string version = 2;
  bool supported = 3;
  string rejection_reason = 4;
}
```

One entry per declaration the client supplied (in the order received,
with header-sourced declarations appearing after proto-sourced ones).
`supported=false` plus a populated `rejection_reason` lets the client
introspect why an extension didn't make it through negotiation.

### `ConnectionAck.server_supported_extensions`

A discovery hint: extensions the server natively supports
(i.e. listed in `KnownExtensions`) that the client did NOT declare. Empty
when the client already declared every server-supported URI. Lets clients
discover what optional surfaces are available without a separate descriptor
endpoint.

---

## Connection-time negotiation

The client declares its extensions on `InitConnection`:

```python
from scitrera_aether_client import AgentClient, make_extension

ext = make_extension(uri="https://example.com/ext/threading", version="1.0", required=True)
client = AgentClient(
    workspace="prod",
    implementation="my-impl",
    specifier="worker-1",
    extensions=[ext],
)
client.connect("aether:50051")
# After connect succeeds:
print(client.negotiated_extensions())          # ["https://example.com/ext/threading"]
print(client.server_supported_extensions())    # discovery hint
```

The gateway resolves declarations in this order:

1. **Native gateway support** — `KnownExtensions[uri]`.
2. **Agent-declared support** — for agent principals, the gateway looks up
   the agent's `AgentRegistration.Extensions` list (added in Phase 5). A
   URI in the agent's registration widens the supported set for that
   agent's own sessions.
3. **Otherwise unsupported** — `supported=false`. If `required=true`, the
   connection is rejected with `codes.FailedPrecondition` and an
   `ERR_EXTENSION_UNSUPPORTED: <uri>` status message.

### The `Aether-Extensions` header

A lighter-weight surface for callers who prefer a header to a proto field
(matches A2A's HTTP header convention). Comma-separated URI list,
trimmable whitespace, optional repeated header values:

```
Aether-Extensions: https://example.com/ext/a, https://example.com/ext/b
```

The gateway unions header-derived declarations with proto-declared ones
into a single negotiation pass.

**Precedence**: header-derived declarations are always non-required —
there's no way to express `required` via the header. If a caller needs
required semantics, use the proto field (`InitConnection.extensions`).
Both surfaces are surfaced in `ConnectionAck.negotiated_extensions` for
introspection.

### Rejection semantics

A `required` extension that ends up unsupported produces:

- A `ConnectionAck` with `negotiated_extensions` populated (so the client
  can pick the rejection reason out before the stream closes).
- An audit row per declaration with operation `extension_negotiated` and
  `success` reflecting whether that URI made it through.
- A gRPC status of `codes.FailedPrecondition` with message
  `ERR_EXTENSION_UNSUPPORTED: <reason>`.

The client SDK surfaces the failure via the existing recoverable-error
discrimination logic; `FailedPrecondition` is treated as non-recoverable
(same as `INVALID_ARGUMENT` / `PERMISSION_DENIED`).

---

## Per-message activation

For features that need per-message gating (e.g. "this message uses
extension X — reject if you don't speak it"), the envelopes carry a
`repeated string active_extensions` field:

```proto
message UpstreamMessage {
  oneof payload { ... }
  repeated string active_extensions = 32;
}

message DownstreamMessage {
  oneof payload { ... }
  repeated string active_extensions = 38;
}
```

Carrying the URI list on the envelope (rather than embedding it in every
oneof payload type) means any payload can opt into extension semantics
without proto rewrites.

Phase 6 ships with no payload that gates on `active_extensions` today —
the data path is in place for later phases. Receivers should treat
unknown URIs the same way they treat the InitConnection-time set:
ignore non-required, reject required.

---

## Server-side `KnownExtensions`

The gateway's intrinsic extension set lives in
`server/internal/gateway/extensions.go`:

```go
var KnownExtensions = map[string]bool{
    // (empty in Phase 6 — concrete entries land in later phases)
}

func IsExtensionKnown(uri string) bool { return KnownExtensions[uri] }
```

Adding an entry here is the only required step to bless an extension at
the gateway level. Negotiation reads from the map directly; no separate
registration call.

### Server-blessed vs agent-declared extensions

The boundary is intentional:

| Source | Scope | When to use |
|--------|-------|-------------|
| `KnownExtensions` | All sessions, all principals | Aether-shipped features, gateway-wide protocol additions |
| `AgentRegistration.Extensions` | Only sessions on that agent's implementation | Customer-shipped agent-private extensions, experimental features that don't warrant gateway-wide blessing |

Agent-declared extensions are discoverable via
`AgentRegistration.Extensions` but do NOT show up in
`ConnectionAck.server_supported_extensions`. They're scoped to the agent.

---

## Audit trail

Every extension declaration produces an audit row in the existing
connection lane (`event_type="connection"`, operation
`"extension_negotiated"`). The metadata bag carries `uri`, `version`,
`supported`, and `required`. Failures (`supported=false` on a required
URI) record the rejection reason in the audit row's `error_message`
column.

This makes it easy to track:

- Who's trying to use extension X.
- Whether their negotiation succeeded.
- Which clients are still on stale URIs.

---

## SDK usage

### Python — `make_extension`

```python
from scitrera_aether_client import make_extension

ext = make_extension(
    uri="https://example.com/ext/foo",
    version="1.0",          # optional
    required=True,          # optional, default False
    json_schema="...",      # optional, informational
)
```

`make_extension` raises `ValueError` if `uri` is empty.

### Passing extensions through the client constructors

All `*Client` and `Async*Client` constructors accept an `extensions=`
kwarg. The list is threaded through the corresponding `create_*_init`
helper into `InitConnection.extensions`:

```python
agent = AgentClient(
    workspace="prod",
    implementation="my-impl",
    specifier="worker-1",
    extensions=[make_extension(uri="https://example.com/ext/foo")],
)
```

### Reading negotiation results

After `connect()` returns, callers can read:

```python
agent.negotiated_extensions()       # list of supported URIs from the negotiated set
agent.server_supported_extensions() # discovery hint: server's native URIs the caller did not declare
```

Both methods return an empty list when the client declared no extensions
or before `connect()` has succeeded.

---

## Wire error codes

| Code | Meaning |
|------|---------|
| `ERR_EXTENSION_UNSUPPORTED` | A required extension declared at connect time (or per-message) is not supported. Connection rejected with `codes.FailedPrecondition` / messages rejected at the handler level. |

---

## Backward compatibility

- Clients that don't supply `InitConnection.extensions` continue to work
  unchanged. The gateway treats an empty declaration list as "no
  negotiation requested" and returns an empty `negotiated_extensions`.
- The `Aether-Extensions` header is absent on pre-Phase-6 clients; the
  gateway treats absence as "no header-supplied extensions".
- Pre-Phase-6 servers will silently drop the new fields (unknown proto
  fields are not an error in proto3). The Phase 6 SDK can talk to a
  pre-Phase-6 gateway, it just won't see anything in
  `negotiated_extensions()`.

---

## Aetherlite parity

Connect-time negotiation runs identically against the sqlite-backed
gateway. `KnownExtensions` is a Go-side map, not backed by storage, so
the lite gateway has nothing to migrate. Agent-declared extensions
(`AgentRegistration.Extensions`) round-trip through the same
storage interface (regstore.Store) on both backends — added in Phase 5.

---

## Deferred to a future phase

Phase 6 stops at the wire foundation. The following are planned for later
phases:

- **AgentCard JSON descriptor**: a publishable descriptor that projects an
  `AgentRegistration` (ResourceSchema + Capabilities + Extensions) into
  the A2A AgentCard shape. Will be a projection over the existing
  registry data, not a separate storage surface.
- **`AgentSkill` with JSON-Schema I/O**: a structured way to advertise
  callable skills with typed input/output schemas. Sits on top of the
  AgentCard work.
- **Signed cards**: cryptographic attestation for AgentCard contents to
  support cross-cluster / cross-org federation.
- **Well-known endpoint hosting**: a `/.well-known/agent-card.json`
  endpoint on the gateway so external A2A consumers can discover
  Aether-hosted agents without speaking gRPC.

These deferred items deliberately do not constrain Phase 6's wire format.
When they land, they'll consume the existing extension URIs and the
Phase 5 registry data — no proto rewrites needed.
