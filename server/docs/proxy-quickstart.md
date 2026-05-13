# Aether Proxy Quickstart

This document covers the developer workflow for the Aether HTTP-over-Aether
proxy stack: the **proxy-sidecar** (terminator and initiator modes), the
client SDK proxy helpers, and how to run the end-to-end integration tests
locally.

## Components

| Component | Path | Purpose |
|---|---|---|
| Gateway proxy routing | `server/internal/gateway/routing_proxy.go` | Wildcard `sv::{impl}` resolution, ACL, tunnel pinning |
| Proxy sidecar | `server/cmd/proxy-sidecar/`, `server/internal/proxysidecar/` | Terminator (gateway -> backend) + Initiator (HTTP -> gateway) |
| Identity headers | `server/pkg/identityheaders/` | Canonical `X-Auth-*` mint shared with auth-proxy |
| Python SDK | `sdk/python-client/scitrera_aether_client/proxy.py` | `proxy_http` / `proxy_http_async` |
| Go SDK | `sdk/go/aether/proxy.go` | `ProxyHTTP` + `AetherRoundTripper` |
| TypeScript SDK | `sdk/typescript/src/proxy.ts` | `proxyHttp` + fetch transport |

## Running the integration tests

The end-to-end integration tests live under `server/tests/integration/`. They
exercise the full proxy data path (caller -> gateway routing -> sidecar
terminator -> mock HTTP backend) **without** requiring RabbitMQ, Redis or
Postgres: an in-process router emulates the gateway's wildcard resolution and
ACL gate, and dispatches assembled `ProxyHttpRequest` envelopes directly to
real `proxysidecar.Terminator` instances.

The tests are gated behind the `integration` build tag so `go test ./...`
does not pick them up by default.

```bash
cd server
/home/drew/sdk/go1.25.5/bin/go test -tags=integration -count=1 -v ./tests/integration/...
```

Expected output (abridged):

```
--- PASS: TestPhase1_GET_HappyPath
--- PASS: TestPhase1_POST_HappyPath_PreservesContentType
--- PASS: TestPhase1_POST_LargeChunkedBody
--- PASS: TestPhase1_ACLDeny_NoGrant
--- PASS: TestPhase1_ACLDeny_NonServiceTarget
--- PASS: TestPhase1_OBO_HeadersByteEqualToAuthProxy
--- PASS: TestPhase1_Wildcard_TwoInstances_FanOut_AndOfflineRouting
--- PASS: TestPhase1_IdleTimeout_TerminatedRequestReturnsTimeout
--- PASS: TestPhase1_SidecarUnavailable_NoInstances
--- PASS: TestPhase1_SidecarUnavailable_AllInstancesOffline
--- PASS: TestPhase1_ConcreteAddress_RoutesToExactInstance
--- PASS: TestTunnelE2E_TenMBEcho_Bytewise
--- PASS: TestTunnelE2E_HalfClose_CallerFinPropagatesToServer
--- PASS: TestTunnelE2E_TwoConcurrentTunnels_Independent
--- PASS: TestTunnelE2E_IdleTimeout_ClosesWithReason
--- PASS: TestTunnelE2E_MaxBytesQuota_ClosesWithReason
--- PASS: TestTunnelE2E_Stickiness_AllFramesOnSameInstance
--- PASS: TestTunnelE2E_BidirectionalFlowControl_NoDeadlock
PASS
ok      github.com/scitrera/aether/tests/integration    2.014s
```

### What is covered

1. **GET / POST happy path** — round-trip headers (incl. `Content-Type`),
   status codes, query string preservation, body sizes including chunked
   bodies above 256 KiB.
2. **ACL deny** — caller without grant gets `ProxyError{ACL_DENIED}`; backend
   never observes the request.
3. **Non-service target** — `ag::*` / `tu::*` targets rejected with
   `ProxyError{ACL_DENIED}` (defense-in-depth check on the proxy envelope).
4. **OBO header injection** — caller emits `AuthorizationContext{authority_mode=on_behalf_of, subject, grant_id}`.
   The mock backend's received `X-Auth-*` header set is asserted **byte-equal**
   to what `identityheaders.Mint` produces (the same minter used by
   auth-proxy's `InjectHeaders`).
5. **Wildcard `sv::{impl}` fan-out** — two terminator instances connect; a
   sample of N=60 requests is verified to touch both. Then one instance is
   marked offline; subsequent requests must all route to the survivor. The
   instance is brought back online; both must receive traffic again.
6. **Idle timeout** — a backend that intentionally sleeps longer than the
   per-request `timeout_ms` produces `ProxyError{TIMEOUT}` instead of
   blocking.
7. **Sidecar unavailable** — bare wildcard target with no online instances
   returns `ProxyError{SIDECAR_UNAVAILABLE}` (both "no instances at all"
   and "all instances offline" paths).
8. **Concrete address routing** — fully-qualified `sv::{impl}::{spec}` is
   delivered to the named instance only.

### What is NOT covered

- Higher-level SDK tunnel API (`TunnelDial` / `tunnelDial` / `tunnel_dial`) — raw proto access works; convenience wrappers are future work.
- Cross-gateway dispatch with real RabbitMQ Streams replay — exercised by
  the unit tests in `server/internal/gateway/proxy_routing_test.go`; a
  wire-level load test against a deployed gateway + RMQ + Redis stack is
  still pending (see [proxy-load-test-results.md](proxy-load-test-results.md)).

## Running the auth-proxy regression suite

The OBO byte-equivalence assertion is the contract between the sidecar's
strict-mode minter and the auth-proxy's `InjectHeaders`. Re-run the
auth-proxy suite to confirm both ends still agree:

```bash
cd server
/home/drew/sdk/go1.25.5/bin/go test -count=1 ./internal/authproxy/...
```

## Running the proxy-sidecar binary

```bash
cd server
/home/drew/sdk/go1.25.5/bin/go run ./cmd/proxy-sidecar --config configs/proxy-sidecar.dev.yaml
```

For a full development-mode boot of the gateway + sidecar pair, see
`docs/quickstart.md` and the reference Docker Compose setup under
`server/deployments/docker-compose/`.

## Related Documents

- [proxy.md](proxy.md) — feature overview, SDK one-liners, ACL/OBO, limits, audit events, failure modes
- [proxy-cutover.md](proxy-cutover.md) — production rollout criteria, SLOs, rollback steps, and runbook
- [proxy-load-test-results.md](proxy-load-test-results.md) — routing-layer benchmark results and scope caveats
