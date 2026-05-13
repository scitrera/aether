# MemoryLayer → Aether Proxy: Cutover Criteria and Rollback Plan

This document is the production rollout playbook for migrating MemoryLayer callers from
direct HTTP (or HTTP + auth-proxy) to the Aether proxy sidecar path. Follow every section
in order. Do not flip the feature flag to aether-only until the sign-off checklist is
complete.

**Related docs:**
- [Proxy overview](proxy.md) — architecture, SDK one-liners, ACL, audit events
- [Proxy quickstart](proxy-quickstart.md) — running the sidecar and integration tests
- [Load test results](proxy-load-test-results.md) — routing-layer benchmark results and scope caveats

---

## 1. Dual-Path A/B Model

Both transport paths run simultaneously during the canary period. No infrastructure is
decommissioned until the sign-off checklist is complete.

### Paths in parallel

| Path | Description |
|---|---|
| **Direct HTTP / auth-proxy** | Current production path. Callers hit MemoryLayer directly (or via the auth-proxy sidecar for header injection). This remains the fallback. |
| **Aether sidecar (canary)** | New path. Callers use an Aether SDK transport adapter or an initiator sidecar. The terminator sidecar (`sv::memorylayer::default`) receives the request and forwards it to the local MemoryLayer HTTP server. |

### Canary traffic-split mechanism

Traffic split is **caller-side**. The gateway has no concept of a canary percentage —
routing is determined by which transport adapter the caller mounts.

**Option A — Feature flag per caller (recommended):**

Each caller reads an environment variable (or config key) at startup:

```
AETHER_PROXY_DISABLED=1   # if set (any non-empty value), skip Aether and use direct HTTP
```

Roll out the canary by deploying the Aether-aware caller binary to a fraction of pods
and setting `AETHER_PROXY_DISABLED=1` on the remaining pods. Increase the fraction over
time.

**Option B — Separate deployment target:**

Deploy a shadow caller service that routes 100% of its traffic via Aether. Direct live
traffic at it incrementally via load-balancer weights or feature flags in the caller
configuration layer.

### Dial-up schedule (suggested — confirm with ops)

| Day | Aether canary fraction |
|---|---|
| 1–2 | 5% (smoke test) |
| 3–4 | 25% |
| 5–6 | 50% |
| 7 | 75% |
| 8–14 | 100% canary — observe for 7 consecutive days before flipping to aether-only |

### Header-mint parity

Header byte-equality between the auth-proxy path and the sidecar path is enforced at the
source: both call `server/pkg/identityheaders.Mint` (the same library). This is verified
by the `TestPhase1_OBO_HeadersByteEqualToAuthProxy` integration test and the
`header_mode_test.go` golden test in the sidecar package. No manual check is required
during normal operation; see [Sign-off checklist](#6-sign-off-checklist) for the
production canary sampling step.

---

## 2. SLOs for the Cutover

These targets apply to the Aether sidecar path measured during the canary period.
Numbers marked **(suggested — confirm with ops)** are starting points; adjust based on
your MemoryLayer SLA and observed direct-HTTP baselines.

### 2.1 REST proxy latency

Measure end-to-end latency at the caller (time from issuing the HTTP request to
receiving the complete response body).

| Metric | Target |
|---|---|
| p50 latency | ≤ 1.5× direct-HTTP p50 **(suggested — confirm with ops)** |
| p99 latency | ≤ 2× direct-HTTP p99, hard ceiling 250 ms **(suggested — confirm with ops)** |

The 250 ms hard ceiling accounts for gRPC stream overhead, gateway routing, and the
local sidecar HTTP roundtrip. Adjust downward if your direct-HTTP p99 is already well
below 125 ms.

### 2.2 Tunnel-open latency

In-process benchmarks: p50 = 20 ms, p99 = 38 ms (100 concurrent opens, no real
network). See [proxy-load-test-results.md](proxy-load-test-results.md) for scope caveats.

| Metric | Target |
|---|---|
| Tunnel-open p99 (real network) | ≤ 250 ms **(suggested — confirm with ops)** |

This accounts for real-network RTT between the caller and the gateway. If your gateway
and callers are co-located in the same AZ, 100 ms p99 is achievable.

### 2.3 Error rate

| Metric | Target |
|---|---|
| Aether-path error rate above direct-HTTP baseline | ≤ 0.1% of requests **(suggested — confirm with ops)** |

Count `ProxyError` responses (`DIAL_FAILED`, `TIMEOUT`, `SIDECAR_UNAVAILABLE`,
`UPSTREAM_RESET`) as errors. `ACL_DENIED` and `PAYLOAD_TOO_LARGE` are configuration
issues, not transport errors, and should be zero.

### 2.4 Availability

| Metric | Target |
|---|---|
| Aether-path availability (rolling 7-day) | ≥ 99.9% of requests successfully proxied **(suggested — confirm with ops)** |

A request is "successfully proxied" if it receives an HTTP response (any status code)
from MemoryLayer. Transport-layer failures (`ProxyError`) count as unavailability.

---

## 3. Audit Completeness Check

Every MemoryLayer call routed through the Aether proxy must appear as a
`proxy_http_routed` (or `proxy_http_failed`) audit event. The audit event includes the
`request_id` field that uniquely identifies each proxy call.

### 3.1 Daily reconciliation

Run this check once per day during the canary period (and permanently in production):

1. **Aether audit count:** Count distinct `request_id` values in the audit log where
   `operation = 'proxy_http_routed' OR operation = 'proxy_http_failed'` in the relevant
   time window.

2. **MemoryLayer access log count:** Count distinct request IDs (or the `X-Request-Id`
   header value forwarded by the sidecar) in MemoryLayer's own access log for the same
   time window.

3. **Match requirement:** `audit_count / memorylayer_count ≥ 99.9%`

Any drift below 99.9% must be investigated before increasing the canary fraction.

Relevant audit operation codes (from `server/internal/audit/types.go`):

```
proxy_http_routed        — request successfully forwarded to the sidecar
proxy_http_failed        — request could not be delivered
proxy_http_stream_closed — streaming response finished (SSE / long-poll)
tunnel_opened            — TunnelOpen successfully established
tunnel_open_failed       — TunnelOpen rejected
tunnel_closed            — tunnel torn down
```

### 3.2 Alert thresholds and escalation (suggested — confirm with ops)

| Condition | Action |
|---|---|
| Drift > 0.1% for one reconciliation window | Alert on-call. Investigate before next window. |
| Drift > 1% for one reconciliation window | Page on-call immediately. Pause canary dial-up. |
| Drift > 5% for one reconciliation window | Roll back to direct HTTP. See [Section 4](#4-rollback-steps). |

Who pages: the audit reconciliation job should fire to the same on-call rotation as
MemoryLayer availability alerts.

---

## 4. Rollback Steps

Target: **full rollback within 5 minutes of signal.**

### 4.1 Feature flag rollback (fastest — per-caller)

Set `AETHER_PROXY_DISABLED=1` in the caller's environment and restart (or send SIGHUP
if hot-reload is supported). The caller short-circuits to direct HTTP before any Aether
SDK call is made.

This is the primary rollback mechanism. It requires no gateway changes and no sidecar
drain.

### 4.2 Per-language transport revert (one-liner each)

If you cannot use the feature flag, revert the transport adapter in code:

**Go** — replace `AetherRoundTripper` with a plain `http.Client`:
```go
// Before (Aether path):
// rt := &aether.AetherRoundTripper{Client: agentClient, Target: "sv::memorylayer::default"}
// httpClient := &http.Client{Transport: rt}

// After (direct HTTP revert):
httpClient := &http.Client{}
```

**Python — httpx** — replace `AetherHTTPXTransport` with the default transport:
```python
# Before (Aether path):
# transport = AetherHTTPXTransport(aether_client, "sv::memorylayer::default")
# http = httpx.Client(transport=transport)

# After (direct HTTP revert):
http = httpx.Client()
```

**Python — requests** — unmount the adapter:
```python
# Before (Aether path):
# session.mount("aether+sv://", AetherRequestsAdapter(aether_client))

# After (direct HTTP revert):
session = requests.Session()  # fresh session with no Aether adapter
```

**TypeScript** — stop using `AetherFetchTransport`:
```typescript
// Before (Aether path):
// const transport = new AetherFetchTransport(agentClient, "sv::memorylayer::default");
// const resp = await transport.fetch("/v1/memories/abc");

// After (direct HTTP revert):
const resp = await fetch("https://memorylayer.internal/v1/memories/abc");
```

### 4.3 Per-deployment revert (all callers)

1. Set `AETHER_PROXY_DISABLED=1` on all caller deployments via your config management
   layer (e.g., update the ConfigMap / Helm values / environment override and roll).
2. Confirm traffic is flowing to direct HTTP by checking that `proxy_http_routed` audit
   events stop arriving within 2 minutes.
3. Leave the terminator sidecar running but idle — do not terminate it until the cause
   of the rollback is diagnosed and resolved. This preserves the sidecar's gRPC
   connection for debugging.
4. Once the root cause is resolved and the sidecar is healthy, remove
   `AETHER_PROXY_DISABLED` and redeploy to resume canary.

### 4.4 Rollback decision authority

Any on-call engineer may initiate rollback unilaterally if:
- Any SLO in Section 2 is breached for more than 5 minutes, or
- Audit drift exceeds 5% (Section 3.2), or
- MemoryLayer returns a sustained error spike traceable to the Aether path.

---

## 5. Runbook Entries

### 5.1 Sidecar pod down

**Detection:**
- Redis lock for `sv::memorylayer::default` has expired (30 s TTL). The gateway's
  wildcard resolver will drop this instance from the candidate set once its lock TTL
  decays below 5 s.
- Audit events for `proxy_http_routed` stop or drop sharply.
- If using a specific (non-wildcard) target, callers receive
  `ProxyError{SIDECAR_UNAVAILABLE}`.

**Automatic mitigation (wildcard target):**
Callers using `sv::memorylayer` (wildcard) are automatically routed to other healthy
instances. No manual action is needed if at least one instance remains online.

**Manual drain (if needed):**
1. Cordon the sidecar pod from receiving new requests (e.g., remove it from the
   load-balancer target group or set `AETHER_PROXY_DISABLED=1` on callers pinned to
   that instance).
2. Allow in-flight requests to complete (the lock TTL is 30 s; wait at least 30 s after
   the pod stops receiving new traffic).
3. Restart or reschedule the sidecar pod.
4. Verify the new instance registers its lock in Redis and appears in the gateway's
   candidate set (check the admin UI or `proxy_http_routed` audit events resuming).

**When to page:** Page if all sidecar instances are offline simultaneously (see 5.2).

### 5.2 Wildcard target has zero healthy instances

**Symptom:** Every `proxy_http_routed` audit event is replaced by `proxy_http_failed`.
Callers receive `ProxyError{SIDECAR_UNAVAILABLE}`.

**Alert:** Fire an alert when the rate of `proxy_http_failed` with kind
`SIDECAR_UNAVAILABLE` exceeds 1% of proxy requests for more than 60 seconds.

**Failover path:**
1. Immediately set `AETHER_PROXY_DISABLED=1` on all caller deployments (Section 4.3).
   Callers fall back to direct HTTP within seconds.
2. Investigate why all sidecar instances are down (pod crash loop, connectivity loss,
   gateway auth failure).
3. Bring at least one sidecar instance online and verify it registers before removing
   `AETHER_PROXY_DISABLED`.

### 5.3 Auth header mismatch

**Symptom:** MemoryLayer returns HTTP 401 for requests arriving via the Aether sidecar
path. Direct-HTTP requests (bypassing Aether) succeed.

**Diagnosis:**
1. Check that the terminator sidecar's `header_mode` is set to `strict` (the default).
   In `strict` mode the sidecar strips all inbound `Authorization` / `X-Auth-*` headers
   and mints fresh ones from the `ProxyHttpRequest.authorization` field.
2. Verify the minted header values match what `identityheaders.Mint` produces. Both the
   sidecar terminator and the auth-proxy call the same `server/pkg/identityheaders`
   library. Run the OBO byte-equality test to confirm:
   ```bash
   cd server
   /home/drew/sdk/go1.25.5/bin/go test -count=1 -run TestPhase1_OBO_HeadersByteEqualToAuthProxy -tags=integration ./tests/integration/...
   ```
3. Check that the `AuthorizationContext` in the `ProxyHttpRequest` envelope is populated
   correctly by the caller's SDK (inspect via gateway debug logging at `log_level: debug`).
4. If the minted headers differ from the auth-proxy output, file a bug against
   `server/pkg/identityheaders`. Do not patch the sidecar independently — the library
   is the single source of truth.

**Resolution:** Fix the `AuthorizationContext` construction in the caller or the
`identityheaders` library, then re-run the integration test suite before re-enabling
the canary.

### 5.4 Stuck tunnel (pin not refreshing, lock TTL expired)

**Symptom:** A tunnel stops transferring data. `TunnelData` frames are sent but no
response arrives. The tunnel-side goroutines are still alive but blocked.

**Diagnosis:**
1. Check whether the target sidecar instance's Redis lock has expired. If the lock TTL
   decayed to zero (gateway crashed, network partition) the tunnel's Redis pin is
   stale.
2. In the gateway logs, look for `tunnel pin expired` or `tunnel closed: lock expired`
   log lines for the affected `tunnel_id`.
3. Check that `tunnel_closed` audit events were emitted for the tunnel.

**Resolution:**
1. Force-close the tunnel from the caller side by closing the Aether gRPC stream or
   calling `TunnelClose` from the SDK. The gateway will clean up the Redis pin on
   stream disconnect.
2. If the gateway itself is hung, restart the gateway instance. Its lock TTL (30 s)
   will expire automatically; the tunnel is torn down on lock expiry.
3. Reconnect and open a new tunnel.

**Prevention:** Ensure the gateway lock-refresh goroutine is not starved. Under extreme
CPU load, lock refresh (every 10 s against a 30 s TTL) may fall behind. Monitor the
`aether_redis_lock_refresh_errors_total` Prometheus counter.

### 5.5 Quota / rate-limit hit

**Symptom:** Callers receive `ProxyError{PAYLOAD_TOO_LARGE}` or tunnel opens fail.

**Relevant config knobs** (gateway YAML, `proxy` section):

| Config field | Default | What to adjust |
|---|---|---|
| `proxy.max_request_body_bytes` | 8 MiB | Cap on the inline body carried in the parent `ProxyHttpRequest` envelope. Streaming uploads via `ProxyHttpBodyChunk` are bounded instead by the per-backend `max_body_bytes` (default 10 MiB). |
| `proxy.max_concurrent_tunnels_per_workspace` | 256 | Increase if running many simultaneous TCP tunnels per workspace. |
| `proxy.max_tunnel_bytes` | 0 (unlimited) | Set a non-zero value to cap cumulative bytes per tunnel session. |

Adjust these in the gateway config and redeploy. No code changes are needed.

---

## 6. Sign-off Checklist

Do not flip MemoryLayer to aether-only traffic until every item below is checked.

- [ ] **Header-mint byte-equality verified in production canary.** Sample at least
  10,000 requests during the canary period. For each sampled request, compare the
  `X-Auth-*` headers received by MemoryLayer via the Aether sidecar path against the
  headers produced by a shadowed direct-HTTP call to the auth-proxy. Require 100% match.

- [ ] **SLOs met for 7 consecutive days at 100% canary.** All metrics in Section 2
  (p50/p99 latency, error rate, availability) are within target for the final 7-day
  canary window before the flip.

- [ ] **Audit reconciliation drift < 0.1% for 7 consecutive days.** The daily
  reconciliation defined in Section 3 shows ≥ 99.9% match between Aether audit events
  and MemoryLayer access log for 7 consecutive days at 100% canary.

- [ ] **Rollback procedure rehearsed at least once in staging.** Execute Section 4.3
  in the staging environment: set `AETHER_PROXY_DISABLED=1`, confirm traffic switches
  to direct HTTP within 2 minutes, then remove the flag and confirm Aether traffic
  resumes. Time the total round-trip; it should be under 5 minutes.

- [ ] **Service-side gRPC delivery validated with a real gateway.** The in-process
  harness confirmed routing primitives. Before the aether-only flip, run an
  end-to-end test with a real gateway, real RabbitMQ Streams, and a real Redis instance.
  This validates the full dispatch path including `TunnelData`/`TunnelClose` handling
  under real network conditions (see Section 7).

- [ ] **`proxy_path` and `tunnel_target` ACL scopes reviewed.** If any authority grants
  carry `proxy_path` or `tunnel_target` resource scopes, verify the patterns are correct
  before going aether-only: confirm that each grant's `proxy_path` patterns cover the
  HTTP methods and paths the caller actually uses, and that each `tunnel_target` pattern
  covers the protocol + remote-hint strings the caller sends. A misconfigured scope
  silently denies requests with `ACL_DENIED`; test in staging first.

- [ ] **All on-call runbook entries documented and drilled.** Every runbook in Section
  5 has been walked through in a tabletop exercise or staging drill. Participating
  engineers are familiar with the `AETHER_PROXY_DISABLED` flag and the sidecar drain
  procedure.

- [ ] **Monitoring in place.** Prometheus alerts are configured for:
  - `proxy_http_failed` rate exceeding the error-rate SLO
  - `SIDECAR_UNAVAILABLE` error kind sustained > 60 s
  - Audit reconciliation drift > 0.1%
  - Tunnel-open p99 exceeding the latency SLO

---

## 7. Known Limitations

### 7.1 Real-network load test pending

The in-process benchmark used an in-process harness (no real gRPC, no real RabbitMQ, no
real Redis). See [proxy-load-test-results.md](proxy-load-test-results.md) for the full
scope caveat. Measured tunnel-open latency (p50=20 ms, p99=38 ms) reflects the routing
layer only; real-network numbers will be higher.

**Limitation:** A wire-level load test against a deployed gateway + RMQ + Redis stack is
required as part of the sign-off checklist (item 5 above) and is not yet done.
