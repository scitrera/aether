# Aether Proxy — Routing-Layer Benchmark Results

These numbers come from an in-process benchmark of the gateway's proxy and
tunnel routing primitives. See [Scope and Limitations](#scope-and-limitations)
before drawing production conclusions.

## REST Proxy Throughput

| Scenario                      | p50 | p99 | Notes                                                 |
|-------------------------------|-----|-----|-------------------------------------------------------|
| GET, small body (< 1 KB)      | —   | —   | Not benchmarked separately; dominated by gRPC framing |
| POST, 256 KB body (chunked)   | —   | —   | Not benchmarked separately                            |
| 100 concurrent proxy requests | —   | —   | Not benchmarked in this run                           |

REST proxy benchmark numbers are not yet available from the in-process harness.
The integration tests in `server/tests/integration/` confirm correctness; a
throughput benchmark is pending.

## Tunnel Routing

| Scenario                                      | p50   | p99   | Concurrency    |
|-----------------------------------------------|-------|-------|----------------|
| Tunnel open (wildcard resolution + Redis pin) | 20 ms | 38 ms | 100 concurrent |

These numbers reflect the routing layer only (wildcard resolution, Redis pin
write, ACL check). They do not include real gRPC stream overhead, real RabbitMQ
dispatch, or real Redis network latency.

## Scope and Limitations

**Limitation:** The benchmark used an in-process harness with no real network,
no real gRPC transport, no real RabbitMQ Streams, and no real Redis. All I/O
was simulated in-process.

Measured values represent a lower bound on routing-layer overhead, not
end-to-end latency in a deployed system. Real-network numbers will be higher
depending on:

- RTT between caller and gateway (typically 1–10 ms in same-AZ deployments)
- Redis round-trip time for lock scan and pin write (typically 1–5 ms)
- gRPC stream serialization overhead

**Future work:** A wire-level load test against a deployed gateway + RabbitMQ
Streams + Redis stack is required before production sign-off. See the
[cutover checklist](proxy-cutover.md#6-sign-off-checklist), item 5.

## Related Documents

- [proxy.md](proxy.md) — feature overview, limits, audit events
- [proxy-quickstart.md](proxy-quickstart.md) — running the integration test suite
- [proxy-cutover.md](proxy-cutover.md) — production SLOs and rollout criteria

## Wire-level results

Numbers below are produced by the harness at `server/cmd/loadtest/proxy`,
driven by `server/scripts/run_proxy_load_test.sh`. Each run brings up its
own ephemeral RabbitMQ Streams + Valkey containers (on dedicated ports so
they don't clobber an operator-managed dev stack), starts the Aether
gateway in dev mode and two proxy-sidecar terminators (`sv::echo::a` and
`sv::echo::b`) over local TCP / HTTP echo backends, then drives 100
concurrent caller agents through the Go SDK for 60 s of steady-state
traffic. Soft assertions (p99 ≤ 250 ms tunnel-open, p99 ≤ 500 ms REST,
goroutine-leak delta) print "WARN" but never fail the run — the harness
is observational by design.

To run:

```
# One-shot run — starts/stops its own RMQ + Valkey:
bash server/scripts/run_proxy_load_test.sh

# Build only, skip infra/run (CI sanity):
bash server/scripts/run_proxy_load_test.sh --check

# Reuse an already-running RMQ/Valkey on the configured ports:
bash server/scripts/run_proxy_load_test.sh --no-infra-up

# Override defaults via env vars:
CALLERS=200 DURATION=120s TARGET_MIBPS=2.0 \
  bash server/scripts/run_proxy_load_test.sh

# Override test-only ports if 56552 / 56380 are in use:
RMQ_STREAM_PORT=57552 REDIS_PORT=57380 \
  bash server/scripts/run_proxy_load_test.sh
```

Each successful run appends a new "Wire-level results (run …)" section
below. The first row of every run records the host, Go toolchain, and any
soft-assertion warnings so future regressions are easy to spot.

**Operator note (placeholder section).** First-run production-like
measurements have not yet been captured here. The harness builds cleanly
(`go build ./...` and `--check` mode both pass), boots the gateway plus
two sidecars plus its own RMQ + Valkey containers, and the gateway
authenticates and registers `sv::echo::a` and `sv::echo::b` as service
principals over the wire.

During smoke validation on the build host the wire path showed an
end-to-end stall: agents connected and ProxyHTTP / TunnelDial calls
went out, but proxy responses did not arrive within the 5 s per-call
deadline (REST returned `context deadline exceeded`; tunnels opened in
microseconds but echo round-trips timed out). Sidecar logs show the
service principal reconnecting with a fresh consumer (`no stored
offset, starting from next message`), suggesting in-flight stream frames
are being skipped during sidecar reconnect churn against an
ephemerally-restarted RabbitMQ Streams broker. This is plausibly a
sidecar reconnect / RMQ stored-offset issue rather than a harness
defect — the SDK send queue is delivering, the gateway routing is
unchanged, and the auth-proxy regression suite still passes
(`go test ./internal/authproxy/...` → `ok`).

When the operator runs the script on a deployed gateway with a warmed
RMQ instance — or after the sidecar reconnect tuning lands — the
harness will append a populated "Wire-level results (run …)" section
with measured numbers. Until then, treat the configuration block above
as the test plan and the in-process numbers earlier in this document as
the only validated latency baseline.

