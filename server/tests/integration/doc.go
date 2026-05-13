// Package integration contains end-to-end integration tests for Aether's
// Phase 1 proxy/sidecar pipeline.
//
// All tests in this package are gated behind the `integration` build tag so
// they do not run as part of the normal `go test ./...` invocation. To run
// them:
//
//	cd server
//	/home/drew/sdk/go1.25.5/bin/go test -tags=integration ./tests/integration/...
//
// or
//
//	cd server
//	/home/drew/sdk/go1.25.5/bin/go test -tags=integration -v ./tests/integration/ -run TestPhase1
//
// The tests intentionally avoid external infrastructure (no RabbitMQ, Redis,
// or Postgres). They exercise the full proxy data path by driving the real
// proxysidecar.Terminator code (which mints X-Auth-* headers via
// pkg/identityheaders, the same minter the auth-proxy uses) through an
// in-process router that mimics the gateway's wildcard fan-out and ACL deny
// semantics.
//
// What is covered (per task #5 brief):
//
//   - GET / POST happy path: round-trip headers, status codes, body sizes
//     (including chunked > 256 KB), preserve content-type and query string.
//   - ACL deny path: caller without grant -> ProxyError{ACL_DENIED}.
//   - OBO header injection: caller emits AuthorizationContext with
//     authority_mode=on_behalf_of -> backend sees X-Auth-* headers byte-equal
//     to what the auth-proxy/identityheaders.Mint produces for the same grant.
//   - Wildcard sv::{impl}: TWO sidecar instances connected; sample of N >= 30
//     requests to bare sv::memorylayer hits both; taking one offline routes
//     to remaining; bringing it back -> both reachable.
//   - Idle timeout on the proxy side (sidecar config idle_timeout_ms).
//   - Sidecar unavailable: target with no instances -> ProxyError{SIDECAR_UNAVAILABLE}.
package integration
