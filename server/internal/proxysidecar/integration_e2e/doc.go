// Package integration_e2e exercises the proxy-sidecar composite-mode
// multiplexer + backpressure code end-to-end against a real Aether SDK
// client.
//
// These tests stand up a fully in-process stack:
//
//   - a routing fake-gateway (gRPC over an ephemeral TCP listener) that
//     receives Connect streams from both the sidecar runtime (as a
//     Service principal) and from caller-side SDK clients (as Agent
//     principals), and routes proxy / tunnel envelopes between them
//     by request_id / tunnel_id;
//   - a proxy-sidecar Runner in composite mode (terminator + relay)
//     attached to the fake gateway;
//   - a tiny in-process HTTP backend (with /slow, /fast, and /echo
//     handlers) that the terminator forwards to;
//   - a tiny in-process TCP echo server that the tunnel test opens
//     against the terminator.
//
// All listeners bind to 127.0.0.1:0 (ephemeral ports). Multiple
// invocations on the same host therefore do not collide and the tests
// are safe to run in parallel.
//
// The harness is the Go replacement for
// backend/.slop/test_bp_via_sidecar_alt.py which lived in the platform
// monorepo. Putting it here keeps the validation co-located with the
// sidecar code it exercises (closes the gap that python testing across
// repositories left open) and lets us pin the load shapes via Go test
// flags rather than env vars.
//
// Build tag: every *_test.go file in this package is gated behind
// //go:build e2e so the heavy fanout/timing scenarios stay out of the
// default `go test ./...` run. To execute them:
//
//	cd server
//	go test -mod=mod -tags=e2e -count=1 -timeout 600s \
//	    ./internal/proxysidecar/integration_e2e/...
//
// Each test targets <30s wall time so the package-level 600s timeout
// has substantial headroom for slow CI runners.
package integration_e2e
