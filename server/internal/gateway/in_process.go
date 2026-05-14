// Package gateway in-process trust marker for embedded callers.
//
// AetherLite runs the gateway and the workflow engine in the same process.
// Wiring TLS over a loopback handshake there is pure overhead (no security
// gain — the bytes never leave the process), and forgetting to wire it at
// all causes the workflow engine to fail handshake against an mTLS-required
// gateway and enter a reconnect loop.
//
// The fix is to expose a second gRPC listener backed by a memory-only
// `google.golang.org/grpc/test/bufconn` connection, register the SAME
// gateway service implementation on it, and tag incoming requests with an
// `inProcessConnKey{}` context value. The auth path treats these as
// "trust the InitConnection-claimed identity" — the same trust shape as
// the existing anonymous-mTLS path documented in auth_handler.go around
// the "anonymous mTLS certificate detected (transport-only)" log line.
//
// This file owns the trust marker and the unary/stream interceptors that
// apply it. cmd/aetherlite/main.go wires the bufconn listener and starts
// the second grpc.Server; internal/gateway/auth_handler.go honors the
// marker. msgbridge would reuse the same primitives if/when it gets the
// same embedded-vs-network treatment — currently out of scope.

package gateway

import (
	"context"

	"google.golang.org/grpc"
)

// inProcessConnKey is an unexported context key used to tag requests that
// arrived on the in-process bufconn listener. Unexported so only the gateway
// package can set/read it.
type inProcessConnKey struct{}

// IsInProcessConn reports whether the request arrived via the in-process
// gRPC listener (bufconn) rather than a network TLS connection.
//
// Used by authenticateCredentials and the connect path to trust the
// client's InitConnection-claimed identity, mirroring the anonymous-mTLS
// trust model.
//
// Trust rationale: bufconn connections never leave the process. They are
// in the same trust domain as direct Go calls. Identity is still explicit,
// audited, and validated by InitConnection's identity_type/workspace/etc —
// we just skip the per-handshake transport-cert overhead.
func IsInProcessConn(ctx context.Context) bool {
	v, _ := ctx.Value(inProcessConnKey{}).(bool)
	return v
}

// InProcessUnaryInterceptor marks unary RPC contexts as in-process.
// Install on the in-process grpc.Server only.
func InProcessUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return handler(context.WithValue(ctx, inProcessConnKey{}, true), req)
}

// InProcessStreamInterceptor marks stream RPC contexts as in-process.
// Install on the in-process grpc.Server only.
func InProcessStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, &inProcessStreamWrapper{ServerStream: ss, ctx: context.WithValue(ss.Context(), inProcessConnKey{}, true)})
}

// inProcessStreamWrapper overrides Context() so the wrapped value
// propagates to per-request handlers (Connect uses stream.Context() to
// drive the session lifetime).
type inProcessStreamWrapper struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *inProcessStreamWrapper) Context() context.Context { return w.ctx }
