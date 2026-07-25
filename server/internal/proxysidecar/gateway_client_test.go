package proxysidecar

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/aether/sdk/go/aether"
)

// fakeConn is a programmable gatewayConn for driving runConnectionLoop without
// a live gateway. runFn receives the 1-based attempt count.
type fakeConn struct {
	mu       sync.Mutex
	attempts int
	connErr  error
	runFn    func(ctx context.Context, attempt int) error
}

func (f *fakeConn) Connect(_ context.Context) error { return f.connErr }

func (f *fakeConn) Run(ctx context.Context) error {
	f.mu.Lock()
	f.attempts++
	attempt := f.attempts
	fn := f.runFn
	f.mu.Unlock()
	return fn(ctx, attempt)
}

func (f *fakeConn) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

// newTestRuntime builds a runtime wired to a fake conn with fast, deterministic
// backoff and a task-token file the loop can refresh from.
func newTestRuntime(t *testing.T, tokenPath string, conn gatewayConn) *gatewayRuntime {
	t.Helper()
	rt := newGatewayRuntime(&Config{
		Gateway: GatewayConfig{TaskTokenPath: tokenPath, Insecure: true},
	})
	rt.connOverride = conn
	rt.creds = aether.NewCredentials()
	rt.initialBackoff = time.Millisecond
	rt.maxBackoff = 2 * time.Millisecond
	rt.stableThreshold = 50 * time.Millisecond
	rt.maxTerminalFailures = 3
	return rt
}

func writeToken(t *testing.T, dir, val string) string {
	t.Helper()
	p := filepath.Join(dir, "token")
	if err := os.WriteFile(p, []byte(val), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	return p
}

// A dead token retried forever is the incident: the loop must escalate to a
// fatal give-up after maxTerminalFailures rather than spin. Because Connect()
// always "succeeds" (the transport opens) and Run() fails fast, this also
// proves the backoff/terminal-streak no longer resets on Connect success.
func TestRunConnectionLoop_GivesUpAfterTerminalFailures(t *testing.T) {
	dir := t.TempDir()
	tokenPath := writeToken(t, dir, "dead-token")

	conn := &fakeConn{
		runFn: func(_ context.Context, _ int) error {
			return aether.NewAuthenticationError("token not found")
		},
	}
	rt := newTestRuntime(t, tokenPath, conn)

	err := rt.runConnectionLoop(context.Background())
	if err == nil {
		t.Fatal("expected fatal give-up error, got nil")
	}
	if got := conn.count(); got != rt.maxTerminalFailures {
		t.Fatalf("expected exactly %d attempts before give-up, got %d", rt.maxTerminalFailures, got)
	}
	// Re-establishment was attempted (creds re-read from the path).
	if rt.creds["token"] != "dead-token" {
		t.Fatalf("expected creds refreshed from path, got %q", rt.creds["token"])
	}
}

// Recoverable errors that escape Run() (rare, since AutoReconnect handles most
// internally) must NOT count toward the terminal give-up — they back off and
// retry the same credential indefinitely until ctx cancellation.
func TestRunConnectionLoop_TransientErrorsDoNotGiveUp(t *testing.T) {
	dir := t.TempDir()
	tokenPath := writeToken(t, dir, "live-token")

	conn := &fakeConn{
		runFn: func(_ context.Context, _ int) error {
			return errors.New("connection reset by peer") // recoverable (no auth/grpc class)
		},
	}
	rt := newTestRuntime(t, tokenPath, conn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	if err := rt.runConnectionLoop(ctx); err != nil {
		t.Fatalf("transient errors must not produce a fatal give-up, got %v", err)
	}
	if conn.count() < rt.maxTerminalFailures {
		t.Fatalf("expected the loop to retry past the terminal cap (%d), only saw %d attempts",
			rt.maxTerminalFailures, conn.count())
	}
}

// When an external pairing helper rewrites the token file, the next reconnect
// must present the fresh credential and recover — no fatal give-up.
func TestRunConnectionLoop_ReestablishesCredentialsAndRecovers(t *testing.T) {
	dir := t.TempDir()
	tokenPath := writeToken(t, dir, "dead-token")

	var conn *fakeConn
	rt := newTestRuntime(t, tokenPath, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn = &fakeConn{
		runFn: func(rctx context.Context, attempt int) error {
			// Simulate the operator re-pairing after the first terminal fail.
			if attempt == 1 {
				writeToken(t, dir, "fresh-token")
				return aether.NewAuthenticationError("token not found")
			}
			// Fresh token accepted: stay "connected" until shutdown, long
			// enough to clear the stability threshold.
			<-rctx.Done()
			return rctx.Err()
		},
	}
	rt.connOverride = conn

	done := make(chan error, 1)
	go func() { done <- rt.runConnectionLoop(ctx) }()

	// Give the loop time to fail once, refresh, reconnect, and settle.
	time.Sleep(150 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("expected clean exit after recovery, got %v", err)
	}
	if rt.creds["token"] != "fresh-token" {
		t.Fatalf("expected credential re-established to fresh-token, got %q", rt.creds["token"])
	}
}

func TestNextBackoff(t *testing.T) {
	const max = 30 * time.Second
	if got := nextBackoff(time.Second, max, 2.0); got != 2*time.Second {
		t.Fatalf("nextBackoff(1s) = %v, want 2s", got)
	}
	if got := nextBackoff(20*time.Second, max, 2.0); got != max {
		t.Fatalf("nextBackoff(20s) capped = %v, want %v", got, max)
	}
	if got := nextBackoff(max, max, 2.0); got != max {
		t.Fatalf("nextBackoff at cap = %v, want %v", got, max)
	}
}

func TestBackoffWithJitter_StaysInBand(t *testing.T) {
	base := 4 * time.Second
	for i := 0; i < 1000; i++ {
		got := backoffWithJitter(base)
		if got < time.Duration(0.75*float64(base)) || got > time.Duration(1.25*float64(base)) {
			t.Fatalf("jittered backoff %v outside ±25%% of %v", got, base)
		}
	}
}
