//go:build e2e

package integration_e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/aether/sdk/go/aether"
)

// =============================================================================
// Multi-caller scenarios.
//
// Regression coverage for the per-client inflight registry fix. The
// Aether Go SDK now scopes its proxy and tunnel inflight tables onto
// BaseClient (see BaseClient.proxyInflights / BaseClient.tunnelInflights
// in sdk/go/aether/client.go) so that two BaseClients in the same
// process do not collide on request_id / tunnel_id slots. Prior to the
// fix the tables were package-globals keyed by NextRequestID() which
// restarts at "req-1" per client, so the second caller's registration
// silently overwrote the first.
//
// The four tests below exercise the multi-caller paths that used to
// regress: two callers fanning out fast non-streaming calls, one slow
// streaming caller alongside fast non-streaming callers, two callers
// running independent TCP tunnels, and N-caller fan-out.
// =============================================================================

// TestE2E_TwoCallers_DistinctRequestIDs_NoCollision validates that two
// independently-dialled AgentClients in the same process can fire
// concurrent ProxyHTTP calls without the responses being misrouted.
//
// Regression coverage for the per-client inflight registry fix: prior
// to that fix, both callers register "req-1..req-50" in a single shared
// sync.Map and the second registration evicts the first.
func TestE2E_TwoCallers_DistinctRequestIDs_NoCollision(t *testing.T) {
	const perCaller = 50

	h := NewE2EHarness(t)
	a := dialAgentClient(t, h, "multicaller-a")
	b := dialAgentClient(t, h, "multicaller-b")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		wg      sync.WaitGroup
		aOK     atomic.Int32
		bOK     atomic.Int32
		aErrors atomic.Int32
		bErrors atomic.Int32
	)

	for i := 0; i < perCaller; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := driveFast(ctx, a, h.ServiceTopic, "/fast", 10*time.Second); err != nil {
				aErrors.Add(1)
			} else {
				aOK.Add(1)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := driveFast(ctx, b, h.ServiceTopic, "/fast", 10*time.Second); err != nil {
				bErrors.Add(1)
			} else {
				bOK.Add(1)
			}
		}()
	}
	wg.Wait()

	if aOK.Load() != int32(perCaller) || aErrors.Load() != 0 {
		t.Errorf("caller-a: %d ok / %d errors (want %d/0)", aOK.Load(), aErrors.Load(), perCaller)
	}
	if bOK.Load() != int32(perCaller) || bErrors.Load() != 0 {
		t.Errorf("caller-b: %d ok / %d errors (want %d/0)", bOK.Load(), bErrors.Load(), perCaller)
	}
}

// TestE2E_TwoCallers_OneSlowOneFastCalls validates that caller B's fast
// calls stay fast while caller A is mid-SSE-stream.
//
// Regression coverage for the per-client inflight registry fix: prior
// to that fix, the slow caller's "req-1" streaming inflight collided
// with the fast caller's "req-1" non-streaming inflight in a shared
// sync.Map. The fast caller's response delete-on-completion evicted
// the slow caller's slot, orphaning the slow stream.
func TestE2E_TwoCallers_OneSlowOneFastCalls(t *testing.T) {
	const fastCalls = 10

	h := NewE2EHarness(t)
	slowCaller := dialAgentClient(t, h, "multicaller-slow")
	fastCaller := dialAgentClient(t, h, "multicaller-fast")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Slow caller opens a streaming /slow request that drips for the
	// harness's default SlowStreamDuration (5s). We expect to receive at
	// least one chunk before EOF.
	slowDone := make(chan error, 1)
	go func() {
		chunks, err := driveStream(ctx, slowCaller, h.ServiceTopic, "/slow", 15*time.Second)
		if err != nil {
			slowDone <- fmt.Errorf("driveStream: %w", err)
			return
		}
		if chunks == 0 {
			slowDone <- fmt.Errorf("driveStream returned 0 chunks (stream was orphaned)")
			return
		}
		slowDone <- nil
	}()

	// Give the slow streaming inflight time to register on the SDK side
	// before the fast caller starts firing.
	time.Sleep(100 * time.Millisecond)

	var (
		wg          sync.WaitGroup
		fastOK      atomic.Int32
		fastErrors  atomic.Int32
		fastErrLast atomic.Value // error
	)
	for i := 0; i < fastCalls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := driveFast(ctx, fastCaller, h.ServiceTopic, "/fast", 10*time.Second); err != nil {
				fastErrors.Add(1)
				fastErrLast.Store(err)
				return
			}
			fastOK.Add(1)
		}()
	}
	wg.Wait()

	if got := fastOK.Load(); got != int32(fastCalls) {
		var lastErr error
		if v := fastErrLast.Load(); v != nil {
			lastErr = v.(error)
		}
		t.Errorf("fast caller: %d/%d ok, %d errors (last err: %v)", got, fastCalls, fastErrors.Load(), lastErr)
	}

	select {
	case err := <-slowDone:
		if err != nil {
			t.Errorf("slow streaming caller failed: %v", err)
		}
	case <-ctx.Done():
		t.Errorf("slow streaming caller did not complete within ctx deadline")
	}
}

// TestE2E_TwoCallers_DistinctTunnels_NoMixup validates two
// simultaneous TCP tunnels from independent callers don't cross-talk.
//
// Regression coverage for the per-client tunnel registry fix: prior
// to that fix, caller A's tunnel_id "req-1" collided with caller B's
// tunnel_id "req-1" in a shared sync.Map; the second TunnelDial
// overwrote the first's slot and inbound TunnelData was misrouted.
func TestE2E_TwoCallers_DistinctTunnels_NoMixup(t *testing.T) {
	const payloadSize = 4096

	h := NewE2EHarness(t)
	a := dialAgentClient(t, h, "tun-a")
	b := dialAgentClient(t, h, "tun-b")

	payloadA := make([]byte, payloadSize)
	payloadB := make([]byte, payloadSize)
	if _, err := rand.Read(payloadA); err != nil {
		t.Fatalf("rand A: %v", err)
	}
	if _, err := rand.Read(payloadB); err != nil {
		t.Fatalf("rand B: %v", err)
	}
	if bytes.Equal(payloadA, payloadB) {
		t.Fatal("random payloads collided")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	connA, err := a.TunnelDial(ctx, h.ServiceTopic, "tcp", h.TCPBackendAddr,
		aether.WithTunnelBackend("tcp-echo"))
	if err != nil {
		t.Fatalf("TunnelDial A: %v", err)
	}
	defer connA.Close()
	connB, err := b.TunnelDial(ctx, h.ServiceTopic, "tcp", h.TCPBackendAddr,
		aether.WithTunnelBackend("tcp-echo"))
	if err != nil {
		t.Fatalf("TunnelDial B: %v", err)
	}
	defer connB.Close()

	time.Sleep(500 * time.Millisecond)
	_ = connA.SetDeadline(time.Now().Add(8 * time.Second))
	_ = connB.SetDeadline(time.Now().Add(8 * time.Second))

	var wg sync.WaitGroup
	var aMismatch, bMismatch atomic.Int32

	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, err := connA.Write(payloadA); err != nil {
			t.Errorf("connA Write: %v", err)
			return
		}
		got, err := readN(connA, payloadSize)
		if err != nil {
			t.Errorf("connA Read: %v (got %d bytes)", err, len(got))
			return
		}
		if !bytes.Equal(got, payloadA) {
			aMismatch.Add(1)
			t.Errorf("connA payload mismatch: first 16 got=%s want=%s",
				hex.EncodeToString(got[:16]), hex.EncodeToString(payloadA[:16]))
		}
	}()
	go func() {
		defer wg.Done()
		if _, err := connB.Write(payloadB); err != nil {
			t.Errorf("connB Write: %v", err)
			return
		}
		got, err := readN(connB, payloadSize)
		if err != nil {
			t.Errorf("connB Read: %v (got %d bytes)", err, len(got))
			return
		}
		if !bytes.Equal(got, payloadB) {
			bMismatch.Add(1)
			t.Errorf("connB payload mismatch: first 16 got=%s want=%s",
				hex.EncodeToString(got[:16]), hex.EncodeToString(payloadB[:16]))
		}
	}()
	wg.Wait()

	if aMismatch.Load() != 0 || bMismatch.Load() != 0 {
		t.Fatalf("tunnel cross-talk detected: aMismatch=%d bMismatch=%d",
			aMismatch.Load(), bMismatch.Load())
	}
}

// TestE2E_NCallers_Fanout_RoundtripIntegrity validates N callers ×
// K calls each all succeed with per-caller bookkeeping.
//
// Regression coverage for the per-client inflight registry fix: prior
// to that fix, 5 callers all registering "req-1..req-K" in a shared
// sync.Map evicted each other and callers 2-4 typically saw 0
// successful responses.
func TestE2E_NCallers_Fanout_RoundtripIntegrity(t *testing.T) {
	const (
		callers   = 5
		perCaller = 20
	)

	h := NewE2EHarness(t)

	clients := make([]*aether.AgentClient, callers)
	for i := 0; i < callers; i++ {
		clients[i] = dialAgentClient(t, h, fmt.Sprintf("fanout-%d", i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	results := make([]atomic.Int32, callers)
	errsByCaller := make([]atomic.Int32, callers)

	for i := 0; i < callers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < perCaller; k++ {
				if _, err := driveFast(ctx, clients[i], h.ServiceTopic, "/fast", 10*time.Second); err != nil {
					errsByCaller[i].Add(1)
					continue
				}
				results[i].Add(1)
			}
		}()
	}
	wg.Wait()

	total := int32(0)
	for i := 0; i < callers; i++ {
		got := results[i].Load()
		errs := errsByCaller[i].Load()
		total += got
		if got != int32(perCaller) {
			t.Errorf("caller %d: %d successful (want %d), %d errors", i, got, perCaller, errs)
		}
	}
	wantTotal := int32(callers * perCaller)
	if total != wantTotal {
		t.Errorf("total successes %d, want %d", total, wantTotal)
	}
}

// readN fills a buffer of length n from conn or returns the bytes read
// so far alongside an error. Kept available for the post-fix runnable
// version of TestE2E_TwoCallers_DistinctTunnels_NoMixup.
func readN(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	got := 0
	for got < n {
		k, err := r.Read(buf[got:])
		if k > 0 {
			got += k
		}
		if err != nil {
			if err == io.EOF && got == n {
				return buf, nil
			}
			return buf[:got], err
		}
	}
	return buf, nil
}
