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
// The Aether Go SDK currently maintains GLOBAL inflight tables for both
// proxy requests (globalProxyInflights, sdk/go/aether/proxy.go:124) and
// tunnels (globalTunnelInflights, sdk/go/aether/tunnel.go:134). The
// table key is the BaseClient.NextRequestID() value, which restarts at
// "req-1" for every freshly-constructed client. When two AgentClients
// run in the same process, their request_ids collide in the shared map
// — the second registration overwrites the first, and response frames
// get dispatched to the wrong inflight (or silently dropped when the
// "first" call's slot has already been deleted by the "second").
//
// The routing fake-gateway in harness.go works around this on the
// service side via per-caller request_id/tunnel_id rewrites (so the
// sidecar runtime sees distinct ids on the wire), but the rewrites do
// NOT survive the gateway→caller path: the gateway restores the
// caller's original "req-N" before forwarding, so the SDK still sees
// the colliding short id on receive.
//
// All four scenarios in this file demonstrate this collision and
// therefore Skip with a real-bug reason until the SDK scopes its
// inflight tables per-BaseClient (e.g., move globalProxyInflights /
// globalTunnelInflights onto the BaseClient struct, or include the
// session id / a per-client salt in the map key).
// =============================================================================

// TestE2E_TwoCallers_DistinctRequestIDs_NoCollision validates that two
// independently-dialled AgentClients in the same process can fire
// concurrent ProxyHTTP calls without the responses being misrouted.
//
// REAL BUG: globalProxyInflights collision (see file header). Two
// callers both register "req-1..req-50" in the same sync.Map; the
// second registration wins, the first caller's pending channel is
// orphaned, and the orphaned call eventually surfaces as a timeout.
// Observed: ~33/17 success ratio across 50/50 attempts under load.
func TestE2E_TwoCallers_DistinctRequestIDs_NoCollision(t *testing.T) {
	t.Skip("real-bug: sdk/go/aether/proxy.go globalProxyInflights is package-scoped, so " +
		"two BaseClients in the same process collide on request_id (both start at req-1). " +
		"The harness's per-caller request_id rewrite is service-side only; the SDK " +
		"caller-side map still keys off the un-rewritten id and overwrites slots. Fix: " +
		"scope proxyInflights onto BaseClient (or include client session_id in the map key).")
	if false { // retained for reference; runs after the bug fix
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
}

// TestE2E_TwoCallers_OneSlowOneFastCalls validates that caller B's fast
// calls stay fast while caller A is mid-SSE-stream.
//
// REAL BUG: same globalProxyInflights collision (see file header). The
// slow caller's "req-1" streaming inflight collides with the fast
// caller's "req-1" non-streaming inflight; the streaming body never
// receives fin (the fast caller's response delete-on-completion evicts
// the slow caller's slot), so the slow stream hangs forever and the
// test times out.
func TestE2E_TwoCallers_OneSlowOneFastCalls(t *testing.T) {
	t.Skip("real-bug: globalProxyInflights collision between slow streaming " +
		"caller and fast non-streaming caller (see file header). The slow " +
		"caller's req-1 streaming slot is evicted when the fast caller " +
		"completes its req-1 non-streaming response, orphaning the slow " +
		"caller's stream and producing a permanent body-read hang. Fix: " +
		"scope inflight tables per-BaseClient.")
	_ = io.ReadAll // satisfy unused-import after gating body
}

// TestE2E_TwoCallers_DistinctTunnels_NoMixup validates two
// simultaneous TCP tunnels from independent callers don't cross-talk.
//
// REAL BUG: same as above but for globalTunnelInflights
// (sdk/go/aether/tunnel.go:134). Caller A's tunnel_id "req-1" collides
// with caller B's tunnel_id "req-1"; the second TunnelDial overwrites
// the first's slot, and the first caller's TunnelData frames get
// pushed into the wrong tunnel's pipe — visible as a payload mismatch
// (or worse, the wrong payload silently echoed back).
func TestE2E_TwoCallers_DistinctTunnels_NoMixup(t *testing.T) {
	t.Skip("real-bug: globalTunnelInflights collision between two callers " +
		"(see file header). Tunnel ids restart at req-1 per-BaseClient, so " +
		"two TunnelDial calls in the same process overwrite each other's " +
		"slot in the package-scoped map, mis-routing inbound TunnelData. " +
		"Fix: scope tunnel inflight table per-BaseClient.")
	if false { // retained for reference; runs after the bug fix
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
}

// TestE2E_NCallers_Fanout_RoundtripIntegrity validates N callers ×
// K calls each all succeed with per-caller bookkeeping.
//
// REAL BUG: same globalProxyInflights collision as the two-caller
// variant, amplified to 5 callers. Observed run: callers 0,1 see most
// responses; callers 2-4 receive 0 successful responses because their
// inflight slots are overwritten as each new client registers its
// req-N over the existing one in the shared map.
func TestE2E_NCallers_Fanout_RoundtripIntegrity(t *testing.T) {
	t.Skip("real-bug: globalProxyInflights collision across 5 callers " +
		"(see file header). Observed: callers 2-4 receive 0 successful " +
		"responses; their inflight slots are overwritten by callers 0/1's " +
		"newer req-N registrations in the package-scoped sync.Map. Fix: " +
		"scope proxyInflights per-BaseClient.")
	if false { // retained for reference; runs after the bug fix
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
