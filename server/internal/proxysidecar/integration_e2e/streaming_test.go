//go:build e2e

package integration_e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/aether/sdk/go/aether"
)

// TestE2E_StreamsPlusFastCalls_StayFast is the Go port of the original
// Python bp loadtest. Across the fanouts {1, 2, 5, 10, 20} it opens N
// concurrent streaming /slow requests through the composite-mode
// sidecar and concurrently fires batches of 5 /fast calls every 100ms.
//
// Pass criteria:
//
//   - every /slow stream completes with at least one chunk;
//   - every /fast call returns 200 with no error;
//   - the /fast call p99 stays ≤ 50ms across the whole batch.
//
// The 50ms ceiling is comfortably above the locally-observed steady-
// state (~5-10ms) but well below the broken behaviour where fast calls
// would back up behind the streaming dispatch loop and saturate at the
// 15s SDK timeout.
func TestE2E_StreamsPlusFastCalls_StayFast(t *testing.T) {
	// Subtests run serially: each one spins up a full sidecar runtime
	// + routing fake gateway + agent client + N concurrent streams,
	// and the 5-second slow-stream drip plus per-fanout warm-up means
	// the test wall time is dominated by fanout count, not by SDK
	// throughput. Running them in parallel saturates the runner's
	// receive goroutine across tests and pushes the package over the
	// 600s timeout on CI hardware. Serial keeps the package at the
	// fanouts × 5-10s budget, which is well under the package timeout.
	fanouts := []int{1, 2, 5, 10, 20}
	for _, fanout := range fanouts {
		fanout := fanout
		t.Run(fmt.Sprintf("fanout=%d", fanout), func(t *testing.T) {
			runStreamsPlusFastCalls(t, fanout)
		})
	}
}

func runStreamsPlusFastCalls(t *testing.T, fanout int) {
	t.Helper()

	const (
		slowDur      = 5 * time.Second
		fastInterval = 100 * time.Millisecond
		fastBatch    = 5
		p99Budget    = 50 * time.Millisecond
	)

	h := NewE2EHarness(t, E2EHarnessOptions{SlowStreamDuration: slowDur})
	client := dialAgentClient(t, h, fmt.Sprintf("streams-%d", fanout))

	rootCtx, rootCancel := context.WithTimeout(context.Background(), slowDur+15*time.Second)
	defer rootCancel()

	var (
		wg            sync.WaitGroup
		streamDone    atomic.Int32
		streamErr     atomic.Int32
		streamChunks  atomic.Int64
		fastLatencies = make([]time.Duration, 0, 1024)
		fastMu        sync.Mutex
		fastErrors    atomic.Int32
		fastDone      atomic.Int32
	)

	// Streaming workers.
	for i := 0; i < fanout; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			chunks, err := driveStream(rootCtx, client, h.ServiceTopic, "/slow", slowDur+5*time.Second)
			if err != nil {
				streamErr.Add(1)
				t.Logf("stream %d error: %v", i, err)
				return
			}
			streamChunks.Add(int64(chunks))
			streamDone.Add(1)
		}(i)
	}

	// Fast call worker — terminates when streams are done.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(fastInterval)
		defer ticker.Stop()
		for {
			select {
			case <-rootCtx.Done():
				return
			case <-ticker.C:
				if streamDone.Load()+streamErr.Load() >= int32(fanout) {
					return
				}
				for i := 0; i < fastBatch; i++ {
					dur, err := driveFast(rootCtx, client, h.ServiceTopic, "/fast", 5*time.Second)
					fastDone.Add(1)
					if err != nil {
						fastErrors.Add(1)
						continue
					}
					fastMu.Lock()
					fastLatencies = append(fastLatencies, dur)
					fastMu.Unlock()
				}
			}
		}
	}()

	wg.Wait()

	// Assertions.
	if int(streamDone.Load()) != fanout {
		t.Errorf("streams: %d completed of %d (errors=%d)",
			streamDone.Load(), fanout, streamErr.Load())
	}
	if streamChunks.Load() < int64(fanout) {
		t.Errorf("streams: only %d total chunks observed across %d streams",
			streamChunks.Load(), fanout)
	}
	if fastDone.Load() == 0 {
		t.Fatalf("fast calls: none observed — pacing bug?")
	}
	if fastErrors.Load() > 0 {
		t.Errorf("fast calls: %d errors / %d attempts", fastErrors.Load(), fastDone.Load())
	}

	p50, p95, p99 := percentiles(fastLatencies)
	t.Logf("fanout=%d  fast-calls=%d  errors=%d  p50=%s p95=%s p99=%s  streams=%d chunks=%d",
		fanout, fastDone.Load(), fastErrors.Load(), p50, p95, p99,
		streamDone.Load(), streamChunks.Load())

	if p99 > p99Budget {
		t.Errorf("fast-call p99 %s exceeds budget %s (fanout=%d)", p99, p99Budget, fanout)
	}
}

// driveStream issues a streaming proxy GET and drains the response
// body, counting bytes consumed (≥ 1 implies the stream produced
// data). The aether SDK's stream_response_indefinitely opt-in is used.
func driveStream(ctx context.Context, client *aether.AgentClient, target, path string, deadline time.Duration) (int, error) {
	reqCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", "http://ignored"+path, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.ProxyHTTP(reqCtx, target, req,
		aether.WithBackend("local"),
		aether.WithStreamResponse(int64(deadline/time.Millisecond), 0),
	)
	if err != nil {
		return 0, fmt.Errorf("ProxyHTTP: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("status=%d", resp.StatusCode)
	}
	buf := make([]byte, 4096)
	chunks := 0
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			chunks++
		}
		if err != nil {
			if err == io.EOF {
				return chunks, nil
			}
			return chunks, err
		}
	}
}

// driveFast issues a non-streaming proxy GET and returns the wall-time
// round-trip. Errors include the elapsed time so the caller can include
// it in tail-latency analysis if desired.
func driveFast(ctx context.Context, client *aether.AgentClient, target, path string, timeout time.Duration) (time.Duration, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", "http://ignored"+path, nil)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	resp, err := client.ProxyHTTP(reqCtx, target, req, aether.WithBackend("local"))
	if err != nil {
		return time.Since(start), err
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	elapsed := time.Since(start)
	if resp.StatusCode != 200 {
		return elapsed, fmt.Errorf("status=%d body=%q", resp.StatusCode, string(body))
	}
	if !bytes.Contains(body, []byte(`"ok":true`)) {
		return elapsed, fmt.Errorf("unexpected body: %q", string(body))
	}
	return elapsed, nil
}

// dialAgentClient connects an aether AgentClient to the harness's fake
// gateway as a unique caller and waits for ConnectionConfirmed.
func dialAgentClient(t *testing.T, h *E2EHarness, callerID string) *aether.AgentClient {
	t.Helper()

	client, err := aether.NewAgentClient(aether.AgentOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr: h.GatewayAddr,
			Connection: aether.ConnectionOptions{
				MaxRetries:        1,
				InitialBackoff:    50 * time.Millisecond,
				MaxBackoff:        500 * time.Millisecond,
				BackoffMultiplier: 2.0,
				AutoReconnect:     false,
				ConnectTimeout:    5 * time.Second,
				KeepAliveInterval: 10 * time.Second,
			},
		},
		Workspace:      "e2e",
		Implementation: "caller",
		Specifier:      callerID,
	})
	if err != nil {
		t.Fatalf("NewAgentClient: %v", err)
	}

	// Use a long-lived context for Connect — the SDK ties its internal
	// streamCtx to whatever context we pass in here, so cancelling a
	// short-lived connect ctx (e.g. via defer cancel()) tears the
	// stream down on dialAgentClient return. We instead bound the
	// connect-time deadline via the explicit polling loop below.
	connectCtx, connectCancel := context.WithCancel(context.Background())
	if err := client.Connect(connectCtx); err != nil {
		connectCancel()
		t.Fatalf("Connect: %v", err)
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(runCtx) }()
	t.Cleanup(func() {
		runCancel()
		connectCancel()
		_ = client.CloseConnection()
	})

	deadline := time.Now().Add(10 * time.Second)
	for !client.ConnectionConfirmed() {
		select {
		case err := <-runDone:
			t.Fatalf("client.Run exited prematurely before ConnectionAck: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("ConnectionAck not observed within 10s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return client
}

// percentiles returns p50, p95, p99 across the supplied durations.
// Empty input returns zeros.
func percentiles(d []time.Duration) (p50, p95, p99 time.Duration) {
	if len(d) == 0 {
		return
	}
	sorted := make([]time.Duration, len(d))
	copy(sorted, d)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pick := func(p float64) time.Duration {
		idx := int(float64(len(sorted)-1)*p + 0.5)
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}
	return pick(0.50), pick(0.95), pick(0.99)
}
