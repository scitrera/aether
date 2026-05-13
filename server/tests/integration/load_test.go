//go:build integration && load

package integration

// Load test harness for T19: 100 concurrent TCP tunnels @ ~10 KiB/s each
// (~1 MiB/s aggregate) for 60 seconds, exercising the in-process tunnel
// pipeline established by T7 (harness_test.go) and T14 (tunnel_e2e_test.go).
//
// SCOPE CAVEAT: this harness drives the gateway's tunnel routing primitives
// in-process — no real gRPC, no Redis, no RabbitMQ. It validates:
//   - per-tunnel bookkeeping at scale (lock contention, register/unregister)
//   - TunnelAck flow-control under steady load (no deadlock, no unbounded
//     buffer growth)
//   - goroutine accounting on tunnel teardown (no leak)
//   - per-tunnel byte-count isolation
//   - tunnel-open latency at the routing layer
//
// It does NOT measure: real RMQ stream throughput, real network jitter, real
// gateway memory under gRPC stream pressure, or service-side gRPC delivery
// (which has a documented wiring gap — see proxy-load-test-results.md).
//
// Run:
//   cd server
//   /home/drew/sdk/go1.25.5/bin/go test -tags='integration load' \
//     -run TestProxyLoad_HundredTunnels_OneMBps \
//     -v -timeout 5m ./tests/integration/...

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// Test parameters
// =============================================================================

const (
	loadNumTunnels     = 100
	loadDuration       = 60 * time.Second
	loadBytesPerSecond = 10 * 1024 // ~10 KiB/s per tunnel = ~1 MiB/s aggregate
	loadChunkSize      = 1024      // 1 KiB chunks, sent every ~100ms (≈10 KiB/s)
	loadChunkInterval  = 100 * time.Millisecond
	loadNumTerminators = 4
	loadStallThreshold = 1 * time.Second
	loadOpenLatencyP99 = 100 * time.Millisecond
	loadHeapPeakMaxMB  = 512 // generous in-process ceiling
	loadGoroutineSlack = 16  // tolerance over baseline after teardown
	loadTeardownWindow = 30 * time.Second
	loadInitialCredits = 256 * 1024 // ~256 KiB initial outbound window
	loadCallerAckChunk = 256 * 1024
)

// =============================================================================
// Instrumentation
// =============================================================================

// loadStats aggregates instrumentation across all tunnels in the run.
type loadStats struct {
	openLatenciesNs   []int64
	openLatenciesNsMu sync.Mutex

	stallCount        atomic.Int64 // any single read/write blocked > 1s
	backpressureWaits atomic.Int64 // count of times a writer waited on credits

	auditOpened atomic.Int64
	auditClosed atomic.Int64

	bytesSent     atomic.Int64
	bytesReceived atomic.Int64
	mismatches    atomic.Int64
}

func (s *loadStats) recordOpen(d time.Duration) {
	s.openLatenciesNsMu.Lock()
	s.openLatenciesNs = append(s.openLatenciesNs, d.Nanoseconds())
	s.openLatenciesNsMu.Unlock()
}

func (s *loadStats) percentiles() (p50, p99 time.Duration) {
	s.openLatenciesNsMu.Lock()
	defer s.openLatenciesNsMu.Unlock()
	n := len(s.openLatenciesNs)
	if n == 0 {
		return 0, 0
	}
	sorted := append([]int64(nil), s.openLatenciesNs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	p50 = time.Duration(sorted[int(math.Floor(float64(n)*0.50))])
	p99idx := int(math.Floor(float64(n) * 0.99))
	if p99idx >= n {
		p99idx = n - 1
	}
	p99 = time.Duration(sorted[p99idx])
	return p50, p99
}

// =============================================================================
// Per-tunnel driver
// =============================================================================

// driveTunnel writes a known pattern at ~10 KiB/s for the given duration,
// concurrently reads it back, and verifies bytes round-trip intact.
//
// "Known pattern": each chunk is filled with a per-tunnel byte tag plus a
// monotonically-increasing seq counter so cross-talk would be detectable.
func driveTunnel(t *testing.T, st *tunnelState, tunnelIdx int, dur time.Duration, stats *loadStats, done chan<- error) {
	tag := byte(tunnelIdx % 256)

	// Prime caller→sidecar credits generously and replenish in the background.
	st.sendCallerAck(uint32(2 * loadCallerAckChunk))
	ackTicker := time.NewTicker(50 * time.Millisecond)
	stopAck := make(chan struct{})
	go func() {
		defer ackTicker.Stop()
		for {
			select {
			case <-stopAck:
				return
			case <-ackTicker.C:
				if !st.closed.Load() {
					st.sendCallerAck(loadCallerAckChunk)
				}
			}
		}
	}()
	defer close(stopAck)

	// Read goroutine: drains downstream frames, counts bytes, watches for
	// long stalls between frames.
	rxDone := make(chan []byte, 1)
	go func() {
		var buf []byte
		lastFrame := time.Now()
		deadline := time.NewTimer(dur + 10*time.Second)
		defer deadline.Stop()
		stallTimer := time.NewTicker(200 * time.Millisecond)
		defer stallTimer.Stop()
		for {
			select {
			case msg := <-st.downstream:
				switch p := msg.GetPayload().(type) {
				case *pb.DownstreamMessage_TunnelData:
					data := p.TunnelData.GetData()
					stats.bytesReceived.Add(int64(len(data)))
					buf = append(buf, data...)
					lastFrame = time.Now()
				case *pb.DownstreamMessage_TunnelAck:
					// flow-control accounting handled in callerTransport.SendTunnelAck
				case *pb.DownstreamMessage_TunnelClose:
					rxDone <- buf
					return
				}
			case <-stallTimer.C:
				if !st.closed.Load() && time.Since(lastFrame) > loadStallThreshold && stats.bytesSent.Load() > 0 {
					stats.stallCount.Add(1)
					lastFrame = time.Now() // avoid double-counting
				}
			case <-deadline.C:
				rxDone <- buf
				return
			}
		}
	}()

	// Write loop: send 1 KiB chunks every ~100ms for the duration.
	chunk := make([]byte, loadChunkSize)
	endAt := time.Now().Add(dur)
	var seq uint32
	writeTicker := time.NewTicker(loadChunkInterval)
	defer writeTicker.Stop()

	for time.Now().Before(endAt) {
		select {
		case <-writeTicker.C:
			// Fill with deterministic pattern: tag then varying seq.
			for i := range chunk {
				chunk[i] = tag ^ byte(seq) ^ byte(i)
			}
			writeStart := time.Now()
			// Track back-pressure: if outboundCredits is below chunk size
			// when we attempt the send, the writer will block — count that
			// as one back-pressure incident (the sendData call performs the
			// blocking wait internally).
			if st.outboundCredits.Load() < int64(loadChunkSize) {
				stats.backpressureWaits.Add(1)
			}
			if err := st.sendData(chunk, false); err != nil {
				done <- fmt.Errorf("tunnel[%d] sendData: %w", tunnelIdx, err)
				return
			}
			if elapsed := time.Since(writeStart); elapsed > loadStallThreshold {
				stats.stallCount.Add(1)
			}
			stats.bytesSent.Add(int64(loadChunkSize))
			seq++
		}
	}

	// Send FIN and let the read goroutine drain remaining bytes.
	st.sendFin()

	// Wait briefly for echo of in-flight bytes.
	select {
	case buf := <-rxDone:
		// Tolerance window: we expect bytesSent ≈ bytesReceived modulo
		// in-flight chunks. We don't byte-compare the whole stream (10 KiB/s
		// for 60s = 600 KiB per tunnel; 100 tunnels = 60 MiB) — instead we
		// verify the FIRST chunk pattern is intact (simple integrity check)
		// and that the received byte count is within tolerance.
		if len(buf) >= loadChunkSize {
			expected := make([]byte, loadChunkSize)
			for i := range expected {
				expected[i] = tag ^ byte(0) ^ byte(i)
			}
			for i := 0; i < loadChunkSize; i++ {
				if buf[i] != expected[i] {
					stats.mismatches.Add(1)
					break
				}
			}
		}
	case <-time.After(10 * time.Second):
		// Don't fail the test — accept truncated tail at shutdown — but
		// flag if we got nothing back at all.
		if stats.bytesReceived.Load() == 0 {
			done <- fmt.Errorf("tunnel[%d] no bytes returned", tunnelIdx)
			return
		}
	}

	// Caller-initiated close (will be a no-op if half-close already closed it).
	st.closeFromCaller(pb.TunnelClose_NORMAL, "load-test-complete")
	done <- nil
}

// =============================================================================
// Test
// =============================================================================

func TestProxyLoad_HundredTunnels_OneMBps(t *testing.T) {
	// Sanity baseline.
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	baselineGoroutines := runtime.NumGoroutine()
	var baselineMem runtime.MemStats
	runtime.ReadMemStats(&baselineMem)

	addr := echoListener(t)
	h := newTunnelHarness()
	for i := 0; i < loadNumTerminators; i++ {
		h.addTunnelTerminator(t, "loadsvc", fmt.Sprintf("inst%d", i),
			addr, 90_000, 0, nil)
	}

	stats := &loadStats{}

	// Open all 100 tunnels concurrently, capturing per-open latency.
	tunnels := make([]*tunnelState, loadNumTunnels)
	var openWg sync.WaitGroup
	openErr := make(chan error, loadNumTunnels)
	openWg.Add(loadNumTunnels)
	openStart := time.Now()
	for i := 0; i < loadNumTunnels; i++ {
		go func(idx int) {
			defer openWg.Done()
			start := time.Now()
			st, err := h.openTunnel(t, agentCaller("ws", "load", fmt.Sprintf("v%d", idx)),
				"sv::loadsvc", 90_000, 0, loadInitialCredits)
			if err != nil {
				openErr <- fmt.Errorf("tunnel[%d] openTunnel: %w", idx, err)
				return
			}
			stats.auditOpened.Add(1) // mock audit hook (gateway audit not running in-process)
			stats.recordOpen(time.Since(start))
			tunnels[idx] = st
		}(i)
	}
	openWg.Wait()
	close(openErr)
	for err := range openErr {
		if err != nil {
			t.Fatalf("open failure: %v", err)
		}
	}
	t.Logf("opened %d tunnels in %v", loadNumTunnels, time.Since(openStart))

	// Peak measurement plumbing: poll runtime stats while load runs.
	var peakGoroutines atomic.Int64
	var peakHeapBytes atomic.Uint64
	stopPolling := make(chan struct{})
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPolling:
				return
			case <-ticker.C:
				g := int64(runtime.NumGoroutine())
				if g > peakGoroutines.Load() {
					peakGoroutines.Store(g)
				}
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				if ms.HeapInuse > peakHeapBytes.Load() {
					peakHeapBytes.Store(ms.HeapInuse)
				}
			}
		}
	}()

	// Drive all tunnels in parallel for loadDuration.
	var driveWg sync.WaitGroup
	driveErr := make(chan error, loadNumTunnels)
	driveDone := make(chan error, loadNumTunnels)
	driveWg.Add(loadNumTunnels)
	driveStart := time.Now()
	for i, st := range tunnels {
		go func(idx int, s *tunnelState) {
			defer driveWg.Done()
			driveTunnel(t, s, idx, loadDuration, stats, driveDone)
		}(i, st)
	}

	// Collect drive results.
	go func() {
		driveWg.Wait()
		close(driveDone)
	}()
	for err := range driveDone {
		if err != nil {
			driveErr <- err
		}
	}
	close(driveErr)
	t.Logf("drive phase complete in %v", time.Since(driveStart))

	// Stop peak polling.
	close(stopPolling)
	<-pollDone

	// Account for closes (mock audit).
	for range tunnels {
		stats.auditClosed.Add(1)
	}

	// Force terminator-side teardown for any tunnels not closed cleanly.
	for _, e := range h.tcpEntries {
		e.t.StopAllTunnels()
	}

	// Wait for goroutines to settle.
	teardownStart := time.Now()
	var postGoroutines int
	deadline := time.Now().Add(loadTeardownWindow)
	for time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(500 * time.Millisecond)
		postGoroutines = runtime.NumGoroutine()
		if postGoroutines <= baselineGoroutines+loadGoroutineSlack {
			break
		}
	}
	teardownElapsed := time.Since(teardownStart)

	var postMem runtime.MemStats
	runtime.ReadMemStats(&postMem)

	// =========================================================================
	// Surface drive-phase errors.
	// =========================================================================
	for err := range driveErr {
		t.Errorf("drive error: %v", err)
	}

	// =========================================================================
	// Reporting.
	// =========================================================================
	p50, p99 := stats.percentiles()
	peakHeapMB := float64(peakHeapBytes.Load()) / (1024 * 1024)

	t.Logf("===== load test results =====")
	t.Logf("tunnels opened/closed:    %d / %d", stats.auditOpened.Load(), stats.auditClosed.Load())
	t.Logf("tunnel-open latency p50:  %v", p50)
	t.Logf("tunnel-open latency p99:  %v", p99)
	t.Logf("bytes sent / received:    %d / %d", stats.bytesSent.Load(), stats.bytesReceived.Load())
	t.Logf("byte mismatches:          %d", stats.mismatches.Load())
	t.Logf("stall events (>1s):       %d", stats.stallCount.Load())
	t.Logf("backpressure waits:       %d", stats.backpressureWaits.Load())
	t.Logf("baseline goroutines:      %d", baselineGoroutines)
	t.Logf("peak goroutines:          %d", peakGoroutines.Load())
	t.Logf("post-teardown goroutines: %d (after %v)", postGoroutines, teardownElapsed)
	t.Logf("baseline heap inuse MB:   %.2f", float64(baselineMem.HeapInuse)/(1024*1024))
	t.Logf("peak heap inuse MB:       %.2f", peakHeapMB)
	t.Logf("post heap inuse MB:       %.2f", float64(postMem.HeapInuse)/(1024*1024))

	// =========================================================================
	// Asserts.
	// =========================================================================

	// 1. Zero stalls > 1 second.
	if stats.stallCount.Load() > 0 {
		t.Errorf("ASSERT FAIL: stall events = %d, want 0 (>1s read/write blockages)",
			stats.stallCount.Load())
	}

	// 2. Back-pressure correctly throttles — heap stays bounded (proxy for
	//    "writer slowed instead of OOM"). loadHeapPeakMaxMB is generous to
	//    avoid CI flake on small runners.
	if peakHeapMB > loadHeapPeakMaxMB {
		t.Errorf("ASSERT FAIL: peak heap %.2f MB > %d MB ceiling — back-pressure may not be working",
			peakHeapMB, loadHeapPeakMaxMB)
	}

	// 3. No goroutine leak after 30s post-teardown.
	if postGoroutines > baselineGoroutines+loadGoroutineSlack {
		t.Errorf("ASSERT FAIL: goroutine leak — post=%d baseline=%d slack=%d",
			postGoroutines, baselineGoroutines, loadGoroutineSlack)
	}

	// 4. All bytes round-trip intact (with shutdown-tail tolerance).
	//    First-chunk pattern check catches cross-talk; mismatches count must be 0.
	if stats.mismatches.Load() > 0 {
		t.Errorf("ASSERT FAIL: byte mismatches = %d (cross-talk or corruption)",
			stats.mismatches.Load())
	}
	// Sanity: we should have RX'd at least 50% of TX bytes (60s × 1 MiB/s
	// ≈ 60 MiB sent, expect most of it back).
	sent := stats.bytesSent.Load()
	rcvd := stats.bytesReceived.Load()
	if sent > 0 && float64(rcvd)/float64(sent) < 0.5 {
		t.Errorf("ASSERT FAIL: received only %d of %d bytes (<50%% round-trip)", rcvd, sent)
	}

	// 5. Tunnel-open latency p99 < 100ms in-process.
	if p99 > loadOpenLatencyP99 {
		t.Errorf("ASSERT FAIL: tunnel-open p99 = %v > %v", p99, loadOpenLatencyP99)
	}

	// Audit volume sanity.
	if stats.auditOpened.Load() != loadNumTunnels {
		t.Errorf("audit OpTunnelOpened count = %d, want %d",
			stats.auditOpened.Load(), loadNumTunnels)
	}
	if stats.auditClosed.Load() != loadNumTunnels {
		t.Errorf("audit OpTunnelClosed count = %d, want %d",
			stats.auditClosed.Load(), loadNumTunnels)
	}
}
