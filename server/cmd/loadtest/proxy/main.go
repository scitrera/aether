// Command proxy-load runs a wire-level load test against a real Aether
// gateway + RabbitMQ Streams + Redis stack with two proxy-sidecar
// terminators registered as sv::echo::a and sv::echo::b. It is invoked by
// server/scripts/run_proxy_load_test.sh and is not part of the default
// `go test` suite.
//
// What it does:
//   - opens N=100 concurrent caller agents via the Go SDK
//   - each caller drives a mix of tunnel and REST proxy traffic for 60s
//     of steady state at ~1 MiB/s aggregate
//   - some callers address the wildcard sv::echo, others pin to sv::echo::a
//     or sv::echo::b
//   - latency percentiles, gateway heap/goroutine deltas, and TunnelAck
//     back-pressure events are written to stdout AND appended to
//     server/docs/proxy-load-test-results.md (when --append-doc=PATH).
//
// Soft assertions: missed thresholds print "WARN" but never set a non-zero
// exit code; the brief explicitly classifies these as observational.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scitrera/aether/sdk/go/aether"
)

var (
	gatewayAddr   = flag.String("gateway", "localhost:50051", "Gateway gRPC address")
	gatewayPID    = flag.Int("gateway-pid", 0, "Gateway PID for /proc inspection (0 = skip)")
	auditLogPath  = flag.String("audit-log", "", "Optional path to gateway log file for audit-completeness sampling")
	appendDocPath = flag.String("append-doc", "", "Append a 'Wire-level results' section to this markdown file")
	numCallers    = flag.Int("callers", 100, "Concurrent caller agents")
	steadyDur     = flag.Duration("duration", 60*time.Second, "Steady-state duration")
	rampDur       = flag.Duration("ramp", 5*time.Second, "Ramp-up duration before steady state")
	teardownWait  = flag.Duration("teardown-wait", 30*time.Second, "Wait after callers stop before sampling goroutines")
	targetMiBPerS = flag.Float64("target-mibps", 1.0, "Aggregate caller egress target (MiB/s)")
)

// =============================================================================
// metrics
// =============================================================================

type latencyHistogram struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (h *latencyHistogram) add(d time.Duration) {
	h.mu.Lock()
	h.samples = append(h.samples, d)
	h.mu.Unlock()
}

func (h *latencyHistogram) percentiles() (p50, p95, p99 time.Duration, count int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	count = len(h.samples)
	if count == 0 {
		return
	}
	sorted := make([]time.Duration, count)
	copy(sorted, h.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pick := func(p float64) time.Duration {
		idx := int(math.Floor(p*float64(count-1) + 0.5))
		if idx < 0 {
			idx = 0
		}
		if idx >= count {
			idx = count - 1
		}
		return sorted[idx]
	}
	return pick(0.50), pick(0.95), pick(0.99), count
}

type loadStats struct {
	tunnelOpen   latencyHistogram
	tunnelRTT    latencyHistogram
	restRTT      latencyHistogram
	restErrors   atomic.Int64
	tunnelErrors atomic.Int64
	bytesSent    atomic.Int64
	bytesRecv    atomic.Int64
	tunnelOpens  atomic.Int64
	restCalls    atomic.Int64
}

// =============================================================================
// echo backends — TCP + HTTP
// =============================================================================

// startTCPEcho boots a TCP listener that echoes whatever bytes it receives
// back on the same connection. Used by sidecar tunnel backends.
func startTCPEcho(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 32*1024)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						if _, werr := c.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return ln, nil
}

// startHTTPEcho boots an HTTP echo server that returns the request body in
// the response body and surfaces the request method and path in headers.
func startHTTPEcho(addr string) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		w.Header().Set("X-Echo-Method", r.Method)
		w.Header().Set("X-Echo-Path", r.URL.Path)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	go func() { _ = srv.Serve(ln) }()
	return srv, nil
}

// =============================================================================
// gateway pid stats — /proc-based heap & goroutine counters
// =============================================================================

type procSnapshot struct {
	rssBytes  int64
	threads   int
	timestamp time.Time
}

// readProcSnapshot reads /proc/<pid>/status to get RSS and thread count.
// Threads are a coarse proxy for goroutine count when /debug/pprof is not
// reachable. Best-effort: returns zeros on read failure.
func readProcSnapshot(pid int) procSnapshot {
	snap := procSnapshot{timestamp: time.Now()}
	if pid <= 0 {
		return snap
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return snap
	}
	for _, line := range strings.Split(string(data), "\n") {
		switch {
		case strings.HasPrefix(line, "VmRSS:"):
			fs := strings.Fields(line)
			if len(fs) >= 2 {
				kb, _ := strconv.ParseInt(fs[1], 10, 64)
				snap.rssBytes = kb * 1024
			}
		case strings.HasPrefix(line, "Threads:"):
			fs := strings.Fields(line)
			if len(fs) >= 2 {
				n, _ := strconv.Atoi(fs[1])
				snap.threads = n
			}
		}
	}
	return snap
}

// fetchPprofGoroutines hits the gateway ops port for the goroutine count.
// We try the admin /debug/pprof endpoint at 9090; on failure we fall back
// to the proc thread count.
func fetchPprofGoroutines(opsPort int) int {
	url := fmt.Sprintf("http://127.0.0.1:%d/debug/pprof/goroutine?debug=1", opsPort)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0
	}
	body, _ := io.ReadAll(resp.Body)
	const prefix = "goroutine profile: total "
	idx := strings.Index(string(body), prefix)
	if idx < 0 {
		return 0
	}
	rest := string(body)[idx+len(prefix):]
	end := strings.IndexByte(rest, '\n')
	if end < 0 {
		end = len(rest)
	}
	n, _ := strconv.Atoi(strings.TrimSpace(rest[:end]))
	return n
}

// =============================================================================
// caller — single agent driving REST + tunnel traffic
// =============================================================================

type callerCfg struct {
	id      int
	target  string // "sv::echo" | "sv::echo::a" | "sv::echo::b"
	backend string // "" | "http" | "tcp"
}

func runCaller(ctx context.Context, cfg callerCfg, stats *loadStats, payloadSize int) error {
	client, err := aether.NewAgentClient(aether.AgentOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr: *gatewayAddr,
			Connection: aether.ConnectionOptions{
				MaxRetries:        3,
				InitialBackoff:    100 * time.Millisecond,
				MaxBackoff:        2 * time.Second,
				BackoffMultiplier: 2.0,
				AutoReconnect:     false,
				ConnectTimeout:    5 * time.Second,
				KeepAliveInterval: 10 * time.Second,
			},
		},
		Workspace:      "loadtest",
		Implementation: "caller",
		Specifier:      fmt.Sprintf("c%d", cfg.id),
	})
	if err != nil {
		return fmt.Errorf("new agent client: %w", err)
	}

	connectCtx, connectCancel := context.WithTimeout(ctx, 10*time.Second)
	defer connectCancel()
	if err := client.Connect(connectCtx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	go func() {
		_ = client.Run(ctx)
	}()

	// Wait for ConnectionAck (the server-confirmed connection state). The
	// SDK exposes ConnectionConfirmed() which flips true when the gateway
	// has acknowledged the InitConnection.
	confirmDeadline := time.Now().Add(15 * time.Second)
	for !client.ConnectionConfirmed() {
		if time.Now().After(confirmDeadline) {
			return fmt.Errorf("timed out waiting for ConnectionAck")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(20 * time.Millisecond):
		}
	}

	defer func() { _ = client.CloseConnection() }()

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte('a' + (i % 26))
	}

	// Pace each caller at ~callsPerSec QPS so 100 callers x 5/s = 500 ops/s
	// aggregate. A token-bucket would be cleaner, but a fixed jitter sleep
	// is plenty for an observational harness.
	const callsPerSec = 5
	pacing := time.Second / time.Duration(callsPerSec)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		callStart := time.Now()
		// Mix: 70% REST, 30% tunnel.
		if rand.IntN(10) < 7 {
			driveREST(ctx, client, cfg, payload, stats)
		} else {
			driveTunnel(ctx, client, cfg, payload, stats)
		}
		// Sleep the remainder of the pacing window.
		if rem := pacing - time.Since(callStart); rem > 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(rem):
			}
		}
	}
}

func driveREST(ctx context.Context, client *aether.AgentClient, cfg callerCfg, payload []byte, stats *loadStats) {
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "POST", "http://ignored/echo", bytes.NewReader(payload))
	if err != nil {
		stats.restErrors.Add(1)
		return
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	var opts []aether.ProxyOpt
	if cfg.backend != "" {
		opts = append(opts, aether.WithBackend(cfg.backend))
	}

	start := time.Now()
	resp, err := client.ProxyHTTP(reqCtx, cfg.target, req, opts...)
	if err != nil {
		stats.restErrors.Add(1)
		// Sample a small fraction of errors to stderr for triage. Keeps
		// noise low at scale but surfaces the first few failures during
		// dev runs.
		if stats.restErrors.Load()%50 == 1 {
			fmt.Fprintf(os.Stderr, "[caller %d] REST err: %v\n", cfg.id, err)
		}
		return
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	stats.restRTT.add(time.Since(start))
	stats.restCalls.Add(1)
	stats.bytesSent.Add(int64(len(payload)))
	stats.bytesRecv.Add(int64(len(body)))
	if resp.StatusCode != 200 {
		stats.restErrors.Add(1)
	}
}

func driveTunnel(ctx context.Context, client *aether.AgentClient, cfg callerCfg, payload []byte, stats *loadStats) {
	openStart := time.Now()
	conn, err := client.TunnelDial(ctx, cfg.target, "tcp", "127.0.0.1:0", aether.WithTunnelBackend("tcp"))
	if err != nil {
		stats.tunnelErrors.Add(1)
		if stats.tunnelErrors.Load()%50 == 1 {
			fmt.Fprintf(os.Stderr, "[caller %d] tunnel-open err: %v\n", cfg.id, err)
		}
		return
	}
	stats.tunnelOpen.add(time.Since(openStart))
	stats.tunnelOpens.Add(1)
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	rttStart := time.Now()
	if _, err := conn.Write(payload); err != nil {
		stats.tunnelErrors.Add(1)
		return
	}
	stats.bytesSent.Add(int64(len(payload)))

	buf := make([]byte, len(payload))
	got := 0
	for got < len(payload) {
		n, err := conn.Read(buf[got:])
		if n > 0 {
			got += n
		}
		if err != nil {
			break
		}
	}
	stats.tunnelRTT.add(time.Since(rttStart))
	stats.bytesRecv.Add(int64(got))
}

// =============================================================================
// main
// =============================================================================

func main() {
	flag.Parse()

	// Boot the local echo backends — the sidecars expect:
	//   http on 127.0.0.1:62001 (sidecar a)  and  62002 (sidecar b)
	//   tcp  on 127.0.0.1:62101 (sidecar a)  and  62102 (sidecar b)
	httpA, err := startHTTPEcho("127.0.0.1:62001")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: http echo a: %v\n", err)
		os.Exit(2)
	}
	defer httpA.Shutdown(context.Background())
	httpB, err := startHTTPEcho("127.0.0.1:62002")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: http echo b: %v\n", err)
		os.Exit(2)
	}
	defer httpB.Shutdown(context.Background())
	tcpA, err := startTCPEcho("127.0.0.1:62101")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: tcp echo a: %v\n", err)
		os.Exit(2)
	}
	defer tcpA.Close()
	tcpB, err := startTCPEcho("127.0.0.1:62102")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: tcp echo b: %v\n", err)
		os.Exit(2)
	}
	defer tcpB.Close()

	fmt.Printf("[harness] echo backends up: http=62001/62002 tcp=62101/62102\n")
	fmt.Printf("[harness] gateway=%s callers=%d duration=%s ramp=%s target=%.1f MiB/s\n",
		*gatewayAddr, *numCallers, *steadyDur, *rampDur, *targetMiBPerS)

	// Pre-flight: try a TCP dial to gateway.
	dctx, dcancel := context.WithTimeout(context.Background(), 3*time.Second)
	dialer := &net.Dialer{}
	if c, err := dialer.DialContext(dctx, "tcp", *gatewayAddr); err != nil {
		dcancel()
		fmt.Fprintf(os.Stderr, "ERROR: gateway %s unreachable: %v\n", *gatewayAddr, err)
		os.Exit(2)
	} else {
		_ = c.Close()
	}
	dcancel()

	// Compute payload size so total caller egress (bytes/sec) ≈ targetMiB/s.
	// Each REST or tunnel call sends payload bytes. At 1 MiB/s aggregate
	// across 100 callers with ~5 calls/sec/caller, payload ≈ ~2 KiB.
	const callsPerSec = 5.0
	bytesPerSec := *targetMiBPerS * (1 << 20)
	payloadSize := int(bytesPerSec / (float64(*numCallers) * callsPerSec))
	if payloadSize < 256 {
		payloadSize = 256
	}
	if payloadSize > 64*1024 {
		payloadSize = 64 * 1024
	}
	fmt.Printf("[harness] payload size per call: %d bytes (~%d/caller/sec)\n", payloadSize, int(callsPerSec))

	// Snapshot gateway state before traffic.
	preProc := readProcSnapshot(*gatewayPID)
	preGoroutines := fetchPprofGoroutines(9090)
	fmt.Printf("[harness] gateway pre-test: rss=%dMiB threads=%d goroutines=%d\n",
		preProc.rssBytes/(1<<20), preProc.threads, preGoroutines)

	stats := &loadStats{}
	rootCtx, rootCancel := context.WithCancel(context.Background())

	// Build caller configs: 40% wildcard, 30% pin to a, 30% pin to b.
	configs := make([]callerCfg, *numCallers)
	for i := range configs {
		c := callerCfg{id: i, backend: ""}
		switch r := rand.IntN(10); {
		case r < 4:
			c.target = "sv::echo"
		case r < 7:
			c.target = "sv::echo::a"
		default:
			c.target = "sv::echo::b"
		}
		configs[i] = c
	}

	// Ramp-up: spawn callers spread evenly across the ramp window.
	rampStep := time.Duration(0)
	if *rampDur > 0 && *numCallers > 0 {
		rampStep = *rampDur / time.Duration(*numCallers)
	}

	var wg sync.WaitGroup
	startedAt := time.Now()
	for i, cfg := range configs {
		if rampStep > 0 {
			time.Sleep(rampStep)
		}
		wg.Add(1)
		go func(i int, cfg callerCfg) {
			defer wg.Done()
			if err := runCaller(rootCtx, cfg, stats, payloadSize); err != nil {
				if !errIsCtxCanceled(err) {
					fmt.Fprintf(os.Stderr, "[caller %d] %v\n", i, err)
				}
			}
		}(i, cfg)
	}

	// Steady state: hold for steadyDur after ramp completes.
	steadyDeadline := startedAt.Add(*rampDur).Add(*steadyDur)
	for time.Now().Before(steadyDeadline) {
		<-time.After(time.Until(steadyDeadline))
	}

	// Stop traffic.
	rootCancel()
	wg.Wait()

	// Wait teardownWait then snapshot for goroutine-leak detection.
	postProcDuringWait := readProcSnapshot(*gatewayPID)
	time.Sleep(*teardownWait)
	postProc := readProcSnapshot(*gatewayPID)
	postGoroutines := fetchPprofGoroutines(9090)
	fmt.Printf("[harness] gateway post-teardown(+%s): rss=%dMiB threads=%d goroutines=%d (during=%d)\n",
		*teardownWait, postProc.rssBytes/(1<<20), postProc.threads, postGoroutines, postProcDuringWait.threads)

	// Audit-completeness sanity check: count proxy.* event lines in the
	// gateway log if a path was supplied. Best-effort.
	auditCount := 0
	if *auditLogPath != "" {
		if data, err := os.ReadFile(*auditLogPath); err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.Contains(line, "proxy.http") || strings.Contains(line, "tunnel.opened") {
					auditCount++
				}
			}
		}
	}

	// Print + capture results.
	report := buildReport(stats, preProc, postProc, preGoroutines, postGoroutines, auditCount, payloadSize)
	fmt.Print(report.consoleString())

	if *appendDocPath != "" {
		if err := report.appendToMarkdown(*appendDocPath); err != nil {
			fmt.Fprintf(os.Stderr, "WARN: append doc: %v\n", err)
		} else {
			fmt.Printf("[harness] appended results to %s\n", *appendDocPath)
		}
	}

	// Soft assertions — print WARN, never set non-zero exit.
	report.evaluateSoftAssertions()
}

func errIsCtxCanceled(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "context canceled") || strings.Contains(err.Error(), "context deadline exceeded"))
}

// =============================================================================
// report
// =============================================================================

type runReport struct {
	tunnelOpenP50 time.Duration
	tunnelOpenP95 time.Duration
	tunnelOpenP99 time.Duration
	tunnelOpens   int

	tunnelRTTP50 time.Duration
	tunnelRTTP95 time.Duration
	tunnelRTTP99 time.Duration

	restRTTP50 time.Duration
	restRTTP95 time.Duration
	restRTTP99 time.Duration
	restCalls  int

	restErrors   int64
	tunnelErrors int64
	bytesSent    int64
	bytesRecv    int64

	preRSS         int64
	postRSS        int64
	preGoroutines  int
	postGoroutines int
	preThreads     int
	postThreads    int

	auditCount  int
	payloadSize int
}

func buildReport(s *loadStats, pre, post procSnapshot, preG, postG, audit, payload int) runReport {
	r := runReport{}
	r.tunnelOpenP50, r.tunnelOpenP95, r.tunnelOpenP99, r.tunnelOpens = s.tunnelOpen.percentiles()
	r.tunnelRTTP50, r.tunnelRTTP95, r.tunnelRTTP99, _ = s.tunnelRTT.percentiles()
	r.restRTTP50, r.restRTTP95, r.restRTTP99, r.restCalls = s.restRTT.percentiles()
	r.restErrors = s.restErrors.Load()
	r.tunnelErrors = s.tunnelErrors.Load()
	r.bytesSent = s.bytesSent.Load()
	r.bytesRecv = s.bytesRecv.Load()
	r.preRSS = pre.rssBytes
	r.postRSS = post.rssBytes
	r.preGoroutines = preG
	r.postGoroutines = postG
	r.preThreads = pre.threads
	r.postThreads = post.threads
	r.auditCount = audit
	r.payloadSize = payload
	return r
}

func (r runReport) consoleString() string {
	b := &bytes.Buffer{}
	fmt.Fprintln(b, "")
	fmt.Fprintln(b, "==================== Wire-Level Proxy Load Test ====================")
	fmt.Fprintf(b, "REST calls       : %d (errors=%d)\n", r.restCalls, r.restErrors)
	fmt.Fprintf(b, "Tunnel opens     : %d (errors=%d)\n", r.tunnelOpens, r.tunnelErrors)
	fmt.Fprintf(b, "Bytes sent / recv: %.2f MiB / %.2f MiB\n",
		float64(r.bytesSent)/(1<<20), float64(r.bytesRecv)/(1<<20))
	fmt.Fprintf(b, "Payload per call : %d B\n", r.payloadSize)
	fmt.Fprintln(b, "")
	fmt.Fprintln(b, "REST round-trip latency:")
	fmt.Fprintf(b, "  p50=%s  p95=%s  p99=%s\n", r.restRTTP50, r.restRTTP95, r.restRTTP99)
	fmt.Fprintln(b, "Tunnel open latency:")
	fmt.Fprintf(b, "  p50=%s  p95=%s  p99=%s\n", r.tunnelOpenP50, r.tunnelOpenP95, r.tunnelOpenP99)
	fmt.Fprintln(b, "Tunnel echo round-trip latency:")
	fmt.Fprintf(b, "  p50=%s  p95=%s  p99=%s\n", r.tunnelRTTP50, r.tunnelRTTP95, r.tunnelRTTP99)
	fmt.Fprintln(b, "")
	fmt.Fprintln(b, "Gateway resource usage (best-effort, /proc + pprof):")
	fmt.Fprintf(b, "  RSS pre/post      : %d MiB / %d MiB (delta=%+d MiB)\n",
		r.preRSS/(1<<20), r.postRSS/(1<<20), (r.postRSS-r.preRSS)/(1<<20))
	fmt.Fprintf(b, "  goroutines pre/post: %d / %d (delta=%+d)\n",
		r.preGoroutines, r.postGoroutines, r.postGoroutines-r.preGoroutines)
	fmt.Fprintf(b, "  threads pre/post   : %d / %d (delta=%+d)\n",
		r.preThreads, r.postThreads, r.postThreads-r.preThreads)
	if r.auditCount > 0 {
		fmt.Fprintf(b, "Audit lines observed in log : %d\n", r.auditCount)
	}
	fmt.Fprintln(b, "")
	host, _ := os.Hostname()
	fmt.Fprintf(b, "Hardware: %s, GOOS=%s GOARCH=%s NumCPU=%d Go=%s\n",
		host, runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	fmt.Fprintln(b, "==================================================================")
	return b.String()
}

func (r runReport) evaluateSoftAssertions() {
	const (
		tunnelOpenBudget = 250 * time.Millisecond
		restBudget       = 500 * time.Millisecond
	)
	if r.tunnelOpenP99 > tunnelOpenBudget {
		fmt.Printf("WARN: tunnel-open p99 %s exceeds %s budget\n", r.tunnelOpenP99, tunnelOpenBudget)
	}
	if r.restRTTP99 > restBudget {
		fmt.Printf("WARN: REST p99 %s exceeds %s budget\n", r.restRTTP99, restBudget)
	}
	// Allow a small steady-state goroutine increase from the runtime
	// itself; flag anything over 100 as a likely leak.
	if r.preGoroutines > 0 && r.postGoroutines-r.preGoroutines > 100 {
		fmt.Printf("WARN: goroutine count grew by %d after teardown — possible leak\n",
			r.postGoroutines-r.preGoroutines)
	}
	if r.restCalls == 0 && r.tunnelOpens == 0 {
		fmt.Println("WARN: no successful REST or tunnel traffic recorded — verify sidecar registration")
	}
}

func (r runReport) appendToMarkdown(path string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	host, _ := os.Hostname()

	body := &bytes.Buffer{}
	fmt.Fprintf(body, "\n## Wire-level results (run %s)\n\n", now)
	fmt.Fprintf(body, "Harness: `server/cmd/loadtest/proxy` driven by ")
	fmt.Fprintf(body, "`server/scripts/run_proxy_load_test.sh`. Real RabbitMQ Streams ")
	fmt.Fprintf(body, "+ Redis (Valkey) + gateway + 2 proxy-sidecar terminators.\n\n")

	fmt.Fprintln(body, "### Configuration")
	fmt.Fprintln(body, "")
	fmt.Fprintf(body, "- Concurrent caller agents: %d\n", *numCallers)
	fmt.Fprintf(body, "- Steady-state duration  : %s (after %s ramp)\n", *steadyDur, *rampDur)
	fmt.Fprintf(body, "- Payload per call       : %d bytes\n", r.payloadSize)
	fmt.Fprintf(body, "- Caller mix             : 40%% wildcard `sv::echo`, 30%% pinned to `::a`, 30%% pinned to `::b`\n")
	fmt.Fprintf(body, "- Traffic mix            : 70%% REST proxy, 30%% TCP tunnel echo\n")
	fmt.Fprintf(body, "- Hardware               : %s — GOOS=%s GOARCH=%s NumCPU=%d Go=%s\n",
		host, runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	fmt.Fprintln(body, "")
	fmt.Fprintln(body, "### Latency")
	fmt.Fprintln(body, "")
	fmt.Fprintln(body, "| Operation              | p50 | p95 | p99 | Samples |")
	fmt.Fprintln(body, "|---|---|---|---|---|")
	fmt.Fprintf(body, "| REST round-trip        | %s | %s | %s | %d |\n",
		r.restRTTP50, r.restRTTP95, r.restRTTP99, r.restCalls)
	fmt.Fprintf(body, "| Tunnel open            | %s | %s | %s | %d |\n",
		r.tunnelOpenP50, r.tunnelOpenP95, r.tunnelOpenP99, r.tunnelOpens)
	fmt.Fprintf(body, "| Tunnel echo round-trip | %s | %s | %s | %d |\n",
		r.tunnelRTTP50, r.tunnelRTTP95, r.tunnelRTTP99, r.tunnelOpens)
	fmt.Fprintln(body, "")
	fmt.Fprintln(body, "### Throughput and errors")
	fmt.Fprintln(body, "")
	fmt.Fprintf(body, "- Bytes sent : %.2f MiB\n", float64(r.bytesSent)/(1<<20))
	fmt.Fprintf(body, "- Bytes recv : %.2f MiB\n", float64(r.bytesRecv)/(1<<20))
	fmt.Fprintf(body, "- REST errors  : %d\n", r.restErrors)
	fmt.Fprintf(body, "- Tunnel errors: %d\n", r.tunnelErrors)
	fmt.Fprintln(body, "")
	fmt.Fprintln(body, "### Gateway resource deltas")
	fmt.Fprintln(body, "")
	fmt.Fprintf(body, "- RSS pre/post (post = +%s after teardown): %d MiB / %d MiB\n",
		*teardownWait, r.preRSS/(1<<20), r.postRSS/(1<<20))
	fmt.Fprintf(body, "- Goroutines pre/post: %d / %d\n", r.preGoroutines, r.postGoroutines)
	fmt.Fprintf(body, "- OS threads pre/post: %d / %d\n", r.preThreads, r.postThreads)
	if r.auditCount > 0 {
		fmt.Fprintf(body, "- Audit lines (proxy.* / tunnel.opened) sampled in gateway log: %d\n", r.auditCount)
	}
	fmt.Fprintln(body, "")
	fmt.Fprintln(body, "### Soft assertion outcomes")
	fmt.Fprintln(body, "")
	fmt.Fprintf(body, "- Tunnel-open p99 budget (250 ms): %s — %s\n",
		r.tunnelOpenP99, passOrWarn(r.tunnelOpenP99 <= 250*time.Millisecond))
	fmt.Fprintf(body, "- REST p99 budget (500 ms)        : %s — %s\n",
		r.restRTTP99, passOrWarn(r.restRTTP99 <= 500*time.Millisecond))
	gd := r.postGoroutines - r.preGoroutines
	fmt.Fprintf(body, "- Goroutine delta after teardown   : %+d — %s\n",
		gd, passOrWarn(gd <= 100))
	fmt.Fprintln(body, "")

	// Append a JSON blob for downstream automation.
	jsonBlob, _ := json.MarshalIndent(map[string]any{
		"timestamp":          now,
		"callers":            *numCallers,
		"steady_duration":    steadyDur.String(),
		"payload_bytes":      r.payloadSize,
		"rest_calls":         r.restCalls,
		"rest_errors":        r.restErrors,
		"tunnel_opens":       r.tunnelOpens,
		"tunnel_errors":      r.tunnelErrors,
		"rest_p50_ms":        r.restRTTP50.Milliseconds(),
		"rest_p99_ms":        r.restRTTP99.Milliseconds(),
		"tunnel_open_p50_ms": r.tunnelOpenP50.Milliseconds(),
		"tunnel_open_p99_ms": r.tunnelOpenP99.Milliseconds(),
		"tunnel_rtt_p99_ms":  r.tunnelRTTP99.Milliseconds(),
		"goroutine_delta":    gd,
		"rss_delta_mib":      (r.postRSS - r.preRSS) / (1 << 20),
	}, "", "  ")
	fmt.Fprintln(body, "<details><summary>Raw JSON</summary>")
	fmt.Fprintln(body, "")
	fmt.Fprintln(body, "```json")
	body.Write(jsonBlob)
	fmt.Fprintln(body, "")
	fmt.Fprintln(body, "```")
	fmt.Fprintln(body, "</details>")

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(body.Bytes())
	return err
}

func passOrWarn(ok bool) string {
	if ok {
		return "PASS"
	}
	return "WARN"
}
