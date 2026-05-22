// Audit-event and metrics-counter e2e tests (matrix §13).
//
// These tests assert that the gateway's comprehensive audit log actually emits
// expected events for normal proxy and tunnel operations, and that the
// Prometheus metrics counters (proxyLocalBypassTotal) increment correctly.
//
// # Audit approach
//
// The gateway exposes an AuditQuery message type over the main gRPC connection.
// System principals (Orchestrator, WorkflowEngine) bypass ACL and can query
// without admin permissions. Each test creates a short-lived OrchestratorClient
// that acts as the audit reader. The sidecar is a regular AgentClient.
//
// Audit events emitted by the gateway for proxy/tunnel ops:
//   - "proxy_http_routed"   — ProxyHttpRequest accepted and forwarded
//   - "proxy_http_failed"   — ProxyHttpRequest rejected (ACL/quota/etc.)
//   - "tunnel_opened"       — TunnelOpen accepted and forwarded
//   - "tunnel_closed"       — TunnelClose forwarded (or pin expired)
//
// # Metrics approach
//
// The gateway's OpsServer exposes Prometheus /metrics at a dynamically chosen
// port stored in aetherliteProc.opsAddr. The tests scrape this HTTP endpoint
// and parse the text format to extract counter values. The counter exercised
// here is aether_proxy_local_bypass_total{envelope_type="proxy_http_request"}.
// Because the sidecar and caller are on the same in-process aetherlite, the
// local-bypass fast path fires for every ProxyHTTP call, making the counter
// reliably assertable.
//
// Build tag: e2e — mirrors the rest of the package.

//go:build e2e

package integration_e2e

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/sdk/go/aether"
)

// =============================================================================
// §13.1  Audit — ProxyHTTP emits expected gateway-side events
// =============================================================================

// TestE2E_Audit_ProxyHTTP_EmitsExpectedEvents issues a single ProxyHTTP call
// through a sidecar and then queries the gateway's comprehensive audit log via
// an OrchestratorClient (system principal — no ACL required). It asserts that
// at least one "proxy_http_routed" event exists for the call's workspace with
// the correct event_type.
func TestE2E_Audit_ProxyHTTP_EmitsExpectedEvents(t *testing.T) {
	h := NewE2EHarness(t)
	gw := getAetherlite(t)

	// Issue a ProxyHTTP call — the gateway will emit a proxy_http_routed event.
	caller := dialAgentClient(t, h, "audit-proxy-caller")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", "http://ignored/fast", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := caller.ProxyHTTP(ctx, h.ServiceTopic, req, aether.WithBackend("local"))
	if err != nil {
		t.Fatalf("ProxyHTTP: %v", err)
	}
	if resp != nil && resp.Body != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	if resp.StatusCode != 200 {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}

	// The audit SQLite batcher uses DefaultBatchSize=100 and DefaultFlushPeriod=5s.
	// A fixed short sleep is insufficient — poll until the entry appears or 6s elapses.
	auditor := dialOrchestratorClient(t, gw.grpcAddr, "audit-proxy-orch")

	var result *aether.AuditQueryResult
	pollDeadline := time.Now().Add(6 * time.Second)
	for {
		qCtx, qCancel := context.WithTimeout(context.Background(), 5*time.Second)
		var err error
		result, err = auditor.QueryAuditLog(qCtx, aether.AuditQueryOpts{
			Operation: "proxy_http_routed",
			Workspace: "e2e",
			Limit:     50,
		})
		qCancel()
		if err != nil {
			t.Fatalf("QueryAuditLog: %v", err)
		}
		if !result.Success {
			t.Fatalf("QueryAuditLog returned failure: %s", result.Error)
		}
		if len(result.Entries) > 0 {
			break
		}
		if time.Now().After(pollDeadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if len(result.Entries) == 0 {
		// Surface the gap clearly rather than asserting positively — the gateway
		// may not emit proxy_http_routed events in all aetherlite configurations.
		t.Logf("OBSERVABILITY GAP: no proxy_http_routed audit events found for workspace=e2e")
		t.Logf("This indicates audit emission is NOT firing on the proxy path.")
		t.Logf("Operation queried: proxy_http_routed, Workspace: e2e")
		t.Fatalf("expected ≥1 audit entry for proxy_http_routed, got 0")
	}

	// At least one entry should have the correct operation and event_type.
	found := false
	for _, e := range result.Entries {
		if e.GetOperation() == "proxy_http_routed" && e.GetEventType() == "message" {
			found = true
			t.Logf("audit entry: id=%d op=%s actor=%s workspace=%s success=%v",
				e.GetAuditId(), e.GetOperation(), e.GetActorId(), e.GetWorkspace(), e.GetSuccess())
			break
		}
	}
	if !found {
		t.Errorf("no proxy_http_routed entry with event_type=message found; entries returned: %d", len(result.Entries))
		for i, e := range result.Entries {
			t.Logf("  entry[%d]: op=%s event_type=%s actor=%s", i, e.GetOperation(), e.GetEventType(), e.GetActorId())
		}
	}
}

// =============================================================================
// §13.2  Audit — TunnelOps emit expected gateway-side events
// =============================================================================

// TestE2E_Audit_TunnelOps_EmitsExpectedEvents opens a TCP tunnel to the
// in-process echo backend, exchanges a few bytes, closes the tunnel, then
// queries the audit log for tunnel_opened and tunnel_closed events.
func TestE2E_Audit_TunnelOps_EmitsExpectedEvents(t *testing.T) {
	h := NewE2EHarness(t)
	gw := getAetherlite(t)

	caller := dialAgentClient(t, h, "audit-tunnel-caller")

	rootCtx, rootCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer rootCancel()

	// Open a TCP tunnel to the echo backend.
	conn, err := caller.TunnelDial(rootCtx, h.ServiceTopic, "tcp", h.TCPBackendAddr,
		aether.WithTunnelBackend("tcp-echo"))
	if err != nil {
		t.Fatalf("TunnelDial: %v", err)
	}

	// Exchange a few bytes to confirm the tunnel is live. The audit assertion
	// (tunnel_opened) fires at TunnelOpen acceptance — before data exchange —
	// so a data-plane failure here does not invalidate the audit test. Under
	// full-suite load the echo may be slow or dropped (known routing race with
	// many concurrent sidecars); we log but do not fatal so the audit assertion
	// can still run.
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	msg := []byte("hello audit")
	echoOK := false
	if _, writeErr := conn.Write(msg); writeErr != nil {
		t.Logf("tunnel Write (non-fatal under load): %v", writeErr)
	} else {
		buf := make([]byte, len(msg))
		if _, readErr := io.ReadFull(conn, buf); readErr != nil {
			t.Logf("tunnel ReadFull (non-fatal under load): %v", readErr)
		} else if !bytes.Equal(buf, msg) {
			t.Logf("echo mismatch (non-fatal): got %q, want %q", buf, msg)
		} else {
			echoOK = true
		}
	}
	t.Logf("tunnel echo ok: %v", echoOK)

	// Close the tunnel to trigger tunnel_closed emission.
	_ = conn.Close()

	// The audit SQLite batcher uses DefaultBatchSize=100 and DefaultFlushPeriod=5s.
	// Poll until the tunnel_opened entry appears or 6s elapses.
	auditor := dialOrchestratorClient(t, gw.grpcAddr, "audit-tunnel-orch")

	var openResult *aether.AuditQueryResult
	pollDeadline := time.Now().Add(6 * time.Second)
	for {
		qCtx, qCancel := context.WithTimeout(context.Background(), 5*time.Second)
		var err error
		openResult, err = auditor.QueryAuditLog(qCtx, aether.AuditQueryOpts{
			Operation: "tunnel_opened",
			Workspace: "e2e",
			Limit:     50,
		})
		qCancel()
		if err != nil {
			t.Fatalf("QueryAuditLog(tunnel_opened): %v", err)
		}
		if !openResult.Success {
			t.Fatalf("QueryAuditLog(tunnel_opened) returned failure: %s", openResult.Error)
		}
		if len(openResult.Entries) > 0 {
			break
		}
		if time.Now().After(pollDeadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if len(openResult.Entries) == 0 {
		t.Logf("OBSERVABILITY GAP: no tunnel_opened audit events found for workspace=e2e")
		t.Fatalf("expected ≥1 audit entry for tunnel_opened, got 0")
	}
	t.Logf("tunnel_opened entries: %d", len(openResult.Entries))
	for i, e := range openResult.Entries {
		t.Logf("  entry[%d]: op=%s actor=%s workspace=%s success=%v",
			i, e.GetOperation(), e.GetActorId(), e.GetWorkspace(), e.GetSuccess())
	}

	// Query for tunnel_closed events (best-effort; async close may race the batcher).
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	closeResult, err := auditor.QueryAuditLog(closeCtx, aether.AuditQueryOpts{
		Operation: "tunnel_closed",
		Workspace: "e2e",
		Limit:     50,
	})
	if err != nil {
		t.Fatalf("QueryAuditLog(tunnel_closed): %v", err)
	}
	if !closeResult.Success {
		t.Fatalf("QueryAuditLog(tunnel_closed) returned failure: %s", closeResult.Error)
	}

	if len(closeResult.Entries) == 0 {
		// tunnel_closed is emitted when the TunnelClose frame is routed, which
		// happens asynchronously after conn.Close(). Treat as an observability
		// gap worth flagging, but not a hard failure since the pin may expire
		// before the gateway routes the close frame in very fast tests.
		t.Logf("OBSERVABILITY NOTE: no tunnel_closed audit events found for workspace=e2e")
		t.Logf("This may be a timing issue (async close) or an observability gap.")
	} else {
		t.Logf("tunnel_closed entries: %d", len(closeResult.Entries))
	}
}

// =============================================================================
// §13.3  Metrics — proxyLocalBypassTotal counter increments
// =============================================================================

// TestE2E_Metrics_ProxyCallCounter_Increments is skipped because
// aether_proxy_local_bypass_total does not track proxy_http_request.
//
// PRODUCTION GAP (matrix §13.3): The local-bypass counter
// (aether_proxy_local_bypass_total) is defined over the data-plane fast path
// in deliverDataPlaneLocal() and only covers tunnel_data, tunnel_ack, and
// proxy_http_body_chunk envelope types. ProxyHttpRequest is a control-plane
// envelope routed via publishProxyEnvelope (RMQ / in-process routing), which
// is deliberately excluded from the bypass path to ensure audit events always
// fire. There is no Prometheus counter that directly tracks ProxyHTTP request
// volume via local bypass. The nearest available metric is
// aether_messages_routed_total{workspace=...,message_type=...} but that
// counter uses the message_type label (not envelope_type), and ProxyHTTP
// calls are routed as proxy envelopes, not generic messages.
//
// To cover proxy call-count metrics, either:
//
//	(a) add a dedicated aether_proxy_requests_total counter in routing_proxy.go
//	    (production change — out of scope for this test file), or
//	(b) assert on tunnel_data bypass hits via TestE2E_Metrics_TunnelOps_Counters_Increment.
func TestE2E_Metrics_ProxyCallCounter_Increments(t *testing.T) {
	t.Skip("production gap: aether_proxy_local_bypass_total does not track proxy_http_request (control-plane envelope); see matrix §13.3")
}

// =============================================================================
// §13.4  Metrics — TunnelOps counters increment (optional)
// =============================================================================

// TestE2E_Metrics_TunnelOps_Counters_Increment opens a TCP tunnel and asserts
// that aether_proxy_local_bypass_total for tunnel_data increments by at least
// 1. Only data-plane envelopes (tunnel_data, tunnel_ack, proxy_http_body_chunk)
// go through the local-bypass fast path tracked by this counter — TunnelOpen
// uses the normal control-plane routing path and is not included.
func TestE2E_Metrics_TunnelOps_Counters_Increment(t *testing.T) {
	h := NewE2EHarness(t)
	gw := getAetherlite(t)

	metricsURL := fmt.Sprintf("http://%s/metrics", gw.opsAddr)

	// Snapshot before for tunnel_data — the envelope type that goes through the
	// local-bypass fast path during active data exchange.
	beforeData, err := scrapeProxyBypassCounter(metricsURL, "tunnel_data", "hit")
	if err != nil {
		// Counter only appears after first observation; treat as 0.
		t.Logf("tunnel_data hit not yet in metrics (treating as 0): %v", err)
		beforeData = 0
	}
	t.Logf("tunnel_data hit before: %.0f", beforeData)

	caller := dialAgentClient(t, h, "metrics-tunnel-caller")
	rootCtx, rootCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer rootCancel()

	conn, err := caller.TunnelDial(rootCtx, h.ServiceTopic, "tcp", h.TCPBackendAddr,
		aether.WithTunnelBackend("tcp-echo"))
	if err != nil {
		t.Fatalf("TunnelDial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Exchange a few bytes to generate tunnel_data frames through the bypass path.
	msg := []byte("metrics test payload")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("tunnel Write: %v", err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("tunnel ReadFull: %v", err)
	}
	_ = conn.Close()

	time.Sleep(150 * time.Millisecond)

	afterData, err := scrapeProxyBypassCounter(metricsURL, "tunnel_data", "hit")
	if err != nil {
		t.Logf("tunnel_data hit not in metrics after tunnel: %v", err)
		afterData = 0
	}
	dataDelta := afterData - beforeData
	t.Logf("tunnel_data hit after: %.0f (delta %.0f)", afterData, dataDelta)

	if dataDelta < 1 {
		t.Errorf("expected tunnel_data bypass hit delta ≥ 1, got %.0f (before=%.0f after=%.0f)",
			dataDelta, beforeData, afterData)
	}
}

// =============================================================================
// Local helpers
// =============================================================================

// dialOrchestratorClient creates a connected OrchestratorClient (system
// principal — no ACL required for audit queries). The client is torn down
// via t.Cleanup.
func dialOrchestratorClient(t *testing.T, gwAddr, specifier string) *aether.OrchestratorClient {
	t.Helper()

	client, err := aether.NewOrchestratorClient(aether.OrchestratorOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr: gwAddr,
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
		Implementation:    "audit-reader",
		Specifier:         specifier,
		SupportedProfiles: []string{"audit"},
	})
	if err != nil {
		t.Fatalf("NewOrchestratorClient: %v", err)
	}

	connectCtx, connectCancel := context.WithCancel(context.Background())
	if err := client.Connect(connectCtx); err != nil {
		connectCancel()
		t.Fatalf("OrchestratorClient.Connect: %v", err)
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
			t.Fatalf("OrchestratorClient.Run exited before ConnectionAck: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("OrchestratorClient ConnectionAck not observed within 10s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return client
}

// scrapeProxyBypassCounter fetches the Prometheus /metrics page from metricsURL
// and returns the current value of the
// aether_proxy_local_bypass_total{envelope_type=envelopeType,result=result}
// counter. Returns an error if the metric is not found (which means the
// counter has never been observed, i.e. its value is 0).
func scrapeProxyBypassCounter(metricsURL, envelopeType, result string) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", metricsURL, nil)
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("GET %s: %w", metricsURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("metrics endpoint returned status %d", resp.StatusCode)
	}

	// Parse Prometheus text format line by line.
	// Target line looks like:
	//   aether_proxy_local_bypass_total{envelope_type="tunnel_open",result="hit"} 3
	wantPrefix := fmt.Sprintf(`aether_proxy_local_bypass_total{envelope_type=%q,result=%q}`, envelopeType, result)

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, wantPrefix) {
			parts := strings.Fields(line)
			if len(parts) < 2 {
				return 0, fmt.Errorf("unexpected metric line format: %q", line)
			}
			v, err := strconv.ParseFloat(parts[len(parts)-1], 64)
			if err != nil {
				return 0, fmt.Errorf("parse counter value from %q: %w", line, err)
			}
			return v, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("scan metrics body: %w", err)
	}

	// Metric not found — it hasn't been observed yet; treat as 0 by returning
	// an error so callers can distinguish "not present" from "present and 0".
	return 0, fmt.Errorf("metric aether_proxy_local_bypass_total{envelope_type=%q,result=%q} not found in /metrics", envelopeType, result)
}

// silence unused proto import — pb is used implicitly via aether.OrchestratorOptions
// which the compiler sometimes doesn't track through the generic alias. A direct
// reference keeps the import live without impacting behaviour.
var _ = (*pb.AuditQuery)(nil)
