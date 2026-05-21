// Routing fake-gateway + in-process HTTP/TCP backends + composite-mode
// proxy-sidecar Runner wiring used by the e2e tests.
//
// This file is NOT gated behind the e2e build tag because it carries
// helpers used by the test files (which are gated). The file builds
// against the default `go test ./...` invocation, but contains no
// `Test*` functions of its own — those live in the *_test.go siblings.

package integration_e2e

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/proxysidecar"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// debugEnabled toggles verbose harness tracing — set via E2E_DEBUG=1.
// Cheap, off in normal runs.
var debugEnabled = os.Getenv("E2E_DEBUG") == "1"

// debugLog dumps a debugger-only line to stderr when E2E_DEBUG=1.
func debugLog(format string, args ...any) {
	if !debugEnabled {
		return
	}
	fmt.Fprintf(os.Stderr, "[e2e-gw] "+format+"\n", args...)
}

// =============================================================================
// E2EHarness — public test fixture
// =============================================================================

// E2EHarness bundles every in-process service the e2e tests need:
//
//   - GatewayAddr is the TCP address (127.0.0.1:<port>) of the routing
//     fake-gateway that the SDK clients dial.
//   - RelayAddr is the TCP address of the proxy-sidecar relay surface
//     for direct sandbox-side attachment (not exercised by every test
//     but exposed for completeness).
//   - ServiceTopic is the sv::<impl>::<spec> address that targets the
//     sidecar runtime.
//   - HTTPBackendURL is the http://127.0.0.1:<port> address of the
//     in-process HTTP backend the terminator forwards to.
//   - TCPBackendAddr is the TCP echo server address for tunnel tests.
//   - SlowStreamDuration controls how long /slow drips for; defaults
//     to 5s and can be overridden by tests via the New constructor.
type E2EHarness struct {
	GatewayAddr        string
	RelayAddr          string
	ServiceTopic       string
	HTTPBackendURL     string
	TCPBackendAddr     string
	SlowStreamDuration time.Duration

	// Internals — exposed for advanced tests if needed.
	gateway *routingFakeGateway
}

// E2EHarnessOptions customises the harness. The zero value is sane.
type E2EHarnessOptions struct {
	// SlowStreamDuration is how long /slow drips data for. Defaults to
	// 5s — short enough to keep individual tests under 30s, long
	// enough to keep multiple streams in flight while fast calls fire.
	SlowStreamDuration time.Duration

	// Implementation / Specifier identify the sidecar runtime. Defaults
	// to "bp-sidecar" / "e2e".
	Implementation string
	Specifier      string
}

// NewE2EHarness brings up the full in-process stack and returns a
// harness handle. All resources are torn down via t.Cleanup.
func NewE2EHarness(t *testing.T, opts ...E2EHarnessOptions) *E2EHarness {
	t.Helper()

	o := E2EHarnessOptions{}
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.SlowStreamDuration == 0 {
		o.SlowStreamDuration = 5 * time.Second
	}
	if o.Implementation == "" {
		o.Implementation = "bp-sidecar"
	}
	if o.Specifier == "" {
		o.Specifier = "e2e"
	}

	// --- 1. Routing fake gateway --------------------------------------------
	gw, gwAddr := startRoutingFakeGateway(t)

	// --- 2. In-process HTTP backend -----------------------------------------
	backend := newHTTPBackend(t, o.SlowStreamDuration)

	// --- 3. In-process TCP echo backend -------------------------------------
	tcpAddr := startTCPEcho(t)

	// --- 4. Composite-mode proxy-sidecar Runner -----------------------------
	relayPath := filepath.Join(t.TempDir(), "relay.sock")
	relayListen := "unix://" + relayPath

	cfg := &proxysidecar.Config{
		Gateway: proxysidecar.GatewayConfig{
			Address:  gwAddr,
			Insecure: true,
		},
		Service: proxysidecar.ServiceConfig{
			Implementation: o.Implementation,
			Specifier:      o.Specifier,
		},
		Terminator: proxysidecar.TerminatorConfig{
			Enabled: true,
			Backends: []proxysidecar.BackendConfig{{
				Name:          "local",
				Kind:          proxysidecar.BackendKindHTTP,
				URL:           backend.URL,
				AllowPaths:    []string{"/*"},
				AllowMethods:  []string{"GET", "POST", "PUT", "DELETE"},
				MaxBodyBytes:  32 << 20, // 32 MiB headroom for chunked upload
				IdleTimeoutMs: 60_000,
				HeaderMode:    proxysidecar.HeaderModePassthrough,
			}, {
				Name:          "tcp-echo",
				Kind:          proxysidecar.BackendKindTCP,
				URL:           "tcp://" + tcpAddr,
				MaxBodyBytes:  32 << 20,
				IdleTimeoutMs: 60_000,
			}},
		},
		Relay: proxysidecar.RelayConfig{
			Enabled: true,
			Listen:  relayListen,
			AllowedOps: proxysidecar.AllowedOpsConfig{
				Profile: proxysidecar.AllowedOpsProfileSandboxTunnels,
				Set:     true,
			},
		},
		TenantID: "tenant-e2e",
	}

	runner, err := proxysidecar.NewRunner(cfg, "")
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	runnerDone := make(chan struct{})
	go func() {
		defer close(runnerDone)
		_ = runner.Run(runCtx)
	}()

	t.Cleanup(func() {
		runCancel()
		select {
		case <-runnerDone:
		case <-time.After(15 * time.Second):
			t.Logf("warning: runner did not exit within 15s of cancel")
		}
	})

	// --- 5. Wait for sidecar runtime to register on the fake gateway --------
	deadline := time.Now().Add(15 * time.Second)
	serviceTopic := fmt.Sprintf("sv::%s::%s", o.Implementation, o.Specifier)
	for time.Now().Before(deadline) {
		if gw.hasService(serviceTopic) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !gw.hasService(serviceTopic) {
		t.Fatalf("sidecar runtime never registered as %s on fake gateway", serviceTopic)
	}

	return &E2EHarness{
		GatewayAddr:        gwAddr,
		RelayAddr:          relayPath,
		ServiceTopic:       serviceTopic,
		HTTPBackendURL:     backend.URL,
		TCPBackendAddr:     tcpAddr,
		SlowStreamDuration: o.SlowStreamDuration,
		gateway:            gw,
	}
}

// =============================================================================
// routingFakeGateway — gRPC AetherGatewayServer that routes proxy /
// tunnel envelopes between SDK clients and the sidecar runtime.
// =============================================================================

// routingFakeGateway is a minimal stand-in for the production gateway.
// It receives Connect streams from any caller and:
//
//  1. records Service-typed clients (the sidecar runtime) so it can
//     forward inbound caller traffic to them by topic;
//  2. for caller-side clients (Agent etc.), routes ProxyHttpRequest /
//     ProxyHttpBodyChunk / TunnelOpen / TunnelData / TunnelAck /
//     TunnelClose envelopes to the Service whose topic matches the
//     envelope's target_topic;
//  3. routes inbound Service envelopes (ProxyHttpResponse, body chunks
//     in the response direction, tunnel data/ack/close) back to the
//     originating caller by request_id / tunnel_id.
//
// SendMessage envelopes are recorded but not delivered (the e2e tests
// do not need cross-agent message routing).
type routingFakeGateway struct {
	pb.UnimplementedAetherGatewayServer

	mu sync.Mutex
	// services keyed by their sv::<impl>::<spec> topic.
	services map[string]*gatewayClient
	// callers is the set of currently-attached non-Service clients.
	callers map[*gatewayClient]struct{}
	// requestRoutes maps ProxyHttpRequest.request_id → originating caller.
	requestRoutes map[string]*gatewayClient
	// tunnelRoutes maps TunnelOpen.tunnel_id → originating caller.
	tunnelRoutes map[string]*gatewayClient

	// totalSends is the count of SendMessage envelopes received (any
	// caller). Used by the priority-shed test to confirm best-effort
	// blast traffic actually reached the gateway.
	totalSends   atomic.Int64
	bestEffort   atomic.Int64
	highPriority atomic.Int64
}

func newRoutingFakeGateway() *routingFakeGateway {
	return &routingFakeGateway{
		services:      make(map[string]*gatewayClient),
		callers:       make(map[*gatewayClient]struct{}),
		requestRoutes: make(map[string]*gatewayClient),
		tunnelRoutes:  make(map[string]*gatewayClient),
	}
}

// gatewayClient is one attached gRPC Connect stream.
type gatewayClient struct {
	stream pb.AetherGateway_ConnectServer

	// identityTopic is the canonical topic for this client (sv::, ag::,
	// or empty for unidentified streams). Used as routing key.
	identityTopic string

	// sendMu serialises Send on the bidi stream (gRPC requires this).
	sendMu sync.Mutex
}

func (c *gatewayClient) send(msg *pb.DownstreamMessage) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.stream.Send(msg)
}

// hasService reports whether a Service with the given topic is connected.
func (g *routingFakeGateway) hasService(topic string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.services[topic]
	return ok
}

// SendStats returns a snapshot of SendMessage counters (total, best-
// effort by MessageType_METRIC, and high-priority by MessageType_CONTROL).
func (g *routingFakeGateway) SendStats() (total, bestEffort, highPriority int64) {
	return g.totalSends.Load(), g.bestEffort.Load(), g.highPriority.Load()
}

// Connect is the bidi handler the SDK and the sidecar runtime both call.
func (g *routingFakeGateway) Connect(stream pb.AetherGateway_ConnectServer) error {
	c := &gatewayClient{stream: stream}

	// Acknowledge connection so the runtime / SDK proceed with InitConnection.
	if err := c.send(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ConnectionAck{
			ConnectionAck: &pb.ConnectionAck{SessionId: "fake-gw-session"},
		},
	}); err != nil {
		return err
	}

	defer g.cleanupClient(c)

	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		g.dispatchUpstream(c, msg)
	}
}

func (g *routingFakeGateway) cleanupClient(c *gatewayClient) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c.identityTopic != "" {
		if existing, ok := g.services[c.identityTopic]; ok && existing == c {
			delete(g.services, c.identityTopic)
		}
	}
	delete(g.callers, c)
	for rid, owner := range g.requestRoutes {
		if owner == c {
			delete(g.requestRoutes, rid)
		}
	}
	for tid, owner := range g.tunnelRoutes {
		if owner == c {
			delete(g.tunnelRoutes, tid)
		}
	}
}

// dispatchUpstream applies routing rules to one inbound upstream
// envelope. Service-originated frames flow caller-ward; caller-
// originated frames flow service-ward.
func (g *routingFakeGateway) dispatchUpstream(c *gatewayClient, msg *pb.UpstreamMessage) {
	debugLog("dispatchUpstream from=%q payload=%T", c.identityTopic, msg.GetPayload())
	switch p := msg.GetPayload().(type) {

	case *pb.UpstreamMessage_Init:
		g.handleInit(c, p.Init)

	case *pb.UpstreamMessage_ProxyHttpRequest:
		g.routeProxyRequest(c, p.ProxyHttpRequest)

	case *pb.UpstreamMessage_ProxyHttpBodyChunk:
		g.routeProxyBodyChunk(c, p.ProxyHttpBodyChunk)

	case *pb.UpstreamMessage_ProxyHttpResponse:
		// Sent by the sidecar runtime in response to a proxy request.
		g.routeProxyResponse(p.ProxyHttpResponse)

	case *pb.UpstreamMessage_TunnelOpen:
		g.routeTunnelOpen(c, p.TunnelOpen)

	case *pb.UpstreamMessage_TunnelData:
		g.routeTunnelData(c, p.TunnelData)

	case *pb.UpstreamMessage_TunnelAck:
		g.routeTunnelAck(c, p.TunnelAck)

	case *pb.UpstreamMessage_TunnelClose:
		g.routeTunnelClose(c, p.TunnelClose)

	case *pb.UpstreamMessage_Send:
		// Used by the priority-shed test as a high-volume best-effort
		// blast against the runtime's send pipeline.
		g.totalSends.Add(1)
		switch p.Send.GetMessageType() {
		case pb.MessageType_METRIC, pb.MessageType_EVENT:
			g.bestEffort.Add(1)
		case pb.MessageType_CONTROL:
			g.highPriority.Add(1)
		}
	}
}

func (g *routingFakeGateway) handleInit(c *gatewayClient, init *pb.InitConnection) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if svc, ok := init.GetClientType().(*pb.InitConnection_Service); ok {
		topic := fmt.Sprintf("sv::%s::%s",
			svc.Service.GetImplementation(), svc.Service.GetSpecifier())
		c.identityTopic = topic
		g.services[topic] = c
		debugLog("handleInit Service topic=%s", topic)
		return
	}
	if ag, ok := init.GetClientType().(*pb.InitConnection_Agent); ok {
		topic := fmt.Sprintf("ag::%s::%s::%s",
			ag.Agent.GetWorkspace(),
			ag.Agent.GetImplementation(),
			ag.Agent.GetSpecifier())
		c.identityTopic = topic
		g.callers[c] = struct{}{}
		debugLog("handleInit Agent topic=%s", topic)
		return
	}
	g.callers[c] = struct{}{}
	debugLog("handleInit unidentified client")
}

// serviceFor resolves a service target_topic to the connected Service
// client. Supports wildcard sv::<impl> (picks any matching service).
func (g *routingFakeGateway) serviceFor(target string) *gatewayClient {
	g.mu.Lock()
	defer g.mu.Unlock()

	if svc, ok := g.services[target]; ok {
		return svc
	}
	// Wildcard sv::<impl> — first matching concrete.
	if !strings.HasPrefix(target, "sv::") {
		return nil
	}
	rest := strings.TrimPrefix(target, "sv::")
	for topic, svc := range g.services {
		if strings.HasPrefix(strings.TrimPrefix(topic, "sv::"), rest+"::") {
			return svc
		}
	}
	return nil
}

func (g *routingFakeGateway) routeProxyRequest(caller *gatewayClient, req *pb.ProxyHttpRequest) {
	svc := g.serviceFor(req.GetTargetTopic())
	debugLog("routeProxyRequest target=%s req_id=%s svc_found=%v", req.GetTargetTopic(), req.GetRequestId(), svc != nil)
	if svc == nil {
		_ = caller.send(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_ProxyHttpResponse{
				ProxyHttpResponse: &pb.ProxyHttpResponse{
					RequestId: req.GetRequestId(),
					Error: &pb.ProxyError{
						Kind:    pb.ProxyError_SIDECAR_UNAVAILABLE,
						Message: fmt.Sprintf("no service registered for %s", req.GetTargetTopic()),
					},
				},
			},
		})
		return
	}
	g.mu.Lock()
	g.requestRoutes[req.GetRequestId()] = caller
	g.mu.Unlock()
	_ = svc.send(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ProxyHttpRequest{ProxyHttpRequest: req},
	})
}

func (g *routingFakeGateway) routeProxyBodyChunk(caller *gatewayClient, chunk *pb.ProxyHttpBodyChunk) {
	// Request-direction chunks (is_request=true) follow the same target as
	// the originating ProxyHttpRequest. Caller-side issues these for
	// chunked uploads. Response-direction chunks (is_request=false) flow
	// from the service back to the caller; route by request_id.
	if chunk.GetIsRequest() {
		// Caller → Service: look up the request route to find the service.
		g.mu.Lock()
		owner := g.requestRoutes[chunk.GetRequestId()]
		g.mu.Unlock()
		if owner != caller {
			// Either no route or sent by the wrong caller — drop silently.
			return
		}
		// We don't track per-request-id which service was chosen; in
		// practice every request in our tests goes to the singleton
		// sidecar service. Resolve by scanning services for any (the
		// terminator-only test stack has exactly one).
		g.mu.Lock()
		var svc *gatewayClient
		for _, s := range g.services {
			svc = s
			break
		}
		g.mu.Unlock()
		if svc == nil {
			return
		}
		_ = svc.send(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_ProxyHttpBodyChunk{ProxyHttpBodyChunk: chunk},
		})
		return
	}
	// Service → Caller response-direction body chunk.
	g.mu.Lock()
	owner := g.requestRoutes[chunk.GetRequestId()]
	if chunk.GetFin() {
		delete(g.requestRoutes, chunk.GetRequestId())
	}
	g.mu.Unlock()
	if owner == nil {
		return
	}
	_ = owner.send(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ProxyHttpBodyChunk{ProxyHttpBodyChunk: chunk},
	})
}

func (g *routingFakeGateway) routeProxyResponse(resp *pb.ProxyHttpResponse) {
	g.mu.Lock()
	owner := g.requestRoutes[resp.GetRequestId()]
	if !resp.GetBodyChunked() {
		delete(g.requestRoutes, resp.GetRequestId())
	}
	g.mu.Unlock()
	debugLog("routeProxyResponse req_id=%s owner=%v status=%d chunked=%v",
		resp.GetRequestId(), owner != nil, resp.GetStatusCode(), resp.GetBodyChunked())
	if owner == nil {
		return
	}
	_ = owner.send(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ProxyHttpResponse{ProxyHttpResponse: resp},
	})
}

func (g *routingFakeGateway) routeTunnelOpen(caller *gatewayClient, open *pb.TunnelOpen) {
	debugLog("routeTunnelOpen tunnel_id=%s target=%s proto=%v backend=%q",
		open.GetTunnelId(), open.GetTargetTopic(), open.GetProtocol(), open.GetBackendName())
	svc := g.serviceFor(open.GetTargetTopic())
	if svc == nil {
		_ = caller.send(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_TunnelClose{
				TunnelClose: &pb.TunnelClose{
					TunnelId: open.GetTunnelId(),
					Reason:   pb.TunnelClose_ERROR,
					Detail:   fmt.Sprintf("no service registered for %s", open.GetTargetTopic()),
				},
			},
		})
		return
	}
	g.mu.Lock()
	g.tunnelRoutes[open.GetTunnelId()] = caller
	g.mu.Unlock()

	// The sidecar's runtime receives TunnelOpen as a TunnelData with
	// seq=0 whose payload is the marshalled TunnelOpen — see
	// runner.go::tunnelDataIsOpen for the decode side. Wrap accordingly.
	openBytes, err := proto.Marshal(open)
	if err != nil {
		_ = caller.send(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_TunnelClose{
				TunnelClose: &pb.TunnelClose{
					TunnelId: open.GetTunnelId(),
					Reason:   pb.TunnelClose_ERROR,
					Detail:   "fake-gw: marshal TunnelOpen failed: " + err.Error(),
				},
			},
		})
		return
	}
	_ = svc.send(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_TunnelData{
			TunnelData: &pb.TunnelData{
				TunnelId: open.GetTunnelId(),
				Seq:      0,
				Data:     openBytes,
			},
		},
	})
}

func (g *routingFakeGateway) routeTunnelData(from *gatewayClient, data *pb.TunnelData) {
	g.mu.Lock()
	caller := g.tunnelRoutes[data.GetTunnelId()]
	svc := g.serviceForTunnelLocked()
	g.mu.Unlock()

	fromIsCaller := caller != nil && from == caller
	fromIsService := svc != nil && from == svc
	debugLog("routeTunnelData tunnel_id=%s seq=%d fin=%v bytes=%d from_caller=%v from_service=%v caller=%v svc=%v",
		data.GetTunnelId(), data.GetSeq(), data.GetFin(), len(data.GetData()),
		fromIsCaller, fromIsService, caller != nil, svc != nil)

	// If `from` is the caller, forward to service. If from is the
	// service, forward to caller.
	if fromIsCaller && svc != nil {
		_ = svc.send(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_TunnelData{TunnelData: data},
		})
		return
	}
	if fromIsService && caller != nil {
		_ = caller.send(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_TunnelData{TunnelData: data},
		})
		return
	}
	debugLog("routeTunnelData: DROPPED — no valid route")
}

// serviceForTunnelLocked returns one connected service (caller must
// hold g.mu). For the e2e tests we have a single service; this is
// sufficient for routing tunnel frames in the absence of a per-tunnel
// service binding map.
func (g *routingFakeGateway) serviceForTunnelLocked() *gatewayClient {
	for _, s := range g.services {
		return s
	}
	return nil
}

func (g *routingFakeGateway) routeTunnelAck(from *gatewayClient, ack *pb.TunnelAck) {
	g.mu.Lock()
	caller := g.tunnelRoutes[ack.GetTunnelId()]
	svc := g.serviceForTunnelLocked()
	g.mu.Unlock()
	fromIsCaller := caller != nil && from == caller
	fromIsService := svc != nil && from == svc
	if fromIsCaller && svc != nil {
		_ = svc.send(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_TunnelAck{TunnelAck: ack},
		})
		return
	}
	if fromIsService && caller != nil {
		_ = caller.send(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_TunnelAck{TunnelAck: ack},
		})
	}
}

func (g *routingFakeGateway) routeTunnelClose(from *gatewayClient, tc *pb.TunnelClose) {
	g.mu.Lock()
	caller := g.tunnelRoutes[tc.GetTunnelId()]
	svc := g.serviceForTunnelLocked()
	delete(g.tunnelRoutes, tc.GetTunnelId())
	g.mu.Unlock()
	fromIsCaller := caller != nil && from == caller
	fromIsService := svc != nil && from == svc
	if fromIsCaller && svc != nil {
		_ = svc.send(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_TunnelClose{TunnelClose: tc},
		})
		return
	}
	if fromIsService && caller != nil {
		_ = caller.send(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_TunnelClose{TunnelClose: tc},
		})
	}
}

// startRoutingFakeGateway listens on 127.0.0.1:0 and registers a
// routingFakeGateway. Returns the gateway and its address. Cleanup is
// wired via t.Cleanup.
func startRoutingFakeGateway(t *testing.T) (*routingFakeGateway, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("gateway listen: %v", err)
	}
	gw := newRoutingFakeGateway()
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(16*1024*1024),
		grpc.MaxSendMsgSize(16*1024*1024),
	)
	pb.RegisterAetherGatewayServer(srv, gw)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.GracefulStop()
		_ = lis.Close()
	})
	return gw, lis.Addr().String()
}

// =============================================================================
// In-process HTTP backend with /slow, /fast, /echo handlers
// =============================================================================

// httpBackend wraps an *httptest.Server with handlers the e2e tests
// rely on.
type httpBackend struct {
	*httptest.Server
}

func newHTTPBackend(t *testing.T, slowDur time.Duration) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	// /fast — immediate {"ok":true} response.
	mux.HandleFunc("/fast", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{"ok":true}`)
	})

	// /slow — chunked SSE drip for slowDur. Each tick emits one event.
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		const tickInterval = 200 * time.Millisecond
		end := time.Now().Add(slowDur)
		i := 0
		for time.Now().Before(end) {
			if _, err := fmt.Fprintf(w, "data: ping %d\n\n", i); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			i++
			select {
			case <-r.Context().Done():
				return
			case <-time.After(tickInterval):
			}
		}
	})

	// /echo — POST body echoed back verbatim.
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		w.Header().Set("Content-Type", r.Header.Get("Content-Type"))
		w.WriteHeader(200)
		_, _ = w.Write(body)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// =============================================================================
// In-process TCP echo server
// =============================================================================

// startTCPEcho boots a TCP listener that echoes incoming bytes back on
// the same connection. Returns the listener's host:port.
func startTCPEcho(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("tcp echo listen: %v", err)
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
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

// =============================================================================
// gRPC dial helper
// =============================================================================

// dialGateway dials the fake gateway with insecure credentials. Caller
// owns the returned conn (defer conn.Close()).
func dialGateway(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// readSSE drains a chunked SSE response body and returns the number of
// "data:" events observed plus any read error.
func readSSE(body io.Reader) (int, error) {
	br := bufio.NewReader(body)
	events := 0
	for {
		line, err := br.ReadString('\n')
		if len(line) > 0 && strings.HasPrefix(line, "data:") {
			events++
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return events, nil
			}
			return events, err
		}
	}
}
