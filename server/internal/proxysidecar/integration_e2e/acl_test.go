//go:build e2e

package integration_e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/proxysidecar"
	"github.com/scitrera/aether/sdk/go/aether"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// echoBackendForACL spawns a minimal HTTP backend that always succeeds.
// Returned alongside its t.Cleanup-registered teardown via httptest.
func echoBackendForACL(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestE2E_ACL_DeniedPath_ReturnsError configures a sidecar with
// AllowPaths: ["/echo"] and verifies that a request to /forbidden
// surfaces as a ProxyTransportError of kind ACL_DENIED — not a hang and
// not a 200.
func TestE2E_ACL_DeniedPath_ReturnsError(t *testing.T) {
	srv := echoBackendForACL(t)
	backends := []proxysidecar.BackendConfig{{
		Name:          "scoped",
		Kind:          proxysidecar.BackendKindHTTP,
		URL:           srv.URL,
		AllowPaths:    []string{"/echo"},
		AllowMethods:  []string{"GET", "POST"},
		MaxBodyBytes:  1 << 20,
		IdleTimeoutMs: 5_000,
		HeaderMode:    proxysidecar.HeaderModePassthrough,
	}}
	h := newCustomSidecarHarness(t, "bp-sidecar", "acl-path", backends)
	client := dialAgentClientForAddr(t, h.GatewayAddr, "acl-path-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Sanity: /echo is allowed.
	allowReq, _ := http.NewRequestWithContext(ctx, "GET", "http://ignored/echo", nil)
	allowResp, err := client.ProxyHTTP(ctx, h.ServiceTopic, allowReq, aether.WithBackend("scoped"))
	if err != nil {
		t.Fatalf("control: /echo unexpectedly errored: %v", err)
	}
	_ = allowResp.Body.Close()
	if allowResp.StatusCode != 200 {
		t.Errorf("control /echo status=%d, want 200", allowResp.StatusCode)
	}

	// Now /forbidden — should be ACL_DENIED.
	denyReq, _ := http.NewRequestWithContext(ctx, "GET", "http://ignored/forbidden", nil)
	start := time.Now()
	denyResp, err := client.ProxyHTTP(ctx, h.ServiceTopic, denyReq, aether.WithBackend("scoped"))
	elapsed := time.Since(start)
	if err == nil {
		_ = denyResp.Body.Close()
		t.Fatalf("expected ACL_DENIED for /forbidden, got status=%d after %s", denyResp.StatusCode, elapsed)
	}
	if elapsed > 5*time.Second {
		t.Errorf("/forbidden took %s; expected immediate ACL denial", elapsed)
	}
	var pe *aether.ProxyTransportError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *ProxyTransportError, got %T: %v", err, err)
	}
	if pe.Kind != pb.ProxyError_ACL_DENIED.String() {
		t.Errorf("ProxyError kind=%q, want %q (msg=%q)",
			pe.Kind, pb.ProxyError_ACL_DENIED.String(), pe.Message)
	}
}

// TestE2E_ACL_DeniedMethod_ReturnsError configures a sidecar that
// permits only GET. A POST should surface as an ACL_DENIED
// ProxyTransportError.
func TestE2E_ACL_DeniedMethod_ReturnsError(t *testing.T) {
	srv := echoBackendForACL(t)
	backends := []proxysidecar.BackendConfig{{
		Name:          "get-only",
		Kind:          proxysidecar.BackendKindHTTP,
		URL:           srv.URL,
		AllowPaths:    []string{"/*"},
		AllowMethods:  []string{"GET"},
		MaxBodyBytes:  1 << 20,
		IdleTimeoutMs: 5_000,
		HeaderMode:    proxysidecar.HeaderModePassthrough,
	}}
	h := newCustomSidecarHarness(t, "bp-sidecar", "acl-method", backends)
	client := dialAgentClientForAddr(t, h.GatewayAddr, "acl-method-caller")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Sanity: GET is allowed.
	getReq, _ := http.NewRequestWithContext(ctx, "GET", "http://ignored/anything", nil)
	getResp, err := client.ProxyHTTP(ctx, h.ServiceTopic, getReq, aether.WithBackend("get-only"))
	if err != nil {
		t.Fatalf("control GET unexpectedly errored: %v", err)
	}
	_ = getResp.Body.Close()
	if getResp.StatusCode != 200 {
		t.Errorf("control GET status=%d, want 200", getResp.StatusCode)
	}

	// POST should be denied.
	postReq, _ := http.NewRequestWithContext(ctx, "POST", "http://ignored/anything", http.NoBody)
	start := time.Now()
	postResp, err := client.ProxyHTTP(ctx, h.ServiceTopic, postReq, aether.WithBackend("get-only"))
	elapsed := time.Since(start)
	if err == nil {
		_ = postResp.Body.Close()
		t.Fatalf("expected ACL_DENIED for POST, got status=%d after %s", postResp.StatusCode, elapsed)
	}
	if elapsed > 5*time.Second {
		t.Errorf("POST denial took %s; expected immediate", elapsed)
	}
	var pe *aether.ProxyTransportError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *ProxyTransportError, got %T: %v", err, err)
	}
	if pe.Kind != pb.ProxyError_ACL_DENIED.String() {
		t.Errorf("ProxyError kind=%q, want %q (msg=%q)",
			pe.Kind, pb.ProxyError_ACL_DENIED.String(), pe.Message)
	}
}

// TestE2E_ACL_TargetTopicClampRejects spins up a sidecar with a relay
// configured to allow only a specific target topic. We then dial the
// relay listener directly (sandbox-side surface) and send a
// ProxyHttpRequest whose target_topic does NOT match the clamp.
// Asserts the relay drops a DownstreamMessage_Error and never forwards
// the envelope to the gateway.
//
// Unlike the harness's standard flow (SDK→fakeGateway directly), this
// test exercises the relay surface explicitly. We can't reuse
// dialAgentClient here because the relay-side gRPC ingress is not the
// gateway and the SDK expects a gateway protocol.
func TestE2E_ACL_TargetTopicClampRejects(t *testing.T) {
	gw := getAetherlite(t)
	gwAddr := gw.grpcAddr

	relayPath := filepath.Join(t.TempDir(), "relay-clamp.sock")
	relayListen := "unix://" + relayPath

	srv := echoBackendForACL(t)

	uniqueSpec := fmt.Sprintf("clamp-%d", nextSidecarSpec.Add(1))

	cfg := &proxysidecar.Config{
		Gateway: proxysidecar.GatewayConfig{
			Address:  gwAddr,
			Insecure: true,
		},
		Service: proxysidecar.ServiceConfig{
			Implementation: "bp-sidecar",
			Specifier:      uniqueSpec,
		},
		Terminator: proxysidecar.TerminatorConfig{
			Enabled: true,
			Backends: []proxysidecar.BackendConfig{{
				Name:          "local",
				Kind:          proxysidecar.BackendKindHTTP,
				URL:           srv.URL,
				AllowPaths:    []string{"/*"},
				AllowMethods:  []string{"GET", "POST"},
				MaxBodyBytes:  1 << 20,
				IdleTimeoutMs: 5_000,
				HeaderMode:    proxysidecar.HeaderModePassthrough,
			}},
		},
		Relay: proxysidecar.RelayConfig{
			Enabled: true,
			Listen:  relayListen,
			AllowedOps: proxysidecar.AllowedOpsConfig{
				Profile: proxysidecar.AllowedOpsProfileSandboxTunnels,
				Set:     true,
			},
			TargetTopicClamp: proxysidecar.TargetClampConfig{
				Mode:           proxysidecar.TargetClampReject,
				AllowedTargets: []string{"sv::data-connectors::*"},
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
			t.Logf("warning: clamp-test runner did not exit within 15s")
		}
	})

	// Wait for the sidecar runtime to register on aetherlite. This also
	// confirms the relay listener is up (the runner brings them up
	// together).
	serviceTopic := fmt.Sprintf("sv::bp-sidecar::%s", uniqueSpec)
	if err := waitForSidecarReadyCustom(t, gwAddr, serviceTopic, "local", srv.URL); err != nil {
		t.Fatalf("clamp sidecar never reached ready state: %v", err)
	}

	// Dial the relay's UDS as a "sandbox" peer using plain gRPC.
	conn, err := grpc.NewClient("unix://"+relayPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial relay: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cli := pb.NewAetherGatewayClient(conn)
	streamCtx, streamCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer streamCancel()
	stream, err := cli.Connect(streamCtx)
	if err != nil {
		t.Fatalf("relay Connect: %v", err)
	}

	// Send a sandbox Init so the relay opens the upstream gateway side.
	initMsg := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Init{
			Init: &pb.InitConnection{
				ClientType: &pb.InitConnection_Agent{
					Agent: &pb.AgentIdentity{
						Workspace:      "sandbox-ws",
						Implementation: "sandbox-impl",
						Specifier:      "sandbox-spec",
					},
				},
				Credentials: map[string]string{"api_key": "fake-sandbox"},
			},
		},
	}
	if err := stream.Send(initMsg); err != nil {
		t.Fatalf("send init: %v", err)
	}

	// Drain the connection-ack the relay forwards.
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("recv ack: %v", err)
	}

	// Now send a ProxyHttpRequest targeted at a topic OUTSIDE the
	// clamp's allow-list. Expect the relay to drop a
	// DownstreamMessage_Error frame.
	bad := &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_ProxyHttpRequest{
			ProxyHttpRequest: &pb.ProxyHttpRequest{
				RequestId:   "req-clamp-deny",
				TargetTopic: "sv::other::nope",
				Method:      "GET",
				Path:        "/echo",
			},
		},
	}
	if err := stream.Send(bad); err != nil {
		t.Fatalf("send bad req: %v", err)
	}

	// Read at most a few frames; one of them must be the deny.
	denyDeadline := time.Now().Add(5 * time.Second)
	var sawDeny bool
	for time.Now().Before(denyDeadline) {
		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv after bad req: %v", err)
		}
		if errResp, ok := msg.GetPayload().(*pb.DownstreamMessage_Error); ok {
			if errResp.Error.GetCode() == "RELAY_TARGET_DENIED" {
				sawDeny = true
				t.Logf("relay denied target clamp: code=%s msg=%q",
					errResp.Error.GetCode(), errResp.Error.GetMessage())
				break
			}
		}
	}
	if !sawDeny {
		t.Fatalf("relay did not emit RELAY_TARGET_DENIED within 5s")
	}
}
