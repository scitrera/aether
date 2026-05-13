//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/proxysidecar"
	"github.com/scitrera/aether/pkg/identityheaders"
)

// terminatorEntry is one connected sidecar instance in the harness. The
// instanceID matches the {specifier} in the sv::{impl}::{specifier} address.
type terminatorEntry struct {
	implementation string
	specifier      string
	t              *proxysidecar.Terminator
	online         atomic.Bool
	hits           atomic.Int64
}

func (e *terminatorEntry) topic() string {
	return "sv::" + e.implementation + "::" + e.specifier
}

// harness mirrors the parts of the gateway's proxy routing pipeline relevant
// to Phase 1 end-to-end tests: wildcard sv::{impl} resolution across connected
// instances, ACL deny enforcement, and dispatch to the corresponding
// Terminator. It deliberately omits Redis/RabbitMQ since those moving parts
// are covered by separate unit suites.
type harness struct {
	mu          sync.RWMutex
	terminators []*terminatorEntry
	// aclAllow returns true when a caller is permitted to talk to the given
	// concrete service topic. nil means allow-all.
	aclAllow func(caller string, target string) bool
}

func newHarness() *harness {
	return &harness{}
}

// addTerminator constructs a real proxysidecar.Terminator pointing at backendURL
// and registers it under the sv::{impl}::{specifier} address.
func (h *harness) addTerminator(t *testing.T, implementation, specifier, backendURL, tenantID, headerMode string) *terminatorEntry {
	t.Helper()
	cfg := &proxysidecar.Config{
		Service: proxysidecar.ServiceConfig{
			Implementation: implementation,
			Specifier:      specifier,
		},
		Gateway: proxysidecar.GatewayConfig{
			Address:  "localhost:0", // gateway connection is unused in tests
			Insecure: true,
		},
		Terminator: proxysidecar.TerminatorConfig{
			Enabled: true,
			Backends: []proxysidecar.BackendConfig{{
				Name:          "primary",
				Kind:          proxysidecar.BackendKindHTTP,
				URL:           backendURL,
				AllowMethods:  []string{"GET", "POST", "PUT", "DELETE", "PATCH"},
				AllowPaths:    []string{"/*"},
				MaxBodyBytes:  20 << 20, // 20 MiB
				IdleTimeoutMs: 5_000,
				HeaderMode:    headerMode,
			}},
		},
		TenantID: tenantID,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validate: %v", err)
	}
	term, err := proxysidecar.NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}
	entry := &terminatorEntry{
		implementation: implementation,
		specifier:      specifier,
		t:              term,
	}
	entry.online.Store(true)

	h.mu.Lock()
	h.terminators = append(h.terminators, entry)
	h.mu.Unlock()
	return entry
}

// setACL installs an ACL predicate. Use nil to disable (allow-all).
func (h *harness) setACL(fn func(caller, target string) bool) {
	h.mu.Lock()
	h.aclAllow = fn
	h.mu.Unlock()
}

// resolveWildcardOrConcrete mirrors the gateway's resolveWildcardOrConcrete:
// a bare sv::{impl} is resolved to one connected, online instance; a fully
// concrete sv::{impl}::{specifier} is returned as-is when registered.
func (h *harness) resolveWildcardOrConcrete(target string) (concrete string, err error) {
	if !strings.HasPrefix(target, "sv::") {
		return "", fmt.Errorf("ACL_DENIED: proxy/tunnel envelopes may only target sv:: principals, got %q", target)
	}
	rest := strings.TrimPrefix(target, "sv::")
	parts := strings.Split(rest, "::")
	if len(parts) == 0 || parts[0] == "" {
		return "", fmt.Errorf("invalid service target: %q", target)
	}
	impl := parts[0]
	wildcard := len(parts) == 1

	h.mu.RLock()
	defer h.mu.RUnlock()
	candidates := make([]*terminatorEntry, 0, len(h.terminators))
	for _, e := range h.terminators {
		if e.implementation != impl {
			continue
		}
		if !e.online.Load() {
			continue
		}
		if !wildcard && e.specifier != parts[1] {
			continue
		}
		candidates = append(candidates, e)
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("no healthy sv::%s instances available", impl)
	}
	pick := candidates[rand.IntN(len(candidates))]
	pick.hits.Add(1)
	return pick.topic(), nil
}

// findTerminator returns the entry matching a fully-qualified topic, or nil.
func (h *harness) findTerminator(topic string) *terminatorEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, e := range h.terminators {
		if e.topic() == topic {
			return e
		}
	}
	return nil
}

// proxyHTTP is the in-process equivalent of an upstream caller invoking
// proxy_http through the gateway. It performs the same checks the real
// gateway does (ACL, wildcard resolution, sender stamping) and dispatches
// the assembled request to the corresponding Terminator.
//
// Returns the assembled ProxyHttpResponse (which may carry an Error). Errors
// returned out-of-band indicate harness/wiring failures, not protocol errors.
func (h *harness) proxyHTTP(ctx context.Context, caller string, req *pb.ProxyHttpRequest) (*pb.ProxyHttpResponse, []byte, error) {
	if req == nil {
		return nil, nil, errors.New("nil request")
	}
	requestID := req.GetRequestId()

	concrete, err := h.resolveWildcardOrConcrete(req.GetTargetTopic())
	if err != nil {
		// Gateway-style mapping: parse / instance failures => SIDECAR_UNAVAILABLE
		// or ACL_DENIED for non-sv:: targets.
		kind := pb.ProxyError_SIDECAR_UNAVAILABLE
		if strings.HasPrefix(err.Error(), "ACL_DENIED:") {
			kind = pb.ProxyError_ACL_DENIED
		}
		return errResp(requestID, kind, err.Error()), nil, nil
	}

	// Rewrite target to concrete (matches gateway behavior).
	req.TargetTopic = concrete

	// ACL gate.
	h.mu.RLock()
	allow := h.aclAllow
	h.mu.RUnlock()
	if allow != nil && !allow(caller, concrete) {
		return errResp(requestID, pb.ProxyError_ACL_DENIED,
			fmt.Sprintf("caller %s not permitted on %s", caller, concrete)), nil, nil
	}

	// Stamp actor topic exactly the way routing_proxy.go does.
	if req.Headers == nil {
		req.Headers = make(map[string]string)
	}
	req.Headers["x-aether-actor-topic"] = caller

	entry := h.findTerminator(concrete)
	if entry == nil {
		return errResp(requestID, pb.ProxyError_SIDECAR_UNAVAILABLE,
			fmt.Sprintf("no terminator for %s", concrete)), nil, nil
	}

	// Apply per-request timeout if set, mirroring backend behaviour.
	dispatchCtx := ctx
	if to := req.GetTimeoutMs(); to > 0 {
		var cancel context.CancelFunc
		dispatchCtx, cancel = context.WithTimeout(ctx, time.Duration(to)*time.Millisecond)
		defer cancel()
	}

	resp, body := entry.t.HandleProxyRequest(dispatchCtx, req, req.GetBody())
	return resp, body, nil
}

// proxyHTTPChunked simulates the SDK's chunked-upload path through the
// in-process pipeline. It splits the body into PROXY_BODY_CHUNK_SIZE-sized
// frames, drives the terminator's chunked-request accumulator end-to-end,
// and returns the assembled ProxyHttpResponse. Mirrors what the real gateway
// would orchestrate via the request-pin.
func (h *harness) proxyHTTPChunked(ctx context.Context, caller string, req *pb.ProxyHttpRequest, body []byte, chunkSize int) (*pb.ProxyHttpResponse, []byte, error) {
	if req == nil {
		return nil, nil, errors.New("nil request")
	}
	if !req.GetBodyChunked() {
		return nil, nil, errors.New("proxyHTTPChunked: req.body_chunked must be true")
	}
	requestID := req.GetRequestId()

	concrete, err := h.resolveWildcardOrConcrete(req.GetTargetTopic())
	if err != nil {
		kind := pb.ProxyError_SIDECAR_UNAVAILABLE
		if strings.HasPrefix(err.Error(), "ACL_DENIED:") {
			kind = pb.ProxyError_ACL_DENIED
		}
		return errResp(requestID, kind, err.Error()), nil, nil
	}
	req.TargetTopic = concrete

	h.mu.RLock()
	allow := h.aclAllow
	h.mu.RUnlock()
	if allow != nil && !allow(caller, concrete) {
		return errResp(requestID, pb.ProxyError_ACL_DENIED,
			fmt.Sprintf("caller %s not permitted on %s", caller, concrete)), nil, nil
	}

	if req.Headers == nil {
		req.Headers = make(map[string]string)
	}
	req.Headers["x-aether-actor-topic"] = caller

	entry := h.findTerminator(concrete)
	if entry == nil {
		return errResp(requestID, pb.ProxyError_SIDECAR_UNAVAILABLE,
			fmt.Sprintf("no terminator for %s", concrete)), nil, nil
	}

	transport := newCapturingTransport()
	if err := entry.t.BeginChunkedRequestForTest(req, transport); err != nil {
		return nil, nil, err
	}
	if chunkSize <= 0 {
		chunkSize = 256 * 1024
	}
	for offset, seq := 0, uint32(0); ; seq++ {
		end := offset + chunkSize
		if end >= len(body) {
			end = len(body)
		}
		fin := end == len(body)
		chunk := &pb.ProxyHttpBodyChunk{
			RequestId: requestID,
			IsRequest: true,
			Seq:       seq,
			Data:      body[offset:end],
			Fin:       fin,
		}
		if err := entry.t.HandleChunkedRequestFrameForTest(ctx, chunk, transport); err != nil {
			return nil, nil, err
		}
		offset = end
		if fin {
			break
		}
	}

	header, respBody := transport.assembledResponse()
	return header, respBody, nil
}

// capturingTransport implements the proxysidecar.tunnelTransport behaviour
// the harness needs: collect ProxyHttpResponse + body chunks emitted by the
// terminator. Tunnel-direction methods are no-ops since the harness only
// drives HTTP-direction chunked-body tests.
type capturingTransport struct {
	mu     sync.Mutex
	resps  []*pb.ProxyHttpResponse
	chunks []*pb.ProxyHttpBodyChunk
}

func newCapturingTransport() *capturingTransport { return &capturingTransport{} }

func (c *capturingTransport) SendTunnelData(*pb.TunnelData) error   { return nil }
func (c *capturingTransport) SendTunnelClose(*pb.TunnelClose) error { return nil }
func (c *capturingTransport) SendTunnelAck(*pb.TunnelAck) error     { return nil }
func (c *capturingTransport) SendProxyHttpResponse(r *pb.ProxyHttpResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resps = append(c.resps, r)
	return nil
}
func (c *capturingTransport) SendProxyHttpBodyChunk(ch *pb.ProxyHttpBodyChunk) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.chunks = append(c.chunks, ch)
	return nil
}

// assembledResponse returns the header + reassembled body from the captured
// frames. Mirrors what the gateway would deliver to a caller after
// re-assembling chunked-response frames.
func (c *capturingTransport) assembledResponse() (*pb.ProxyHttpResponse, []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.resps) == 0 {
		return nil, nil
	}
	header := c.resps[0]
	if !header.GetBodyChunked() {
		return header, header.GetBody()
	}
	var body []byte
	for _, ch := range c.chunks {
		body = append(body, ch.GetData()...)
	}
	return header, body
}

// errResp builds a synthetic ProxyHttpResponse that carries an error. Used by
// the harness to surface gateway-side failures the same way the real gateway
// does (the caller never sees a Go error; it sees a ProxyError frame).
func errResp(requestID string, kind pb.ProxyError_Kind, msg string) *pb.ProxyHttpResponse {
	return &pb.ProxyHttpResponse{
		RequestId: requestID,
		Error: &pb.ProxyError{
			Kind:    kind,
			Message: msg,
		},
	}
}

// ---------------------------------------------------------------------------
// Mock backends
// ---------------------------------------------------------------------------

// echoBackend is a small httptest.Server that records each request and
// returns a deterministic response. Useful to assert on request shape and to
// produce identifiable per-instance responses (counter etc).
type echoBackend struct {
	srv      *httptest.Server
	mu       sync.Mutex
	requests []*recordedRequest
	tag      string
}

type recordedRequest struct {
	Method      string
	URL         string
	Path        string
	RawQuery    string
	Headers     http.Header
	Body        []byte
	ContentType string
}

func newEchoBackend(t *testing.T, tag string) *echoBackend {
	t.Helper()
	b := &echoBackend{tag: tag}
	b.srv = httptest.NewServer(http.HandlerFunc(b.handle))
	t.Cleanup(b.srv.Close)
	return b
}

func (b *echoBackend) handle(w http.ResponseWriter, r *http.Request) {
	body := make([]byte, 0, 1024)
	if r.Body != nil {
		buf := make([]byte, 1<<14)
		for {
			n, err := r.Body.Read(buf)
			if n > 0 {
				body = append(body, buf[:n]...)
			}
			if err != nil {
				break
			}
		}
		_ = r.Body.Close()
	}
	rec := &recordedRequest{
		Method:      r.Method,
		URL:         r.URL.String(),
		Path:        r.URL.Path,
		RawQuery:    r.URL.RawQuery,
		Headers:     r.Header.Clone(),
		Body:        body,
		ContentType: r.Header.Get("Content-Type"),
	}
	b.mu.Lock()
	b.requests = append(b.requests, rec)
	b.mu.Unlock()

	if r.URL.Path == "/slow" {
		time.Sleep(2 * time.Second)
	}

	if r.URL.Path == "/echo-bytes" {
		// Echo the request body verbatim so chunked-body round-trip can be
		// asserted by the caller.
		w.Header().Set("Content-Type", r.Header.Get("Content-Type"))
		w.Header().Set("X-Backend-Tag", b.tag)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Backend-Tag", b.tag)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"tag":%q,"path":%q,"query":%q,"method":%q,"len":%d}`,
		b.tag, r.URL.Path, r.URL.RawQuery, r.Method, len(body))
}

func (b *echoBackend) lastRequest() *recordedRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.requests) == 0 {
		return nil
	}
	return b.requests[len(b.requests)-1]
}

func (b *echoBackend) requestCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.requests)
}

// ---------------------------------------------------------------------------
// Helpers used across tests
// ---------------------------------------------------------------------------

// expectError asserts that resp carries the given error kind and returns the
// embedded message. Fails fast otherwise.
func expectError(t *testing.T, resp *pb.ProxyHttpResponse, kind pb.ProxyError_Kind) string {
	t.Helper()
	if resp == nil {
		t.Fatalf("expected ProxyError %s, got nil response", kind)
	}
	if resp.Error == nil {
		t.Fatalf("expected ProxyError %s, got status=%d body-len=%d",
			kind, resp.GetStatusCode(), len(resp.GetBody()))
	}
	if resp.Error.GetKind() != kind {
		t.Fatalf("expected ProxyError %s, got %s: %s",
			kind, resp.Error.GetKind(), resp.Error.GetMessage())
	}
	return resp.Error.GetMessage()
}

// expectOK asserts that resp completed without error and returns the status.
func expectOK(t *testing.T, resp *pb.ProxyHttpResponse) int32 {
	t.Helper()
	if resp == nil {
		t.Fatal("expected response, got nil")
	}
	if resp.Error != nil {
		t.Fatalf("expected success, got ProxyError %s: %s",
			resp.Error.GetKind(), resp.Error.GetMessage())
	}
	return resp.GetStatusCode()
}

// callerOf returns the canonical actor topic for a User caller, used as the
// "x-aether-actor-topic" header value.
func userCaller(userID, windowID string) string {
	return fmt.Sprintf("us::%s::%s", userID, windowID)
}

func agentCaller(workspace, impl, spec string) string {
	return fmt.Sprintf("ag::%s::%s::%s", workspace, impl, spec)
}

// validHeaderModes for parameterised tests.
var validHeaderModes = []string{
	proxysidecar.HeaderModeStrict,
	proxysidecar.HeaderModePassthrough,
	proxysidecar.HeaderModeBoth,
}

// authProxyMintForUser builds the canonical X-Auth-* header set the
// auth-proxy would inject for a directly-authenticated user; tests use this
// as the byte-level reference for the sidecar-minted set.
func authProxyMintForUser(tenantID, userID, principalType, scopes, apiKeyID string) http.Header {
	return identityheaders.Mint(context.Background(), tenantID, identityheaders.Identity{
		UserID:        userID,
		PrincipalType: principalType,
		Scopes:        scopes,
		APIKeyID:      apiKeyID,
	})
}

// silenceUnused references all helper symbols so go vet doesn't flag them
// when individual test files temporarily disable a scenario.
var _ = []interface{}{
	validHeaderModes,
	authProxyMintForUser,
	userCaller,
	agentCaller,
}
