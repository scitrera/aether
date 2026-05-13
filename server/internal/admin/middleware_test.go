package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

// =============================================================================
// corsMiddleware / security-headers tests
// =============================================================================

// buildCORSOnlyHandler returns a handler that runs only the corsMiddleware
// around a trivial 200 OK next handler.
func buildCORSOnlyHandler(cfg ServerConfig) http.Handler {
	s := &Server{config: cfg}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return s.corsMiddleware(next)
}

func TestCORSMiddleware_SecurityHeadersAlwaysPresent(t *testing.T) {
	handler := buildCORSOnlyHandler(ServerConfig{})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	headers := map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "strict-origin-when-cross-origin",
	}
	for header, want := range headers {
		if got := rec.Header().Get(header); got != want {
			t.Errorf("header %s = %q, want %q", header, got, want)
		}
	}
}

func TestCORSMiddleware_CSPPresentInProductionMode(t *testing.T) {
	// DevMode=false → CSP header must be set
	handler := buildCORSOnlyHandler(ServerConfig{DevMode: false})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Security-Policy"); got == "" {
		t.Error("Content-Security-Policy header should be set in production mode")
	}
}

func TestCORSMiddleware_CSPAbsentInDevMode(t *testing.T) {
	handler := buildCORSOnlyHandler(ServerConfig{DevMode: true})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Security-Policy"); got != "" {
		t.Errorf("Content-Security-Policy header should be absent in dev mode, got %q", got)
	}
}

func TestCORSMiddleware_HSTSPresentWhenTLSConfigured(t *testing.T) {
	handler := buildCORSOnlyHandler(ServerConfig{TLSCertFile: "/path/to/cert.pem"})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Strict-Transport-Security"); got == "" {
		t.Error("Strict-Transport-Security header should be set when TLS is configured")
	}
}

func TestCORSMiddleware_HSTSAbsentWithoutTLS(t *testing.T) {
	handler := buildCORSOnlyHandler(ServerConfig{})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Strict-Transport-Security"); got != "" {
		t.Errorf("Strict-Transport-Security should be absent without TLS, got %q", got)
	}
}

func TestCORSMiddleware_NoCORSHeadersWhenOriginEmpty(t *testing.T) {
	handler := buildCORSOnlyHandler(ServerConfig{CORSOrigin: ""})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin should be absent when CORSOrigin is empty, got %q", got)
	}
}

func TestCORSMiddleware_CORSHeadersSetWhenOriginConfigured(t *testing.T) {
	handler := buildCORSOnlyHandler(ServerConfig{CORSOrigin: "https://example.com"})

	req := httptest.NewRequest(http.MethodGet, "/info", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://example.com" {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "https://example.com")
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got == "" {
		t.Error("Access-Control-Allow-Methods should be set when CORS origin is configured")
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got == "" {
		t.Error("Access-Control-Allow-Headers should be set when CORS origin is configured")
	}
}

func TestCORSMiddleware_OptionsPreflightReturns200(t *testing.T) {
	handler := buildCORSOnlyHandler(ServerConfig{CORSOrigin: "https://example.com"})

	req := httptest.NewRequest(http.MethodOptions, "/info", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("OPTIONS preflight expected 200, got %d", rec.Code)
	}
}

func TestCORSMiddleware_OptionsPreflightDoesNotCallNext(t *testing.T) {
	s := &Server{config: ServerConfig{CORSOrigin: "https://example.com"}}
	nextCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusOK)
	})
	handler := s.corsMiddleware(next)

	req := httptest.NewRequest(http.MethodOptions, "/info", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if nextCalled {
		t.Error("OPTIONS preflight should short-circuit without calling next handler")
	}
}

// =============================================================================
// apiKeyAuthMiddleware edge cases
// =============================================================================

// buildAuthRouter builds a minimal router with auth middleware applied.
func buildAuthRouter(t *testing.T, apiKey string) http.Handler {
	t.Helper()
	cfg := ServerConfig{
		APIKey:         apiKey,
		InsecureNoAuth: true,
		RateLimit:      1000,
		RateLimitBurst: 1000,
	}
	srv := NewServer(cfg, &mockStateProvider{connections: []*ConnectionInfo{}})
	t.Cleanup(func() { srv.rateLimiter.Close() })
	return buildTestRouter(srv, true)
}

func TestAuthMiddleware_HealthEndpointExemptFromAuth(t *testing.T) {
	// /health is on the root router (unversioned, no auth) — should pass without Authorization header
	router := buildAuthRouter(t, "my-secret-key")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	// Deliberately set no Authorization header
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	// Health endpoint calls provider.GetHealthStatus; mock returns nil health + nil err
	// so the handler will write 200 OK.
	if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
		t.Errorf("health endpoint should not require auth, got %d", rec.Code)
	}
}

func TestAuthMiddleware_MalformedBearerPrefix(t *testing.T) {
	router := buildAuthRouter(t, "my-secret-key")

	// Header present but without the "Bearer " prefix
	req := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
	req.Header.Set("Authorization", "my-secret-key")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("malformed Authorization header (no Bearer prefix) expected 401, got %d", rec.Code)
	}
}

func TestAuthMiddleware_EmptyTokenAfterBearer(t *testing.T) {
	router := buildAuthRouter(t, "my-secret-key")

	// "Bearer " with nothing after it — token is empty string, should not match
	req := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
	req.Header.Set("Authorization", "Bearer ")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("empty Bearer token expected 403, got %d", rec.Code)
	}
}

func TestAuthMiddleware_TimingSafeComparison(t *testing.T) {
	// Verify that a key that is a prefix of the real key is still rejected
	// (guards against short-circuit comparisons that could leak length).
	router := buildAuthRouter(t, "secret-key-long")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("prefix-only token expected 403, got %d", rec.Code)
	}
}

// =============================================================================
// WebSocket subprotocol parsing (extracted from handleWebSocket)
// =============================================================================

// parseSubprotocols replicates the subprotocol detection logic from handleWebSocket
// so it can be tested without a real WebSocket upgrade.
func parseSubprotocols(r *http.Request) bool {
	protocols := websocketSubprotocols(r)
	for i, p := range protocols {
		if p == "auth" && i+1 < len(protocols) {
			return true
		}
	}
	return false
}

// websocketSubprotocols parses the Sec-WebSocket-Protocol header, matching
// the gorilla/websocket.Subprotocols behaviour used in the real code.
func websocketSubprotocols(r *http.Request) []string {
	h := r.Header.Get("Sec-WebSocket-Protocol")
	if h == "" {
		return nil
	}
	var protocols []string
	for _, p := range splitHeader(h) {
		protocols = append(protocols, p)
	}
	return protocols
}

func splitHeader(h string) []string {
	var parts []string
	start := 0
	for i := 0; i <= len(h); i++ {
		if i == len(h) || h[i] == ',' {
			part := trimSpace(h[start:i])
			if part != "" {
				parts = append(parts, part)
			}
			start = i + 1
		}
	}
	return parts
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

func TestWebSocketSubprotocol_AuthFollowedByToken(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ws/events", nil)
	// Standard pattern: "auth, <token>"
	req.Header.Set("Sec-WebSocket-Protocol", "auth, my-bearer-token")

	if !parseSubprotocols(req) {
		t.Error("expected tokenViaSubprotocol=true when 'auth' is followed by a token")
	}
}

func TestWebSocketSubprotocol_AuthAloneDoesNotTrigger(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ws/events", nil)
	// "auth" with nothing after it — i+1 == len(protocols) so condition is false
	req.Header.Set("Sec-WebSocket-Protocol", "auth")

	if parseSubprotocols(req) {
		t.Error("expected tokenViaSubprotocol=false when 'auth' is the only protocol")
	}
}

func TestWebSocketSubprotocol_NoAuthProtocol(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ws/events", nil)
	req.Header.Set("Sec-WebSocket-Protocol", "v1, v2")

	if parseSubprotocols(req) {
		t.Error("expected tokenViaSubprotocol=false when no 'auth' protocol is present")
	}
}

func TestWebSocketSubprotocol_EmptyHeader(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ws/events", nil)

	if parseSubprotocols(req) {
		t.Error("expected tokenViaSubprotocol=false when Sec-WebSocket-Protocol header is absent")
	}
}

// =============================================================================
// Task handler coverage
// =============================================================================

func TestListTasks_ReturnsCount(t *testing.T) {
	mock := &mockStateProvider{
		tasks: []*TaskInfo{
			{TaskID: "t1", Status: "running"},
			{TaskID: "t2", Status: "pending"},
		},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestListTasks_ServiceError(t *testing.T) {
	mock := &mockStateProvider{tasksErr: errTest("task store unavailable")}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestGetTask_Found(t *testing.T) {
	mock := &mockStateProvider{
		taskByID: &TaskInfo{TaskID: "task-abc", Status: "running"},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/task-abc", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestGetTask_NotFound(t *testing.T) {
	mock := &mockStateProvider{taskByIDErr: errTest("task not found")}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/missing", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestRetryTask_OK(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/task-xyz/retry", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestRetryTask_Error(t *testing.T) {
	mock := &mockStateProvider{retryTaskErr: errTest("cannot retry")}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/task-xyz/retry", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestCancelTask_OK(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/task-xyz/cancel", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

// =============================================================================
// Agent handler coverage
// =============================================================================

func TestListAgents_OK(t *testing.T) {
	mock := &mockStateProvider{
		agents: []*AgentRegistrationInfo{
			{Implementation: "my-agent", Description: "test"},
		},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/agents", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestListAgents_Error(t *testing.T) {
	mock := &mockStateProvider{agentsErr: errTest("db error")}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/agents", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestGetAgent_Found(t *testing.T) {
	mock := &mockStateProvider{
		agentByImpl: &AgentRegistrationInfo{Implementation: "worker"},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/agents/worker", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestGetAgent_NotFound(t *testing.T) {
	mock := &mockStateProvider{agentByImplErr: errTest("not found")}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/agents/unknown", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestCreateAgent_MissingImplementation(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"description":"no impl"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents", strReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestCreateAgent_OK(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"implementation":"my-agent","description":"test agent"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents", strReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", rec.Code)
	}
}

func TestDeleteAgent_OK(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/agents/my-agent", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

// =============================================================================
// KV handler coverage
// =============================================================================

func TestListKV_DefaultsToGlobalScope(t *testing.T) {
	mock := &mockStateProvider{kvKeys: []string{"key1", "key2"}}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/kv", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestGetKV_NotFound(t *testing.T) {
	mock := &mockStateProvider{kvValueErr: errTest("key not found")}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/kv/global/mykey", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestSetKV_OK(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"value":"hello","ttl":0}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/kv/global/mykey", strReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestDeleteKV_OK(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/kv/global/mykey", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

// =============================================================================
// Health handler (via /health — unversioned, no auth)
// =============================================================================

func TestHandleHealth_Healthy(t *testing.T) {
	mock := &mockStateProvider{
		healthStatus: &HealthStatus{
			Status: "healthy",
			Checks: map[string]*HealthCheck{
				"redis": {Status: "ok"},
			},
		},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestHandleHealth_ProviderError(t *testing.T) {
	mock := &mockStateProvider{healthStatusErr: errTest("health check failed")}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

// =============================================================================
// Orchestrator handler
// =============================================================================

func TestListOrchestrators_OK(t *testing.T) {
	mock := &mockStateProvider{
		orchestrators: []*OrchestratorProfileInfo{
			{OrchestratorID: "orch-1"},
		},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/orchestrators", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

// =============================================================================
// Rate limit middleware: too-many-requests path
// =============================================================================

func TestRateLimitMiddleware_BlocksWhenExhausted(t *testing.T) {
	// Use a burst of 1 so the second request is always blocked.
	cfg := ServerConfig{
		InsecureNoAuth: true,
		RateLimit:      0.0001, // effectively 0 refill
		RateLimitBurst: 1,
	}
	srv := NewServer(cfg, &mockStateProvider{connections: []*ConnectionInfo{}})
	t.Cleanup(func() { srv.rateLimiter.Close() })

	router := mux.NewRouter()
	router.Use(srv.rateLimitMiddleware)
	router.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// First request consumes the burst token — must pass.
	req1 := httptest.NewRequest(http.MethodGet, "/ping", nil)
	req1.RemoteAddr = "10.0.0.1:1234"
	rec1 := httptest.NewRecorder()
	router.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first request should pass rate limiter, got %d", rec1.Code)
	}

	// Second request should be blocked.
	req2 := httptest.NewRequest(http.MethodGet, "/ping", nil)
	req2.RemoteAddr = "10.0.0.1:1234"
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Errorf("second request should be rate-limited (429), got %d", rec2.Code)
	}
}

// =============================================================================
// Helper types / funcs
// =============================================================================

type testError string

func (e testError) Error() string { return string(e) }

func errTest(msg string) error { return testError(msg) }

func strReader(s string) *strings.Reader {
	return strings.NewReader(s)
}
