package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// =============================================================================
// Mock StateProvider
// =============================================================================

type mockStateProvider struct {
	gatewayInfo             *GatewayInfo
	gatewayInfoErr          error
	healthStatus            *HealthStatus
	healthStatusErr         error
	connections             []*ConnectionInfo
	connectionsErr          error
	connectionByID          *ConnectionInfo
	connectionByIDErr       error
	disconnectErr           error
	tasks                   []*TaskInfo
	tasksErr                error
	taskByID                *TaskInfo
	taskByIDErr             error
	retryTaskErr            error
	cancelTaskErr           error
	agents                  []*AgentRegistrationInfo
	agentsErr               error
	agentByImpl             *AgentRegistrationInfo
	agentByImplErr          error
	registerAgentErr        error
	updateAgentErr          error
	deleteAgentErr          error
	orchestrators           []*OrchestratorProfileInfo
	orchestratorsErr        error
	launchAgentResp         *LaunchAgentResponse
	launchAgentErr          error
	workspaces              []*WorkspaceInfo
	workspacesErr           error
	workspaceByID           *WorkspaceInfo
	workspaceByIDErr        error
	createWorkspaceErr      error
	updateWorkspaceErr      error
	deleteWorkspaceErr      error
	kvKeys                  []string
	kvKeysErr               error
	kvValue                 *KVEntry
	kvValueErr              error
	setKVErr                error
	deleteKVErr             error
	subscribeEventsCh       chan *Event
	subscribeEventsErr      error
	sendMessageErr          error
	subscribeTopicErr       error
	aclRules                []*ACLRuleInfo
	aclRulesErr             error
	aclRule                 *ACLRuleInfo
	aclRuleErr              error
	grantACLRule            *ACLRuleInfo
	grantACLErr             error
	revokeACLErr            error
	authorityGrants         []*ACLAuthorityGrantInfo
	authorityGrantsErr      error
	authorityGrant          *ACLAuthorityGrantInfo
	authorityGrantErr       error
	createAuthorityGrant    *ACLAuthorityGrantInfo
	createAuthorityGrantErr error
	renewAuthorityGrant     *ACLAuthorityGrantInfo
	renewAuthorityGrantErr  error
	revokeAuthorityGrantErr error
	setFallbackPolicyErr    error
	fallbackPolicy          *ACLFallbackPolicyInfo
	fallbackPolicyErr       error
	auditLogEntries         []*ACLAuditLogEntryInfo
	auditLogErr             error
	cleanupExpiredCount     int64
	cleanupExpiredErr       error
	cleanupAuditCount       int64
	cleanupAuditErr         error
	messageFlow             *MessageFlowInfo
	messageFlowErr          error
}

func (m *mockStateProvider) GetGatewayInfo(_ context.Context) (*GatewayInfo, error) {
	return m.gatewayInfo, m.gatewayInfoErr
}
func (m *mockStateProvider) GetHealthStatus(_ context.Context) (*HealthStatus, error) {
	return m.healthStatus, m.healthStatusErr
}
func (m *mockStateProvider) GetConnections(_ context.Context, _ *ConnectionFilter) ([]*ConnectionInfo, error) {
	return m.connections, m.connectionsErr
}
func (m *mockStateProvider) GetConnectionByID(_ context.Context, _ string) (*ConnectionInfo, error) {
	return m.connectionByID, m.connectionByIDErr
}
func (m *mockStateProvider) DisconnectSession(_ context.Context, _ string) error {
	return m.disconnectErr
}
func (m *mockStateProvider) GetTasks(_ context.Context, _ *TaskFilter) ([]*TaskInfo, error) {
	return m.tasks, m.tasksErr
}
func (m *mockStateProvider) GetTaskByID(_ context.Context, _ string) (*TaskInfo, error) {
	return m.taskByID, m.taskByIDErr
}
func (m *mockStateProvider) RetryTask(_ context.Context, _ string) error  { return m.retryTaskErr }
func (m *mockStateProvider) CancelTask(_ context.Context, _ string) error { return m.cancelTaskErr }
func (m *mockStateProvider) GetAgentRegistrations(_ context.Context) ([]*AgentRegistrationInfo, error) {
	return m.agents, m.agentsErr
}
func (m *mockStateProvider) GetAgentByImplementation(_ context.Context, _ string) (*AgentRegistrationInfo, error) {
	return m.agentByImpl, m.agentByImplErr
}
func (m *mockStateProvider) RegisterAgent(_ context.Context, _ *AgentRegistrationInfo) error {
	return m.registerAgentErr
}
func (m *mockStateProvider) UpdateAgent(_ context.Context, _ string, _ *AgentRegistrationInfo) error {
	return m.updateAgentErr
}
func (m *mockStateProvider) DeleteAgent(_ context.Context, _ string) error { return m.deleteAgentErr }
func (m *mockStateProvider) GetOrchestratorProfiles(_ context.Context) ([]*OrchestratorProfileInfo, error) {
	return m.orchestrators, m.orchestratorsErr
}
func (m *mockStateProvider) LaunchAgent(_ context.Context, _ *LaunchAgentRequest) (*LaunchAgentResponse, error) {
	return m.launchAgentResp, m.launchAgentErr
}
func (m *mockStateProvider) GetWorkspaces(_ context.Context) ([]*WorkspaceInfo, error) {
	return m.workspaces, m.workspacesErr
}
func (m *mockStateProvider) GetWorkspaceByID(_ context.Context, _ string) (*WorkspaceInfo, error) {
	return m.workspaceByID, m.workspaceByIDErr
}
func (m *mockStateProvider) CreateWorkspace(_ context.Context, _ *WorkspaceInfo) error {
	return m.createWorkspaceErr
}
func (m *mockStateProvider) UpdateWorkspace(_ context.Context, _ string, _ *WorkspaceInfo) error {
	return m.updateWorkspaceErr
}
func (m *mockStateProvider) DeleteWorkspace(_ context.Context, _ string) error {
	return m.deleteWorkspaceErr
}
func (m *mockStateProvider) GetKVKeys(_ context.Context, _, _ string) ([]string, error) {
	return m.kvKeys, m.kvKeysErr
}
func (m *mockStateProvider) GetKVValue(_ context.Context, _, _ string) (*KVEntry, error) {
	return m.kvValue, m.kvValueErr
}
func (m *mockStateProvider) SetKVValue(_ context.Context, _, _, _ string, _ int64) error {
	return m.setKVErr
}
func (m *mockStateProvider) DeleteKVKey(_ context.Context, _, _ string) error {
	return m.deleteKVErr
}
func (m *mockStateProvider) SubscribeEvents(_ context.Context) (<-chan *Event, error) {
	if m.subscribeEventsErr != nil {
		return nil, m.subscribeEventsErr
	}
	if m.subscribeEventsCh != nil {
		return m.subscribeEventsCh, nil
	}
	return make(chan *Event), nil
}
func (m *mockStateProvider) SendMessage(_ context.Context, _ *SendMessageRequest) error {
	return m.sendMessageErr
}
func (m *mockStateProvider) SubscribeToTopic(_ context.Context, _ string, _ func(*MonitoredMessage)) (func(), error) {
	if m.subscribeTopicErr != nil {
		return nil, m.subscribeTopicErr
	}
	return func() {}, nil
}
func (m *mockStateProvider) ListACLRules(_ context.Context, _ *ACLRuleFilter) ([]*ACLRuleInfo, error) {
	return m.aclRules, m.aclRulesErr
}
func (m *mockStateProvider) GetACLRule(_ context.Context, _, _, _, _ string) (*ACLRuleInfo, error) {
	return m.aclRule, m.aclRuleErr
}
func (m *mockStateProvider) GrantACLAccess(_ context.Context, _ *GrantACLAccessRequest) (*ACLRuleInfo, error) {
	return m.grantACLRule, m.grantACLErr
}
func (m *mockStateProvider) RevokeACLAccess(_ context.Context, _, _, _, _ string) error {
	return m.revokeACLErr
}
func (m *mockStateProvider) ListACLAuthorityGrants(_ context.Context, _ *ACLAuthorityGrantFilter) ([]*ACLAuthorityGrantInfo, error) {
	return m.authorityGrants, m.authorityGrantsErr
}
func (m *mockStateProvider) GetACLAuthorityGrant(_ context.Context, _ string) (*ACLAuthorityGrantInfo, error) {
	return m.authorityGrant, m.authorityGrantErr
}
func (m *mockStateProvider) CreateACLAuthorityGrant(_ context.Context, _ *CreateACLAuthorityGrantRequest) (*ACLAuthorityGrantInfo, error) {
	return m.createAuthorityGrant, m.createAuthorityGrantErr
}
func (m *mockStateProvider) RenewACLAuthorityGrant(_ context.Context, _ *RenewACLAuthorityGrantRequest) (*ACLAuthorityGrantInfo, error) {
	return m.renewAuthorityGrant, m.renewAuthorityGrantErr
}
func (m *mockStateProvider) RevokeACLAuthorityGrant(_ context.Context, _ string) error {
	return m.revokeAuthorityGrantErr
}
func (m *mockStateProvider) SetACLFallbackPolicy(_ context.Context, _ *SetFallbackPolicyRequest) error {
	return m.setFallbackPolicyErr
}
func (m *mockStateProvider) GetACLFallbackPolicy(_ context.Context, _ string) (*ACLFallbackPolicyInfo, error) {
	return m.fallbackPolicy, m.fallbackPolicyErr
}
func (m *mockStateProvider) QueryACLAuditLog(_ context.Context, _ *ACLAuditLogFilter) ([]*ACLAuditLogEntryInfo, error) {
	return m.auditLogEntries, m.auditLogErr
}
func (m *mockStateProvider) CleanupExpiredACLRules(_ context.Context) (int64, error) {
	return m.cleanupExpiredCount, m.cleanupExpiredErr
}
func (m *mockStateProvider) CleanupOldACLAuditLogs(_ context.Context, _ int) (int64, error) {
	return m.cleanupAuditCount, m.cleanupAuditErr
}
func (m *mockStateProvider) GetMessageFlow(_ context.Context, _ string) (*MessageFlowInfo, error) {
	return m.messageFlow, m.messageFlowErr
}
func (m *mockStateProvider) ListTokens(_ context.Context, _, _ int, _ bool) ([]*TokenInfo, error) {
	return nil, nil
}
func (m *mockStateProvider) GetToken(_ context.Context, _ string) (*TokenInfo, error) {
	return nil, fmt.Errorf("token not found")
}
func (m *mockStateProvider) CreateToken(_ context.Context, _ *CreateTokenRequest) (*CreateTokenResult, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *mockStateProvider) DeleteToken(_ context.Context, _ string) error {
	return fmt.Errorf("not implemented")
}
func (m *mockStateProvider) RevokeToken(_ context.Context, _ string) error {
	return fmt.Errorf("not implemented")
}
func (m *mockStateProvider) SetWorkspaceRateLimit(_ string, _ float64) error {
	return nil
}
func (m *mockStateProvider) GetWorkspaceRateLimit(_ string) (float64, error) {
	return 0, nil
}
func (m *mockStateProvider) RemoveWorkspaceRateLimit(_ string) error {
	return nil
}
func (m *mockStateProvider) ListWorkspaceRateLimits() (map[string]float64, error) {
	return map[string]float64{}, nil
}

// =============================================================================
// Test helper: build a gorilla/mux router from a Server without binding a port
// =============================================================================

// newTestServer creates a Server with InsecureNoAuth, marks it ready, and
// returns the router handler plus a cleanup function.
func newTestServer(t *testing.T, provider StateProvider) (*Server, http.Handler) {
	t.Helper()
	cfg := ServerConfig{
		InsecureNoAuth: true,
		RateLimit:      1000,
		RateLimitBurst: 1000,
	}
	srv := NewServer(cfg, provider)

	router := buildTestRouter(srv, false)
	t.Cleanup(func() { srv.rateLimiter.Close() })
	return srv, router
}

// buildTestRouter constructs the mux router. If withAuth is true and the
// server has an APIKey configured, the apiKeyAuthMiddleware is applied.
func buildTestRouter(s *Server, withAuth bool) http.Handler {
	router := mux.NewRouter()
	router.Use(s.rateLimitMiddleware)
	router.Use(s.corsMiddleware)

	// Stable, unversioned endpoints (no auth required) — mirrors Start()
	router.HandleFunc("/health", s.handleHealth).Methods("GET")
	router.HandleFunc("/info", s.handleInfo).Methods("GET")

	api := router.PathPrefix("/api/v1").Subrouter()
	if withAuth && s.config.APIKey != "" {
		api.Use(s.apiKeyAuthMiddleware)
	}
	s.registerAPIRoutes(api)
	api.HandleFunc("/ws/events", s.handleWebSocket)

	return router
}

// =============================================================================
// OpsServer health endpoint tests
// =============================================================================

// newTestOpsHandler returns an http.Handler for the OpsServer endpoints.
func newTestOpsHandler(provider StateProvider, ready bool) http.Handler {
	ops := NewOpsServer(0, provider)
	ops.SetReady(ready)
	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", ops.handleLive)
	mux.HandleFunc("/health/ready", ops.handleReady)
	mux.HandleFunc("/health/startup", ops.handleStartup)
	return mux
}

func TestHealthLive(t *testing.T) {
	handler := newTestOpsHandler(&mockStateProvider{}, true)

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestHealthStartup_Ready(t *testing.T) {
	handler := newTestOpsHandler(&mockStateProvider{}, true)

	req := httptest.NewRequest(http.MethodGet, "/health/startup", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestHealthStartup_NotReady(t *testing.T) {
	handler := newTestOpsHandler(&mockStateProvider{}, false)

	req := httptest.NewRequest(http.MethodGet, "/health/startup", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rec.Code)
	}
}

func TestHealthReady_Healthy(t *testing.T) {
	mock := &mockStateProvider{
		healthStatus: &HealthStatus{
			Status: "healthy",
			Checks: map[string]*HealthCheck{
				"redis": {Status: "ok"},
			},
		},
	}
	handler := newTestOpsHandler(mock, true)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestHealthReady_Unhealthy(t *testing.T) {
	mock := &mockStateProvider{
		healthStatus: &HealthStatus{
			Status: "unhealthy",
			Checks: map[string]*HealthCheck{
				"redis": {Status: "error", Error: "connection refused"},
			},
		},
	}
	handler := newTestOpsHandler(mock, true)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rec.Code)
	}
}

// =============================================================================
// Connection endpoint tests
// =============================================================================

func TestListConnections(t *testing.T) {
	now := time.Now()
	mock := &mockStateProvider{
		connections: []*ConnectionInfo{
			{
				SessionID:   "sess-1",
				Type:        "agent",
				Identity:    "ag::default::myagent::main",
				Workspace:   "default",
				ConnectedAt: now,
				Duration:    "1m",
			},
		},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if count, _ := resp["count"].(float64); count != 1 {
		t.Errorf("expected count=1, got %v", resp["count"])
	}
}

func TestDisconnectSession(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/connections/sess-123", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestGetConnection_NotFound(t *testing.T) {
	mock := &mockStateProvider{
		connectionByIDErr: fmt.Errorf("session not found"),
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/connections/nonexistent-id", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestGetConnection_Found(t *testing.T) {
	now := time.Now()
	mock := &mockStateProvider{
		connectionByID: &ConnectionInfo{
			SessionID:   "sess-abc",
			Type:        "agent",
			ConnectedAt: now,
		},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/connections/sess-abc", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

// =============================================================================
// Auth middleware tests
// =============================================================================

func TestAuthMiddleware_MissingKey(t *testing.T) {
	cfg := ServerConfig{
		APIKey:         "secret-key",
		InsecureNoAuth: true,
		RateLimit:      1000,
		RateLimitBurst: 1000,
	}
	srv := NewServer(cfg, &mockStateProvider{connections: []*ConnectionInfo{}})

	router := buildTestRouter(srv, true)
	t.Cleanup(func() { srv.rateLimiter.Close() })

	req := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestAuthMiddleware_WrongKey(t *testing.T) {
	cfg := ServerConfig{
		APIKey:         "secret-key",
		InsecureNoAuth: true,
		RateLimit:      1000,
		RateLimitBurst: 1000,
	}
	srv := NewServer(cfg, &mockStateProvider{connections: []*ConnectionInfo{}})

	router := buildTestRouter(srv, true)
	t.Cleanup(func() { srv.rateLimiter.Close() })

	req := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("expected 403, got %d", rec.Code)
	}
}

func TestAuthMiddleware_CorrectKey(t *testing.T) {
	cfg := ServerConfig{
		APIKey:         "secret-key",
		InsecureNoAuth: true,
		RateLimit:      1000,
		RateLimitBurst: 1000,
	}
	srv := NewServer(cfg, &mockStateProvider{connections: []*ConnectionInfo{}})

	router := buildTestRouter(srv, true)
	t.Cleanup(func() { srv.rateLimiter.Close() })

	req := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestAuthMiddleware_InsecureNoAuth(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{connections: []*ConnectionInfo{}})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/connections", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

// =============================================================================
// Info & Stats endpoints
// =============================================================================

func TestHandleInfo(t *testing.T) {
	mock := &mockStateProvider{
		gatewayInfo: &GatewayInfo{GatewayID: "gw-1", Version: "test"},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/info", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var info GatewayInfo
	if err := json.NewDecoder(rec.Body).Decode(&info); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if info.GatewayID != "gw-1" {
		t.Errorf("expected gateway_id=gw-1, got %s", info.GatewayID)
	}
}

func TestHandleInfo_Error(t *testing.T) {
	mock := &mockStateProvider{gatewayInfoErr: fmt.Errorf("backend error")}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/info", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestHandleStats(t *testing.T) {
	mock := &mockStateProvider{
		healthStatus: &HealthStatus{
			Status: "healthy",
			Stats: &GatewayStats{
				AgentConnections: 3,
				TaskConnections:  1,
			},
		},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

// =============================================================================
// Workspace endpoints
// =============================================================================

func TestListWorkspaces(t *testing.T) {
	mock := &mockStateProvider{
		workspaces: []*WorkspaceInfo{
			{WorkspaceID: "ws-1", DisplayName: "Production"},
		},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if count, _ := resp["count"].(float64); count != 1 {
		t.Errorf("expected count=1, got %v", resp["count"])
	}
}

func TestCreateWorkspace_MissingID(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"display_name":"Test"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

// =============================================================================
// Message send endpoint
// =============================================================================

func TestSendMessage_MissingTopic(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"payload":"hello"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/messages/send", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestSendMessage_InvalidType(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"target_topic":"ag::default::foo::bar","message_type":"INVALID"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/messages/send", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestSendMessage_OK(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"target_topic":"ag::default::foo::bar","message_type":"CHAT","payload":"hello"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/messages/send", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}
