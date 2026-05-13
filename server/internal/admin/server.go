package admin

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/scitrera/aether/internal/logging"
	"golang.org/x/time/rate"
)

const (
	// defaultPageSize is the number of items returned by list endpoints when no limit is specified.
	defaultPageSize = 100
	// maxPageSize is the maximum number of items a caller may request in a single list call.
	maxPageSize = 1000
)

//go:embed static/*
var staticFS embed.FS

// ServerConfig configures the admin server
type ServerConfig struct {
	Port           int
	DevMode        bool   // If true, serve files from disk instead of embedded
	DevPath        string // Path to static files in dev mode
	CORSOrigin     string
	APIKey         string        // Bearer token for admin API authentication; if empty, admin API requires InsecureNoAuth=true
	GetAPIKey      func() string // Optional: if set, used instead of APIKey for dynamic key rotation (SIGHUP reload)
	InsecureNoAuth bool          // If true, allow unauthenticated access when APIKey is empty (dev/testing only)
	TLSCertFile    string        // Path to TLS certificate file; if set with TLSKeyFile, enables HTTPS
	TLSKeyFile     string        // Path to TLS private key file; if set with TLSCertFile, enables HTTPS
	RateLimit      float64       // requests per second (0 = use default of 10)
	RateLimitBurst int           // burst size (0 = use default of 20)
}

// Server provides the admin web UI and API
type Server struct {
	config      ServerConfig
	provider    StateProvider
	server      *http.Server
	upgrader    websocket.Upgrader
	rateLimiter *ipRateLimiter

	// stopCh is closed when the server is stopping, used to terminate background goroutines.
	stopCh chan struct{}

	// WebSocket connections
	wsMu    sync.RWMutex
	wsConns map[*websocket.Conn]*wsConnection
}

// wsConnection tracks a WebSocket connection with its monitors
type wsConnection struct {
	conn       *websocket.Conn
	cancel     context.CancelFunc
	monitors   map[string]func() // topic -> cancel function
	remoteAddr string            // client address, used for audit logging
	mu         sync.Mutex
}

// WSClientMessage represents a message from the client
type WSClientMessage struct {
	Action string `json:"action"` // "subscribe_monitor", "unsubscribe_monitor"
	Topic  string `json:"topic"`
}

// WSServerMessage represents a message to the client
type WSServerMessage struct {
	Type    string            `json:"type"` // "event", "monitor_message", "monitor_subscribed", "monitor_unsubscribed", "error"
	Event   *Event            `json:"event,omitempty"`
	Message *MonitoredMessage `json:"message,omitempty"`
	Topic   string            `json:"topic,omitempty"`
	Error   string            `json:"error,omitempty"`
}

// NewServer creates a new admin server
func NewServer(config ServerConfig, provider StateProvider) *Server {
	rl := config.RateLimit
	if rl <= 0 {
		rl = 10
	}
	burst := config.RateLimitBurst
	if burst <= 0 {
		burst = 20
	}

	s := &Server{
		config:      config,
		provider:    provider,
		rateLimiter: newIPRateLimiter(rate.Limit(rl), burst, false),
		stopCh:      make(chan struct{}),
		wsConns:     make(map[*websocket.Conn]*wsConnection),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				// In dev/insecure mode, allow all origins
				if config.DevMode || config.InsecureNoAuth {
					return true
				}
				// In production, deny wildcard origins for WebSocket (prevents CSWSH)
				if config.CORSOrigin == "*" || config.CORSOrigin == "" {
					return false
				}
				origin := r.Header.Get("Origin")
				return origin == config.CORSOrigin
			},
		},
	}

	return s
}

// Start starts the admin server
func (s *Server) Start() error {
	router := mux.NewRouter()

	// Add rate limiting middleware (skips health probe paths)
	router.Use(s.rateLimitMiddleware)

	// Add CORS middleware
	router.Use(s.corsMiddleware)

	// Stable, unversioned endpoints (no auth required)
	router.HandleFunc("/health", s.handleHealth).Methods("GET")
	router.HandleFunc("/info", s.handleInfo).Methods("GET")

	// Versioned API routes
	api := router.PathPrefix("/api/v1").Subrouter()
	if s.config.APIKey != "" || s.config.GetAPIKey != nil {
		api.Use(s.apiKeyAuthMiddleware)
	} else if s.config.InsecureNoAuth {
		logging.Logger.Warn().Msg("admin API is running without authentication (InsecureNoAuth=true); NOT FOR PRODUCTION")
	} else {
		return fmt.Errorf("AETHER_ADMIN_API_KEY is required; set --insecure-admin to override")
	}
	s.registerAPIRoutes(api)

	// WebSocket endpoint (under /api so it inherits apiKeyAuthMiddleware)
	api.HandleFunc("/ws/events", s.handleWebSocket)

	// Static files (serve last to not interfere with API routes)
	s.registerStaticRoutes(router)

	addr := fmt.Sprintf(":%d", s.config.Port)
	s.server = &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Periodically refresh Prometheus gauges that require querying gateway state.
	// The active_connections gauge is updated every 5 seconds from GetHealthStatus.
	// Counter metrics (messages_routed, message_errors) are instrumented directly
	// at the routing layer and are not updated here.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logging.Logger.Error().Interface("panic", r).Str("stack", string(debug.Stack())).Str("goroutine", "metricsRefresh").Msg("recovered from panic in background goroutine")
			}
		}()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if s.provider != nil {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					if health, err := s.provider.GetHealthStatus(ctx); err == nil && health.Stats != nil {
						updateActiveConnectionsMetric(health.Stats)
					}
					cancel()
				}
			case <-s.stopCh:
				return
			}
		}
	}()

	if s.config.TLSCertFile != "" && s.config.TLSKeyFile != "" {
		logging.Logger.Info().Str("addr", s.server.Addr).Msg("admin server starting with TLS")
		return s.server.ListenAndServeTLS(s.config.TLSCertFile, s.config.TLSKeyFile)
	}
	if s.config.APIKey != "" {
		// API key without TLS = bearer token traveling in plaintext. Refuse to start
		// in this configuration unless the operator has explicitly acknowledged the
		// risk by setting InsecureNoAuth=true (development only).
		if !s.config.InsecureNoAuth {
			return fmt.Errorf("admin API key requires TLS; configure tls_cert_file and tls_key_file, or explicitly opt in to insecure local development by setting admin.insecure_no_auth=true")
		}
		logging.Logger.Error().Msg("admin API key configured but TLS is not enabled — API key will be transmitted in plaintext (INSECURE MODE; do not use in production)")
	}
	logging.Logger.Info().Str("addr", s.server.Addr).Msg("admin server starting (no TLS)")
	return s.server.ListenAndServe()
}

// Stop gracefully shuts down the admin server
func (s *Server) Stop(ctx context.Context) error {
	// Signal background goroutines (e.g. metrics refresh) to stop.
	select {
	case <-s.stopCh:
		// already closed
	default:
		close(s.stopCh)
	}

	// Close all WebSocket connections
	s.wsMu.Lock()
	for conn, wsConn := range s.wsConns {
		// Cancel all monitors
		wsConn.mu.Lock()
		for _, cancel := range wsConn.monitors {
			cancel()
		}
		wsConn.mu.Unlock()
		wsConn.cancel()
		conn.Close()
	}
	s.wsConns = make(map[*websocket.Conn]*wsConnection)
	s.wsMu.Unlock()

	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// =============================================================================
// API Routes
// =============================================================================

func (s *Server) registerAPIRoutes(r *mux.Router) {
	// Note: /health and /info are registered on the root router (unversioned, no auth)
	r.HandleFunc("/stats", s.handleStats).Methods("GET")

	// Connections
	r.HandleFunc("/connections", s.handleListConnections).Methods("GET")
	r.HandleFunc("/connections/{session_id}", s.handleGetConnection).Methods("GET")
	r.HandleFunc("/connections/{session_id}", s.handleDisconnect).Methods("DELETE")

	// Tasks
	r.HandleFunc("/tasks", s.handleListTasks).Methods("GET")
	r.HandleFunc("/tasks/{task_id}", s.handleGetTask).Methods("GET")
	r.HandleFunc("/tasks/{task_id}/retry", s.handleRetryTask).Methods("POST")
	r.HandleFunc("/tasks/{task_id}/cancel", s.handleCancelTask).Methods("POST")

	// Workspaces
	r.HandleFunc("/workspaces", s.handleListWorkspaces).Methods("GET")
	r.HandleFunc("/workspaces", s.handleCreateWorkspace).Methods("POST")
	r.HandleFunc("/workspaces/{workspace_id}", s.handleGetWorkspace).Methods("GET")
	r.HandleFunc("/workspaces/{workspace_id}", s.handleUpdateWorkspace).Methods("PUT")
	r.HandleFunc("/workspaces/{workspace_id}", s.handleDeleteWorkspace).Methods("DELETE")
	r.HandleFunc("/workspaces/{workspace_id}/message-flow", s.handleGetMessageFlow).Methods("GET")

	// Agents & Orchestration
	r.HandleFunc("/agents", s.handleListAgents).Methods("GET")
	r.HandleFunc("/agents", s.handleCreateAgent).Methods("POST")
	r.HandleFunc("/agents/{implementation}", s.handleGetAgent).Methods("GET")
	r.HandleFunc("/agents/{implementation}", s.handleUpdateAgent).Methods("PUT")
	r.HandleFunc("/agents/{implementation}", s.handleDeleteAgent).Methods("DELETE")
	r.HandleFunc("/agents/{implementation}/launch", s.handleLaunchAgent).Methods("POST")
	r.HandleFunc("/orchestrators", s.handleListOrchestrators).Methods("GET")

	// KV Store
	r.HandleFunc("/kv", s.handleListKV).Methods("GET")
	r.HandleFunc("/kv/{scope}/{key:.*}", s.handleGetKV).Methods("GET")
	r.HandleFunc("/kv/{scope}/{key:.*}", s.handleSetKV).Methods("PUT")
	r.HandleFunc("/kv/{scope}/{key:.*}", s.handleDeleteKV).Methods("DELETE")

	// ACL Management
	r.HandleFunc("/acl/rules", s.handleListACLRules).Methods("GET")
	r.HandleFunc("/acl/rules", s.handleGrantACLAccess).Methods("POST")
	r.HandleFunc("/acl/rules/{rule_id}", s.handleGetACLRule).Methods("GET")
	r.HandleFunc("/acl/rules/{rule_id}", s.handleRevokeACLAccess).Methods("DELETE")
	r.HandleFunc("/acl/audit", s.handleQueryACLAuditLog).Methods("GET")
	r.HandleFunc("/acl/authority-grants", s.handleListACLAuthorityGrants).Methods("GET")
	r.HandleFunc("/acl/authority-grants", s.handleCreateACLAuthorityGrant).Methods("POST")
	r.HandleFunc("/acl/authority-grants/{grant_id}", s.handleGetACLAuthorityGrant).Methods("GET")
	r.HandleFunc("/acl/authority-grants/{grant_id}/renew", s.handleRenewACLAuthorityGrant).Methods("POST")
	r.HandleFunc("/acl/authority-grants/{grant_id}/revoke", s.handleRevokeACLAuthorityGrant).Methods("POST")
	r.HandleFunc("/acl/fallback-policy", s.handleGetACLFallbackPolicy).Methods("GET")
	r.HandleFunc("/acl/fallback-policy", s.handleSetACLFallbackPolicy).Methods("PUT")
	r.HandleFunc("/acl/cleanup/expired-rules", s.handleCleanupExpiredACLRules).Methods("POST")
	r.HandleFunc("/acl/cleanup/audit-logs", s.handleCleanupOldACLAuditLogs).Methods("POST")

	// API Tokens
	r.HandleFunc("/tokens", s.handleListTokens).Methods("GET")
	r.HandleFunc("/tokens", s.handleCreateToken).Methods("POST")
	r.HandleFunc("/tokens/{token_id}", s.handleGetToken).Methods("GET")
	r.HandleFunc("/tokens/{token_id}", s.handleDeleteToken).Methods("DELETE")
	r.HandleFunc("/tokens/{token_id}/revoke", s.handleRevokeToken).Methods("POST")

	// Messaging
	r.HandleFunc("/messages/send", s.handleSendMessage).Methods("POST")

	// Workspace Rate Limits
	r.HandleFunc("/rate-limits", s.handleListRateLimits).Methods("GET")
	r.HandleFunc("/workspaces/{workspace_id}/rate-limit", s.handleSetRateLimit).Methods("PUT")
	r.HandleFunc("/workspaces/{workspace_id}/rate-limit", s.handleGetRateLimit).Methods("GET")
	r.HandleFunc("/workspaces/{workspace_id}/rate-limit", s.handleRemoveRateLimit).Methods("DELETE")
}

// parsePagination reads ?limit=N&offset=N from a request, applying defaults and caps.
func parsePagination(r *http.Request) (limit, offset int) {
	limit = defaultPageSize
	offset = 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	return limit, offset
}

// applyPagination slices a generic slice according to limit/offset, returning the page
// and the total count before slicing.
func applyPagination[T any](items []T, limit, offset int) (page []T, total int) {
	total = len(items)
	if offset >= total {
		return []T{}, total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return items[offset:end], total
}

// =============================================================================
// Health & Info Handlers
// =============================================================================

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	health, err := s.provider.GetHealthStatus(r.Context())
	if err != nil {
		s.respondInternalError(w, "failed to get health status", err)
		return
	}
	respondJSON(w, http.StatusOK, health)
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	info, err := s.provider.GetGatewayInfo(r.Context())
	if err != nil {
		s.respondInternalError(w, "failed to get gateway info", err)
		return
	}
	respondJSON(w, http.StatusOK, info)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	health, err := s.provider.GetHealthStatus(r.Context())
	if err != nil {
		s.respondInternalError(w, "failed to get stats", err)
		return
	}
	respondJSON(w, http.StatusOK, health.Stats)
}

// =============================================================================
// Connection Handlers
// =============================================================================

func (s *Server) handleListConnections(w http.ResponseWriter, r *http.Request) {
	filter := &ConnectionFilter{
		Type:      r.URL.Query().Get("type"),
		Workspace: r.URL.Query().Get("workspace"),
	}

	connections, err := s.provider.GetConnections(r.Context(), filter)
	if err != nil {
		s.respondInternalError(w, "failed to list connections", err)
		return
	}

	limit, offset := parsePagination(r)
	page, total := applyPagination(connections, limit, offset)
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"connections": page,
		"count":       len(page),
		"total":       total,
		"limit":       limit,
		"offset":      offset,
	})
}

func (s *Server) handleGetConnection(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	sessionID := vars["session_id"]

	conn, err := s.provider.GetConnectionByID(r.Context(), sessionID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, conn)
}

func (s *Server) handleDisconnect(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	sessionID := vars["session_id"]

	if err := s.provider.DisconnectSession(r.Context(), sessionID); err != nil {
		s.respondInternalError(w, "failed to disconnect session", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("session %s disconnected", sessionID),
	})
}

// =============================================================================
// Task Handlers
// =============================================================================

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := &TaskFilter{
		Status:               q.Get("status"),
		Workspace:            q.Get("workspace"),
		TaskType:             q.Get("type"),
		SubjectType:          q.Get("subject_type"),
		SubjectID:            q.Get("subject_id"),
		AuthorityMode:        q.Get("authority_mode"),
		AuthorityGrantID:     q.Get("authority_grant_id"),
		RootAuthorityGrantID: q.Get("root_authority_grant_id"),
		ParentTaskID:         q.Get("parent_task_id"),
	}

	tasks, err := s.provider.GetTasks(r.Context(), filter)
	if err != nil {
		s.respondInternalError(w, "failed to list tasks", err)
		return
	}

	limit, offset := parsePagination(r)
	page, total := applyPagination(tasks, limit, offset)
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"tasks":  page,
		"count":  len(page),
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	taskID := vars["task_id"]

	task, err := s.provider.GetTaskByID(r.Context(), taskID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, task)
}

func (s *Server) handleRetryTask(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	taskID := vars["task_id"]

	if err := s.provider.RetryTask(r.Context(), taskID); err != nil {
		s.respondInternalError(w, "failed to retry task", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("task %s scheduled for retry", taskID),
	})
}

func (s *Server) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	taskID := vars["task_id"]

	if err := s.provider.CancelTask(r.Context(), taskID); err != nil {
		s.respondInternalError(w, "failed to cancel task", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("task %s cancelled", taskID),
	})
}

// =============================================================================
// Agent & Orchestrator Handlers
// =============================================================================

func (s *Server) handleListAgents(w http.ResponseWriter, r *http.Request) {
	agents, err := s.provider.GetAgentRegistrations(r.Context())
	if err != nil {
		s.respondInternalError(w, "failed to list agents", err)
		return
	}

	limit, offset := parsePagination(r)
	page, total := applyPagination(agents, limit, offset)
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"agents": page,
		"count":  len(page),
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (s *Server) handleGetAgent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	implementation := vars["implementation"]

	agent, err := s.provider.GetAgentByImplementation(r.Context(), implementation)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, agent)
}

func (s *Server) handleCreateAgent(w http.ResponseWriter, r *http.Request) {
	body := decodeJSON[struct {
		Implementation string                 `json:"implementation"`
		Description    string                 `json:"description"`
		LaunchParams   map[string]interface{} `json:"launch_params"`
	}](w, r)
	if body == nil {
		return
	}

	if body.Implementation == "" {
		respondError(w, http.StatusBadRequest, "implementation is required")
		return
	}

	agent := &AgentRegistrationInfo{
		Implementation: body.Implementation,
		Description:    body.Description,
		LaunchParams:   body.LaunchParams,
	}

	if err := s.provider.RegisterAgent(r.Context(), agent); err != nil {
		s.respondInternalError(w, "failed to register agent", err)
		return
	}

	respondJSON(w, http.StatusCreated, map[string]string{
		"message": fmt.Sprintf("agent %s registered successfully", body.Implementation),
	})
}

func (s *Server) handleUpdateAgent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	implementation := vars["implementation"]

	body := decodeJSON[struct {
		Description  string                 `json:"description"`
		LaunchParams map[string]interface{} `json:"launch_params"`
	}](w, r)
	if body == nil {
		return
	}

	agent := &AgentRegistrationInfo{
		Implementation: implementation,
		Description:    body.Description,
		LaunchParams:   body.LaunchParams,
	}

	if err := s.provider.UpdateAgent(r.Context(), implementation, agent); err != nil {
		s.respondInternalError(w, "failed to update agent", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("agent %s updated successfully", implementation),
	})
}

func (s *Server) handleDeleteAgent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	implementation := vars["implementation"]

	if err := s.provider.DeleteAgent(r.Context(), implementation); err != nil {
		s.respondInternalError(w, "failed to delete agent", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("agent %s deleted successfully", implementation),
	})
}

func (s *Server) handleListOrchestrators(w http.ResponseWriter, r *http.Request) {
	orchestrators, err := s.provider.GetOrchestratorProfiles(r.Context())
	if err != nil {
		s.respondInternalError(w, "failed to list orchestrators", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"orchestrators": orchestrators,
		"count":         len(orchestrators),
	})
}

func (s *Server) handleLaunchAgent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	implementation := vars["implementation"]

	body := decodeJSON[struct {
		Specifier string `json:"specifier"`
		Workspace string `json:"workspace"`
	}](w, r)
	if body == nil {
		return
	}

	// Default specifier to "default" if not provided
	if body.Specifier == "" {
		body.Specifier = "default"
	}

	// Default workspace to "default" if not provided
	if body.Workspace == "" {
		body.Workspace = "default"
	}

	req := &LaunchAgentRequest{
		Implementation: implementation,
		Specifier:      body.Specifier,
		Workspace:      body.Workspace,
	}

	resp, err := s.provider.LaunchAgent(r.Context(), req)
	if err != nil {
		s.respondInternalError(w, "failed to launch agent", err)
		return
	}

	respondJSON(w, http.StatusOK, resp)
}

// =============================================================================
// KV Handlers
// =============================================================================

func (s *Server) handleListKV(w http.ResponseWriter, r *http.Request) {
	scope := r.URL.Query().Get("scope")
	if scope == "" {
		scope = "global"
	}
	prefix := r.URL.Query().Get("prefix")

	keys, err := s.provider.GetKVKeys(r.Context(), scope, prefix)
	if err != nil {
		s.respondInternalError(w, "failed to list KV keys", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"keys":  keys,
		"count": len(keys),
		"scope": scope,
	})
}

func (s *Server) handleGetKV(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	scope := vars["scope"]
	key := vars["key"]

	entry, err := s.provider.GetKVValue(r.Context(), scope, key)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, entry)
}

func (s *Server) handleSetKV(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	scope := vars["scope"]
	key := vars["key"]

	body := decodeJSON[struct {
		Value string `json:"value"`
		TTL   int64  `json:"ttl"`
	}](w, r)
	if body == nil {
		return
	}

	if err := s.provider.SetKVValue(r.Context(), scope, key, body.Value, body.TTL); err != nil {
		s.respondInternalError(w, "failed to set KV value", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": "key set successfully",
	})
}

func (s *Server) handleDeleteKV(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	scope := vars["scope"]
	key := vars["key"]

	if err := s.provider.DeleteKVKey(r.Context(), scope, key); err != nil {
		s.respondInternalError(w, "failed to delete KV key", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": "key deleted successfully",
	})
}

// =============================================================================
// Messaging Handlers
// =============================================================================

func (s *Server) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	req := decodeJSON[SendMessageRequest](w, r)
	if req == nil {
		return
	}

	// Validate required fields
	if req.TargetTopic == "" {
		respondError(w, http.StatusBadRequest, "target_topic is required")
		return
	}

	// Validate message type if provided
	validTypes := map[string]bool{"CHAT": true, "CONTROL": true, "TOOL_CALL": true, "EVENT": true, "METRIC": true, "": true}
	if !validTypes[req.MessageType] {
		respondError(w, http.StatusBadRequest, "invalid message_type")
		return
	}

	if err := s.provider.SendMessage(r.Context(), req); err != nil {
		s.respondInternalError(w, "failed to send message", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": "Message sent successfully",
		"target":  req.TargetTopic,
	})
}

// =============================================================================
// Workspace Rate Limit Handlers
// =============================================================================

func (s *Server) handleListRateLimits(w http.ResponseWriter, r *http.Request) {
	limits, err := s.provider.ListWorkspaceRateLimits()
	if err != nil {
		s.respondInternalError(w, "failed to list workspace rate limits", err)
		return
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"rate_limits": limits,
	})
}

func (s *Server) handleGetRateLimit(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workspace := vars["workspace_id"]

	rate, err := s.provider.GetWorkspaceRateLimit(workspace)
	if err != nil {
		s.respondInternalError(w, "failed to get workspace rate limit", err)
		return
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"workspace":           workspace,
		"messages_per_second": rate,
	})
}

func (s *Server) handleSetRateLimit(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workspace := vars["workspace_id"]

	body := decodeJSON[struct {
		MessagesPerSecond float64 `json:"messages_per_second"`
	}](w, r)
	if body == nil {
		return
	}

	if body.MessagesPerSecond < 0 {
		respondError(w, http.StatusBadRequest, "messages_per_second must be >= 0")
		return
	}

	if err := s.provider.SetWorkspaceRateLimit(workspace, body.MessagesPerSecond); err != nil {
		s.respondInternalError(w, "failed to set workspace rate limit", err)
		return
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"workspace":           workspace,
		"messages_per_second": body.MessagesPerSecond,
		"message":             "rate limit updated",
	})
}

func (s *Server) handleRemoveRateLimit(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workspace := vars["workspace_id"]

	if err := s.provider.RemoveWorkspaceRateLimit(workspace); err != nil {
		s.respondInternalError(w, "failed to remove workspace rate limit", err)
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("rate limit removed for workspace %s", workspace),
	})
}

// =============================================================================
// WebSocket Handler
// =============================================================================

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Auth is handled by apiKeyAuthMiddleware on the /api subrouter.
	// Check if the client authenticated via Sec-WebSocket-Protocol subprotocol so we
	// can echo back the "auth" subprotocol in the upgrade response.
	tokenViaSubprotocol := false
	protocols := websocket.Subprotocols(r)
	for i, p := range protocols {
		if p == "auth" && i+1 < len(protocols) {
			tokenViaSubprotocol = true
			break
		}
	}

	var responseHeader http.Header
	if tokenViaSubprotocol {
		responseHeader = http.Header{"Sec-WebSocket-Protocol": []string{"auth"}}
	}

	conn, err := s.upgrader.Upgrade(w, r, responseHeader)
	if err != nil {
		logging.Logger.Error().Err(err).Msg("WebSocket upgrade error")
		return
	}

	ctx, cancel := context.WithCancel(r.Context())

	wsConn := &wsConnection{
		conn:       conn,
		cancel:     cancel,
		monitors:   make(map[string]func()),
		remoteAddr: r.RemoteAddr,
	}

	// Track connection
	s.wsMu.Lock()
	s.wsConns[conn] = wsConn
	s.wsMu.Unlock()

	defer func() {
		// Cleanup all monitors
		wsConn.mu.Lock()
		for topic, cancelFn := range wsConn.monitors {
			cancelFn()
			logging.Logger.Info().Str("topic", topic).Str("remote_addr", wsConn.remoteAddr).Msg("WebSocket monitor subscription cleaned up")
		}
		wsConn.mu.Unlock()

		s.wsMu.Lock()
		delete(s.wsConns, conn)
		s.wsMu.Unlock()
		cancel()
		conn.Close()
	}()

	// Subscribe to events
	eventCh, err := s.provider.SubscribeEvents(ctx)
	if err != nil {
		logging.Logger.Error().Err(err).Msg("failed to subscribe to events")
		return
	}

	// Send events to WebSocket
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logging.Logger.Error().Interface("panic", r).Str("stack", string(debug.Stack())).Str("goroutine", "wsEventForwarder").Msg("recovered from panic in background goroutine")
			}
		}()
		for event := range eventCh {
			wsConn.sendJSON(&WSServerMessage{
				Type:  "event",
				Event: event,
			})
		}
	}()

	// Read and process client messages
	for {
		var msg WSClientMessage
		err := conn.ReadJSON(&msg)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logging.Logger.Error().Err(err).Msg("WebSocket error")
			}
			break
		}

		s.handleWSCommand(ctx, wsConn, &msg)
	}
}

func (s *Server) handleWSCommand(ctx context.Context, wsConn *wsConnection, msg *WSClientMessage) {
	switch msg.Action {
	case "subscribe_monitor":
		s.subscribeMonitor(ctx, wsConn, msg.Topic)
	case "unsubscribe_monitor":
		s.unsubscribeMonitor(wsConn, msg.Topic)
	default:
		// Unknown action, ignore
	}
}

func (s *Server) subscribeMonitor(ctx context.Context, wsConn *wsConnection, topic string) {
	if topic == "" {
		wsConn.sendJSON(&WSServerMessage{
			Type:  "error",
			Error: "topic is required for subscribe_monitor",
		})
		return
	}

	wsConn.mu.Lock()
	// Check if already subscribed
	if _, exists := wsConn.monitors[topic]; exists {
		wsConn.mu.Unlock()
		wsConn.sendJSON(&WSServerMessage{
			Type:  "error",
			Error: fmt.Sprintf("already subscribed to topic %s", topic),
		})
		return
	}
	wsConn.mu.Unlock()

	// Create subscription
	cancelFn, err := s.provider.SubscribeToTopic(ctx, topic, func(msg *MonitoredMessage) {
		wsConn.sendJSON(&WSServerMessage{
			Type:    "monitor_message",
			Message: msg,
			Topic:   topic,
		})
	})

	if err != nil {
		wsConn.sendJSON(&WSServerMessage{
			Type:  "error",
			Error: fmt.Sprintf("failed to subscribe to topic %s: %v", topic, err),
		})
		return
	}

	wsConn.mu.Lock()
	wsConn.monitors[topic] = cancelFn
	wsConn.mu.Unlock()

	logging.Logger.Info().Str("topic", topic).Str("remote_addr", wsConn.remoteAddr).Msg("WebSocket monitor subscription started")

	wsConn.sendJSON(&WSServerMessage{
		Type:  "monitor_subscribed",
		Topic: topic,
	})
}

func (s *Server) unsubscribeMonitor(wsConn *wsConnection, topic string) {
	wsConn.mu.Lock()
	defer wsConn.mu.Unlock()

	if cancelFn, exists := wsConn.monitors[topic]; exists {
		cancelFn()
		delete(wsConn.monitors, topic)
		logging.Logger.Info().Str("topic", topic).Str("remote_addr", wsConn.remoteAddr).Msg("WebSocket monitor subscription ended")

		wsConn.sendJSON(&WSServerMessage{
			Type:  "monitor_unsubscribed",
			Topic: topic,
		})
	}
}

func (wsc *wsConnection) sendJSON(msg *WSServerMessage) {
	wsc.mu.Lock()
	defer wsc.mu.Unlock()
	if err := wsc.conn.WriteJSON(msg); err != nil {
		logging.Logger.Error().Err(err).Msg("WebSocket write error")
	}
}

// BroadcastEvent sends an event to all connected WebSocket clients
func (s *Server) BroadcastEvent(event *Event) {
	s.wsMu.RLock()
	defer s.wsMu.RUnlock()

	for _, wsConn := range s.wsConns {
		wsConn.sendJSON(&WSServerMessage{
			Type:  "event",
			Event: event,
		})
	}
}

// =============================================================================
// Static File Serving
// =============================================================================

func (s *Server) registerStaticRoutes(r *mux.Router) {
	if s.config.DevMode && s.config.DevPath != "" {
		// Serve from filesystem in dev mode
		logging.Logger.Info().Str("path", s.config.DevPath).Msg("admin UI serving static files")
		r.PathPrefix("/").Handler(http.FileServer(http.Dir(s.config.DevPath)))
	} else {
		// Serve from embedded filesystem
		subFS, err := fs.Sub(staticFS, "static")
		if err != nil {
			logging.Logger.Warn().Err(err).Msg("failed to create sub filesystem")
			return
		}

		// Custom handler to serve index.html for SPA routes
		r.PathPrefix("/").Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := strings.TrimPrefix(r.URL.Path, "/")
			if path == "" {
				path = "index.html"
			}

			// Try to open the file
			f, err := subFS.Open(path)
			if err != nil {
				// If not found, serve index.html (SPA routing)
				path = "index.html"
			} else {
				f.Close()
			}

			// Serve the file
			http.ServeFileFS(w, r, subFS, path)
		}))
	}
}

// =============================================================================
// Middleware
// =============================================================================

// apiKeyAuthMiddleware validates the Authorization: Bearer <token> header
// against the configured API key. Health endpoint is exempt.
func (s *Server) apiKeyAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			// WebSocket clients cannot send arbitrary headers in the browser; they may
			// pass the token via the Sec-WebSocket-Protocol subprotocol instead.
			protocols := websocket.Subprotocols(r)
			for i, p := range protocols {
				if p == "auth" && i+1 < len(protocols) {
					authHeader = "Bearer " + protocols[i+1]
					break
				}
			}
		}
		if authHeader == "" {
			respondError(w, http.StatusUnauthorized, "missing Authorization header")
			return
		}

		const bearerPrefix = "Bearer "
		if len(authHeader) < len(bearerPrefix) || authHeader[:len(bearerPrefix)] != bearerPrefix {
			respondError(w, http.StatusUnauthorized, "invalid Authorization header format")
			return
		}

		token := authHeader[len(bearerPrefix):]
		// Use GetAPIKey callback when set (enables SIGHUP hot-reload); fall back to static APIKey.
		currentKey := s.config.APIKey
		if s.config.GetAPIKey != nil {
			currentKey = s.config.GetAPIKey()
		}
		if subtle.ConstantTimeCompare([]byte(token), []byte(currentKey)) != 1 {
			respondError(w, http.StatusForbidden, "invalid API key")
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Security headers applied to all responses
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		if !s.config.DevMode {
			w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self' wss: ws:; img-src 'self' data:")
		}
		// Dev mode: no CSP header — allow CDN scripts, inline styles, etc.
		w.Header().Set("X-XSS-Protection", "0") // Disabled per OWASP — CSP supersedes
		if s.config.TLSCertFile != "" {
			w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		}

		origin := s.config.CORSOrigin
		if origin == "" {
			// No CORS headers = same-origin only (browser default)
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// =============================================================================
// Helpers
// =============================================================================

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		logging.Logger.Error().Err(err).Msg("error encoding JSON response")
	}
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{
		"error": message,
	})
}

// respondInternalError logs the full error server-side and returns a sanitized
// generic message to the client, preventing internal details from leaking.
func (s *Server) respondInternalError(w http.ResponseWriter, msg string, err error) {
	logging.Logger.Error().Err(err).Msg(msg)
	respondError(w, http.StatusInternalServerError, msg)
}

// decodeJSON decodes the JSON request body into a value of type T.
// Returns a pointer to the decoded value, or writes a 400 Bad Request response
// and returns nil if decoding fails. Internal decode errors are logged but not
// exposed to the client to avoid leaking implementation details.
func decodeJSON[T any](w http.ResponseWriter, r *http.Request) *T {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB limit
	var v T
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		logging.Logger.Debug().Err(err).Msg("failed to decode JSON request body")
		respondError(w, http.StatusBadRequest, "invalid JSON request body")
		return nil
	}
	return &v
}
