package gateway

import (
	"context"
	"database/sql"
	"runtime/debug"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/auth"
	"github.com/scitrera/aether/internal/circuitbreaker"
	"github.com/scitrera/aether/internal/cleanup"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/orchestration"
	"github.com/scitrera/aether/internal/quota"
	"github.com/scitrera/aether/internal/timer"
	"github.com/scitrera/aether/pkg/tasks"
)

// GatewayServer implements the Aether gateway gRPC service.
type GatewayServer struct {
	pb.UnimplementedAetherGatewayServer
	sessions    SessionManager
	router      MessageRouter
	kv          KVReadWriter
	checkpoints CheckpointManager
	taskStore   *tasks.TaskStore
	timerSeq    *timer.TimerSequence
	timeoutHdlr *timer.TimeoutHandler
	// acl is retained directly for use in connection and workspace-switch checks.
	acl         *acl.Service
	auditLogger *audit.AuditLogger
	gatewayID   string
	// Map of active streams by session ID
	activeStreams sync.Map
	// Secondary index: identity string -> sessionID for O(1) lookup
	identityIndex sync.Map
	// implementationIndex maps "workspace:implementation" -> []*ClientSession (agents only).
	// Used for O(1) pool task worker lookup with power-of-two-choices load balancing.
	implIndexMu         sync.RWMutex
	implementationIndex map[string][]*ClientSession
	// orchestratorIndex maps "workspace:profile" -> []*ClientSession (orchestrators only).
	// Used for O(1) orchestrator lookup by profile in findOrchestratorByProfile.
	orchestratorIndexMu sync.RWMutex
	orchestratorIndex   map[string][]*ClientSession
	// Orchestration: Task assignment and orchestration
	orchestration *OrchestrationServices
	// Background cleanup runner (manages reconciliation, task purge, etc.)
	cleanupRunner *cleanup.BackgroundRunner
	// Checkpoint default TTL (used when client sends -1)
	checkpointDefaultTTL time.Duration
	// Cached KV handler (stateless, reused across all KV operations)
	kvHandler *KVHandler
	// WaitGroup tracking active Connect goroutines for graceful shutdown
	activeConns sync.WaitGroup
	// Circuit breaker protecting Redis lock refresh calls
	redisBreaker *circuitbreaker.CircuitBreaker
	// Circuit breaker protecting RabbitMQ publish calls
	publishBreaker *circuitbreaker.CircuitBreaker
	// authHandler encapsulates mTLS, identity resolution, and credential authentication.
	authHandler *AuthHandler
	// quotaEnforcer encapsulates per-tenant quota and per-client rate limit configuration.
	quotaEnforcer *QuotaEnforcer
	// Pending workflow operation requests: request_id -> *pendingWorkflowRequest
	pendingWorkflowRequests sync.Map
	// offlineTopicCache caches topics known to be offline to avoid Redis round-trips.
	// Values are time.Time (last-checked timestamp). Entries expire after offlineCacheTTL.
	offlineTopicCache sync.Map
	// envelopeSyncMap holds per-message proto parse results keyed by (pointer, len) pair.
	// Enables single parse per fan-out cycle when multiple clients share a broadcast topic.
	envelopeSyncMap sync.Map
	// adminProvider provides access to admin state for gRPC workspace/agent/ACL/token operations.
	adminProvider admin.StateProvider
	// bgCtx and bgCancel manage the lifetime of server-owned background goroutines.
	bgCtx    context.Context
	bgCancel context.CancelFunc
	// deliveryBufferSize is the capacity of each client's outbound message channel.
	// Defaults to the package-level defaultDeliveryBufferSize constant.
	deliveryBufferSize int
	// stopOnce ensures Stop() is idempotent (safe to call multiple times).
	stopOnce sync.Once
	// activeTunnels holds the per-workspace live-tunnel counter for proxy quota
	// enforcement. Keyed by workspace string → *activeTunnelCounter.
	activeTunnels sync.Map
	// tunnelByteCounters holds per-tunnel cumulative byte counters used to
	// enforce maxTunnelBytes. Keyed by tunnel_id → *atomic.Int64.
	tunnelByteCounters sync.Map
	// proxyLocalBypassEnabled gates the single-node data-plane fast path that
	// delivers TunnelData / TunnelAck / ProxyHttpBodyChunk directly to a
	// locally-connected target sidecar instead of round-tripping through RMQ.
	// Defaults to true; control-plane envelopes always take the RMQ path
	// regardless so audit emission is preserved.
	proxyLocalBypassEnabled bool
}

// MTLSConfig holds mTLS configuration for the gateway server
type MTLSConfig struct {
	// Required specifies whether mTLS is required for all connections.
	// If true, connections without a client certificate will be rejected.
	// Default: true
	Required bool

	// Mode specifies how strictly to interpret client certificate identities.
	// - strict: Certificate CN must fully specify the identity (existing behavior)
	// - relaxed: Certificate only confirms principal type, details from InitConnection
	// Default: MTLSModeStrict
	Mode MTLSMode
}

// DefaultMTLSConfig returns the default mTLS configuration
func DefaultMTLSConfig() MTLSConfig {
	return MTLSConfig{
		Required: true,
		Mode:     MTLSModeStrict,
	}
}

// GatewayOption configures optional features of the GatewayServer.
type GatewayOption func(*GatewayServer)

// WithOrchestrationServices enables orchestration (task dispatch, task service, agent registration).
func WithOrchestrationServices(orchestrationSvc *OrchestrationServices) GatewayOption {
	return func(s *GatewayServer) {
		s.SetOrchestrationServices(orchestrationSvc)
	}
}

// WithAuthenticator sets the composite authentication provider for API key and OAuth validation.
func WithAuthenticator(authenticator *auth.CompositeAuthenticator) GatewayOption {
	return func(s *GatewayServer) {
		s.authHandler.authenticator = authenticator
	}
}

// WithCleanupService configures and starts the cleanup service for background jobs.
// Must be applied after WithOrchestrationServices if both are used.
func WithCleanupService(cleanupConfig *cleanup.Config) GatewayOption {
	return func(s *GatewayServer) {
		s.SetCleanupService(cleanupConfig)
	}
}

// WithCheckpointDefaultTTL sets the default TTL for checkpoints when client sends -1.
// A value of 0 means no expiration.
func WithCheckpointDefaultTTL(ttl time.Duration) GatewayOption {
	return func(s *GatewayServer) {
		s.checkpointDefaultTTL = ttl
	}
}

// WithMessageRateLimit sets per-client message rate limiting.
// ratePerSec is the sustained rate (messages/second), burst is the maximum burst size.
func WithMessageRateLimit(ratePerSec float64, burst int) GatewayOption {
	return func(s *GatewayServer) {
		s.quotaEnforcer.messageRateLimit = ratePerSec
		s.quotaEnforcer.messageRateBurst = burst
	}
}

// WithCircuitBreaker sets a custom circuit breaker for protecting Redis lock refresh calls.
// If not set, a default circuit breaker (maxFailures=5, resetTimeout=30s) is used.
func WithCircuitBreaker(cb *circuitbreaker.CircuitBreaker) GatewayOption {
	return func(s *GatewayServer) {
		s.redisBreaker = cb
	}
}

// WithPublishCircuitBreaker sets a custom circuit breaker for protecting RabbitMQ publish calls.
// If not set, a default circuit breaker (maxFailures=10, resetTimeout=15s) is used.
func WithPublishCircuitBreaker(cb *circuitbreaker.CircuitBreaker) GatewayOption {
	return func(s *GatewayServer) {
		s.publishBreaker = cb
	}
}

// WithQuotaManager sets the quota checker for multi-tenant connection and rate limiting.
// Accepts any QuotaChecker implementation (e.g., *quota.QuotaManager for Redis-backed,
// or an in-memory implementation for lite mode).
func WithQuotaManager(qm QuotaChecker) GatewayOption {
	return func(s *GatewayServer) {
		s.quotaEnforcer.quotaManager = qm
	}
}

// WithWorkspaceRateLimiter sets the workspace-level message rate limiter.
// When set, each workspace's total message throughput is enforced against its configured limit.
func WithWorkspaceRateLimiter(wrl *quota.WorkspaceRateLimiter) GatewayOption {
	return func(s *GatewayServer) {
		s.quotaEnforcer.workspaceRateLimiter = wrl
	}
}

// WithForeignAuditRateLimiter sets the per-principal rate limiter that gates
// foreign audit submissions (SubmitAuditEvent). When set, each authenticated
// principal is enforced against its configured rate to prevent DoS-style log
// flooding from compromised or buggy clients. Pass nil (or omit the option)
// to disable per-principal rate limiting.
func WithForeignAuditRateLimiter(rl *quota.PrincipalRateLimiter) GatewayOption {
	return func(s *GatewayServer) {
		s.quotaEnforcer.foreignAuditRateLimiter = rl
	}
}

// WithDeliveryBufferSize sets the per-client outbound message channel capacity.
// If not set or set to zero, defaults to 256 (defaultDeliveryBufferSize).
func WithDeliveryBufferSize(size int) GatewayOption {
	return func(s *GatewayServer) {
		if size > 0 {
			s.deliveryBufferSize = size
		}
	}
}

// WithMaxTaskPayloadSize sets the maximum allowed size (in bytes) for task payloads.
// Default is 512KB if not set or zero.
func WithMaxTaskPayloadSize(size int) GatewayOption {
	return func(s *GatewayServer) {
		s.quotaEnforcer.maxTaskPayloadSize = size
	}
}

// WithProxyLocalBypassEnabled toggles the single-node proxy/tunnel data-plane
// fast path. Defaults to true. Pass false (or set
// AETHER_PROXY_LOCAL_BYPASS_DISABLED=1 in the environment) to roll back to the
// always-via-RMQ behavior. Control-plane envelopes are unaffected and always
// go through RMQ for audit preservation.
func WithProxyLocalBypassEnabled(enabled bool) GatewayOption {
	return func(s *GatewayServer) {
		s.proxyLocalBypassEnabled = enabled
	}
}

// WithProxyQuotas configures the proxy/tunnel quota knobs:
//   - maxConcurrentTunnelsPerWorkspace caps live tunnels per workspace.
//   - maxRequestBodyBytes caps a single ProxyHttpRequest body size.
//   - maxTunnelBytes caps cumulative bytes per tunnel; 0 means unlimited.
//
// Pass 0 for any value to keep the existing default.
func WithProxyQuotas(maxConcurrentTunnelsPerWorkspace, maxRequestBodyBytes int, maxTunnelBytes int64) GatewayOption {
	return func(s *GatewayServer) {
		if maxConcurrentTunnelsPerWorkspace > 0 {
			s.quotaEnforcer.maxConcurrentTunnelsPerWorkspace = maxConcurrentTunnelsPerWorkspace
		}
		if maxRequestBodyBytes > 0 {
			s.quotaEnforcer.maxRequestBodyBytes = maxRequestBodyBytes
		}
		if maxTunnelBytes > 0 {
			s.quotaEnforcer.maxTunnelBytes = maxTunnelBytes
		}
	}
}

// WithProxyMaxChainDepth caps the proxy/tunnel hop count. Each gateway hop
// increments ProxyHttpRequest.proxy_chain_depth and TunnelOpen.proxy_chain_depth
// by 1; envelopes whose inbound depth >= maxChainDepth are rejected before
// forwarding, breaking sandbox→sandbox proxy loops. 0 keeps the default (8).
func WithProxyMaxChainDepth(maxChainDepth uint32) GatewayOption {
	return func(s *GatewayServer) {
		if maxChainDepth > 0 {
			s.quotaEnforcer.maxChainDepth = maxChainDepth
		}
	}
}

// WithACLService injects a pre-constructed ACL service into the gateway server,
// replacing any ACL service that would have been created internally from db+gatewayID.
// Use this to share a single ACL service instance across the gateway and state provider.
func WithACLService(svc *acl.Service) GatewayOption {
	return func(s *GatewayServer) {
		if svc == nil {
			return
		}
		s.acl = svc
		s.kvHandler = newKVHandlerFromService(s.kv, s.auditLogger, svc)
		// Preserve tokenStore across authHandler rebuild: options can set it before
		// this rebuild fires (e.g., WithOrchestrationServices → SetOrchestrationServices).
		// newAuthHandler constructs fresh, so without this copy the tokenStore becomes nil
		// and orchestration task-token validation silently stops working.
		previousTokenStore := s.authHandler.tokenStore
		s.authHandler = newAuthHandler(s.authHandler.authenticator, s.authHandler.mtlsRequired, s.authHandler.mtlsMode, svc, s.auditLogger)
		s.authHandler.tokenStore = previousTokenStore
	}
}

// NewGatewayServer creates a new GatewayServer with the given dependencies and options.
func NewGatewayServer(sessions SessionManager, router MessageRouter, kvStore KVReadWriter, checkpointStore CheckpointManager, taskStore *tasks.TaskStore, db *sql.DB, gatewayID string, auditLogger *audit.AuditLogger, mtlsConfig MTLSConfig, opts ...GatewayOption) *GatewayServer {
	ts := timer.NewTimerSequence()

	// Implement actual reschedule function that persists retry timing
	rescheduleFn := func(taskID string, delay time.Duration) {
		if taskStore == nil {
			logging.Logger.Warn().Str("task_id", taskID).Msg("cannot reschedule task, no taskStore")
			return
		}

		ctx := context.Background()
		retryAt := time.Now().Add(delay)
		err := taskStore.RescheduleTaskAt(ctx, taskID, retryAt)
		if err != nil {
			logging.Logger.Error().Err(err).Str("task_id", taskID).Msg("failed to reschedule task")
		} else {
			logging.Logger.Info().Str("task_id", taskID).Time("retry_at", retryAt).Msg("rescheduled task for retry")
		}
	}

	th := timer.NewTimeoutHandler(taskStore, ts, rescheduleFn)

	// Audit logger is now passed from main.go with configuration from environment

	// Apply defaults for unset mTLS config values
	cfg := mtlsConfig
	if cfg.Mode == "" {
		cfg.Mode = MTLSModeStrict
	}

	// Validate mode
	if !cfg.Mode.IsValid() {
		logging.Logger.Warn().Str("mode", string(cfg.Mode)).Msg("invalid mTLS mode, falling back to strict mode")
		cfg.Mode = MTLSModeStrict
	}

	s := &GatewayServer{
		sessions:                sessions,
		router:                  router,
		kv:                      kvStore,
		checkpoints:             checkpointStore,
		taskStore:               taskStore,
		timerSeq:                ts,
		timeoutHdlr:             th,
		auditLogger:             auditLogger,
		gatewayID:               gatewayID,
		implementationIndex:     make(map[string][]*ClientSession),
		orchestratorIndex:       make(map[string][]*ClientSession),
		kvHandler:               newKVHandlerFromService(kvStore, auditLogger, nil),
		redisBreaker:            circuitbreaker.New("redis", circuitbreaker.WithMaxFailures(5), circuitbreaker.WithResetTimeout(30*time.Second)),
		publishBreaker:          circuitbreaker.New("rabbitmq-publish", circuitbreaker.WithMaxFailures(10), circuitbreaker.WithResetTimeout(15*time.Second)),
		authHandler:             newAuthHandler(nil, cfg.Required, cfg.Mode, nil, auditLogger),
		quotaEnforcer:           newQuotaEnforcer(100, 200),
		deliveryBufferSize:      defaultDeliveryBufferSize,
		proxyLocalBypassEnabled: true,
	}
	// Apply options first — WithACLService may inject a pre-built ACL service,
	// in which case we must not create a second one from db below.
	for _, opt := range opts {
		opt(s)
	}

	// Initialize ACL service from db only when none was supplied via WithACLService.
	// Note: ACL schema is created by migrations/002_acl_schema.sql
	if s.acl == nil && db != nil {
		aclService := acl.NewService(db, gatewayID)
		logging.Logger.Debug().Msg("ACL service initialized")
		s.acl = aclService
		s.kvHandler = newKVHandlerFromService(kvStore, auditLogger, aclService)
		// Preserve tokenStore across authHandler rebuild (see WithACLService note above).
		previousTokenStore := s.authHandler.tokenStore
		s.authHandler = newAuthHandler(s.authHandler.authenticator, cfg.Required, cfg.Mode, aclService, auditLogger)
		s.authHandler.tokenStore = previousTokenStore
	}
	if s.acl != nil && s.orchestration != nil && s.orchestration.TaskService != nil {
		s.orchestration.TaskService.SetAuthorityGrantService(s.acl)
	}

	// Seed default-allow fallback for KV scope permissions (agent + task).
	// This ensures KV operations are allowed by default when no explicit rules are set.
	// Admins can restrict access by creating explicit GRANT rules with lower access levels
	// or by updating the fallback policy via the admin API.
	if s.acl != nil {
		ctx := context.Background()
		for _, principalType := range []string{"agent", "task"} {
			for _, resType := range []string{acl.ResourceTypeKVScope, acl.ResourceTypeKVKey} {
				category := acl.RuleCategory(principalType, resType)
				if _, err := s.acl.GetFallbackPolicy(ctx, category); err != nil {
					if setErr := s.acl.SetFallbackPolicy(ctx, category, acl.AccessReadWrite, acl.SystemPrincipal); setErr != nil {
						logging.Logger.Warn().Err(setErr).Str("category", category).Msg("failed to seed KV fallback policy")
					}
				}
			}
		}
	}

	// Start background goroutines under a cancellable context so Stop() can shut them down.
	bgCtx, bgCancel := context.WithCancel(context.Background())
	s.bgCtx = bgCtx
	s.bgCancel = bgCancel
	s.startWorkflowRequestSweeper(bgCtx)
	s.startOfflineCacheEvictor(bgCtx)
	// Disconnect reaper: connection-as-heartbeat enforcement. Worker stream
	// closes mark tasks with disconnected_at; this reaper fails tasks whose
	// grace window has elapsed without a reconnect. Multi-gateway safe —
	// FailTask is a state-machine transition, no leader election required.
	if s.taskStore != nil && s.orchestration != nil && s.orchestration.TaskService != nil {
		reaper := orchestration.NewDisconnectReaper(s.taskStore, s.orchestration.TaskService, s)
		go reaper.Run(bgCtx)
	}

	return s
}

// HasActiveSessionForTask satisfies orchestration.SessionLivenessProbe.
// Returns true if any active client stream is currently associated with the
// given taskID, meaning the worker has reconnected. The reaper uses this for
// race protection between its SELECT and the FailTask call.
func (s *GatewayServer) HasActiveSessionForTask(_ context.Context, taskID string) bool {
	if taskID == "" {
		return false
	}
	found := false
	s.activeStreams.Range(func(_, value interface{}) bool {
		cs, ok := value.(*ClientSession)
		if !ok {
			return true
		}
		if cs.AssociatedTaskID == taskID {
			found = true
			return false // stop iteration
		}
		return true
	})
	return found
}

// SetAdminProvider sets the admin state provider for gRPC workspace/agent/ACL operations.
// Must be called after construction, typically in main after NewGatewayStateProvider.
func (s *GatewayServer) SetAdminProvider(p admin.StateProvider) {
	s.adminProvider = p
}

// Stop gracefully shuts down the gateway and timer systems.
// Safe to call multiple times.
func (s *GatewayServer) Stop() {
	s.stopOnce.Do(s.doStop)
}

func (s *GatewayServer) doStop() {
	// Cancel background goroutines (workflow sweeper, offline cache evictor)
	if s.bgCancel != nil {
		s.bgCancel()
	}

	// Send GRACEFUL_DISCONNECT to all connected clients
	var disconnectCount int
	s.activeStreams.Range(func(key, value interface{}) bool {
		session, ok := value.(*ClientSession)
		if !ok {
			return true
		}
		disconnectCount++

		// Mark this disconnect as server-initiated so cleanupSession leaves
		// the associated task alone. Worker will reconnect (here on resume,
		// or elsewhere in the fleet) and pick up its task without auth or
		// authority-grant disruption.
		session.serverInitiatedDisconnect.Store(true)

		// Send disconnect signal
		if session.Stream != nil {
			err := session.SafeSend(&pb.DownstreamMessage{
				Payload: &pb.DownstreamMessage_Signal{
					Signal: &pb.Signal{
						Type:   pb.Signal_GRACEFUL_DISCONNECT,
						Reason: "server shutting down",
					},
				},
			})
			if err != nil {
				logging.Logger.Error().Err(err).Str("identity", session.Identity.String()).Msg("failed to send shutdown signal")
			}
		}

		// Cancel the session context
		if session.Cancel != nil {
			session.Cancel()
		}
		return true
	})
	if disconnectCount > 0 {
		logging.Logger.Info().Int("count", disconnectCount).Msg("sent shutdown signal to connected clients")
	}

	// Wait for active connections to finish cleanup
	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logging.Logger.Error().Interface("panic", r).Str("stack", string(debug.Stack())).Str("goroutine", "shutdownDrain").Msg("recovered from panic in background goroutine")
			}
		}()
		s.activeConns.Wait()
		close(done)
	}()
	select {
	case <-done:
		logging.Logger.Info().Msg("all connections drained successfully")
	case <-time.After(10 * time.Second):
		logging.Logger.Warn().Msg("connection drain timed out, some cleanup may be incomplete")
	}

	if s.timerSeq != nil {
		s.timerSeq.Stop()
	}
	if s.acl != nil {
		s.acl.Close()
		logging.Logger.Info().Msg("ACL service stopped")
	}
	// Stop background cleanup jobs (reconciliation, task purge)
	if s.cleanupRunner != nil {
		s.cleanupRunner.Stop()
		logging.Logger.Info().Msg("cleanup service stopped")
	}
	// Clean up any pending workflow requests with error responses
	s.pendingWorkflowRequests.Range(func(key, value interface{}) bool {
		pending, ok := value.(*pendingWorkflowRequest)
		if ok {
			requestID, _ := key.(string)
			_ = pending.client.SafeSend(&pb.DownstreamMessage{
				Payload: &pb.DownstreamMessage_WorkflowResponse{
					WorkflowResponse: &pb.WorkflowResponse{
						Success:   false,
						Error:     "server shutting down",
						RequestId: requestID,
					},
				},
			})
		}
		s.pendingWorkflowRequests.Delete(key)
		return true
	})
	// orchestration cleanup
	s.CleanupOrchestration()
}

// SetOrchestrationServices sets Orchestration services (called after construction if needed).
//
// Deprecated: Use WithOrchestrationServices option with NewGatewayServer instead.
func (s *GatewayServer) SetOrchestrationServices(orchestration *OrchestrationServices) {
	s.orchestration = orchestration

	// Wire the token store into the auth handler so credential validation
	// can validate orchestration task tokens without needing a full orchestration reference.
	if orchestration.TokenStore != nil {
		s.authHandler.tokenStore = orchestration.TokenStore
	}
	if s.acl != nil && orchestration.TaskService != nil {
		orchestration.TaskService.SetAuthorityGrantService(s.acl)
	}

	// Configure dispatcher callback to route tasks to connected orchestrators
	if orchestration.Dispatcher != nil {
		s.configureOrchestratorDispatcher()

		// Start the dispatcher
		ctx := context.Background()
		if err := orchestration.Dispatcher.Start(ctx); err != nil {
			logging.Logger.Warn().Err(err).Msg("failed to start orchestrator task dispatcher")
		}
	}

	logging.Logger.Debug().Msg("orchestration services initialized")
}

// SetCleanupService configures and starts the cleanup service for background jobs.
// This should be called after SetOrchestrationServices.
//
// Deprecated: Use WithCleanupService option with NewGatewayServer instead.
func (s *GatewayServer) SetCleanupService(cleanupConfig *cleanup.Config) {
	// Adapt the gateway's SessionManager to the cleanup-service surface. Both
	// *state.SessionRegistry (Redis) and *state.BadgerSessionRegistry (lite)
	// satisfy cleanup.SessionRegistry. Earlier code asserted only the concrete
	// Redis type and silently dropped lite-mode leader election (functional
	// regression, not a crash, but still wrong); the narrower interface assert
	// produces a non-nil value for either backend.
	sessionRegistry, _ := s.sessions.(cleanup.SessionRegistry)

	// Create cleanup service with available dependencies
	cleanupSvc := cleanup.NewService(
		s.taskStore,
		nil, // taskService - will be set below if available
		sessionRegistry,
		cleanupConfig,
	)

	// If orchestration is configured, use the task service and dispatcher
	if s.orchestration != nil {
		if s.orchestration.TaskService != nil {
			cleanupSvc = cleanup.NewService(
				s.taskStore,
				s.orchestration.TaskService,
				sessionRegistry,
				cleanupConfig,
			)
		}
		if s.orchestration.Dispatcher != nil {
			cleanupSvc.SetDispatcher(s.orchestration.Dispatcher)
		}
	}

	// Run startup cleanup jobs (stale locks + stale claims + orphaned task reconciliation)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logging.Logger.Error().Interface("panic", r).Str("stack", string(debug.Stack())).Str("goroutine", "cleanupStartup").Msg("recovered from panic in background goroutine")
			}
		}()
		cleanupSvc.RunStartupJobs(context.Background())
	}()

	// Start background cleanup jobs (task purge + periodic reconciliation)
	s.cleanupRunner = cleanupSvc.StartBackground(context.Background())

	// Log what's enabled
	if cleanupConfig.TaskPurgeInterval > 0 {
		logging.Logger.Info().
			Dur("interval", cleanupConfig.TaskPurgeInterval).
			Dur("completed_retention", cleanupConfig.CompletedTaskRetention).
			Dur("failed_retention", cleanupConfig.FailedTaskRetention).
			Dur("cancelled_retention", cleanupConfig.CancelledTaskRetention).
			Msg("task purge enabled")
	} else {
		logging.Logger.Debug().Msg("task purge disabled (interval: 0)")
	}

	if cleanupConfig.ReconciliationInterval > 0 {
		logging.Logger.Info().Dur("interval", cleanupConfig.ReconciliationInterval).Msg("periodic reconciliation enabled")
	} else {
		logging.Logger.Debug().Msg("periodic reconciliation disabled (interval: 0)")
	}
}
