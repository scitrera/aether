package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/auth"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/internal/quota"
	"github.com/scitrera/aether/internal/registry"
	"github.com/scitrera/aether/internal/router"
	"github.com/scitrera/aether/internal/state"
	aclstore "github.com/scitrera/aether/internal/storage/acl"
	regstore "github.com/scitrera/aether/internal/storage/registry"
	taskstore "github.com/scitrera/aether/internal/storage/tasks"
	"github.com/scitrera/aether/pkg/models"
)

const adminVersion = "0.1.0"

// adminIdentity is the Identity used for admin operations (e.g., KV store access).
var adminIdentity = models.Identity{Type: models.PrincipalAgent, ID: "admin", Implementation: "admin", Specifier: "admin"}

// GatewayStateProvider implements admin.StateProvider using direct gateway access.
// This is the integrated implementation for Option A (embedded admin UI).
// For future extraction (Option C), this would be replaced by an API client.
type GatewayStateProvider struct {
	gatewayID string
	startedAt time.Time
	sessions  *state.SessionRegistry
	// kvStore is the KV store used for admin KV and workspace operations.
	// Must be non-nil; both the Redis-backed *kv.Store (full mode) and the
	// Badger-backed *kv.BadgerKVStore (lite mode) satisfy KVReadWriter.
	kvStore KVReadWriter
	// taskStore is the tasks domain Store (internal/storage/tasks).
	taskStore taskstore.Store
	// agentRegistry holds the bundled registry surface (internal/storage/registry).
	// Both the agent-implementation catalog and orchestrator-profile fleet share
	// this single interface field; admin call sites use it for both List+Get on
	// agents AND ListAllProfiles on orchestrators.
	agentRegistry regstore.Store
	// profileMgr aliases the same bundled registry.Store as agentRegistry — kept
	// as a distinct field name so existing admin call sites read naturally.
	profileMgr regstore.Store
	// aclService is the ACL domain Store (internal/storage/acl).
	aclService aclstore.Store
	// prefixIndex is the Phase 5 Stage B in-memory routing table from
	// resource_type_prefix → owning agent implementation. Constructed
	// alongside the state provider and seeded from the registry at startup
	// via initPrefixIndex(). After Register/Update/Delete it is refreshed
	// here so audit attribution via aclService stays in sync without a
	// round-trip to the DB.
	prefixIndex *registry.PrefixIndex
	// apiTokenStore is the API token domain store (internal/auth).
	// Both the PostgreSQL-backed and SQLite-backed implementations satisfy
	// this interface. When nil, admin token endpoints return "not available".
	apiTokenStore auth.APITokenStore
	db            *sql.DB
	router        *router.Router

	// Reference to gateway server for active streams
	gateway *GatewayServer

	// workspaceRateLimiter is the shared rate limiter for per-workspace throughput control.
	// When set, admin API calls to Set/Get/Remove/List workspace rate limits delegate here.
	workspaceRateLimiter *quota.WorkspaceRateLimiter

	// Event broadcasting
	eventMu   sync.RWMutex
	eventSubs map[chan *admin.Event]struct{}

	// Stats tracking
	messageCount atomic.Int64
}

// NewGatewayStateProvider creates a state provider with access to gateway internals.
//
// As of Stage 1 of the storage-interfaces refactor, taskStore/agentRegistry/
// profileMgr/aclService are interface-typed against internal/storage/<domain>.
// Production callers pass the same bundled registry.Store for both
// agentRegistry and profileMgr (the legacy split into AgentRegistry and
// OrchestratorProfileManager is now hidden behind that interface).
func NewGatewayStateProvider(
	gatewayID string,
	sessions *state.SessionRegistry,
	kvStore KVReadWriter,
	taskStore taskstore.Store,
	agentRegistry regstore.Store,
	profileMgr regstore.Store,
	aclService aclstore.Store,
	apiTokenStore auth.APITokenStore,
	db *sql.DB,
	r *router.Router,
) *GatewayStateProvider {
	// Phase 5 Stage B: construct the prefix index, seed it from the current
	// registry state, and wire it into the ACL service. Seeding is
	// best-effort — a registry handle that's missing or temporarily
	// unreachable just leaves the index empty (CheckAccess emits audit
	// rows without owning-agent attribution, identical to pre-Phase-5).
	idx := registry.NewPrefixIndex()
	if agentRegistry != nil {
		if all, err := agentRegistry.List(context.Background(), ""); err == nil {
			idx.Rebuild(all)
		}
	}
	if aclService != nil {
		aclService.SetPrefixIndex(idx)
	}

	return &GatewayStateProvider{
		gatewayID:     gatewayID,
		startedAt:     time.Now(),
		sessions:      sessions,
		kvStore:       kvStore,
		taskStore:     taskStore,
		agentRegistry: agentRegistry,
		profileMgr:    profileMgr,
		aclService:    aclService,
		apiTokenStore: apiTokenStore,
		db:            db,
		router:        r,
		eventSubs:     make(map[chan *admin.Event]struct{}),
		prefixIndex:   idx,
	}
}

// SetGateway sets the gateway server reference for accessing active streams
func (p *GatewayStateProvider) SetGateway(gw *GatewayServer) {
	p.gateway = gw
}

// SetWorkspaceRateLimiter sets the workspace rate limiter used by the admin API.
func (p *GatewayStateProvider) SetWorkspaceRateLimiter(wrl *quota.WorkspaceRateLimiter) {
	p.workspaceRateLimiter = wrl
}

// =============================================================================
// Gateway Info & Health
// =============================================================================

func (p *GatewayStateProvider) GetGatewayInfo(ctx context.Context) (*admin.GatewayInfo, error) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	numConnections := 0
	if p.gateway != nil {
		p.gateway.activeStreams.Range(func(_, _ interface{}) bool {
			numConnections++
			return true
		})
	}

	uptime := time.Since(p.startedAt)
	uptimeStr := formatDuration(uptime)

	return &admin.GatewayInfo{
		GatewayID:      p.gatewayID,
		Version:        adminVersion,
		StartedAt:      p.startedAt,
		Uptime:         uptimeStr,
		GoVersion:      runtime.Version(),
		NumGoroutines:  runtime.NumGoroutine(),
		MemoryAllocMB:  float64(m.Alloc) / 1024 / 1024,
		NumConnections: numConnections,
	}, nil
}

func (p *GatewayStateProvider) GetHealthStatus(ctx context.Context) (*admin.HealthStatus, error) {
	checks := make(map[string]*admin.HealthCheck)
	overallStatus := "healthy"

	// The three external-backend checks below (Redis, PostgreSQL, RabbitMQ)
	// are full-mode infrastructure. In AetherLite, these are intentionally
	// replaced by embedded backends (Badger sessions, per-domain native
	// SQLite, BadgerRouter) and the corresponding fields on GatewayStateProvider
	// are nil by design. A nil field reports status "not_configured" without
	// degrading the overall health, because "not configured" is the correct
	// operational state in lite — not a fault. A configured backend that
	// fails its ping is still reported as "error" + degraded.

	// Check Redis
	redisCheck := &admin.HealthCheck{Status: "ok"}
	if p.sessions != nil {
		start := time.Now()
		client := p.sessions.GetRedisClient()
		if err := client.Ping(ctx).Err(); err != nil {
			redisCheck.Status = "error"
			redisCheck.Error = err.Error()
			overallStatus = "degraded"
		} else {
			redisCheck.Latency = time.Since(start).String()
		}
	} else {
		redisCheck.Status = "not_configured"
	}
	checks["redis"] = redisCheck

	// Check PostgreSQL
	pgCheck := &admin.HealthCheck{Status: "ok"}
	if p.db != nil {
		start := time.Now()
		if err := p.db.PingContext(ctx); err != nil {
			pgCheck.Status = "error"
			pgCheck.Error = err.Error()
			overallStatus = "degraded"
		} else {
			pgCheck.Latency = time.Since(start).String()
		}
	} else {
		pgCheck.Status = "not_configured"
	}
	checks["postgresql"] = pgCheck

	// Check RabbitMQ Streams
	rmqCheck := &admin.HealthCheck{Status: "ok"}
	if p.router != nil {
		start := time.Now()
		if err := p.router.HealthCheck(ctx); err != nil {
			rmqCheck.Status = "error"
			rmqCheck.Error = err.Error()
			overallStatus = "degraded"
		} else {
			rmqCheck.Latency = time.Since(start).String()
		}
	} else {
		rmqCheck.Status = "not_configured"
	}
	checks["rabbitmq"] = rmqCheck

	// Gather stats
	stats := p.gatherStats(ctx)

	return &admin.HealthStatus{
		Status:    overallStatus,
		Timestamp: time.Now(),
		Checks:    checks,
		Stats:     stats,
	}, nil
}

func (p *GatewayStateProvider) gatherStats(ctx context.Context) *admin.GatewayStats {
	stats := &admin.GatewayStats{}

	// Count connections by type
	if p.gateway != nil {
		p.gateway.activeStreams.Range(func(_, value interface{}) bool {
			if session, ok := value.(*ClientSession); ok {
				switch session.Identity.Type {
				case models.PrincipalAgent:
					stats.AgentConnections++
				case models.PrincipalTask:
					stats.TaskConnections++
				case models.PrincipalUser:
					stats.UserConnections++
				case models.PrincipalOrchestrator:
					stats.OrchestratorConnections++
				case models.PrincipalWorkflowEngine:
					stats.WorkflowEngineConnected = true
				case models.PrincipalMetricsBridge:
					stats.MetricsBridgeConnected = true
				case models.PrincipalBridge:
					stats.BridgeConnected = true
				}
			}
			return true
		})
	}

	// Task statistics from database
	if p.taskStore != nil {
		if counts, err := p.taskStore.GetTaskCounts(ctx); err == nil {
			stats.TotalTasks = counts.Total
			stats.PendingTasks = counts.Pending
			stats.RunningTasks = counts.Running
			stats.CompletedTasks = counts.Completed
			stats.FailedTasks = counts.Failed
		}
	}

	stats.TotalMessages = p.messageCount.Load()

	return stats
}

// =============================================================================
// Helpers
// =============================================================================

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
	}
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	return fmt.Sprintf("%dd %dh", days, hours)
}

// Ensure GatewayStateProvider implements StateProvider
var _ admin.StateProvider = (*GatewayStateProvider)(nil)

// Compile-time assertions: both KV backends must satisfy KVReadWriter so that
// callers can pass either *kv.Store (Redis/full mode) or *kv.BadgerKVStore
// (Badger/lite mode) to NewGatewayStateProvider without a nil placeholder.
var (
	_ KVReadWriter = (*kv.Store)(nil)
	_ KVReadWriter = (*kv.BadgerKVStore)(nil)
)
