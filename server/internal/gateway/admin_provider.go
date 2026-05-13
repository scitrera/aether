package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/internal/quota"
	"github.com/scitrera/aether/internal/registry"
	"github.com/scitrera/aether/internal/router"
	"github.com/scitrera/aether/internal/state"
	"github.com/scitrera/aether/pkg/models"
	"github.com/scitrera/aether/pkg/tasks"
)

const adminVersion = "0.1.0"

// adminIdentity is the Identity used for admin operations (e.g., KV store access).
var adminIdentity = models.Identity{Type: models.PrincipalAgent, ID: "admin", Implementation: "admin", Specifier: "admin"}

// GatewayStateProvider implements admin.StateProvider using direct gateway access.
// This is the integrated implementation for Option A (embedded admin UI).
// For future extraction (Option C), this would be replaced by an API client.
type GatewayStateProvider struct {
	gatewayID     string
	startedAt     time.Time
	sessions      *state.SessionRegistry
	kvStore       *kv.Store
	taskStore     *tasks.TaskStore
	agentRegistry *registry.AgentRegistry
	profileMgr    *registry.OrchestratorProfileManager
	aclService    *acl.Service
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

// NewGatewayStateProvider creates a state provider with access to gateway internals
func NewGatewayStateProvider(
	gatewayID string,
	sessions *state.SessionRegistry,
	kvStore *kv.Store,
	taskStore *tasks.TaskStore,
	agentRegistry *registry.AgentRegistry,
	profileMgr *registry.OrchestratorProfileManager,
	aclService *acl.Service,
	db *sql.DB,
	r *router.Router,
) *GatewayStateProvider {
	return &GatewayStateProvider{
		gatewayID:     gatewayID,
		startedAt:     time.Now(),
		sessions:      sessions,
		kvStore:       kvStore,
		taskStore:     taskStore,
		agentRegistry: agentRegistry,
		profileMgr:    profileMgr,
		aclService:    aclService,
		db:            db,
		router:        r,
		eventSubs:     make(map[chan *admin.Event]struct{}),
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
		redisCheck.Status = "error"
		redisCheck.Error = "not configured"
		overallStatus = "degraded"
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
		pgCheck.Status = "error"
		pgCheck.Error = "not configured"
		// PostgreSQL is optional, don't mark as degraded
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
		rmqCheck.Status = "error"
		rmqCheck.Error = "not configured"
		overallStatus = "degraded"
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
