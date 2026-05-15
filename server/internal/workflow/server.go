package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/sdk/go/aether"
)

// Server is the top-level workflow server that orchestrates all components.
type Server struct {
	cfg       *Config
	redis     redis.UniversalClient
	client    *aether.WorkflowEngineClient
	store     WorkflowStore
	router    *Router
	dagEng    *DAGEngine
	scheduler *Scheduler
	leader    LeaderElector
	executor  *Executor
	stateMach *StateMachineEngine
	adminSrv  *AdminServer
}

// NewServer creates a new workflow server bound to the given config and
// pre-built workflow store. The caller is responsible for opening the
// database (postgres or sqlite), running migrations against it, and
// constructing the matching store implementation (wfpg.New for postgres,
// wfsqlite.New for sqlite). The store's underlying resources stay owned
// by the caller; the workflow package never touches `database/sql` or any
// DB driver directly.
//
// Returns an error if store is nil (§14.1: nil Store is a class-A crash).
func NewServer(cfg *Config, store WorkflowStore) (*Server, error) {
	if store == nil {
		return nil, fmt.Errorf("workflow.NewServer requires a non-nil store (§14.1: nil Store is a class-A crash)")
	}
	return &Server{cfg: cfg, store: store}, nil
}

// Run initializes all components and starts the server. Blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	// 1. The store is already constructed and migrated by the caller.
	//    The workflow package never opens databases or runs migrations.

	// 2. Connect to Redis (skipped in lite mode)
	if s.cfg.Mode != ModeLite {
		s.initRedis()
		defer s.redis.Close()
	}

	// 4. Create Aether WorkflowEngineClient
	if err := s.initAetherClient(); err != nil {
		return err
	}

	// 5. Initialize components
	s.initComponents()

	// 6. Register event handler on the Aether client
	s.client.OnMessage(func(msgCtx context.Context, msg *aether.Message) error {
		return s.handleMessage(msgCtx, msg)
	})

	// 6.1 Register workflow operation handler for forwarded CRUD requests from gateway
	s.client.OnWorkflowOperation(s.handleWorkflowOperation)

	// 7. Connect to Aether gateway with reconnection loop.
	// Run() exits cleanly on FORCE_DISCONNECT (e.g., from MaxConnectionAge),
	// so we wrap in a loop to always reconnect while the context is alive.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("recovered from panic in aether client goroutine")
			}
		}()
		backoff := 1 * time.Second
		maxBackoff := 30 * time.Second
		for {
			if ctx.Err() != nil {
				return
			}
			if err := s.client.Connect(ctx); err != nil {
				log.Error().Err(err).Msg("aether connect error")
			} else {
				backoff = 1 * time.Second // reset on successful connect
				if err := s.client.Run(ctx); err != nil {
					log.Error().Err(err).Msg("aether run error")
				}
			}
			if ctx.Err() != nil {
				return
			}
			log.Info().Dur("backoff", backoff).Msg("workflow engine reconnecting to gateway")
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
		}
	}()

	// 8. Start leader election refresh loop (Redis mode only)
	if redisLeader, ok := s.leader.(*RedisLeaderElector); ok {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error().Interface("panic", r).Msg("recovered from panic in leader election goroutine")
				}
			}()
			redisLeader.RunRefreshLoop(ctx)
		}()
	}

	// Wait for leadership before starting scheduler and monitor
	s.waitForLeadership(ctx)

	// 9. Start scheduler
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("recovered from panic in scheduler goroutine")
			}
		}()
		s.scheduler.Run(ctx)
	}()

	// 10. Start DAG monitor
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("recovered from panic in DAG monitor goroutine")
			}
		}()
		s.runDAGMonitor(ctx)
	}()

	// 11. Start state machine timeout monitor
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Interface("panic", r).Msg("recovered from panic in state machine monitor goroutine")
			}
		}()
		s.runStateMachineMonitor(ctx)
	}()

	// 12. Start admin API server
	if s.cfg.Admin.Enabled {
		s.adminSrv = NewAdminServer(
			s.cfg.Admin.GetPort(), s.cfg.Admin.APIKey,
			s.store, s.router, s.dagEng, s.scheduler, s.stateMach,
		)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error().Interface("panic", r).Msg("recovered from panic in admin server goroutine")
				}
			}()
			if err := s.adminSrv.Start(); err != nil {
				log.Error().Err(err).Msg("admin server error")
			}
		}()
	}

	aetherDesc := s.cfg.Aether.Address
	if s.cfg.Aether.InProcessConn != nil {
		aetherDesc = "in-process (bufconn)"
	}
	log.Info().
		Str("aether", aetherDesc).
		Str("workspace", s.cfg.Aether.Workspace).
		Bool("admin", s.cfg.Admin.Enabled).
		Msg("workflow server running")

	// Block until shutdown
	<-ctx.Done()
	log.Info().Msg("workflow server shutting down")
	if s.adminSrv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Best-effort admin-server shutdown during workflow server teardown;
		// the parent process is exiting regardless of the result.
		if err := s.adminSrv.Stop(shutdownCtx); err != nil {
			log.Warn().Err(err).Msg("workflow admin server shutdown returned error")
		}
	}
	s.leader.Release(context.Background())
	return nil
}

func (s *Server) initRedis() {
	addrs := s.cfg.Redis.Cluster
	if len(addrs) == 1 {
		s.redis = redis.NewClient(&redis.Options{
			Addr:     addrs[0],
			Password: s.cfg.Redis.Password,
		})
	} else {
		s.redis = redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:    addrs,
			Password: s.cfg.Redis.Password,
		})
	}
	log.Info().Strs("addrs", addrs).Msg("Redis client initialized")
}

func (s *Server) initAetherClient() error {
	opts := aether.WorkflowEngineOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr: s.cfg.Aether.Address,
			Connection: aether.ConnectionOptions{
				RetryOnDuplicate:  true,
				MaxRetries:        0, // infinite retries
				AutoReconnect:     true,
				InitialBackoff:    1 * time.Second,
				MaxBackoff:        30 * time.Second,
				BackoffMultiplier: 2.0,
				ConnectTimeout:    30 * time.Second,
				KeepAliveInterval: 30 * time.Second,
			},
		},
		Specifier: s.cfg.Aether.Implementation,
	}

	if s.cfg.Aether.Credentials.APIKey != "" {
		opts.Credentials = aether.NewCredentials().WithAPIKey(s.cfg.Aether.Credentials.APIKey)
	}

	// In-process (embedded) mode: reuse a pre-dialed *grpc.ClientConn
	// instead of dialing. Mutually exclusive with the TLS file path below —
	// the conn provides its own transport. AetherLite uses this to avoid
	// TLS-over-loopback for the embedded workflow engine.
	if s.cfg.Aether.InProcessConn != nil {
		// ownsConn=false: the caller (cmd/aetherlite/main.go) owns the
		// bufconn-backed listener lifetime. Closing the SDK client must
		// NOT tear down the listener.
		client, err := aether.NewWorkflowEngineClientWithConn(s.cfg.Aether.InProcessConn, opts, false)
		if err != nil {
			return err
		}
		s.client = client
		log.Info().Msg("Aether client created (in-process conn)")
		return nil
	}

	if s.cfg.Aether.TLS.CertFile != "" {
		tlsCfg := &aether.TLSConfig{Enabled: true}
		if s.cfg.Aether.TLS.CAFile != "" {
			caPEM, err := os.ReadFile(s.cfg.Aether.TLS.CAFile)
			if err != nil {
				return fmt.Errorf("reading TLS CA file %s: %w", s.cfg.Aether.TLS.CAFile, err)
			}
			tlsCfg.RootCAs = caPEM
		}
		certPEM, err := os.ReadFile(s.cfg.Aether.TLS.CertFile)
		if err != nil {
			return fmt.Errorf("reading TLS cert file %s: %w", s.cfg.Aether.TLS.CertFile, err)
		}
		tlsCfg.ClientCert = certPEM
		if s.cfg.Aether.TLS.KeyFile != "" {
			keyPEM, err := os.ReadFile(s.cfg.Aether.TLS.KeyFile)
			if err != nil {
				return fmt.Errorf("reading TLS key file %s: %w", s.cfg.Aether.TLS.KeyFile, err)
			}
			tlsCfg.ClientKey = keyPEM
		}
		opts.TLS = tlsCfg
	}

	client, err := aether.NewWorkflowEngineClient(opts)
	if err != nil {
		return err
	}
	s.client = client
	log.Info().Str("addr", s.cfg.Aether.Address).Msg("Aether client created")
	return nil
}

func (s *Server) initComponents() {
	impl := s.cfg.Aether.Implementation
	if impl == "" {
		impl = "aether-workflow"
	}

	// s.store is guaranteed non-nil by NewServer.
	s.executor = NewExecutor(s.client, s.cfg.Aether.Workspace)
	if s.cfg.Mode == ModeLite {
		s.leader = NewSingleNodeLeaderElector()
	} else {
		s.leader = NewRedisLeaderElector(s.redis, "workflow:leader", impl)
	}

	exprEng := NewExprEngine(s.cfg.Workflow.GetRuleCacheSize())
	tmplEng := NewTemplateEngine(s.cfg.Workflow.GetRuleCacheSize())

	s.router = NewRouter(s.store, exprEng, tmplEng, s.executor, s.cfg.Workflow.GetRuleCacheTTL())
	s.dagEng = NewDAGEngine(s.store, exprEng, tmplEng, s.executor, &s.cfg.Workflow)
	s.scheduler = NewScheduler(s.store, s.executor, s.dagEng, s.leader, s.cfg.Workflow.GetSchedulerPollInterval())
	s.stateMach = NewStateMachineEngine(s.store, s.executor)
}

func (s *Server) handleMessage(ctx context.Context, msg *aether.Message) error {
	// Only process EVENT messages
	if msg.MessageType != pb.MessageType_EVENT {
		return nil
	}

	// Route through the event router (rule matching)
	if err := s.router.HandleEvent(ctx, msg.SourceTopic, msg.Payload); err != nil {
		log.Error().Err(err).Str("source", msg.SourceTopic).Msg("router error")
	}

	// Try to trigger DAG executions from the event
	var event EventPayload
	if err := json.Unmarshal(msg.Payload, &event); err == nil {
		workspace := event.Workspace
		if workspace == "" {
			parts := strings.Split(msg.SourceTopic, ".")
			if len(parts) >= 2 {
				workspace = parts[1]
			}
		}

		sourceAgent := event.SourceAgent
		if sourceAgent == "" {
			parts := strings.Split(msg.SourceTopic, ".")
			if len(parts) >= 3 {
				sourceAgent = parts[2]
			}
		}

		if err := s.dagEng.TryTriggerFromEvent(ctx, sourceAgent, workspace, event.EventNames, event.Data); err != nil {
			log.Error().Err(err).Msg("DAG trigger error")
		}
	}

	return nil
}

func (s *Server) runDAGMonitor(ctx context.Context) {
	interval := s.cfg.Workflow.GetDAGMonitorInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Info().Dur("interval", interval).Msg("DAG monitor started")

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("DAG monitor stopped")
			return
		case <-ticker.C:
			if !s.leader.IsLeader() {
				continue
			}
			if err := s.dagEng.MonitorExecutions(ctx); err != nil {
				log.Error().Err(err).Msg("DAG monitor error")
			}
		}
	}
}

func (s *Server) runStateMachineMonitor(ctx context.Context) {
	interval := s.cfg.Workflow.GetDAGMonitorInterval() // reuse same interval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Info().Dur("interval", interval).Msg("state machine timeout monitor started")

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("state machine timeout monitor stopped")
			return
		case <-ticker.C:
			if !s.leader.IsLeader() {
				continue
			}
			if err := s.stateMach.MonitorTimeouts(ctx); err != nil {
				log.Error().Err(err).Msg("state machine timeout monitor error")
			}
		}
	}
}

func (s *Server) waitForLeadership(ctx context.Context) {
	for {
		if s.leader.TryAcquire(ctx) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}
