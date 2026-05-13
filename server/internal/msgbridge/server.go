package msgbridge

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/scitrera/aether/sdk/go/aether"
	_ "modernc.org/sqlite"

	mbmigrations "github.com/scitrera/aether/internal/msgbridge/migrations"
	mbsqlite "github.com/scitrera/aether/internal/msgbridge/migrations/sqlite"
	"github.com/scitrera/aether/internal/msgbridge/platforms"
	"github.com/scitrera/aether/internal/msgbridge/platforms/discord"
	"github.com/scitrera/aether/internal/msgbridge/platforms/email"
	"github.com/scitrera/aether/internal/msgbridge/platforms/teams"
)

// Server is the top-level messaging bridge server.
type Server struct {
	cfg      *Config
	db       *sql.DB
	store    *Store
	admin    *AdminServer
	client   *aether.BridgeClient
	router   *Router
	adapters map[string]platforms.PlatformAdapter
}

// NewServer creates a new messaging bridge server from the given configuration.
func NewServer(cfg *Config) (*Server, error) {
	return &Server{cfg: cfg}, nil
}

// Run initializes all components and starts the server. Blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	// 1. Connect to database (PostgreSQL or SQLite depending on mode)
	if s.cfg.IsLite() {
		if err := s.initDatabaseLite(); err != nil {
			return err
		}
	} else {
		if err := s.initDatabase(ctx); err != nil {
			return err
		}
	}
	defer s.db.Close()

	// 2. Run msgbridge migrations
	if s.cfg.IsLite() {
		if err := mbsqlite.Run(ctx, s.db); err != nil {
			return err
		}
	} else {
		if err := mbmigrations.Run(ctx, s.db); err != nil {
			return err
		}
	}

	// 3. Create store
	s.store = NewStore(s.db)

	// 4. Create Aether BridgeClient
	if err := s.initAetherClient(); err != nil {
		return err
	}

	// 5. Create adapters map and router
	s.adapters = make(map[string]platforms.PlatformAdapter)

	// Register platform adapters if enabled.
	if s.cfg.Platforms.Discord.Enabled {
		s.adapters["discord"] = discord.NewAdapter(
			s.cfg.Platforms.Discord.BotToken,
			s.cfg.Platforms.Discord.ApplicationID,
		)
	}
	if s.cfg.Platforms.Teams.Enabled {
		s.adapters["teams"] = teams.NewAdapter(
			s.cfg.Platforms.Teams.AppID,
			s.cfg.Platforms.Teams.AppPassword,
			s.cfg.Platforms.Teams.TenantID,
			s.cfg.Platforms.Teams.GetWebhookPort(),
		)
	}
	if s.cfg.Platforms.Email.Enabled {
		smtpCfg := email.SMTPConfig{
			Host:        s.cfg.Platforms.Email.SMTP.Host,
			Port:        s.cfg.Platforms.Email.SMTP.Port,
			Username:    s.cfg.Platforms.Email.SMTP.Username,
			Password:    s.cfg.Platforms.Email.SMTP.Password,
			FromAddress: s.cfg.Platforms.Email.SMTP.FromAddress,
			UseTLS:      s.cfg.Platforms.Email.SMTP.TLS,
		}
		s.adapters["email"] = email.NewAdapter(smtpCfg, nil) // IMAP config wired in later
	}

	s.router = NewRouter(s.store, s.adapters, s.client)

	// Start all adapters and wire inbound handler.
	for name, adapter := range s.adapters {
		adapter.SetInboundHandler(s.router.HandleInbound)
		if err := adapter.Start(ctx); err != nil {
			return fmt.Errorf("failed to start %s adapter: %w", name, err)
		}
	}

	// 6. Start admin server if enabled (after adapters are ready so health can report them)
	if s.cfg.Admin.Enabled {
		s.admin = NewAdminServer(s.store, s.cfg.Admin.GetPort(), s.cfg.Admin.APIKey, s.adapters)
		if err := s.admin.Start(); err != nil {
			return fmt.Errorf("failed to start admin server: %w", err)
		}
	}

	// 7. Background goroutine: update Prometheus gauges for platform health and active mappings
	go s.runMetricsUpdater(ctx)

	// 8. Register message handler — routes outbound requests from Aether agents
	s.client.OnMessage(func(msgCtx context.Context, msg *aether.Message) error {
		s.router.handleOutbound(msgCtx, msg.SourceTopic, msg.Payload)
		return nil
	})

	// 9. Connect to Aether gateway with reconnection loop.
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
			log.Info().Dur("backoff", backoff).Msg("messaging bridge reconnecting to gateway")
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
		}
	}()

	log.Info().
		Str("aether", s.cfg.Aether.Address).
		Str("implementation", s.cfg.Aether.Implementation).
		Str("specifier", s.cfg.Aether.Specifier).
		Bool("discord", s.cfg.Platforms.Discord.Enabled).
		Bool("teams", s.cfg.Platforms.Teams.Enabled).
		Bool("email", s.cfg.Platforms.Email.Enabled).
		Bool("admin", s.cfg.Admin.Enabled).
		Msg("messaging bridge running")

	// Block until shutdown
	<-ctx.Done()
	log.Info().Msg("messaging bridge shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	// Stop all platform adapters.
	for name, adapter := range s.adapters {
		if err := adapter.Stop(shutdownCtx); err != nil {
			log.Error().Err(err).Str("platform", name).Msg("adapter shutdown error")
		}
	}

	// Gracefully stop admin server if running.
	if s.admin != nil {
		if err := s.admin.Stop(shutdownCtx); err != nil {
			log.Error().Err(err).Msg("admin server shutdown error")
		}
	}

	return nil
}

// runMetricsUpdater periodically refreshes Prometheus gauges for platform health
// and the count of enabled channel mappings.
func (s *Server) runMetricsUpdater(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	s.updateMetricsGauges(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.updateMetricsGauges(ctx)
		}
	}
}

// updateMetricsGauges samples adapter health and mapping count once.
func (s *Server) updateMetricsGauges(ctx context.Context) {
	for name, adapter := range s.adapters {
		if adapter.IsHealthy() {
			platformHealthy.WithLabelValues(name).Set(1)
		} else {
			platformHealthy.WithLabelValues(name).Set(0)
		}
	}

	mappings, err := s.store.ListChannelMappings(ctx)
	if err != nil {
		log.Error().Err(err).Msg("msgbridge: failed to query channel mappings for metrics")
		return
	}
	enabled := 0
	for _, m := range mappings {
		if m.Enabled {
			enabled++
		}
	}
	activeMappings.Set(float64(enabled))
}

func (s *Server) initDatabase(ctx context.Context) error {
	db, err := sql.Open("postgres", s.cfg.Postgres.DSN())
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	if s.cfg.Postgres.MaxConnections > 0 {
		db.SetMaxOpenConns(s.cfg.Postgres.MaxConnections)
	}
	if s.cfg.Postgres.MaxIdleConnections > 0 {
		db.SetMaxIdleConns(s.cfg.Postgres.MaxIdleConnections)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	s.db = db
	log.Info().Msg("PostgreSQL connection established")
	return nil
}

func (s *Server) initDatabaseLite() error {
	db, err := sql.Open("sqlite", s.cfg.SQLite.Path)
	if err != nil {
		return fmt.Errorf("failed to open SQLite database: %w", err)
	}
	// SQLite performs best with a single connection for writes.
	db.SetMaxOpenConns(1)
	s.db = db
	log.Info().Str("path", s.cfg.SQLite.Path).Msg("SQLite connection established")
	return nil
}

func (s *Server) initAetherClient() error {
	opts := aether.BridgeOptions{
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
		Implementation: s.cfg.Aether.Implementation,
		Specifier:      s.cfg.Aether.Specifier,
	}

	if s.cfg.Aether.Credentials.APIKey != "" {
		opts.Credentials = aether.NewCredentials().WithAPIKey(s.cfg.Aether.Credentials.APIKey)
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

	client, err := aether.NewBridgeClient(opts)
	if err != nil {
		return fmt.Errorf("failed to create bridge client: %w", err)
	}
	s.client = client
	log.Info().Str("addr", s.cfg.Aether.Address).Msg("Aether bridge client created")
	return nil
}
