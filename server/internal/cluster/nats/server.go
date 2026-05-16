// Package nats provides an embedded NATS server with a co-resident JetStream
// client. It abstracts NATS lifecycle (start, ready-wait, stop) from the rest
// of aetherlite so downstream packages (router, KV, checkpoint, dispatcher,
// backup) can share a single in-process JetStream context.
package nats

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// HAMode controls JetStream replication semantics.
type HAMode int

const (
	// HAModeAuto derives replica count from peer count.
	HAModeAuto HAMode = iota
	// HAModeAsync targets the 2-node B1 source/mirror topology where the
	// primary keeps replicas=1 and mirroring is handled separately.
	HAModeAsync
	// HAModeSync targets the 2-node B2 quorum topology that blocks on partial
	// failure.
	HAModeSync
)

// Config drives EmbeddedServer.Start.
type Config struct {
	DataDir          string
	ClusterName      string
	NodeName         string
	ListenHost       string
	ClientPort       int
	ClusterPort      int
	Peers            []string
	HAMode           HAMode
	TLS              *TLSConfig
	JetStreamMaxMem  int64
	JetStreamMaxFile int64
	Logger           Logger
}

// TLSConfig describes mTLS material for client + route connections.
type TLSConfig struct {
	CertFile, KeyFile, CAFile string
	InsecureSkipVerify        bool
}

// Logger is the minimal logging interface the embedded server adapts to.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// EmbeddedServer owns the lifecycle of an embedded NATS server + a local
// client connection.
type EmbeddedServer struct {
	mu      sync.Mutex
	srv     *natsserver.Server
	conn    *natsgo.Conn
	js      jetstream.JetStream
	peers   int
	haMode  HAMode
	stopped bool
}

// Start boots the embedded server, waits for ReadyForConnections, opens a
// local client, and creates the JetStream context.
func (s *EmbeddedServer) Start(ctx context.Context, cfg Config) error {
	s.mu.Lock()
	if s.srv != nil {
		s.mu.Unlock()
		return errors.New("nats: embedded server already started")
	}
	s.mu.Unlock()

	opts, err := buildOptions(cfg)
	if err != nil {
		return fmt.Errorf("nats: build options: %w", err)
	}

	srv, err := natsserver.NewServer(opts)
	if err != nil {
		return fmt.Errorf("nats: new server: %w", err)
	}
	if cfg.Logger != nil {
		srv.SetLoggerV2(&loggerAdapter{l: cfg.Logger}, false, false, false)
	}

	go srv.Start()

	if err := waitReady(ctx, srv, 10*time.Second); err != nil {
		srv.Shutdown()
		srv.WaitForShutdown()
		return err
	}

	conn, err := natsgo.Connect("", natsgo.InProcessServer(srv), natsgo.Name(cfg.NodeName))
	if err != nil {
		srv.Shutdown()
		srv.WaitForShutdown()
		return fmt.Errorf("nats: in-process connect: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		srv.Shutdown()
		srv.WaitForShutdown()
		return fmt.Errorf("nats: jetstream new: %w", err)
	}

	s.mu.Lock()
	s.srv = srv
	s.conn = conn
	s.js = js
	s.peers = len(cfg.Peers)
	s.haMode = cfg.HAMode
	s.stopped = false
	s.mu.Unlock()
	return nil
}

// Stop closes the local client conn and gracefully shuts the server down.
// Idempotent.
func (s *EmbeddedServer) Stop() {
	s.mu.Lock()
	if s.stopped || s.srv == nil {
		s.stopped = true
		s.mu.Unlock()
		return
	}
	conn := s.conn
	srv := s.srv
	s.stopped = true
	s.mu.Unlock()

	if conn != nil {
		_ = conn.Drain()
		conn.Close()
	}
	if srv != nil {
		srv.Shutdown()
		srv.WaitForShutdown()
	}
}

// Conn returns the local NATS client connection.
func (s *EmbeddedServer) Conn() *natsgo.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn
}

// JetStream returns the local JetStream context.
func (s *EmbeddedServer) JetStream() jetstream.JetStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.js
}

// ReplicasForHA returns the recommended replica count for streams/KV given
// the topology.
func (s *EmbeddedServer) ReplicasForHA() int {
	s.mu.Lock()
	mode := s.haMode
	peers := s.peers
	s.mu.Unlock()
	return replicasFor(mode, peers)
}

func replicasFor(mode HAMode, peers int) int {
	if peers == 0 {
		return 1
	}
	switch mode {
	case HAModeAsync:
		return 1
	case HAModeSync:
		return 2
	default:
		r := peers + 1
		if r > 3 {
			r = 3
		}
		return r
	}
}

func buildOptions(cfg Config) (*natsserver.Options, error) {
	if cfg.DataDir == "" {
		return nil, errors.New("nats: DataDir is required")
	}
	storeDir := filepath.Join(cfg.DataDir, "nats")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return nil, fmt.Errorf("nats: ensure store dir: %w", err)
	}

	nodeName := cfg.NodeName
	if nodeName == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "aetherlite-node"
		}
		nodeName = host
	}

	listenHost := cfg.ListenHost
	if listenHost == "" {
		listenHost = "0.0.0.0"
	}
	clientPort := cfg.ClientPort
	if clientPort == 0 {
		clientPort = 4222
	}
	clusterPort := cfg.ClusterPort
	if clusterPort == 0 {
		clusterPort = 6222
	}

	jsMaxStore := cfg.JetStreamMaxFile
	if jsMaxStore == 0 {
		jsMaxStore = 1 << 30
	}

	opts := &natsserver.Options{
		ServerName:         nodeName,
		Host:               listenHost,
		Port:               clientPort,
		JetStream:          true,
		StoreDir:           storeDir,
		JetStreamMaxMemory: cfg.JetStreamMaxMem,
		JetStreamMaxStore:  jsMaxStore,
		NoSigs:             true,
	}

	if cfg.ClusterName != "" || len(cfg.Peers) > 0 {
		opts.Cluster = natsserver.ClusterOpts{
			Name:      cfg.ClusterName,
			Host:      listenHost,
			Port:      clusterPort,
			ListenStr: fmt.Sprintf("nats://%s:%d", listenHost, clusterPort),
		}
		if len(cfg.Peers) > 0 {
			joined := strings.Join(cfg.Peers, ",")
			opts.Routes = natsserver.RoutesFromStr(joined)
		}
	}

	if cfg.TLS != nil {
		tlsCfg, err := buildTLSConfig(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts.TLSConfig = tlsCfg
		opts.TLS = true
		opts.TLSVerify = !cfg.TLS.InsecureSkipVerify
		if opts.Cluster.Name != "" || len(cfg.Peers) > 0 {
			opts.Cluster.TLSConfig = tlsCfg.Clone()
		}
	}

	return opts, nil
}

func buildTLSConfig(cfg *TLSConfig) (*tls.Config, error) {
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, errors.New("nats: TLS requires CertFile and KeyFile")
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("nats: load keypair: %w", err)
	}
	tlsCfg := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("nats: read CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("nats: failed to parse CA bundle")
		}
		tlsCfg.RootCAs = pool
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tlsCfg, nil
}

func waitReady(ctx context.Context, srv *natsserver.Server, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if srv.ReadyForConnections(100 * time.Millisecond) {
			return nil
		}
	}
	return fmt.Errorf("nats: server not ready within %s", timeout)
}

type loggerAdapter struct {
	l Logger
}

func (a *loggerAdapter) Noticef(format string, v ...any) { a.l.Infof(format, v...) }
func (a *loggerAdapter) Warnf(format string, v ...any)   { a.l.Warnf(format, v...) }
func (a *loggerAdapter) Fatalf(format string, v ...any)  { a.l.Errorf(format, v...) }
func (a *loggerAdapter) Errorf(format string, v ...any)  { a.l.Errorf(format, v...) }
func (a *loggerAdapter) Debugf(format string, v ...any)  {}
func (a *loggerAdapter) Tracef(format string, v ...any)  {}
