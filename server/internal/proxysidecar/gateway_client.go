package proxysidecar

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/scitrera/aether/sdk/go/aether"
)

// gatewayRuntime owns a single gateway connection and the reconnection loop.
// The connection is identity-agnostic: it holds a *aether.BaseClient that is
// backed by EITHER a ServiceClient (workspace-less sv::{impl}::{spec}) or an
// AgentClient (ag::{ws}::{impl}::{spec}), selected in init() by whether a
// workspace is configured. It is mode-agnostic: terminator registers HTTP and
// tunnel handlers on it, while relay mode (T37) attaches its own gRPC mitm
// handlers to the same runtime without going through Terminator.
//
// The runtime does not own dispatcher logic — callers register handlers via
// the underlying BaseClient (exposed through Client()) before calling
// Run. All messaging/handler methods used here (OnMessage, OnProxyHttp*,
// OnTunnel*, SendWithPriority, Connect, Run, Close) are defined on BaseClient,
// so the same wiring serves both identity kinds.
type gatewayRuntime struct {
	cfg       *Config
	client    *aether.BaseClient
	transport *serviceClientTransport

	// creds is the live credential map handed to the client. The SDK
	// rebuilds the InitConnection from this same map on every (re)connect
	// (see sdk/go/aether/{agent,service}.go buildInitMessage), so mutating it in
	// place via refreshCredentials lets a reconnect present freshly-loaded
	// credentials without rebuilding the client or re-installing handlers.
	creds    aether.Credentials
	credKind CredentialKind

	// Reconnect / backoff / give-up policy. Defaults are set in
	// newGatewayRuntime; tests override them for fast, deterministic runs.
	initialBackoff      time.Duration
	maxBackoff          time.Duration
	backoffMultiplier   float64
	stableThreshold     time.Duration // a session lasting this long is "healthy"
	maxTerminalFailures int           // consecutive terminal failures before fatal

	// connOverride, when non-nil, replaces r.client as the connect/run target.
	// Production leaves it nil; tests inject a fake to exercise the loop
	// without a live gateway.
	connOverride gatewayConn
}

// gatewayConn is the minimal connect/run surface runConnectionLoop drives.
// *aether.BaseClient satisfies it (Connect/Run are BaseClient methods, shared
// by both ServiceClient and AgentClient); tests provide a fake.
type gatewayConn interface {
	Connect(ctx context.Context) error
	Run(ctx context.Context) error
}

// newGatewayRuntime builds a runtime from cfg. The gateway client is not
// constructed until init() is called from Run() so callers can configure
// hooks that depend on the runtime before the connection opens.
func newGatewayRuntime(cfg *Config) *gatewayRuntime {
	return &gatewayRuntime{
		cfg:                 cfg,
		initialBackoff:      1 * time.Second,
		maxBackoff:          30 * time.Second,
		backoffMultiplier:   2.0,
		stableThreshold:     30 * time.Second,
		maxTerminalFailures: 5,
	}
}

// conn returns the connect/run target: the test override if set, else the
// real gateway client (service- or agent-backed BaseClient).
func (r *gatewayRuntime) conn() gatewayConn {
	if r.connOverride != nil {
		return r.connOverride
	}
	return r.client
}

// applyCredential populates creds for the given credential kind using the
// SDK's canonical map keys. CredentialKindNone leaves creds empty (mTLS /
// insecure paths).
func applyCredential(creds aether.Credentials, cred string, kind CredentialKind) {
	switch kind {
	case CredentialKindAPIKey:
		creds.WithAPIKey(cred)
	case CredentialKindTaskToken:
		creds.WithTaskToken(cred)
	case CredentialKindNone:
	}
}

// init creates the underlying gateway client using cfg.Gateway and
// cfg.Service. It is idempotent within a single runtime — a second call
// after a successful first call is a no-op.
//
// The client kind is chosen by whether a workspace is configured. Service
// identities are workspace-less (sv::{impl}::{spec}); Agent identities require
// a workspace (ag::{ws}::{impl}::{spec}). Both are backed by BaseClient, so the
// runtime stores the embedded *BaseClient either way and the handler/transport
// wiring is identical downstream.
func (r *gatewayRuntime) init() error {
	if r.client != nil {
		return nil
	}
	creds := aether.NewCredentials()
	cred, kind, err := loadGatewayCredential(r.cfg.Gateway)
	if err != nil {
		return err
	}
	// Credential kinds:
	//   - APIKey: long-lived service key.
	//   - TaskToken: per-task token bound to the gateway TargetIdentity for
	//     the token's lifetime (e.g. an orchestrator's CreateTask with
	//     target_identity=sv::<impl>::<spec>).
	//   - None: rely on mTLS / insecure mode; the gateway fails authn
	//     explicitly if it needs more.
	applyCredential(creds, cred, kind)
	// Hold the live map + kind so runConnectionLoop can refresh credentials
	// in place on a terminal auth failure (re-pairing / token re-mint writes
	// a fresh token to the configured *_path; the next reconnect reads it).
	r.creds = creds
	r.credKind = kind

	tlsCfg, err := buildTLSConfig(r.cfg.Gateway)
	if err != nil {
		return err
	}

	clientOpts := aether.ClientOptions{
		ServerAddr: r.cfg.Gateway.Address,
		Connection: aether.ConnectionOptions{
			RetryOnDuplicate:  true,
			MaxRetries:        0,
			AutoReconnect:     true,
			InitialBackoff:    1 * time.Second,
			MaxBackoff:        30 * time.Second,
			BackoffMultiplier: 2.0,
			ConnectTimeout:    30 * time.Second,
			KeepAliveInterval: 30 * time.Second,
		},
		Credentials: creds,
		TLS:         tlsCfg,
	}

	// Workspace-presence discriminator: an empty workspace means a workspace-
	// less Service identity (sv::{impl}::{spec}); a set workspace means an
	// Agent identity (ag::{ws}::{impl}::{spec}). Both client types embed
	// *BaseClient, so we store the embedded base client and the rest of the
	// runtime is identity-agnostic.
	if r.cfg.Service.Workspace == "" {
		sc, err := aether.NewServiceClient(aether.ServiceOptions{
			ClientOptions:  clientOpts,
			Implementation: r.cfg.Service.Implementation,
			Specifier:      r.cfg.Service.Specifier,
		})
		if err != nil {
			return fmt.Errorf("create service client: %w", err)
		}
		r.client = sc.BaseClient
	} else {
		ac, err := aether.NewAgentClient(aether.AgentOptions{
			ClientOptions:  clientOpts,
			Workspace:      r.cfg.Service.Workspace,
			Implementation: r.cfg.Service.Implementation,
			Specifier:      r.cfg.Service.Specifier,
		})
		if err != nil {
			return fmt.Errorf("create agent client: %w", err)
		}
		r.client = ac.BaseClient
	}
	r.transport = &serviceClientTransport{client: r.client}
	return nil
}

// Client returns the underlying BaseClient (service- or agent-backed). Callers
// register OnMessage, OnProxyHttpRequest, etc. on it before invoking Run.
func (r *gatewayRuntime) Client() *aether.BaseClient {
	return r.client
}

// Transport returns the production tunnelTransport that ships frames
// upstream through the embedded BaseClient.
func (r *gatewayRuntime) Transport() tunnelTransport {
	return r.transport
}

// refreshCredentials re-reads the gateway credential from its configured
// source and replaces the live credential map's contents in place. The next
// reconnect's InitConnection builder reads this same map, so a re-paired /
// re-minted token written to the configured *_path is picked up without
// rebuilding the client. The credential value is never logged.
//
// Re-establishment only helps when the credential is sourced from a path (or
// is otherwise externally refreshable). An inline token in the config is
// re-read as the same dead value — the give-up counter in runConnectionLoop
// is what bounds that case.
func (r *gatewayRuntime) refreshCredentials() error {
	cred, kind, err := loadGatewayCredential(r.cfg.Gateway)
	if err != nil {
		return err
	}
	for k := range r.creds {
		delete(r.creds, k)
	}
	applyCredential(r.creds, cred, kind)
	r.credKind = kind
	return nil
}

// runConnectionLoop owns the sidecar's gateway connection for its lifetime.
//
// It is the single authority for terminal-failure handling: the SDK is built
// with AutoReconnect, so recoverable disconnects (network blips, gateway
// restarts that still honor our token) are retried inside Run() with the
// SDK's own jittered backoff and never surface here. What surfaces here is a
// terminal error — most importantly a token the gateway no longer recognizes
// (codes.Unauthenticated), which the SDK correctly refuses to retry.
//
// Three behaviors distinguish this from the old loop, which reset its backoff
// on every Connect() success and so hammered the gateway ~1/s forever with a
// dead token (Connect only opens the stream — the token is validated later,
// during Run's first Recv):
//
//   - Backoff (capped, jittered) is reset only after a *healthy* session
//     (one that lasted stableThreshold), never merely because the transport
//     opened.
//   - On a terminal failure the credential is re-established (re-read) before
//     the next attempt rather than blindly replaying the dead one.
//   - After maxTerminalFailures consecutive terminal failures (including
//     failed re-establishment) the loop returns a fatal error so the process
//     exits non-zero / signals its wrapped child, letting an orphaned sandbox
//     be reaped instead of spinning forever.
//
// Returns nil on ctx cancellation (clean shutdown) and a non-nil error only
// on the terminal give-up path.
func (r *gatewayRuntime) runConnectionLoop(ctx context.Context) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Error().Interface("panic", rec).Msg("gateway runtime: recovered from panic")
		}
	}()

	conn := r.conn()
	backoff := r.initialBackoff
	terminalFailures := 0

	for {
		if ctx.Err() != nil {
			return nil
		}

		// Log EVERY connect attempt (not just successes) so a connection that
		// keeps dropping — e.g. a gateway GOAWAY "too_many_pings", an auth/ACL
		// reject, or a network blip — is visible per-attempt instead of only via
		// raw gRPC transport errors. Pairs with the cycleErr disconnect-reason
		// logs below and the "connected ... entering run loop" success log.
		log.Info().
			Int("terminal_failures", terminalFailures).
			Dur("backoff", backoff).
			Msg("gateway runtime: connect attempt — dialing gateway")
		cycleStart := time.Now()
		cycleErr := conn.Connect(ctx)
		if cycleErr == nil {
			log.Info().
				Dur("connect_duration", time.Since(cycleStart)).
				Msg("gateway runtime: connected to gateway; entering run loop")
			cycleErr = conn.Run(ctx)
			if cycleErr != nil {
				log.Warn().
					Err(cycleErr).
					Dur("session_duration", time.Since(cycleStart)).
					Msg("gateway runtime: connection dropped (run loop ended)")
			}
		}
		if ctx.Err() != nil {
			// Clean shutdown — the cancellation came from us. Demote any
			// error so shutdown doesn't read as a failure in logs/tests.
			if cycleErr != nil {
				log.Debug().Err(cycleErr).Msg("gateway runtime: connection aborted by shutdown")
			}
			return nil
		}

		// A session that stayed up past the stability threshold (or ended
		// gracefully) is healthy: reset both the backoff and the terminal
		// streak. This is the core fix — backoff must NOT reset just because
		// Connect() returned, only after the connection proved durable.
		if cycleErr == nil || time.Since(cycleStart) >= r.stableThreshold {
			backoff = r.initialBackoff
			terminalFailures = 0
		}

		terminal := cycleErr != nil && !aether.IsRecoverable(cycleErr)
		switch {
		case terminal:
			terminalFailures++
			log.Error().
				Err(cycleErr).
				Int("consecutive_terminal_failures", terminalFailures).
				Int("max_terminal_failures", r.maxTerminalFailures).
				Msg("gateway runtime: terminal connection failure; re-establishing credentials")

			if rerr := r.refreshCredentials(); rerr != nil {
				log.Error().Err(rerr).Msg("gateway runtime: credential re-establishment failed")
			} else {
				log.Info().Msg("gateway runtime: reloaded gateway credential for next attempt")
			}

			if terminalFailures >= r.maxTerminalFailures {
				return fmt.Errorf(
					"gateway runtime: giving up after %d consecutive terminal failures: %w",
					terminalFailures, cycleErr,
				)
			}
		case cycleErr != nil:
			log.Error().Err(cycleErr).Msg("gateway runtime: transient connection error; will retry")
		}

		sleep := backoffWithJitter(backoff)
		log.Info().
			Dur("backoff", sleep).
			Bool("terminal", terminal).
			Msg("gateway runtime: reconnecting to gateway")
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(sleep):
		}
		backoff = nextBackoff(backoff, r.maxBackoff, r.backoffMultiplier)
	}
}

// backoffWithJitter applies ±25% jitter to d so a fleet of sidecars whose
// shared gateway restarted don't reconnect in lockstep (thundering herd).
func backoffWithJitter(d time.Duration) time.Duration {
	jitter := float64(d) * 0.25 * (rand.Float64()*2 - 1)
	out := d + time.Duration(jitter)
	if out <= 0 {
		return d
	}
	return out
}

// nextBackoff grows cur by mult, capped at max (and clamped on overflow).
func nextBackoff(cur, max time.Duration, mult float64) time.Duration {
	next := time.Duration(float64(cur) * mult)
	if next > max || next < cur {
		return max
	}
	return next
}
