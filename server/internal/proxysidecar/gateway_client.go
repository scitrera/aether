package proxysidecar

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/scitrera/aether/sdk/go/aether"
)

// gatewayRuntime owns a single ServiceClient connection to the gateway and
// the reconnection loop. It is mode-agnostic: terminator registers HTTP and
// tunnel handlers on it, while relay mode (T37) attaches its own gRPC mitm
// handlers to the same runtime without going through Terminator.
//
// The runtime does not own dispatcher logic — callers register handlers via
// the underlying ServiceClient (exposed through Client()) before calling
// Run.
type gatewayRuntime struct {
	cfg       *Config
	client    *aether.ServiceClient
	transport *serviceClientTransport
}

// newGatewayRuntime builds a runtime from cfg. The ServiceClient is not
// constructed until init() is called from Run() so callers can configure
// hooks that depend on the runtime before the connection opens.
func newGatewayRuntime(cfg *Config) *gatewayRuntime {
	return &gatewayRuntime{cfg: cfg}
}

// init creates the underlying ServiceClient using cfg.Gateway and
// cfg.Service. It is idempotent within a single runtime — a second call
// after a successful first call is a no-op.
func (r *gatewayRuntime) init() error {
	if r.client != nil {
		return nil
	}
	creds := aether.NewCredentials()
	cred, kind, err := loadGatewayCredential(r.cfg.Gateway)
	if err != nil {
		return err
	}
	switch kind {
	case CredentialKindAPIKey:
		creds = creds.WithAPIKey(cred)
	case CredentialKindTaskToken:
		// Task tokens authenticate as the gateway-bound TargetIdentity for
		// the token's lifetime. Used when the sidecar is spawned under a
		// per-task credential mint (e.g. an orchestrator's CreateTask with
		// target_identity=sv::<impl>::<spec>).
		creds = creds.WithTaskToken(cred)
	case CredentialKindNone:
		// No credential configured; rely on mTLS or insecure mode. Empty
		// creds are valid — the gateway will fail authn explicitly if it
		// needs more.
	}

	opts := aether.ServiceOptions{
		ClientOptions: aether.ClientOptions{
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
		},
		Implementation: r.cfg.Service.Implementation,
		Specifier:      r.cfg.Service.Specifier,
	}

	tlsCfg, err := buildTLSConfig(r.cfg.Gateway)
	if err != nil {
		return err
	}
	opts.TLS = tlsCfg

	client, err := aether.NewServiceClient(opts)
	if err != nil {
		return fmt.Errorf("create service client: %w", err)
	}
	r.client = client
	r.transport = &serviceClientTransport{client: client}
	return nil
}

// Client returns the underlying ServiceClient. Callers register OnMessage,
// OnProxyHttpRequest, etc. on it before invoking Run.
func (r *gatewayRuntime) Client() *aether.ServiceClient {
	return r.client
}

// Transport returns the production tunnelTransport that ships frames
// upstream through the embedded ServiceClient.
func (r *gatewayRuntime) Transport() tunnelTransport {
	return r.transport
}

// runConnectionLoop manages reconnection, mirroring the msgbridge pattern.
// Returns when ctx is cancelled.
func (r *gatewayRuntime) runConnectionLoop(ctx context.Context) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Error().Interface("panic", rec).Msg("gateway runtime: recovered from panic")
		}
	}()
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if err := r.client.Connect(ctx); err != nil {
			log.Error().Err(err).Msg("gateway runtime: connect error")
		} else {
			backoff = 1 * time.Second
			if err := r.client.Run(ctx); err != nil {
				log.Error().Err(err).Msg("gateway runtime: run error")
			}
		}
		if ctx.Err() != nil {
			return
		}
		log.Info().Dur("backoff", backoff).Msg("gateway runtime: reconnecting to gateway")
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}
