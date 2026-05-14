// Bootstrap helpers shared by the full gateway and aetherlite binaries.
//
// Both binaries do the same thing for two pieces of plumbing:
//  1. Build a gRPC server with optional dynamic TLS (driven by a
//     ReloadableConfig so SIGHUP rotation takes effect on the next
//     handshake without restart).
//  2. Run a SIGHUP goroutine that reloads the credential set
//     (admin key, TLS cert/key, token HMAC key) and re-initializes
//     the HMAC engine when the key changes.
//
// Keeping these in one place — instead of inline in each cmd/*/main.go —
// prevents the lite and full mains from drifting out of sync.
package gateway

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/scitrera/aether/internal/config"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/crypto"
	"google.golang.org/grpc"
)

// NewGRPCServerFromConfig builds the gateway's gRPC server.
//
// When cfg.Gateway.TLS has both CertFile and KeyFile set, the server is
// constructed with dynamic certificate loading via reloadableCfg, so a
// subsequent SIGHUP picks up rotated certs on the next handshake without
// a server restart. Otherwise the server is plaintext.
//
// The boolean return is true iff TLS is enabled — callers typically log
// that fact at startup.
func NewGRPCServerFromConfig(
	cfg *config.Config,
	reloadableCfg *config.ReloadableConfig,
	serverOpts ...grpc.ServerOption,
) (*grpc.Server, bool, error) {
	if cfg == nil {
		return nil, false, fmt.Errorf("nil config")
	}
	if cfg.Gateway.TLS.CertFile == "" || cfg.Gateway.TLS.KeyFile == "" {
		return grpc.NewServer(serverOpts...), false, nil
	}
	if reloadableCfg == nil {
		return nil, true, fmt.Errorf("nil reloadable config (required when TLS is enabled)")
	}

	clientAuth := ParseClientAuth(cfg.Gateway.TLS.ClientAuth)
	getCert := func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		cert := reloadableCfg.TLSCertificate()
		if cert == nil {
			return nil, fmt.Errorf("no TLS certificate loaded")
		}
		return cert, nil
	}
	srv, err := NewGRPCServerWithDynamicTLS(getCert, cfg.Gateway.TLS.CAFile, clientAuth, serverOpts...)
	if err != nil {
		return nil, true, err
	}
	return srv, true, nil
}

// RunSIGHUPReloader wires SIGHUP to reloadableCfg.Reload() in a goroutine.
//
// On each SIGHUP it logs the reload, refreshes the credential set, and
// re-initializes the global token HMAC engine if the key changed.
// The goroutine exits and detaches the signal handler when ctx is canceled.
//
// Both `cmd/gateway` and `cmd/aetherlite` call this immediately after
// constructing reloadableCfg.
func RunSIGHUPReloader(ctx context.Context, reloadableCfg *config.ReloadableConfig) {
	if reloadableCfg == nil {
		return
	}
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	go func() {
		defer signal.Stop(sighup)
		for {
			select {
			case <-ctx.Done():
				return
			case <-sighup:
				logging.Logger.Info().Msg("received SIGHUP, reloading configuration...")
				reloaded, err := reloadableCfg.Reload()
				if err != nil {
					logging.Logger.Error().Err(err).Msg("config reload failed")
					continue
				}
				if newKey := reloadableCfg.TokenHMACKey(); newKey != "" {
					crypto.InitTokenHMAC([]byte(newKey))
				}
				if len(reloaded) == 0 {
					logging.Logger.Info().Msg("config reload complete: no credential changes detected")
				} else {
					logging.Logger.Info().Strs("reloaded", reloaded).Msg("config reload complete")
				}
			}
		}
	}()
}
