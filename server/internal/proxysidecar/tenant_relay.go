package proxysidecar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/sdk/go/aether"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// TenantRelay runs the sidecar in tenant-relay mode: it dials the local tenant
// gateway (top-level Gateway section, tenant cert under semi-strict mTLS) and
// the central aggregator's SandboxRelayTunnel surface, then splices a
// provider's gateway session through itself verbatim. It satisfies
// relaySurface.
//
// The provider's InitConnection arrives over the aggregator tunnel as the first
// TunnelFrame.up and is forwarded to the tenant gateway UNTOUCHED — neither the
// claimed identity nor resume_session_id is rewritten. This is the load-bearing
// contrast with relay.go: a relay-mode Relay rewrites every sandbox to its own
// Service identity and discards resume_session_id (relay_init.go), because the
// sandbox is untrusted; here the upstream peer is a trusted provider whose
// session must reach the gateway intact so the gateway can resume the provider's
// pre-existing session/lock. The trust boundary is the provider→aggregator and
// aggregator→relay mTLS hops (peer-cert CN), not per-op filtering — hence the
// default service-passthrough filter profile (a bypass; see
// FilterProfileServicePassthrough).
type TenantRelay struct {
	cfg     *Config
	allowed *allowedOpsSet

	// gatewayDialer opens an outbound gRPC connection to the local tenant
	// gateway. Production code uses dialTenantGateway; tests inject a fake.
	gatewayDialer func(ctx context.Context) (pb.AetherGatewayClient, func() error, error)

	// tunnelDialer opens the aggregator's SandboxRelayTunnel client. Production
	// code uses dialAggregatorTunnel; tests inject a fake.
	tunnelDialer func(ctx context.Context) (pb.SandboxRelayTunnelClient, func() error, error)
}

// NewTenantRelay constructs a TenantRelay from cfg. The tunnel and gateway
// connections are not opened until Run is invoked. cfg.Validate() must have
// been called first (NewRunner does this).
func NewTenantRelay(cfg *Config) (*TenantRelay, error) {
	// FilterProfile is resolved into an allowedOpsSet via the same path the
	// relay surface uses. The default (service-passthrough) yields a bypass
	// set; an explicit profile yields a literal allow-list.
	allowed, err := resolveAllowedOps(AllowedOpsConfig{Profile: cfg.TenantRelay.FilterProfile})
	if err != nil {
		return nil, err
	}
	t := &TenantRelay{
		cfg:     cfg,
		allowed: allowed,
	}
	t.gatewayDialer = t.dialTenantGateway
	t.tunnelDialer = t.dialAggregatorTunnel
	return t, nil
}

// Run opens the aggregator tunnel, announces this relay's tenant via
// TunnelHello, waits for the provider's first frame (its InitConnection),
// dials the local tenant gateway, forwards that init verbatim, and then pumps
// frames in both directions until ctx is cancelled or either side closes.
func (t *TenantRelay) Run(ctx context.Context) error {
	tunnelCli, tunnelClose, err := t.tunnelDialer(ctx)
	if err != nil {
		return fmt.Errorf("tenant-relay: dial aggregator: %w", err)
	}
	defer func() {
		if tunnelClose != nil {
			_ = tunnelClose()
		}
	}()

	tunnel, err := tunnelCli.Tunnel(ctx)
	if err != nil {
		return fmt.Errorf("tenant-relay: open tunnel: %w", err)
	}

	// Announce our tenant. The aggregator validates this hint against our
	// peer-cert CN and pairs us with a provider's gateway session.
	if err := tunnel.Send(&pb.TunnelFrame{
		F: &pb.TunnelFrame_Hello{Hello: &pb.TunnelHello{Tenant: t.cfg.TenantRelay.Tenant}},
	}); err != nil {
		return fmt.Errorf("tenant-relay: send hello: %w", err)
	}

	log.Info().
		Str("tenant", t.cfg.TenantRelay.Tenant).
		Str("aggregator", t.cfg.TenantRelay.Aggregator.Address).
		Str("gateway", t.cfg.Gateway.Address).
		Str("filter_profile", t.cfg.TenantRelay.FilterProfile).
		Strs("allowed_ops", t.allowed.list()).
		Msg("tenant-relay running; awaiting provider init")

	// The first up frame carries the provider's InitConnection. We forward it
	// verbatim (no identity rewrite, resume_session_id preserved) before
	// starting the bidirectional pumps.
	first, err := tunnel.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("tenant-relay: recv init frame: %w", err)
	}
	init := first.GetUp()
	if init == nil {
		return fmt.Errorf("tenant-relay: first tunnel frame was %T, expected up (InitConnection)", first.GetF())
	}
	if _, ok := init.GetPayload().(*pb.UpstreamMessage_Init); !ok {
		return fmt.Errorf("tenant-relay: first up frame was %T, expected InitConnection", init.GetPayload())
	}

	dialCtx, cancelDial := context.WithTimeout(ctx, 30*time.Second)
	gateway, gatewayClose, err := t.gatewayDialer(dialCtx)
	cancelDial()
	if err != nil {
		return fmt.Errorf("tenant-relay: dial gateway: %w", err)
	}
	defer func() {
		if gatewayClose != nil {
			_ = gatewayClose()
		}
	}()

	gwStream, err := gateway.Connect(ctx)
	if err != nil {
		return fmt.Errorf("tenant-relay: open gateway connect: %w", err)
	}

	// Forward the provider's init UNTOUCHED — this is the key passthrough.
	if err := gwStream.Send(init); err != nil {
		return fmt.Errorf("tenant-relay: send init: %w", err)
	}

	log.Info().
		Str("tenant", t.cfg.TenantRelay.Tenant).
		Str("provider_init", describeSandboxIdentity(init.GetInit())).
		Bool("has_resume", init.GetInit().GetResumeSessionId() != "").
		Msg("tenant-relay: provider session opened (init forwarded verbatim)")

	return t.runPumps(ctx, tunnel, gwStream)
}

// runPumps drives the tunnel↔gateway pumps until one direction closes or
// errors. It mirrors relaySession.run: an errgroup-of-two via errCh, cancel on
// the first return, a bounded drain of the sibling, and EOF→nil.
func (t *TenantRelay) runPumps(ctx context.Context, tunnel pb.SandboxRelayTunnel_TunnelClient, gateway pb.AetherGateway_ConnectClient) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)
	go func() { errCh <- t.pumpUp(ctx, tunnel, gateway) }()
	go func() { errCh <- t.pumpDown(ctx, tunnel, gateway) }()

	first := <-errCh
	cancel()
	// Drain the second goroutine so we don't leak it past Run's return; bound
	// the wait so a stuck peer can't wedge us.
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
	}
	if errors.Is(first, io.EOF) {
		return nil
	}
	return first
}

// pumpUp copies tunnel → gateway. Each up frame is unwrapped and, when the
// op-filter is not a bypass, matched against the allow-list before forwarding.
// There is NO target-topic clamp and NO init rewrite: provider envelopes pass
// through verbatim. When the tunnel closes its send-half (EOF) we mirror that
// on the gateway stream via CloseSend so the gateway sees a clean half-close.
func (t *TenantRelay) pumpUp(ctx context.Context, tunnel pb.SandboxRelayTunnel_TunnelClient, gateway pb.AetherGateway_ConnectClient) error {
	defer func() { _ = gateway.CloseSend() }()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		frame, err := tunnel.Recv()
		if err != nil {
			return err
		}
		up := frame.GetUp()
		if up == nil {
			// Non-up frames on the relay→gateway direction are not expected
			// (the aggregator only sends provider up frames after the init).
			// Drop them rather than forwarding garbage to the gateway.
			log.Debug().
				Str("frame", fmt.Sprintf("%T", frame.GetF())).
				Msg("tenant-relay: dropping non-up tunnel frame")
			continue
		}

		// service-passthrough is a bypass: allows() returns true unconditionally
		// so trusted provider ops absent from upstreamOpName (CreateTask,
		// SessionOperation, ...) are forwarded verbatim. A non-bypass profile
		// applies the literal allow-list.
		if !t.allowed.allows(upstreamOpName(up)) {
			log.Debug().
				Str("op", upstreamOpName(up)).
				Msg("tenant-relay: dropped upstream op (denied)")
			continue
		}

		if err := gateway.Send(up); err != nil {
			return err
		}
	}
}

// pumpDown copies gateway → tunnel, wrapping each downstream envelope in a
// TunnelFrame.down for the aggregator to splice back to the provider.
func (t *TenantRelay) pumpDown(ctx context.Context, tunnel pb.SandboxRelayTunnel_TunnelClient, gateway pb.AetherGateway_ConnectClient) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		msg, err := gateway.Recv()
		if err != nil {
			return err
		}
		if err := tunnel.Send(&pb.TunnelFrame{F: &pb.TunnelFrame_Down{Down: msg}}); err != nil {
			return err
		}
	}
}

// dialTenantGateway opens a gRPC connection to the local tenant gateway using
// the top-level Gateway section (tenant cert under semi-strict mTLS). It reuses
// the shared dialGatewayWithTLS helper. The closer must be invoked when the
// caller no longer needs the connection.
func (t *TenantRelay) dialTenantGateway(_ context.Context) (pb.AetherGatewayClient, func() error, error) {
	return dialGatewayWithTLS(t.cfg.Gateway)
}

// dialAggregatorTunnel opens a SandboxRelayTunnel client to the aggregator using
// the TenantRelay.Aggregator dial config (sandboxes-CA client cert under mTLS,
// or insecure for test/dev). The closer must be invoked when the caller no
// longer needs the connection.
func (t *TenantRelay) dialAggregatorTunnel(_ context.Context) (pb.SandboxRelayTunnelClient, func() error, error) {
	agg := t.cfg.TenantRelay.Aggregator

	var dialOpts []grpc.DialOption
	if agg.Insecure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		tlsCfg, err := buildAggregatorTLS(agg.TLS)
		if err != nil {
			return nil, nil, fmt.Errorf("build aggregator tls: %w", err)
		}
		stdTLS, err := materialiseTLS(tlsCfg)
		if err != nil {
			return nil, nil, fmt.Errorf("load aggregator tls credentials: %w", err)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(stdTLS)))
	}
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             15 * time.Second,
		PermitWithoutStream: true,
	}))

	conn, err := grpc.NewClient(agg.Address, dialOpts...)
	if err != nil {
		return nil, nil, err
	}
	return pb.NewSandboxRelayTunnelClient(conn), conn.Close, nil
}

// buildAggregatorTLS reads the aggregator dial config's TLS cert/key/CA into an
// aether.TLSConfig suitable for materialiseTLS. Mirrors buildTLSConfig but is
// keyed off the AggregatorDialConfig's TLSConfig rather than a GatewayConfig.
func buildAggregatorTLS(tlsConf TLSConfig) (*aether.TLSConfig, error) {
	out := &aether.TLSConfig{Enabled: true}
	if tlsConf.CAFile != "" {
		ca, err := os.ReadFile(tlsConf.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read aggregator tls.ca_file %q: %w", tlsConf.CAFile, err)
		}
		out.RootCAs = ca
	}
	if tlsConf.CertFile != "" {
		cert, err := os.ReadFile(tlsConf.CertFile)
		if err != nil {
			return nil, fmt.Errorf("read aggregator tls.cert_file %q: %w", tlsConf.CertFile, err)
		}
		out.ClientCert = cert
	}
	if tlsConf.KeyFile != "" {
		key, err := os.ReadFile(tlsConf.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("read aggregator tls.key_file %q: %w", tlsConf.KeyFile, err)
		}
		out.ClientKey = key
	}
	return out, nil
}
