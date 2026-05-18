// Package proxysidecar relay mode.
//
// Relay sidecars sit between an untrusted sandbox process and the real
// AetherGateway. The sandbox dials the sidecar over UDS (preferred) or TCP
// using plain HTTP/2 gRPC — no TLS, no API key, no credentials. The
// sidecar accepts each bidi stream, opens a paired stream to the real
// gateway with its own credentials, and pumps envelopes between them.
//
// What the relay enforces between the two streams:
//
//   - InitConnection: the sandbox's claimed identity and resume_session_id
//     are discarded. The upstream init carries the sidecar's configured
//     Service identity and gateway API key. (relay_init.go)
//   - Allow-list: every upstream envelope's payload variant is matched
//     against an allow-list (named profile or literal). Denied ops drop a
//     DownstreamMessage_Error onto the sandbox stream and are not
//     forwarded. (relay_filter.go)
//   - Target-topic clamp: ProxyHttpRequest / TunnelOpen target_topic must
//     match the configured allowed_targets glob list, either directly
//     (mode=reject, default) or by rewrite (mode=rewrite_first_match).
//     (relay_clamp.go)
//   - Hop-depth clamp: ProxyHttpRequest.proxy_chain_depth and
//     TunnelOpen.proxy_chain_depth are clamped upward to
//     max(sandbox_claim, last_inbound_depth + 1) so a sandbox cannot
//     understate its position in a proxy chain.
//
// One sandbox stream maps to one upstream gateway stream. There is no
// shared multiplex: each sandbox session gets its own gateway lock and
// session id, which is what we want — credentials and identity are the
// sidecar's, but the per-session state remains per-session.
package proxysidecar

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/sdk/go/aether"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Relay runs the sidecar in relay mode. It owns a local gRPC server (on
// UDS or TCP) and dials the configured upstream gateway once per accepted
// sandbox stream.
type Relay struct {
	pb.UnimplementedAetherGatewayServer

	cfg     *Config
	allowed *allowedOpsSet
	clamp   *targetClamp

	// upstreamDialer builds an outbound gRPC connection to the real
	// gateway. Production code uses dialUpstreamGateway; tests inject a
	// fake.
	upstreamDialer func(ctx context.Context) (pb.AetherGatewayClient, func() error, error)

	srv      *grpc.Server
	listener net.Listener

	// sessionCount is incremented for every accepted sandbox stream and
	// emitted in audit logs so per-session lifecycles can be traced.
	sessionCount atomic.Uint64
}

// NewRelay constructs a Relay from cfg. The local listener is not opened
// until Run is invoked. cfg.Validate() must have been called first.
func NewRelay(cfg *Config) (*Relay, error) {
	allowed, err := resolveAllowedOps(cfg.Relay.AllowedOps)
	if err != nil {
		return nil, err
	}
	r := &Relay{
		cfg:     cfg,
		allowed: allowed,
		clamp:   newTargetClamp(cfg.Relay.TargetTopicClamp),
	}
	r.upstreamDialer = r.dialUpstreamGateway
	return r, nil
}

// SetUpstreamDialer replaces the relay's upstream dialer. The composite
// runtime uses this to share a single gateway connection between the
// terminator and the relay; standalone relay mode keeps the default
// per-session dial.
func (r *Relay) SetUpstreamDialer(dialer func(ctx context.Context) (pb.AetherGatewayClient, func() error, error)) {
	r.upstreamDialer = dialer
}

// Run binds the configured listener, registers the relay as the
// AetherGateway server, and serves until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	listener, cleanup, err := openRelayListener(r.cfg.Relay.Listen)
	if err != nil {
		return fmt.Errorf("relay: open listener: %w", err)
	}
	r.listener = listener
	defer func() {
		_ = listener.Close()
		if cleanup != nil {
			cleanup()
		}
	}()

	// Match the upstream gateway's per-connection resource caps to mitigate
	// memory-exhaustion DoS from an untrusted sandbox process. Values are
	// intentionally identical to the gateway defaults so a sandbox cannot
	// induce different framing characteristics by talking to the sidecar.
	//
	// ConnectionTimeout bounds the HTTP/2 handshake (preface + SETTINGS) on
	// freshly accepted raw connections. The grpc default is 120s, which means
	// a peer that opens a TCP/UDS socket but never sends the HTTP/2 preface
	// pins a handleRawConn goroutine for two minutes. Crucially, that goroutine
	// is tracked in the server's serveWG, so BOTH GracefulStop AND Stop block
	// on serveWG.Wait() until either the preface arrives or this deadline
	// fires — Stop does NOT force-close mid-handshake rawConns. A short
	// handshake deadline is therefore load-bearing for shutdown latency, not
	// just a DoS knob. 10s is well above any legitimate handshake time over
	// UDS / localhost TCP while still letting shutdown complete within the
	// test's 30s budget on a heavily loaded CI runner.
	server := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 15 * time.Second,
		}),
		grpc.ConnectionTimeout(10*time.Second),
		grpc.MaxRecvMsgSize(10*1024*1024),
		grpc.MaxSendMsgSize(10*1024*1024),
		grpc.MaxHeaderListSize(16*1024),
	)
	pb.RegisterAetherGatewayServer(server, r)
	r.srv = server

	serveErr := make(chan error, 1)
	go func() {
		log.Info().
			Str("listen", r.cfg.Relay.Listen).
			Str("identity", r.cfg.Service.Implementation+"/"+r.cfg.Service.Specifier).
			Strs("allowed_ops", r.allowed.list()).
			Str("clamp_mode", r.cfg.Relay.TargetTopicClamp.Mode).
			Int("allowed_targets", len(r.cfg.Relay.TargetTopicClamp.AllowedTargets)).
			Msg("proxy sidecar relay running")
		serveErr <- server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		log.Info().Msg("proxy sidecar relay shutting down")
		// Bound GracefulStop so a mid-handshake inbound connection that
		// never sent the HTTP/2 preface (or any other stuck stream)
		// doesn't pin shutdown forever. grpc.Server.GracefulStop has no
		// internal timeout and waits on its connection WaitGroup
		// indefinitely; if a peer has accept()ed a TCP socket but not
		// yet sent the preface, the goroutine pins forever. Falling back to
		// Stop() after a fixed window force-closes those sockets and
		// lets the Serve goroutine return.
		//
		// 3 s is enough headroom for normal in-flight streams to drain;
		// production sidecars rarely see anywhere near that on clean
		// SIGTERM. Mid-handshake conns add zero latency to the normal
		// path because the GracefulStop completes before the window.
		const gracePeriod = 3 * time.Second
		gracefulDone := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(gracefulDone)
		}()
		select {
		case <-gracefulDone:
		case <-time.After(gracePeriod):
			log.Warn().
				Dur("grace", gracePeriod).
				Msg("relay: GracefulStop exceeded grace window; forcing Stop()")
			server.Stop()
			<-gracefulDone
		}
		<-serveErr
		return nil
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("relay: serve: %w", err)
		}
		return nil
	}
}

// Connect implements pb.AetherGatewayServer. Each invocation is a single
// sandbox session.
func (r *Relay) Connect(stream pb.AetherGateway_ConnectServer) error {
	sessionID := r.sessionCount.Add(1)
	logger := log.With().Uint64("relay_session", sessionID).Logger()

	// First message MUST be InitConnection. We rewrite it before opening
	// the upstream stream so we never leak the sandbox's claimed
	// credentials.
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	init, ok := first.GetPayload().(*pb.UpstreamMessage_Init)
	if !ok || init == nil {
		_ = stream.Send(relayErrorDownstream("RELAY_INVALID_INIT",
			"first message on sandbox stream must be InitConnection"))
		return fmt.Errorf("relay: first message was %T, expected InitConnection", first.GetPayload())
	}

	apiKey, _ := loadAPIKey(r.cfg.Gateway)
	rewritten := rewriteInitConnection(init.Init, r.cfg, apiKey)

	dialCtx, cancelDial := context.WithTimeout(stream.Context(), 30*time.Second)
	upstream, closer, err := r.upstreamDialer(dialCtx)
	cancelDial()
	if err != nil {
		_ = stream.Send(relayErrorDownstream("RELAY_UPSTREAM_DIAL_FAILED",
			fmt.Sprintf("dial gateway: %v", err)))
		return err
	}
	defer func() {
		if closer != nil {
			_ = closer()
		}
	}()

	upStream, err := upstream.Connect(stream.Context())
	if err != nil {
		_ = stream.Send(relayErrorDownstream("RELAY_UPSTREAM_CONNECT_FAILED",
			fmt.Sprintf("open upstream stream: %v", err)))
		return err
	}

	if err := upStream.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Init{Init: rewritten},
	}); err != nil {
		_ = stream.Send(relayErrorDownstream("RELAY_UPSTREAM_SEND_FAILED",
			fmt.Sprintf("send init: %v", err)))
		return err
	}

	logger.Info().
		Str("sandbox_claim", describeSandboxIdentity(init.Init)).
		Msg("relay: session opened")

	session := &relaySession{
		relay:    r,
		sandbox:  stream,
		upstream: upStream,
	}
	err = session.run()
	logger.Info().Err(err).Msg("relay: session closed")
	return err
}

// relaySession holds the shared state for one sandbox<->gateway pair.
type relaySession struct {
	relay    *Relay
	sandbox  pb.AetherGateway_ConnectServer
	upstream pb.AetherGateway_ConnectClient

	// inboundDepth tracks the largest proxy_chain_depth observed on
	// downstream proxy/tunnel envelopes the sandbox is currently
	// servicing. Outbound clamps key off this floor.
	depthMu      sync.Mutex
	inboundDepth uint32
}

// run drives both pump goroutines until one direction closes or errors.
func (s *relaySession) run() error {
	ctx, cancel := context.WithCancel(s.sandbox.Context())
	defer cancel()

	errCh := make(chan error, 2)
	go func() { errCh <- s.pumpUpstream(ctx) }()
	go func() { errCh <- s.pumpDownstream(ctx) }()

	first := <-errCh
	cancel()
	// Drain the second goroutine so we don't leak it past Connect's
	// return; bound the wait so a stuck peer can't wedge us.
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
	}
	if errors.Is(first, io.EOF) {
		return nil
	}
	return first
}

// pumpUpstream copies sandbox -> gateway, applying the filter, init
// rewrite (already done before pump start), target clamp, and hop-depth
// floor. When the sandbox closes its send-half (EOF), we mirror that on
// the outbound stream via CloseSend so the gateway sees a clean half-close.
func (s *relaySession) pumpUpstream(ctx context.Context) error {
	defer func() {
		// CloseSend signals end-of-input on the outbound stream so the
		// gateway can drain and close cleanly. Best-effort.
		_ = s.upstream.CloseSend()
	}()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		msg, err := s.sandbox.Recv()
		if err != nil {
			return err
		}

		op := upstreamOpName(msg)
		if op == OpInitConnection {
			// Sandbox tried to reinitialise mid-stream; the upstream
			// gateway would reject this anyway, but we drop it here so
			// we don't double-init.
			_ = s.sandbox.Send(relayErrorDownstream("RELAY_DOUBLE_INIT",
				"InitConnection is only valid as the first message"))
			continue
		}

		if !s.relay.allowed.allows(op) {
			label := op
			if label == "" {
				label = "<unknown>"
			}
			_ = s.sandbox.Send(relayErrorDownstream("RELAY_OP_DENIED",
				fmt.Sprintf("operation %q not in allowed_ops %v", label, s.relay.allowed.list())))
			log.Debug().
				Str("op", label).
				Msg("relay: dropped upstream op (denied)")
			continue
		}

		// Apply target-topic clamp + hop-depth floor on the proxy/tunnel
		// envelopes before forwarding. Other ops pass through verbatim.
		switch payload := msg.Payload.(type) {
		case *pb.UpstreamMessage_ProxyHttpRequest:
			if !s.applyClampToProxyHttp(payload.ProxyHttpRequest) {
				continue
			}
		case *pb.UpstreamMessage_TunnelOpen:
			if !s.applyClampToTunnelOpen(payload.TunnelOpen) {
				continue
			}
		}

		if err := s.upstream.Send(msg); err != nil {
			return err
		}
	}
}

// pumpDownstream copies gateway -> sandbox. Hop-depth observed on
// inbound proxy/tunnel requests is recorded so subsequent outbound
// envelopes from the sandbox can be floored against it.
func (s *relaySession) pumpDownstream(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		msg, err := s.upstream.Recv()
		if err != nil {
			return err
		}

		switch payload := msg.Payload.(type) {
		case *pb.DownstreamMessage_ProxyHttpRequest:
			s.recordInboundDepth(payload.ProxyHttpRequest.GetProxyChainDepth())
		case *pb.DownstreamMessage_TunnelData:
			// Seq=0 carries an embedded TunnelOpen; for hop-depth the
			// gateway already increments before delivering the payload,
			// so the data envelope itself is fine to forward verbatim.
		}

		if err := s.sandbox.Send(msg); err != nil {
			return err
		}
	}
}

// applyClampToProxyHttp returns false when the envelope was rejected
// (an error has been sent to the sandbox already and the caller should
// drop it).
func (s *relaySession) applyClampToProxyHttp(req *pb.ProxyHttpRequest) bool {
	if req == nil {
		_ = s.sandbox.Send(relayErrorDownstream("RELAY_BAD_ENVELOPE",
			"ProxyHttpRequest payload was nil"))
		return false
	}
	res := s.relay.clamp.evaluate(req.GetTargetTopic())
	if !res.Allowed {
		_ = s.sandbox.Send(relayErrorDownstream("RELAY_TARGET_DENIED", res.Reason))
		log.Debug().
			Str("target_topic", req.GetTargetTopic()).
			Str("reason", res.Reason).
			Msg("relay: dropped ProxyHttpRequest (target clamp)")
		return false
	}
	if res.NewTarget != "" {
		req.TargetTopic = res.NewTarget
	}
	req.ProxyChainDepth = hybridFloor(req.GetProxyChainDepth(), s.observedDepth())
	return true
}

// applyClampToTunnelOpen mirrors applyClampToProxyHttp for TunnelOpen.
func (s *relaySession) applyClampToTunnelOpen(open *pb.TunnelOpen) bool {
	if open == nil {
		_ = s.sandbox.Send(relayErrorDownstream("RELAY_BAD_ENVELOPE",
			"TunnelOpen payload was nil"))
		return false
	}
	res := s.relay.clamp.evaluate(open.GetTargetTopic())
	if !res.Allowed {
		_ = s.sandbox.Send(relayErrorDownstream("RELAY_TARGET_DENIED", res.Reason))
		log.Debug().
			Str("target_topic", open.GetTargetTopic()).
			Str("reason", res.Reason).
			Msg("relay: dropped TunnelOpen (target clamp)")
		return false
	}
	if res.NewTarget != "" {
		open.TargetTopic = res.NewTarget
	}
	open.ProxyChainDepth = hybridFloor(open.GetProxyChainDepth(), s.observedDepth())
	return true
}

// recordInboundDepth bumps the session's observed inbound chain depth
// when the new value is strictly larger than what we've already seen.
func (s *relaySession) recordInboundDepth(depth uint32) {
	if depth == 0 {
		return
	}
	s.depthMu.Lock()
	if depth > s.inboundDepth {
		s.inboundDepth = depth
	}
	s.depthMu.Unlock()
}

// observedDepth returns the largest inbound depth seen so far.
func (s *relaySession) observedDepth() uint32 {
	s.depthMu.Lock()
	d := s.inboundDepth
	s.depthMu.Unlock()
	return d
}

// dialUpstreamGateway opens a gRPC connection to the configured gateway
// using the sidecar's TLS / API-key configuration. The closer must be
// invoked when the caller no longer needs the connection.
func (r *Relay) dialUpstreamGateway(_ context.Context) (pb.AetherGatewayClient, func() error, error) {
	tlsCfg, err := buildTLSConfig(r.cfg.Gateway)
	if err != nil {
		return nil, nil, fmt.Errorf("build tls: %w", err)
	}

	var dialOpts []grpc.DialOption
	if tlsCfg != nil && tlsCfg.Enabled {
		stdTLS, err := materialiseTLS(tlsCfg)
		if err != nil {
			return nil, nil, fmt.Errorf("load tls credentials: %w", err)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(stdTLS)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             15 * time.Second,
		PermitWithoutStream: true,
	}))

	conn, err := grpc.NewClient(r.cfg.Gateway.Address, dialOpts...)
	if err != nil {
		return nil, nil, err
	}
	return pb.NewAetherGatewayClient(conn), conn.Close, nil
}

// materialiseTLS converts the SDK's aether.TLSConfig into a
// crypto/tls.Config suitable for gRPC TLS credentials.
func materialiseTLS(cfg *aether.TLSConfig) (*tls.Config, error) {
	out := &tls.Config{MinVersion: tls.VersionTLS12}
	if len(cfg.RootCAs) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(cfg.RootCAs) {
			return nil, fmt.Errorf("append root cas: no certs parsed")
		}
		out.RootCAs = pool
	}
	if len(cfg.ClientCert) > 0 || len(cfg.ClientKey) > 0 {
		if len(cfg.ClientCert) == 0 || len(cfg.ClientKey) == 0 {
			return nil, fmt.Errorf("both client_cert and client_key must be supplied for mTLS")
		}
		cert, err := tls.X509KeyPair(cfg.ClientCert, cfg.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("parse client keypair: %w", err)
		}
		out.Certificates = []tls.Certificate{cert}
	}
	return out, nil
}

// openRelayListener binds the configured listener. UDS schemes
// (`unix:///path`) are normalised by removing any stale socket file with
// the same name. TCP schemes (`tcp://host:port` or bare `host:port`)
// fall through to net.Listen on a TCP listener.
func openRelayListener(spec string) (net.Listener, func(), error) {
	scheme, addr, err := splitListenSpec(spec)
	if err != nil {
		return nil, nil, err
	}
	switch scheme {
	case "unix":
		// Remove a stale socket if one exists; ignore not-exist errors.
		if err := os.Remove(addr); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, nil, fmt.Errorf("remove stale socket %q: %w", addr, err)
		}
		// Make sure the parent directory exists; UDS bind fails confusingly otherwise.
		if dir := filepath.Dir(addr); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, nil, fmt.Errorf("mkdir %q: %w", dir, err)
			}
		}
		l, err := net.Listen("unix", addr)
		if err != nil {
			return nil, nil, err
		}
		// 0660: owner+group rw, other no access. The sandbox is expected
		// to share the socket's group with the sidecar.
		_ = os.Chmod(addr, 0o660)
		cleanup := func() { _ = os.Remove(addr) }
		return l, cleanup, nil
	case "tcp":
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, nil, err
		}
		return l, nil, nil
	default:
		return nil, nil, fmt.Errorf("relay.listen: unsupported scheme %q (use unix:// or tcp://)", scheme)
	}
}

// splitListenSpec parses a listener spec into (scheme, addr). Bare
// host:port strings default to tcp.
func splitListenSpec(spec string) (string, string, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return "", "", fmt.Errorf("listener spec is empty")
	}
	if strings.HasPrefix(spec, "unix://") {
		return "unix", strings.TrimPrefix(spec, "unix://"), nil
	}
	if strings.HasPrefix(spec, "tcp://") {
		return "tcp", strings.TrimPrefix(spec, "tcp://"), nil
	}
	if strings.Contains(spec, "://") {
		u, err := url.Parse(spec)
		if err != nil {
			return "", "", fmt.Errorf("parse listener spec %q: %w", spec, err)
		}
		return u.Scheme, u.Host + u.Path, nil
	}
	// Bare "host:port" → assume tcp.
	return "tcp", spec, nil
}

// relayErrorDownstream constructs a DownstreamMessage carrying an
// ErrorResponse so the sandbox sees a structured error rather than a
// dropped envelope.
func relayErrorDownstream(code, msg string) *pb.DownstreamMessage {
	return &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Error{
			Error: &pb.ErrorResponse{
				Code:      code,
				Message:   msg,
				Retryable: false,
			},
		},
	}
}
