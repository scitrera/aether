// Package proxysidecar relay-mux mode.
//
// RelayMux is an alternative to Relay (relay.go) that fixes the per-identity
// session-collision problem the legacy relay has when MORE THAN ONE sub-client
// shares the relay socket.
//
// Why the legacy relay collides:
//
//	Relay.Connect opens a SEPARATE upstream gateway stream per accepted
//	sub-client (dialUpstreamGateway → a fresh grpc stream per Connect) and
//	rewrites EVERY sub-client's InitConnection to the SAME service identity
//	(service:<impl>/<spec>). The gateway's session lock is per-identity, so N
//	concurrent sub-client streams collide on one session: request/response
//	replies (KV, TaskOp, TaskQuery, ...) get delivered to whichever stream
//	currently owns the lock, NOT the stream that asked → DEADLINE_EXCEEDED on
//	the loser. One-way SendMessage survives because it expects no reply.
//
// How RelayMux fixes it:
//
//	RelayMux owns ONE shared upstream gateway connection (one InitConnection,
//	rewritten once to the sidecar's service identity + api key). It accepts
//	many sub-client streams on the same AetherGateway server surface, fans all
//	their upstream frames onto the single upstream, and demultiplexes the
//	downstream:
//	  - CORRELATED ops (carry a request_id): the request_id is rewritten to a
//	    globally-unique mux id on the way up and restored on the matching reply
//	    on the way down, which is delivered ONLY to the originating sub-client.
//	    In composite mode the mux ids are prefixed "rmx-" to guarantee they
//	    never collide with the terminator's own "req-N" ids on the same
//	    connection (the pending table lookup is the source of truth for routing;
//	    the prefix eliminates any possibility of cross-surface id collision).
//	  - TUNNEL frames: routed by tunnel id to the sub-client that opened the
//	    tunnel.
//	  - PUSH frames (IncomingMessage, ConfigSnapshot, Signal, TaskAssignment,
//	    ProgressUpdate, inbound ProxyHttpRequest, ...): broadcast to every
//	    sub-client; each uses or ignores at will (e.g. a broadcast
//	    TaskAssignment is picked up by the harness it belongs to).
//
// Two operating modes:
//
//	STANDALONE (no terminator): RelayMux dials its own upstream gateway
//	connection (one InitConnection, rewritten to the sidecar identity). The
//	downstream pump reads from up.stream.Recv and dispatches frames via
//	dispatchDownstream, which enqueues into each sub-client's bounded inbox.
//
//	COMPOSITE (terminator + relay on one shared connection): the terminator
//	owns the single gateway connection via gatewayRuntime. RelayMux does NOT
//	dial its own upstream; instead the runner wires it via SetSharedUpstream:
//	  - Outbound (sub-client → gateway): RelayMux calls sharedSend (which
//	    routes through runtime.Client().SendWithPriority) instead of
//	    up.stream.Send.
//	  - Inbound (gateway → sub-client): the SDK's rawDownstreamTap calls
//	    RouteDownstream inline (no separate inbox channel needed). For
//	    correlated/tunnel frames RouteDownstream enqueues directly to the
//	    owning sub-client's inbox. Broadcast push types (TaskAssignment,
//	    ProgressUpdate) are also claimed inline and enqueued to all sub-client
//	    inboxes. Everything else falls through to SDK typed dispatch so that
//	    OnProxyHttpResponse (terminator wakeup), Signal (SDK state mutations),
//	    Config, and OnMessage callbacks still fire normally.
//	The mux id namespace is prefixed "rmx-" so relay correlated ids never
//	collide with the terminator's "req-N" ids on the same stream.
//
// Backpressure (both modes):
//
//	Each muxSubClient has a bounded inbox channel and a priority-aware
//	deliverSem (backpressure.Semaphore with CoDel). Neither dispatchDownstream
//	(standalone pump) nor RouteDownstream (composite tap) blocks the caller on
//	a slow in-sandbox reader — they enqueue via deliver() which times out and
//	synthesises a BACKPRESSURE error frame on shed, exactly as sharedRuntimeSession
//	does. A per-sub-client writer goroutine drains the inbox and does the
//	blocking stream.Send, so a wedged reader on one sub-client never stalls the
//	shared runtime receive loop or other sub-clients.
//
// Reconnect policy (v1, deliberately simple):
//
//	Standalone: on shared-upstream error/EOF, ALL sub-client streams are
//	cancelled and the pending/tunnels tables are torn down; sub-clients
//	reconnect and re-handshake and the next connect re-establishes the
//	upstream. We do NOT buffer frames across a reconnect.
//	Composite: reconnect is owned by gatewayRuntime; RelayMux cancels all
//	sub-clients on Run ctx cancellation.
//
// Relay (relay.go) is left intact for its existing callers/tests.
// RelayMux is now the default for BOTH standalone and composite modes when
// cfg.Relay.Mux is true (default). See runner.go.
package proxysidecar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/go-backpressure"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// muxSharedSendFn is the function signature for sending an upstream frame
// through the shared runtime in composite mode. It mirrors
// sharedRuntimeSession.Send's SendWithPriority call so the mux can route
// outbound envelopes through the terminator's shared gateway queue.
type muxSharedSendFn func(ctx context.Context, prio backpressure.Priority, msg *pb.UpstreamMessage) error


// RelayMux runs the sidecar in shared-upstream relay mode. It owns a local
// gRPC server (on UDS or TCP) and exactly ONE upstream gateway connection
// shared across all accepted sub-client streams.
//
// In STANDALONE mode (no shared runtime) it dials its own upstream. In
// COMPOSITE mode (terminator + relay) it is wired via SetSharedUpstream to
// send through the terminator's shared runtime queue and receive frames from
// sharedInbox fed by the downstreamRouter.
type RelayMux struct {
	pb.UnimplementedAetherGatewayServer

	cfg     *Config
	allowed *allowedOpsSet
	clamp   *targetClamp

	// upstreamDialer builds the single outbound gRPC connection to the real
	// gateway. Production uses dialUpstreamGateway (shared with Relay); tests
	// inject a fake. Nil in composite mode (no own-dialed upstream).
	upstreamDialer func(ctx context.Context) (pb.AetherGatewayClient, func() error, error)

	// composite-mode upstream: when sharedSend is non-nil the mux is in
	// composite mode and routes outbound frames through the shared runtime.
	// Downstream frames reach sub-clients via RouteDownstream (called from
	// the rawDownstreamTap for correlated/tunnel frames) and via SDK
	// callbacks (routeToRelay) for broadcast frames — no separate inbox
	// channel is needed.
	sharedSend muxSharedSendFn

	srv      *grpc.Server
	listener net.Listener

	// subSeq assigns each accepted sub-client a unique id.
	subSeq atomic.Uint64
	// muxSeq assigns each correlated upstream op a globally-unique request id.
	muxSeq atomic.Uint64

	// mu guards everything below: the shared upstream lifecycle and the
	// routing tables.
	mu sync.Mutex
	// up is the live shared-upstream binding, or nil when not established
	// (always nil in composite mode).
	up *muxUpstream
	// subs is the set of registered sub-clients keyed by sub-client id.
	subs map[uint64]*muxSubClient
	// pending maps a mux request id to the sub-client + original request id
	// that originated it, so the demux pump can restore and route the reply.
	pending map[string]muxPending
	// tunnels maps a tunnel id to the sub-client id that opened it, so tunnel
	// downstream frames route back to the right sub-client.
	tunnels map[string]uint64
}

// muxUpstream is one live shared-upstream binding. A new one is created each
// time the upstream is (re-)established; teardown compares by pointer identity
// so a stale pump's teardown can't tear down a newer binding.
type muxUpstream struct {
	stream pb.AetherGateway_ConnectClient
	closer func() error
	cancel context.CancelFunc
}

// muxPending records the owner of a correlated in-flight upstream op.
type muxPending struct {
	subID      uint64
	origReqID  string
	payloadTag string // for logging only
}

// muxSubClient is one accepted sub-client stream. Downstream delivery is
// decoupled from the upstream receive loop: deliver() enqueues into a bounded
// inbox via a priority-aware CoDel semaphore, and a per-sub-client writer
// goroutine drains the inbox and does the blocking stream.Send. A wedged
// in-sandbox reader therefore cannot stall the shared runtime receive loop or
// any other sub-client.
type muxSubClient struct {
	id     uint64
	stream pb.AetherGateway_ConnectServer
	cancel context.CancelFunc
	closed atomic.Bool

	// inbox + deliverSem implement bounded priority-aware downstream admission.
	// Sized and tuned identically to sharedRuntimeSession so both relay paths
	// behave consistently under sustained inbox pressure.
	inbox      chan *pb.DownstreamMessage
	deliverSem *backpressure.Semaphore

	// inboundDepth is the largest proxy/tunnel chain depth this sub-client has
	// observed on broadcast inbound frames; outbound clamps floor against it.
	depthMu      sync.Mutex
	inboundDepth uint32
}

// NewRelayMux constructs a RelayMux from cfg. The local listener is not opened
// until Run is invoked. cfg.Validate() must have been called first.
func NewRelayMux(cfg *Config) (*RelayMux, error) {
	allowed, err := resolveAllowedOps(cfg.Relay.AllowedOps)
	if err != nil {
		return nil, err
	}
	m := &RelayMux{
		cfg:     cfg,
		allowed: allowed,
		clamp:   newTargetClamp(cfg.Relay.TargetTopicClamp),
		subs:    map[uint64]*muxSubClient{},
		pending: map[string]muxPending{},
		tunnels: map[string]uint64{},
	}
	m.upstreamDialer = m.dialUpstreamGateway
	return m, nil
}

// dialUpstreamGateway reuses Relay's dialer so the TLS/keepalive/api-key dial
// stays in one place. A throwaway Relay value is the cheapest way to share the
// method without restructuring relay.go.
func (m *RelayMux) dialUpstreamGateway(ctx context.Context) (pb.AetherGatewayClient, func() error, error) {
	return (&Relay{cfg: m.cfg}).dialUpstreamGateway(ctx)
}

// SetUpstreamDialer replaces the mux's upstream dialer (used by tests).
func (m *RelayMux) SetUpstreamDialer(dialer func(ctx context.Context) (pb.AetherGatewayClient, func() error, error)) {
	m.upstreamDialer = dialer
}

// SetSharedUpstream configures the mux for COMPOSITE mode. Outbound frames
// are sent through sendFn (the shared runtime's SendWithPriority). Downstream
// frames reach sub-clients via two paths:
//   - Correlated/tunnel: RouteDownstream is called from the rawDownstreamTap
//     and delivers directly to the owning sub-client.
//   - Broadcast/push: the SDK's typed callbacks (OnProxyHttpResponse,
//     OnTunnelDataIn, OnMessage, …) call routeToRelay which broadcasts to all
//     relay sub-clients after the terminator surface has had its chance.
//
// Must be called before Run. Calling this disables own-upstream dialing.
func (m *RelayMux) SetSharedUpstream(sendFn muxSharedSendFn) {
	m.sharedSend = sendFn
}

// RouteDownstream is called by the rawDownstreamTap for correlated-response
// downstream frames in composite mode. It claims a frame only when the mux's
// pending table recognises it as a relay-owned correlated reply (rmx-* mux
// id) or a relay-owned tunnel frame — and delivers it directly to the right
// sub-client.
//
// Broadcast/push frames (TaskAssignment, Signal, Config, ProxyHttpResponse,
// ProxyHttpBodyChunk, IncomingMessage, ...) are NOT claimed here. They travel
// through the normal SDK typed-dispatch path so that OnProxyHttpResponse and
// OnTunnelDataIn etc. still fire for the terminator surface, and those SDK
// callbacks call routeToRelay to fan the frame to relay sub-clients.
// Claiming broadcasts in the tap would swallow terminator wakeups (e.g. the
// "your caller went away" ProxyHttpResponse error that cancels an active
// terminator dispatch).
//
// Returns true only when the frame was consumed (correlated demux delivered,
// or tunnel routed). Returns false for broadcasts, unknown ids, and frames
// the terminator or SDK must handle — letting the SDK continue normal dispatch.
//
// In standalone mode RouteDownstream is never called; frames arrive via the
// mux's own pumpDownstream goroutine.
func (m *RelayMux) RouteDownstream(msg *pb.DownstreamMessage) bool {
	if m.sharedSend == nil {
		return false
	}
	m.mu.Lock()
	dec := m.classifyDownstream(msg)

	switch dec.route {
	case routeCorrelated, routeTunnel:
		// Correlated/tunnel: claim and deliver to the owning sub-client.
		// Apply table mutations before unlocking so a concurrent send for
		// the same id can't double-deliver.
		delete(m.pending, dec.muxReqID)
		if dec.tunnelDone {
			delete(m.tunnels, dec.tunnelID)
		}
		target := m.subs[dec.subID]
		m.mu.Unlock()

		if target == nil {
			if dec.route == routeCorrelated {
				log.Info().Str("mux_request_id", dec.muxReqID).
					Msg("relaymux: correlated reply for departed sub-client; dropping")
			} else {
				log.Info().Str("tunnel_id", dec.tunnelID).
					Msg("relaymux: tunnel frame for departed sub-client; dropping")
			}
			return true // consumed; don't let SDK misroute it
		}
		if dec.route == routeCorrelated {
			restoreDownstreamRequestID(msg, dec.origReqID)
		}
		target.deliver(msg)
		return true

	case routeBroadcast:
		// Broadcast: only claim push frame types that have no SDK-internal
		// side effects AND that the terminator does not need from the typed
		// dispatch path. These types have no registered SDK callbacks in
		// installOn, so claiming them in the tap is the only way relay
		// sub-clients receive them.
		//
		// NOT claimed (must fall through to SDK dispatch):
		//   ProxyHttpResponse  — terminator's activeDispatches wakeup path
		//   ProxyHttpBodyChunk — terminator's chunked-request accumulator
		//   Signal             — SDK mutates forceDisconnect/connected atomics
		//   Config             — bootstraps SDK KV state
		//   Error              — handled by SDK, re-checked by relay tap for correlated
		//   Msg (IncomingMessage) — already forwarded via OnMessage callback
		//
		// Safe to claim (relay-only pushes, no SDK state effects):
		//   TaskAssignment, ProgressUpdate
		switch msg.GetPayload().(type) {
		case *pb.DownstreamMessage_TaskAssignment,
			*pb.DownstreamMessage_ProgressUpdate:
		default:
			// All other broadcast types: let SDK typed dispatch handle them.
			// OnProxyHttpResponse, OnMessage etc. call routeToRelay afterward.
			m.mu.Unlock()
			return false
		}

		broadcast := make([]*muxSubClient, 0, len(m.subs))
		for _, s := range m.subs {
			broadcast = append(broadcast, s)
		}
		m.mu.Unlock()

		for _, s := range broadcast {
			s.deliver(msg)
		}
		return true // claimed; SDK need not process further

	default:
		m.mu.Unlock()
		return false
	}
}

// Run serves the relay-mux until ctx is cancelled.
//
// In STANDALONE mode it binds the configured listener, registers as the
// AetherGateway server, and pumps the shared upstream. In COMPOSITE mode the
// gateway listener is shared with the terminator (via Runner.Run), so Run
// only drains the sharedInbox channel fed by the downstreamRouter and
// dispatches frames to sub-clients.
func (m *RelayMux) Run(ctx context.Context) error {
	if m.sharedSend != nil {
		return m.runComposite(ctx)
	}
	return m.runStandalone(ctx)
}

// runStandalone is the original Run implementation for standalone relay mode.
func (m *RelayMux) runStandalone(ctx context.Context) error {
	listener, cleanup, err := openRelayListener(m.cfg.Relay.Listen)
	if err != nil {
		return fmt.Errorf("relaymux: open listener: %w", err)
	}
	m.listener = listener
	defer func() {
		_ = listener.Close()
		if cleanup != nil {
			cleanup()
		}
	}()

	server := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 15 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.ConnectionTimeout(10*time.Second),
		grpc.MaxRecvMsgSize(10*1024*1024),
		grpc.MaxSendMsgSize(10*1024*1024),
		grpc.MaxHeaderListSize(16*1024),
	)
	pb.RegisterAetherGatewayServer(server, m)
	m.srv = server

	serveErr := make(chan error, 1)
	go func() {
		log.Info().
			Str("listen", m.cfg.Relay.Listen).
			Str("identity", m.cfg.Service.Implementation+"/"+m.cfg.Service.Specifier).
			Strs("allowed_ops", m.allowed.list()).
			Str("clamp_mode", m.cfg.Relay.TargetTopicClamp.Mode).
			Int("allowed_targets", len(m.cfg.Relay.TargetTopicClamp.AllowedTargets)).
			Msg("proxy sidecar relay-mux running")
		serveErr <- server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		log.Info().Msg("proxy sidecar relay-mux shutting down")
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
				Msg("relaymux: GracefulStop exceeded grace window; forcing Stop()")
			server.Stop()
			<-gracefulDone
		}
		m.teardownUpstream(nil)
		<-serveErr
		return nil
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("relaymux: serve: %w", err)
		}
		return nil
	}
}

// runComposite is the Run implementation for composite mode.
//
// The sandbox-facing local gRPC listener is the same as standalone mode —
// sandbox processes still connect over UDS/TCP to the relay address. What
// differs is the upstream path: instead of dialing its own gateway stream,
// the mux routes outbound frames through sharedSend (the shared runtime's
// SendWithPriority). Downstream frames reach sub-clients via two paths that
// need no extra goroutine here:
//   - Correlated/tunnel: RouteDownstream is called inline from the SDK's
//     rawDownstreamTap on the runtime's receive goroutine and delivers
//     directly to the owning sub-client.
//   - Broadcast/push: SDK callbacks (OnProxyHttpResponse, OnTunnelDataIn,
//     OnMessage, …) call routeToRelay which fans to all relay sub-clients.
func (m *RelayMux) runComposite(ctx context.Context) error {
	listener, cleanup, err := openRelayListener(m.cfg.Relay.Listen)
	if err != nil {
		return fmt.Errorf("relaymux: open listener: %w", err)
	}
	m.listener = listener
	defer func() {
		_ = listener.Close()
		if cleanup != nil {
			cleanup()
		}
	}()

	server := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 15 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.ConnectionTimeout(10*time.Second),
		grpc.MaxRecvMsgSize(10*1024*1024),
		grpc.MaxSendMsgSize(10*1024*1024),
		grpc.MaxHeaderListSize(16*1024),
	)
	pb.RegisterAetherGatewayServer(server, m)
	m.srv = server

	serveErr := make(chan error, 1)
	go func() {
		log.Info().
			Str("listen", m.cfg.Relay.Listen).
			Str("identity", m.cfg.Service.Implementation+"/"+m.cfg.Service.Specifier).
			Strs("allowed_ops", m.allowed.list()).
			Str("clamp_mode", m.cfg.Relay.TargetTopicClamp.Mode).
			Int("allowed_targets", len(m.cfg.Relay.TargetTopicClamp.AllowedTargets)).
			Msg("proxy sidecar relay-mux composite running")
		serveErr <- server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		log.Info().Msg("proxy sidecar relay-mux composite shutting down")
		// Cancel all sub-clients so their Connect handlers return promptly.
		m.mu.Lock()
		subs := make([]*muxSubClient, 0, len(m.subs))
		for _, s := range m.subs {
			subs = append(subs, s)
		}
		m.subs = map[uint64]*muxSubClient{}
		m.pending = map[string]muxPending{}
		m.tunnels = map[string]uint64{}
		m.mu.Unlock()
		for _, s := range subs {
			s.closeSubClient()
			s.cancel()
		}

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
				Msg("relaymux: composite GracefulStop exceeded grace window; forcing Stop()")
			server.Stop()
			<-gracefulDone
		}
		<-serveErr
		return nil
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("relaymux: composite serve: %w", err)
		}
		return nil
	}
}

// Connect implements pb.AetherGatewayServer. Each invocation is one sub-client.
func (m *RelayMux) Connect(stream pb.AetherGateway_ConnectServer) error {
	// First message MUST be InitConnection — mirror Relay.Connect. We do NOT
	// forward it upstream; the shared upstream did its single Init already.
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
		return fmt.Errorf("relaymux: first message was %T, expected InitConnection", first.GetPayload())
	}

	// Ensure the shared upstream is live before we admit the sub-client.
	if err := m.ensureUpstream(stream.Context()); err != nil {
		_ = stream.Send(relayErrorDownstream("RELAY_UPSTREAM_UNAVAILABLE",
			fmt.Sprintf("shared upstream not available: %v", err)))
		return err
	}

	subID := m.subSeq.Add(1)
	ctx, cancel := context.WithCancel(stream.Context())
	sub := &muxSubClient{
		id:     subID,
		stream: stream,
		cancel: cancel,
		inbox:  make(chan *pb.DownstreamMessage, sharedRuntimeSessionDeliverCapacity*2),
		deliverSem: backpressure.NewSemaphore(
			5, // 5 priorities (PriorityControl..PriorityBestEffort)
			sharedRuntimeSessionDeliverCapacity,
			backpressure.SemaphoreShortTimeout(sharedRuntimeSessionDeliverShortTimeout),
			backpressure.SemaphoreLongTimeout(sharedRuntimeSessionDeliverLongTimeout),
		),
	}

	m.mu.Lock()
	m.subs[subID] = sub
	m.mu.Unlock()

	logger := log.With().Uint64("mux_sub", subID).Logger()
	logger.Info().
		Str("sandbox_claim", describeSandboxIdentity(init.Init)).
		Msg("relaymux: sub-client connected")

	// Start the per-sub-client writer goroutine. It drains inbox and calls
	// the blocking stream.Send, decoupling downstream delivery from the
	// upstream receive loop (rawDownstreamTap / pumpDownstream). The goroutine
	// exits when ctx is cancelled or the inbox is closed by closeSubClient.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		sub.runWriter(ctx)
	}()

	// Synthesize a ConnectionAck so the sub-client's SDK considers itself
	// connected. The Go SDK confirms the connection on the FIRST downstream
	// frame and handleConnectionAck reads only session_id (+ resumed), so a
	// per-sub-client synthetic session id is sufficient. We mint one rather
	// than echo the shared upstream's so distinct sub-clients never observe a
	// colliding session id.
	sub.deliver(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ConnectionAck{
			ConnectionAck: &pb.ConnectionAck{
				SessionId: fmt.Sprintf("mux-sub-%d", subID),
			},
		},
	})

	// Pump this sub-client's upstream frames onto the shared upstream until
	// the sub-client closes or the upstream is torn down (cancel fires).
	err = m.pumpSubUpstream(ctx, sub)
	sub.closeSubClient()
	<-writerDone
	m.removeSub(subID)
	logger.Info().Err(err).Msg("relaymux: sub-client closed")
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// ensureUpstream lazily establishes the shared upstream connection. It is safe
// to call concurrently; only the first caller dials.
// In composite mode this is a no-op — the upstream is the shared runtime.
func (m *RelayMux) ensureUpstream(ctx context.Context) error {
	if m.sharedSend != nil {
		// Composite mode: no own upstream to establish.
		return nil
	}

	m.mu.Lock()
	if m.up != nil {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	// Dial outside the lock (it can block); then re-check under the lock so a
	// racing caller doesn't double-establish.
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 30*time.Second)
	client, closer, err := m.upstreamDialer(dialCtx)
	cancelDial()
	if err != nil {
		return err
	}

	upCtx, upCancel := context.WithCancel(context.Background())
	stream, err := client.Connect(upCtx)
	if err != nil {
		upCancel()
		if closer != nil {
			_ = closer()
		}
		return err
	}

	apiKey, _ := loadAPIKey(m.cfg.Gateway)
	rewritten := rewriteInitConnection(&pb.InitConnection{}, m.cfg, apiKey)
	if err := stream.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Init{Init: rewritten},
	}); err != nil {
		upCancel()
		if closer != nil {
			_ = closer()
		}
		return err
	}

	m.mu.Lock()
	if m.up != nil {
		// Lost the race; tear down the connection we just built.
		m.mu.Unlock()
		upCancel()
		if closer != nil {
			_ = closer()
		}
		return nil
	}
	up := &muxUpstream{stream: stream, closer: closer, cancel: upCancel}
	m.up = up
	m.mu.Unlock()

	log.Info().Msg("relaymux: shared upstream established")
	go m.pumpDownstream(up)
	return nil
}

// pumpDownstream is the single reader of the shared upstream. It demultiplexes
// each downstream frame to the right sub-client(s) per routeDownstream, and on
// upstream error/EOF tears the whole mux down (all sub-clients + tables).
func (m *RelayMux) pumpDownstream(up *muxUpstream) {
	for {
		msg, err := up.stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Info().Msg("relaymux: shared upstream closed (EOF)")
			} else {
				log.Warn().Err(err).Msg("relaymux: shared upstream error; tearing down sub-clients")
			}
			m.teardownUpstream(up)
			return
		}
		m.dispatchDownstream(msg)
	}
}

// muxRoute is the routing decision for a downstream frame.
type muxRoute int

const (
	// routeCorrelated: deliver to a single sub-client identified by a pending
	// mux request id (which must be restored to origReqID first).
	routeCorrelated muxRoute = iota
	// routeTunnel: deliver to the sub-client that owns tunnelID.
	routeTunnel
	// routeBroadcast: deliver a copy to every registered sub-client.
	routeBroadcast
	// routeDropUnknownCorrelated: a correlated/tunnel frame whose id is not in
	// the routing tables — stale/unknown; log + drop (do NOT broadcast).
	routeDropUnknownCorrelated
)

// downstreamDecision is the pure routing classification of a downstream frame
// against the current pending/tunnel tables. It is computed under m.mu by
// classifyDownstream and consumed by dispatchDownstream. Splitting it out keeps
// the routing logic table-testable without a live stream.
type downstreamDecision struct {
	route      muxRoute
	subID      uint64 // target for routeCorrelated / routeTunnel
	origReqID  string // restore target for routeCorrelated
	muxReqID   string // pending key to delete for routeCorrelated
	tunnelID   string // for routeTunnel close-cleanup
	tunnelDone bool   // routeTunnel + TunnelClose → delete the tunnel entry
}

// classifyDownstream computes the routing decision for msg. It must be called
// with m.mu held. It does NOT mutate the tables; the caller applies any
// deletes after sending so a failed send still cleans up.
func (m *RelayMux) classifyDownstream(msg *pb.DownstreamMessage) downstreamDecision {
	// 1. ErrorResponse: demux when it carries a known pending request id, else
	//    broadcast (connection-level errors have no request_id).
	if reqID, isErr := downstreamErrorRequestID(msg); isErr {
		if reqID != "" {
			if p, ok := m.pending[reqID]; ok {
				return downstreamDecision{route: routeCorrelated, subID: p.subID, origReqID: p.origReqID, muxReqID: reqID}
			}
		}
		return downstreamDecision{route: routeBroadcast}
	}

	// 2. Correlated reply: restore + deliver to the originating sub-client.
	if reqID, correlated := downstreamGetRequestID(msg); correlated {
		if p, ok := m.pending[reqID]; ok {
			return downstreamDecision{route: routeCorrelated, subID: p.subID, origReqID: p.origReqID, muxReqID: reqID}
		}
		// Unknown/stale correlated id: drop, never broadcast.
		return downstreamDecision{route: routeDropUnknownCorrelated, muxReqID: reqID}
	}

	// 3. Tunnel frame: route by tunnel id to the owning sub-client.
	if tunID, isTunnel := downstreamTunnelID(msg); isTunnel {
		if subID, ok := m.tunnels[tunID]; ok {
			_, isClose := msg.Payload.(*pb.DownstreamMessage_TunnelClose)
			return downstreamDecision{route: routeTunnel, subID: subID, tunnelID: tunID, tunnelDone: isClose}
		}
		return downstreamDecision{route: routeDropUnknownCorrelated, tunnelID: tunID}
	}

	// 4. Everything else is a PUSH → broadcast.
	return downstreamDecision{route: routeBroadcast}
}

// dispatchDownstream classifies and delivers one downstream frame.
func (m *RelayMux) dispatchDownstream(msg *pb.DownstreamMessage) {
	m.mu.Lock()
	dec := m.classifyDownstream(msg)
	var target *muxSubClient
	var broadcast []*muxSubClient
	switch dec.route {
	case routeCorrelated, routeTunnel:
		target = m.subs[dec.subID]
		delete(m.pending, dec.muxReqID)
		if dec.tunnelDone {
			delete(m.tunnels, dec.tunnelID)
		}
	case routeBroadcast:
		broadcast = make([]*muxSubClient, 0, len(m.subs))
		for _, s := range m.subs {
			broadcast = append(broadcast, s)
		}
	}
	m.mu.Unlock()

	switch dec.route {
	case routeCorrelated:
		if target == nil {
			log.Info().Str("mux_request_id", dec.muxReqID).Msg("relaymux: correlated reply for departed sub-client; dropping")
			return
		}
		restoreDownstreamRequestID(msg, dec.origReqID)
		target.deliver(msg)
	case routeTunnel:
		if target == nil {
			log.Info().Str("tunnel_id", dec.tunnelID).Msg("relaymux: tunnel frame for departed sub-client; dropping")
			return
		}
		target.deliver(msg)
	case routeBroadcast:
		// Inbound proxy requests carry a hop-depth that floors each sub-client's
		// subsequent OUTBOUND clamps. Since a broadcast inbound ProxyHttpRequest
		// could be serviced by any sub-client, record it on all of them.
		if p, ok := msg.Payload.(*pb.DownstreamMessage_ProxyHttpRequest); ok {
			depth := p.ProxyHttpRequest.GetProxyChainDepth()
			for _, s := range broadcast {
				s.recordInboundDepth(depth)
			}
		}
		for _, s := range broadcast {
			// Each sub-client gets its own view; the proto is read-only after
			// routing so sharing the pointer is safe (no per-sub mutation).
			s.deliver(msg)
		}
	case routeDropUnknownCorrelated:
		if dec.tunnelID != "" {
			log.Info().Str("tunnel_id", dec.tunnelID).Msg("relaymux: downstream tunnel frame for unknown tunnel; dropping")
		} else {
			log.Info().Str("mux_request_id", dec.muxReqID).Msg("relaymux: downstream reply for unknown request_id; dropping")
		}
	}
}

// pumpSubUpstream copies one sub-client's frames onto the shared upstream,
// applying the same allow-list + target clamp + hop-depth floor as Relay, and
// rewriting correlated request ids to mux ids (recorded in pending) / tracking
// opened tunnels.
func (m *RelayMux) pumpSubUpstream(ctx context.Context, sub *muxSubClient) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		msg, err := sub.stream.Recv()
		if err != nil {
			return err
		}

		op := upstreamOpName(msg)
		if op == OpInitConnection {
			sub.deliver(relayErrorDownstream("RELAY_DOUBLE_INIT",
				"InitConnection is only valid as the first message"))
			continue
		}
		if !m.allowed.allows(op) {
			label := op
			if label == "" {
				label = "<unknown>"
			}
			sub.deliver(relayErrorDownstream("RELAY_OP_DENIED",
				fmt.Sprintf("operation %q not in allowed_ops %v", label, m.allowed.list())))
			log.Debug().Str("op", label).Msg("relaymux: dropped upstream op (denied)")
			continue
		}

		// Target-topic clamp + hop-depth floor on proxy/tunnel envelopes.
		switch payload := msg.Payload.(type) {
		case *pb.UpstreamMessage_ProxyHttpRequest:
			if !m.clampProxyHttp(sub, payload.ProxyHttpRequest) {
				continue
			}
		case *pb.UpstreamMessage_TunnelOpen:
			if !m.clampTunnelOpen(sub, payload.TunnelOpen) {
				continue
			}
			// Record tunnel ownership BEFORE forwarding so a fast downstream
			// frame can't arrive before the table entry exists.
			tunID := payload.TunnelOpen.GetTunnelId()
			if tunID != "" {
				m.mu.Lock()
				m.tunnels[tunID] = sub.id
				m.mu.Unlock()
			}
		}

		// Rewrite correlated request ids to a globally-unique mux id and record
		// the owner so the reply can be routed back. In composite mode the id
		// is prefixed "rmx-" to guarantee it never collides with the
		// terminator's own "req-N" ids on the same shared connection; the
		// pending-table lookup is the authoritative routing gate, but the
		// prefix eliminates any possibility of cross-surface collision even
		// before the lookup.
		seq := m.muxSeq.Add(1)
		var muxID string
		if m.sharedSend != nil {
			muxID = "rmx-" + strconv.FormatUint(seq, 10)
		} else {
			muxID = strconv.FormatUint(seq, 10)
		}
		if origID, correlated := upstreamSetRequestID(msg, muxID); correlated {
			m.mu.Lock()
			m.pending[muxID] = muxPending{subID: sub.id, origReqID: origID, payloadTag: op}
			m.mu.Unlock()
		}

		if err := m.sendUpstream(msg); err != nil {
			return err
		}
	}
}

// sendUpstream forwards one frame upstream. In composite mode it routes
// through the shared runtime's priority queue (mirroring
// sharedRuntimeSession.Send but without the per-op registerRequest — the mux
// id rewrite in pumpSubUpstream already handles correlation). In standalone
// mode it writes directly to the own-dialed stream.
func (m *RelayMux) sendUpstream(msg *pb.UpstreamMessage) error {
	if m.sharedSend != nil {
		prio := priorityForSharedRelayUpstream(msg)
		ctx, cancel := context.WithTimeout(context.Background(), sendUpstreamTimeout)
		defer cancel()
		return m.sharedSend(ctx, prio, msg)
	}
	m.mu.Lock()
	up := m.up
	m.mu.Unlock()
	if up == nil {
		return errors.New("relaymux: shared upstream not established")
	}
	return up.stream.Send(msg)
}

// clampProxyHttp applies the target clamp + hop-depth floor to an outbound
// ProxyHttpRequest, sending a relayErrorDownstream to sub and returning false
// when rejected. Mirrors relaySession.applyClampToProxyHttp.
func (m *RelayMux) clampProxyHttp(sub *muxSubClient, req *pb.ProxyHttpRequest) bool {
	if req == nil {
		sub.deliver(relayErrorDownstream("RELAY_BAD_ENVELOPE", "ProxyHttpRequest payload was nil"))
		return false
	}
	res := m.clamp.evaluate(req.GetTargetTopic())
	if !res.Allowed {
		sub.deliver(relayErrorDownstream("RELAY_TARGET_DENIED", res.Reason))
		log.Info().Str("target_topic", req.GetTargetTopic()).Str("reason", res.Reason).
			Msg("relaymux: dropped ProxyHttpRequest (target clamp)")
		return false
	}
	if res.NewTarget != "" {
		req.TargetTopic = res.NewTarget
	}
	req.ProxyChainDepth = hybridFloor(req.GetProxyChainDepth(), sub.observedDepth())
	return true
}

// clampTunnelOpen mirrors clampProxyHttp for TunnelOpen.
func (m *RelayMux) clampTunnelOpen(sub *muxSubClient, open *pb.TunnelOpen) bool {
	if open == nil {
		sub.deliver(relayErrorDownstream("RELAY_BAD_ENVELOPE", "TunnelOpen payload was nil"))
		return false
	}
	res := m.clamp.evaluate(open.GetTargetTopic())
	if !res.Allowed {
		sub.deliver(relayErrorDownstream("RELAY_TARGET_DENIED", res.Reason))
		log.Info().Str("target_topic", open.GetTargetTopic()).Str("reason", res.Reason).
			Msg("relaymux: dropped TunnelOpen (target clamp)")
		return false
	}
	if res.NewTarget != "" {
		open.TargetTopic = res.NewTarget
	}
	open.ProxyChainDepth = hybridFloor(open.GetProxyChainDepth(), sub.observedDepth())
	return true
}

// teardownUpstream closes the shared upstream (when it matches up, or
// unconditionally when up is nil) and cancels every sub-client so they
// reconnect and re-handshake. Pending/tunnel tables are cleared.
func (m *RelayMux) teardownUpstream(up *muxUpstream) {
	m.mu.Lock()
	if m.up == nil {
		m.mu.Unlock()
		return
	}
	if up != nil && m.up != up {
		// A newer upstream already replaced the one that errored; nothing to do.
		m.mu.Unlock()
		return
	}
	cur := m.up
	m.up = nil
	subs := make([]*muxSubClient, 0, len(m.subs))
	for _, s := range m.subs {
		subs = append(subs, s)
	}
	m.subs = map[uint64]*muxSubClient{}
	m.pending = map[string]muxPending{}
	m.tunnels = map[string]uint64{}
	m.mu.Unlock()

	if cur != nil {
		cur.cancel()
		if cur.closer != nil {
			_ = cur.closer()
		}
	}
	for _, s := range subs {
		s.closeSubClient()
		s.cancel()
	}
}

// removeSub deregisters a sub-client and purges any pending/tunnel entries it
// owned so a late reply can't be misrouted to a recycled id.
func (m *RelayMux) removeSub(subID uint64) {
	m.mu.Lock()
	delete(m.subs, subID)
	for k, p := range m.pending {
		if p.subID == subID {
			delete(m.pending, k)
		}
	}
	for k, owner := range m.tunnels {
		if owner == subID {
			delete(m.tunnels, k)
		}
	}
	m.mu.Unlock()
}

// deliver enqueues msg for downstream delivery to the sub-client via the
// bounded inbox. It mirrors sharedRuntimeSession.deliver: priority-aware CoDel
// admission via deliverSem, with a BACKPRESSURE error synthesised on shed so
// the in-sandbox SDK sees a clean failure rather than a silent drop.
//
// deliver never blocks the caller on a slow in-sandbox reader — it returns
// immediately. The actual blocking stream.Send runs in runWriter.
func (s *muxSubClient) deliver(msg *pb.DownstreamMessage) {
	if s.closed.Load() {
		return
	}
	prio := priorityForSharedRelayDownstream(msg)
	acqCtx, cancel := context.WithTimeout(context.Background(), sharedRuntimeSessionDeliverAcquireTimeout)
	defer cancel()
	if err := s.deliverSem.Acquire(acqCtx, prio, 1); err != nil {
		s.emitBackpressureNotice(msg, prio, err)
		return
	}
	pushed := false
	select {
	case s.inbox <- msg:
		pushed = true
	default:
	}
	s.deliverSem.Release(1)
	if !pushed {
		s.emitBackpressureNotice(msg, prio, nil)
	}
}

// emitBackpressureNotice synthesises a BACKPRESSURE error frame into the inbox
// so the in-sandbox SDK sees a clean signal on its next Recv. Mirrors
// sharedRuntimeSession.emitDeliverBackpressureNotice.
func (s *muxSubClient) emitBackpressureNotice(orig *pb.DownstreamMessage, prio backpressure.Priority, cause error) {
	if log.Debug().Enabled() {
		evt := log.Debug().
			Str("payload_type", fmt.Sprintf("%T", orig.GetPayload())).
			Int("priority", int(prio))
		if cause != nil {
			evt = evt.Err(cause).Str("trigger", "acquire-shed")
		} else {
			evt = evt.Str("trigger", "inbox-full")
		}
		evt.Uint64("mux_sub", s.id).Msg("relaymux: sub-client deliver shed, synthesising BACKPRESSURE error")
	}
	notice := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Error{
			Error: &pb.ErrorResponse{
				Code:    "BACKPRESSURE",
				Message: "relay mux sub-client inbox shed by backpressure; reduce send rate or process messages faster",
			},
		},
	}
	select {
	case s.inbox <- notice:
	default:
		log.Warn().
			Uint64("mux_sub", s.id).
			Str("payload_type", fmt.Sprintf("%T", orig.GetPayload())).
			Msg("relaymux: sub-client inbox full, dropping both envelope and BACKPRESSURE notice")
	}
}

// runWriter drains the inbox and calls the blocking stream.Send until ctx is
// cancelled and the inbox is drained. It is the only goroutine that writes to
// stream, so no additional mutex is required for stream-frame integrity.
func (s *muxSubClient) runWriter(ctx context.Context) {
	for {
		select {
		case msg, ok := <-s.inbox:
			if !ok {
				return
			}
			if err := s.stream.Send(msg); err != nil {
				log.Debug().Uint64("mux_sub", s.id).Err(err).
					Msg("relaymux: sub-client writer stream.Send failed")
				// Drain remaining messages so deliver goroutines don't block,
				// then exit; the Connect handler will clean up.
				for {
					select {
					case <-s.inbox:
					default:
						return
					}
				}
			}
		case <-ctx.Done():
			// Drain the inbox so no goroutine is blocked in deliver.
			for {
				select {
				case <-s.inbox:
				default:
					return
				}
			}
		}
	}
}

// closeSubClient marks the sub-client closed, closes the inbox channel so
// runWriter exits, and releases the semaphore resources.
func (s *muxSubClient) closeSubClient() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.inbox)
		s.deliverSem.Close()
	}
}

// recordInboundDepth bumps the sub-client's observed inbound chain depth.
func (s *muxSubClient) recordInboundDepth(depth uint32) {
	if depth == 0 {
		return
	}
	s.depthMu.Lock()
	if depth > s.inboundDepth {
		s.inboundDepth = depth
	}
	s.depthMu.Unlock()
}

// observedDepth returns the largest inbound depth seen so far for this sub.
func (s *muxSubClient) observedDepth() uint32 {
	s.depthMu.Lock()
	d := s.inboundDepth
	s.depthMu.Unlock()
	return d
}

// restoreDownstreamRequestID writes orig back onto a correlated downstream
// reply's request_id field (the inverse of upstreamSetRequestID's rewrite).
// It mirrors the correlated payload set in relaymux_reqid.go.
func restoreDownstreamRequestID(msg *pb.DownstreamMessage, orig string) {
	if msg == nil {
		return
	}
	switch p := msg.Payload.(type) {
	case *pb.DownstreamMessage_Kv:
		p.Kv.RequestId = orig
	case *pb.DownstreamMessage_TaskOp:
		p.TaskOp.RequestId = orig
	case *pb.DownstreamMessage_TaskQuery:
		p.TaskQuery.RequestId = orig
	case *pb.DownstreamMessage_CreateTask:
		p.CreateTask.RequestId = orig
	case *pb.DownstreamMessage_Checkpoint:
		p.Checkpoint.RequestId = orig
	case *pb.DownstreamMessage_Admin:
		p.Admin.RequestId = orig
	case *pb.DownstreamMessage_SessionResponse:
		p.SessionResponse.RequestId = orig
	case *pb.DownstreamMessage_Workspace:
		p.Workspace.RequestId = orig
	case *pb.DownstreamMessage_Agent:
		p.Agent.RequestId = orig
	case *pb.DownstreamMessage_Acl:
		p.Acl.RequestId = orig
	case *pb.DownstreamMessage_Token:
		p.Token.RequestId = orig
	case *pb.DownstreamMessage_AuditResponse:
		p.AuditResponse.RequestId = orig
	case *pb.DownstreamMessage_AuthorityGrant:
		p.AuthorityGrant.RequestId = orig
	case *pb.DownstreamMessage_ResolveAuthorityResponse:
		p.ResolveAuthorityResponse.RequestId = orig
	case *pb.DownstreamMessage_ConnectionStatusResponse:
		p.ConnectionStatusResponse.RequestId = orig
	case *pb.DownstreamMessage_WorkflowResponse:
		p.WorkflowResponse.RequestId = orig
	case *pb.DownstreamMessage_SubmitAuditEventResponse:
		p.SubmitAuditEventResponse.ClientRequestId = orig
	case *pb.DownstreamMessage_AuthorityRequestResponse:
		p.AuthorityRequestResponse.ClientRequestId = orig
	case *pb.DownstreamMessage_TaskSubscriptionResponse:
		p.TaskSubscriptionResponse.ClientRequestId = orig
	case *pb.DownstreamMessage_Error:
		p.Error.RequestId = orig
	}
}
