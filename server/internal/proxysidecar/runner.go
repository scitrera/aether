// Runner orchestrates one or more enabled sidecar surfaces sharing a single
// gateway connection (one Aether identity, one distributed lock).
//
// Surfaces are independent features the operator opts into via the per-section
// Enabled flags in Config. The runner builds whichever ones are turned on and
// runs them concurrently:
//
//   - terminator: receives gateway → service envelopes (ProxyHttpRequest,
//     Tunnel*) and forwards them to local backends.
//   - relay: accepts a sandbox process's plain-gRPC AetherGateway stream and
//     pumps filtered envelopes upstream over the shared connection.
//   - initiator: exposes a local HTTP listener that translates each request
//     into a ProxyHttpRequest envelope.
//
// When terminator and relay are both enabled, downstream envelopes are split
// by payload type via the downstreamRouter so a single gateway connection can
// serve both surfaces. Two streams from one identity would race for the same
// Redis lock and the second would be rejected with DuplicateIdentityError —
// hence the shared connection.
package proxysidecar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/sdk/go/aether"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// Runner owns the shared gateway connection and the enabled surfaces.
type Runner struct {
	cfg     *Config
	cfgPath string

	// runtime is the shared gateway client. Built only when at least one
	// surface needs it (currently: terminator); nil otherwise.
	runtime *gatewayRuntime
	router  *downstreamRouter

	term  *Terminator
	relay *Relay
	init  *Initiator
}

// NewRunner builds a Runner from cfg. cfg.Validate() is invoked here so the
// caller does not need to call it separately.
func NewRunner(cfg *Config, cfgPath string) (*Runner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	r := &Runner{cfg: cfg, cfgPath: cfgPath}

	if cfg.Terminator.Enabled {
		r.runtime = newGatewayRuntime(cfg)
		r.router = &downstreamRouter{}
		t, err := newTerminatorAttached(cfg, r.runtime, cfgPath)
		if err != nil {
			return nil, fmt.Errorf("terminator: %w", err)
		}
		r.term = t
		r.router.term = t
	}

	if cfg.Relay.Enabled {
		relay, err := NewRelay(cfg)
		if err != nil {
			return nil, fmt.Errorf("relay: %w", err)
		}
		r.relay = relay
		// When the shared runtime is available, route the relay's upstream
		// envelopes through it so both surfaces ride one gateway lock.
		// Otherwise the relay keeps its default per-session dialer.
		if r.runtime != nil {
			sink := newSharedRelaySink(r.runtime)
			r.router.relay = sink
			relay.SetUpstreamDialer(sink.dial)
		}
	}

	if cfg.Initiator.Enabled {
		ini, err := NewInitiator(cfg)
		if err != nil {
			return nil, fmt.Errorf("initiator: %w", err)
		}
		r.init = ini
	}

	if r.term == nil && r.relay == nil && r.init == nil {
		// Validate() guards against this, but keep a defensive error so a
		// future caller that bypasses Validate gets a clear message.
		return nil, fmt.Errorf("runner: no surfaces enabled")
	}
	return r, nil
}

// Terminator exposes the runner's terminator (or nil when disabled). Tests
// reach for this when they need to drive HandleProxyRequest directly.
func (r *Runner) Terminator() *Terminator { return r.term }

// Relay exposes the runner's relay (or nil when disabled).
func (r *Runner) Relay() *Relay { return r.relay }

// Initiator exposes the runner's initiator (or nil when disabled).
func (r *Runner) Initiator() *Initiator { return r.init }

// Run connects the shared runtime (if any) and serves every enabled surface
// concurrently until ctx is cancelled or any surface returns a non-nil error.
func (r *Runner) Run(ctx context.Context) error {
	if r.runtime != nil {
		if err := r.runtime.init(); err != nil {
			return fmt.Errorf("runner: build client: %w", err)
		}
		r.router.installOn(r.runtime.Client(), r.runtime.Transport())
		go r.runtime.runConnectionLoop(ctx)
	}

	log.Info().
		Str("gateway", r.cfg.Gateway.Address).
		Strs("surfaces", r.cfg.EnabledSurfaces()).
		Str("identity", r.cfg.Service.Implementation+"/"+r.cfg.Service.Specifier).
		Msg("proxy sidecar runner starting")

	g, gctx := errgroup.WithContext(ctx)

	if r.term != nil {
		g.Go(func() error {
			log.Info().
				Int("backends", len(r.term.backends)).
				Msg("proxy sidecar terminator running")
			<-gctx.Done()
			log.Info().Msg("proxy sidecar terminator shutting down")
			return nil
		})
	}

	if r.relay != nil {
		g.Go(func() error {
			if err := r.relay.Run(gctx); err != nil {
				return fmt.Errorf("relay: %w", err)
			}
			return nil
		})
	}

	if r.init != nil {
		g.Go(func() error {
			if err := r.init.Run(gctx); err != nil {
				return fmt.Errorf("initiator: %w", err)
			}
			return nil
		})
	}

	err := g.Wait()
	if err != nil && errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// Reload re-reads the config file and applies any reloadable changes.
// Currently only the terminator's backends respond; surface enable/disable
// flips are not reloadable.
func (r *Runner) Reload() {
	if r.term != nil {
		r.term.Reload()
	}
}

// =============================================================================
// downstreamRouter — splits inbound envelopes between terminator and relay.
// =============================================================================

// downstreamRouter is wired into the shared runtime's ServiceClient so that
// envelopes the gateway publishes to our sv:: topic land in whichever surface
// owns them. When relay is nil this collapses to standalone-terminator
// semantics; when both are present the router preserves the composite-mode
// semantics from the previous design (terminator owns its registered
// tunnels, the rest fall through to the active relay session).
type downstreamRouter struct {
	term  *Terminator
	relay *sharedRelaySink
}

// installOn wires the dispatcher hooks on client. The supplied transport
// ships outbound proxy/tunnel envelopes upstream.
func (r *downstreamRouter) installOn(client *aether.ServiceClient, transport tunnelTransport) {
	if r.term == nil {
		// The runtime is only built when terminator is enabled, so r.term is
		// non-nil in practice. Defensive guard for future callers.
		return
	}

	// Plain peer messages: log only — the sidecar has no message-relay
	// surface, terminators don't expect peer-to-peer messages.
	client.OnMessage(func(_ context.Context, msg *aether.Message) error {
		log.Debug().
			Str("source", msg.SourceTopic).
			Int("payload_bytes", len(msg.Payload)).
			Msg("runner: received message via OnMessage path")
		return nil
	})

	// Inbound HTTP request: terminator handles. Relay never receives this
	// (the gateway never publishes ProxyHttpRequest as a *response* to an
	// outbound caller).
	client.OnProxyHttpRequest(func(reqCtx context.Context, req *pb.ProxyHttpRequest) error {
		if req.GetBodyChunked() {
			return r.term.beginChunkedRequest(req, transport)
		}
		return r.term.dispatchAndRespond(reqCtx, req, req.GetBody(), transport)
	})

	// ProxyHttpBodyChunk: is_request=true → terminator (chunked inbound
	// body); is_request=false → relay (chunked response to a sandbox-issued
	// outbound request) when relay is co-enabled, otherwise drop.
	client.OnProxyHttpBodyChunk(func(chunkCtx context.Context, chunk *pb.ProxyHttpBodyChunk) error {
		if chunk.GetIsRequest() {
			return r.term.handleChunkedRequestFrame(chunkCtx, chunk, transport)
		}
		if r.relay != nil {
			r.relay.routeMessage(&pb.DownstreamMessage{
				Payload: &pb.DownstreamMessage_ProxyHttpBodyChunk{ProxyHttpBodyChunk: chunk},
			})
		}
		return nil
	})

	// TunnelData: a seq=0 frame carrying a TunnelOpen body is the gateway's
	// "open" signal — terminator handles. All other tunnel data frames are
	// by-id, so consult the terminator's tunnel manager first; unknown ids
	// fall to the relay (composite mode) or to terminator's default
	// PEER_RESET path (standalone).
	client.OnTunnelDataIn(func(dataCtx context.Context, frame *pb.TunnelData) error {
		if frame.GetSeq() == 0 && len(frame.GetData()) > 0 {
			open := &pb.TunnelOpen{}
			if err := tunnelDataIsOpen(frame, open); err == nil {
				if cm := r.term.HandleTunnelOpen(dataCtx, open, transport); cm != nil {
					_ = transport.SendTunnelClose(cm)
				}
				return nil
			}
		}
		if r.term.tunnels != nil && r.term.tunnels.get(frame.GetTunnelId()) != nil {
			r.term.HandleTunnelData(frame, transport)
			return nil
		}
		if r.relay != nil {
			r.relay.routeMessage(&pb.DownstreamMessage{
				Payload: &pb.DownstreamMessage_TunnelData{TunnelData: frame},
			})
			return nil
		}
		// Standalone terminator: emit PEER_RESET for unknown tunnel.
		r.term.HandleTunnelData(frame, transport)
		return nil
	})

	client.OnTunnelAckIn(func(_ context.Context, ack *pb.TunnelAck) error {
		if r.term.tunnels != nil && r.term.tunnels.get(ack.GetTunnelId()) != nil {
			r.term.HandleTunnelAck(ack)
			return nil
		}
		if r.relay != nil {
			r.relay.routeMessage(&pb.DownstreamMessage{
				Payload: &pb.DownstreamMessage_TunnelAck{TunnelAck: ack},
			})
			return nil
		}
		// Standalone: HandleTunnelAck silently no-ops on unknown tunnels.
		return nil
	})

	client.OnTunnelCloseIn(func(_ context.Context, cm *pb.TunnelClose) error {
		if r.term.tunnels != nil && r.term.tunnels.get(cm.GetTunnelId()) != nil {
			r.term.HandleTunnelClose(cm)
			return nil
		}
		if r.relay != nil {
			r.relay.routeMessage(&pb.DownstreamMessage{
				Payload: &pb.DownstreamMessage_TunnelClose{TunnelClose: cm},
			})
			return nil
		}
		// Standalone: HandleTunnelClose silently no-ops on unknown tunnels.
		return nil
	})
}

// =============================================================================
// sharedRelaySink — relay's upstream view of the shared runtime.
// =============================================================================

// sharedRelaySink is the bridge between the relay surface and the shared
// gateway runtime. The relay sees a per-session AetherGatewayClient (via
// dial); under the hood, every accepted sandbox session is funnelled through
// the same gateway connection that the terminator surface uses.
//
// Only one sandbox session is active at a time in this configuration: the
// runtime owns one gateway lock under one identity, so two parallel sandbox
// sessions would each try to drive the same upstream stream.
type sharedRelaySink struct {
	runtime *gatewayRuntime

	mu            sync.Mutex
	activeSession *sharedRuntimeSession
}

func newSharedRelaySink(runtime *gatewayRuntime) *sharedRelaySink {
	return &sharedRelaySink{runtime: runtime}
}

// dial is the relay's upstreamDialer in the shared-runtime configuration.
// Each invocation returns a new fake AetherGatewayClient whose Connect()
// returns a session-scoped stream wired to the shared runtime's send queue
// and a per-session inbox of downstream envelopes selected by the
// downstreamRouter.
func (s *sharedRelaySink) dial(_ context.Context) (pb.AetherGatewayClient, func() error, error) {
	return &sharedRuntimeClient{owner: s}, func() error { return nil }, nil
}

// routeMessage enqueues msg on the active relay session's inbox. With no
// active session, the message is dropped (no sandbox cares about it) and a
// debug line is emitted so operators can spot stray traffic.
func (s *sharedRelaySink) routeMessage(msg *pb.DownstreamMessage) {
	s.mu.Lock()
	sess := s.activeSession
	s.mu.Unlock()
	if sess == nil {
		log.Debug().
			Str("payload_type", fmt.Sprintf("%T", msg.GetPayload())).
			Msg("runner: dropping downstream envelope, no active relay session")
		return
	}
	sess.deliver(msg)
}

// attachSession registers a new active relay session. Returns false when
// another session is already attached so the caller can reject the
// concurrent open.
func (s *sharedRelaySink) attachSession(sess *sharedRuntimeSession) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeSession != nil {
		return false
	}
	s.activeSession = sess
	return true
}

// detachSession clears the active session pointer when sess matches the
// currently-attached session.
func (s *sharedRelaySink) detachSession(sess *sharedRuntimeSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeSession == sess {
		s.activeSession = nil
	}
}

// =============================================================================
// Fake AetherGatewayClient that the relay sees via the shared sink.
// =============================================================================

// sharedRuntimeClient implements pb.AetherGatewayClient by issuing a fake
// bidirectional stream backed by the shared runtime. One stream per sandbox
// session; in shared-runtime mode at most one session is active at a time.
type sharedRuntimeClient struct {
	owner *sharedRelaySink
}

// Connect returns a fake server-bound stream. The grpc.CallOption args are
// ignored; production callers don't pass any.
func (c *sharedRuntimeClient) Connect(ctx context.Context, _ ...grpc.CallOption) (pb.AetherGateway_ConnectClient, error) {
	sessCtx, cancel := context.WithCancel(ctx)
	sess := &sharedRuntimeSession{
		owner:  c.owner,
		ctx:    sessCtx,
		cancel: cancel,
		inbox:  make(chan *pb.DownstreamMessage, 64),
	}
	if !c.owner.attachSession(sess) {
		cancel()
		return nil, fmt.Errorf("runner: relay session already attached (one sandbox per sidecar)")
	}
	// Synthesise a ConnectionAck so the sandbox sees the same wire shape it
	// would in standalone relay mode. The actual gateway-level ack landed on
	// the runtime's connection long before this session opened.
	sess.inbox <- &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ConnectionAck{
			ConnectionAck: &pb.ConnectionAck{SessionId: sess.syntheticSessionID()},
		},
	}
	return sess, nil
}

// =============================================================================
// sharedRuntimeSession — the AetherGateway_ConnectClient the relay drives.
// =============================================================================

// sharedRuntimeSession is the relay-side session view of the shared runtime.
// The relay treats it as an upstream client stream: Send writes envelopes
// upstream through the runtime's send queue (skipping the sandbox's Init
// which the runtime already owns), Recv blocks on the per-session inbox
// until sharedRelaySink.routeMessage enqueues a frame.
type sharedRuntimeSession struct {
	owner  *sharedRelaySink
	ctx    context.Context
	cancel context.CancelFunc
	inbox  chan *pb.DownstreamMessage

	closeOnce sync.Once
	closed    atomic.Bool

	// idCounter is bumped per-session (one session per sink at a time). The
	// synthetic session id distinguishes restarts in logs.
	idCounter atomic.Uint64
}

// Send is invoked by the relay session pump for every envelope the sandbox
// emits. The first envelope is always Init (relay.Connect already rewrote
// it); we drop it because the runtime's BaseClient sent its own Init when it
// dialled the real gateway.
func (s *sharedRuntimeSession) Send(msg *pb.UpstreamMessage) error {
	if s.closed.Load() {
		return io.ErrClosedPipe
	}
	if _, ok := msg.GetPayload().(*pb.UpstreamMessage_Init); ok {
		log.Debug().Msg("runner: dropping relay-rewritten Init (runtime owns identity)")
		return nil
	}
	return s.owner.runtime.Client().Send(msg)
}

// Recv blocks until the next downstream envelope is delivered to this
// session's inbox or the session is closed.
func (s *sharedRuntimeSession) Recv() (*pb.DownstreamMessage, error) {
	select {
	case <-s.ctx.Done():
		return nil, io.EOF
	case msg, ok := <-s.inbox:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	}
}

// CloseSend tears the session down so subsequent Recv calls unblock.
func (s *sharedRuntimeSession) CloseSend() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		s.owner.detachSession(s)
	})
	return nil
}

// deliver enqueues msg onto the inbox. Drops with a warning when the inbox
// is full so a wedged sandbox can't permanently block the runtime's
// dispatcher goroutine.
func (s *sharedRuntimeSession) deliver(msg *pb.DownstreamMessage) {
	if s.closed.Load() {
		return
	}
	select {
	case s.inbox <- msg:
	default:
		log.Warn().
			Str("payload_type", fmt.Sprintf("%T", msg.GetPayload())).
			Msg("runner: relay session inbox full, dropping downstream envelope")
	}
}

// syntheticSessionID emits a label "shared-<n>" for the synthesised
// ConnectionAck. The runtime's real session id is not exposed on this
// surface.
func (s *sharedRuntimeSession) syntheticSessionID() string {
	return fmt.Sprintf("shared-%d", s.idCounter.Add(1))
}

// AetherGateway_ConnectClient interface boilerplate.
//
// gRPC generated code requires Header / Trailer / CloseSend / Context /
// SendMsg / RecvMsg on a stream client; most are not used by the relay's
// session pump but they have to compile.
func (s *sharedRuntimeSession) Header() (metadata.MD, error) { return nil, nil }
func (s *sharedRuntimeSession) Trailer() metadata.MD         { return nil }
func (s *sharedRuntimeSession) Context() context.Context     { return s.ctx }
func (s *sharedRuntimeSession) SendMsg(_ any) error {
	return errors.New("runner: SendMsg is not implemented on the shared-runtime stream")
}
func (s *sharedRuntimeSession) RecvMsg(_ any) error {
	return errors.New("runner: RecvMsg is not implemented on the shared-runtime stream")
}

// =============================================================================
// Helpers
// =============================================================================

// newTerminatorAttached constructs a Terminator that shares the supplied
// runtime and records cfgPath for SIGHUP reload. Internal helper for the
// runner; external callers use NewTerminator / NewTerminatorFromPath.
func newTerminatorAttached(cfg *Config, runtime *gatewayRuntime, cfgPath string) (*Terminator, error) {
	return newTerminatorInternal(cfg, cfgPath, runtime)
}

// tunnelDataIsOpen attempts to decode a TunnelData seq=0 frame as an embedded
// TunnelOpen. Returns nil iff the decode succeeds and the open carries a
// non-empty tunnel id; otherwise returns a non-nil error so the caller can
// treat the frame as plain data.
func tunnelDataIsOpen(frame *pb.TunnelData, into *pb.TunnelOpen) error {
	if frame.GetSeq() != 0 || len(frame.GetData()) == 0 {
		return errors.New("not an open frame")
	}
	if err := proto.Unmarshal(frame.GetData(), into); err != nil {
		return err
	}
	if into.GetTunnelId() == "" {
		return errors.New("empty tunnel_id")
	}
	return nil
}
