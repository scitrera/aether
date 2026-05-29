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
	"time"

	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/sdk/go/aether"
	"github.com/scitrera/go-backpressure"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// sharedRuntimeSessionDeliverCapacity bounds the concurrent in-flight
// downstream envelopes a single relay session can have queued toward its
// in-sandbox SDK reader. Sized at 8 to match the per-session inbox cap
// historically used here (64) divided down to a value that gives CoDel
// room to schedule across priorities. The Semaphore itself does the
// queue-length management via its CoDel queues; the staging chan below
// only buffers admitted envelopes for the Recv side.
const sharedRuntimeSessionDeliverCapacity = 8

// sharedRuntimeSessionDeliverShortTimeout / LongTimeout configure the
// CoDel target/interval for the per-session admission queue. 50ms target
// matches the SDK's BaseClient backpressure target; 100ms interval gives
// enough sample window to differentiate persistent overload from a brief
// burst.
const (
	sharedRuntimeSessionDeliverShortTimeout = 50 * time.Millisecond
	sharedRuntimeSessionDeliverLongTimeout  = 100 * time.Millisecond
)

// sharedRuntimeSessionDeliverAcquireTimeout caps how long deliver() will
// block trying to acquire a delivery-Semaphore token. Sized small so a
// wedged in-sandbox reader can't permanently block the runtime's
// downstream dispatcher: on shed, deliver synthesizes a BACKPRESSURE
// error frame into the inbox so the in-sandbox SDK observes the failure.
const sharedRuntimeSessionDeliverAcquireTimeout = 30 * time.Second

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
			sink := newSharedRelaySink(r.runtime, cfg.Relay.MaxSessions)
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
	}

	log.Info().
		Str("gateway", r.cfg.Gateway.Address).
		Strs("surfaces", r.cfg.EnabledSurfaces()).
		Str("identity", r.cfg.Service.Implementation+"/"+r.cfg.Service.Specifier).
		Msg("proxy sidecar runner starting")

	g, gctx := errgroup.WithContext(ctx)

	// The shared gateway connection runs as a surface in the errgroup: a fatal
	// give-up (too many consecutive terminal auth failures) cancels gctx,
	// drains the other surfaces, and propagates out of Run so the process
	// exits non-zero / signals its wrapped child — making an orphaned sandbox
	// reapable instead of a zombie spamming the gateway.
	if r.runtime != nil {
		g.Go(func() error {
			return r.runtime.runConnectionLoop(gctx)
		})
	}

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
	log.Info().
		Str("path", r.cfgPath).
		Strs("surfaces", r.cfg.EnabledSurfaces()).
		Msg("proxy sidecar: config reload requested")
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

	// Plain peer messages: forward to attached relay sessions so Python
	// AsyncServiceClient instances connected via the local relay socket
	// receive peer messages addressed to this implementation. Pre-relay,
	// the proxy-sidecar runner was a pure HTTP-RPC terminator and the
	// OnMessage path was a no-op; the workclaw chat envelope flow
	// (gateway → sv::sandbox-sidecar::<id> → Python bridge OnMessage)
	// requires this relay path. Wraps the SDK-level Message into the
	// same DownstreamMessage_Msg envelope the gateway uses for normal
	// subscription delivery (gateway/subscription.go:228) so the relay
	// session's downstream stream is wire-compatible.
	client.OnMessage(func(_ context.Context, msg *aether.Message) error {
		log.Debug().
			Str("source", msg.SourceTopic).
			Int("payload_bytes", len(msg.Payload)).
			Msg("runner: received message via OnMessage path")
		if r.relay == nil {
			return nil
		}
		r.relay.routeMessage(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Msg{
				Msg: &pb.IncomingMessage{
					SourceTopic: msg.SourceTopic,
					Payload:     msg.Payload,
					MessageType: msg.MessageType,
				},
			},
		})
		return nil
	})

	// Inbound HTTP request: terminator handles. Relay never receives this
	// (the gateway never publishes ProxyHttpRequest as a *response* to an
	// outbound caller).
	//
	// Chunked-request registration runs synchronously on the receive loop
	// so beginChunkedRequest's accumulator entry is committed before the
	// first OnProxyHttpBodyChunk frame (also synchronous) tries to look it
	// up — otherwise the chunk handler races the registration goroutine
	// and silently drops frames.
	//
	// Non-chunked dispatch is spawned on its own goroutine via the same
	// 3-minute-ceiling pattern as aether.Async: a streaming response
	// (stream_response_indefinitely=true) would otherwise hold the SDK's
	// single-goroutine receiveLoop for its entire lifetime, starving
	// every other envelope multiplexed onto the same shared runtime —
	// loopback ProxyHttpResponses for relay-mediated fast calls,
	// follow-on stream ProxyHttpRequests, etc.
	client.OnProxyHttpRequest(func(reqCtx context.Context, req *pb.ProxyHttpRequest) error {
		if log.Debug().Enabled() {
			log.Debug().
				Str("dir", "terminator-in").
				Str("op", "ProxyHttpRequest").
				Str("request_id", req.GetRequestId()).
				Str("method", req.GetMethod()).
				Str("path", req.GetPath()).
				Str("backend", req.GetBackendName()).
				Int("body_bytes", len(req.GetBody())).
				Bool("body_chunked", req.GetBodyChunked()).
				Bool("stream_indef", req.GetStreamResponseIndefinitely()).
				Msg("terminator: envelope")
		}
		if req.GetBodyChunked() {
			return r.term.beginChunkedRequest(req, transport)
		}
		go runInlineProxyHttpDispatch(r.term, req, transport)
		return nil
	})

	// ProxyHttpResponse: only fires here when the SDK's caller-side
	// inflight resolver missed — i.e. the response is for a request the
	// relay forwarded on someone else's behalf, OR a peer-end
	// notification from the gateway addressed at the terminator's own
	// in-flight dispatch (H3 wakeup). Try the terminator's active
	// dispatch table first: an error envelope keyed by a known dispatch
	// requestID is a "your caller went away" wakeup and should cancel
	// the dispatch immediately. Otherwise route to the relay sink so
	// the originating sandbox session sees it on its inbox.
	client.OnProxyHttpResponse(func(_ context.Context, resp *pb.ProxyHttpResponse) error {
		if resp != nil && resp.GetError() != nil && r.term != nil {
			if v, ok := r.term.activeDispatches.LoadAndDelete(resp.GetRequestId()); ok {
				if cancel, ok := v.(context.CancelFunc); ok {
					cancel()
				}
				return nil
			}
		}
		if r.relay != nil {
			r.relay.routeMessage(&pb.DownstreamMessage{
				Payload: &pb.DownstreamMessage_ProxyHttpResponse{ProxyHttpResponse: resp},
			})
		}
		return nil
	})

	// ProxyHttpBodyChunk: is_request=true → terminator (chunked inbound
	// body); is_request=false → relay (chunked response to a sandbox-issued
	// outbound request) when relay is co-enabled, otherwise drop.
	client.OnProxyHttpBodyChunk(func(chunkCtx context.Context, chunk *pb.ProxyHttpBodyChunk) error {
		if log.Debug().Enabled() && chunk.GetIsRequest() {
			log.Debug().
				Str("dir", "terminator-in").
				Str("op", "ProxyHttpBodyChunk").
				Str("request_id", chunk.GetRequestId()).
				Uint32("seq", chunk.GetSeq()).
				Bool("fin", chunk.GetFin()).
				Int("data_bytes", len(chunk.GetData())).
				Msg("terminator: envelope")
		}
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
	//
	// TunnelOpen dispatch goes through a goroutine because HandleTunnelOpen
	// dials the upstream backend (TCP/WS/UDP) and that dial is slow on
	// unreachable or backpressured hosts. Without the hand-off, a single
	// slow dial would wedge the receive loop and starve every other surface
	// multiplexed onto this connection (loopback ProxyHttpResponses for
	// relay-mediated fast calls, follow-on ProxyHttpRequests, peer data
	// frames for other already-open tunnels). Data frames for an already-
	// open tunnel stay sync because they route into the tunnel's existing
	// per-connection goroutine where the tunnel manager owns ordering.
	client.OnTunnelDataIn(func(dataCtx context.Context, frame *pb.TunnelData) error {
		if frame.GetSeq() == 0 && len(frame.GetData()) > 0 {
			open := &pb.TunnelOpen{}
			if err := tunnelDataIsOpen(frame, open); err == nil {
				// Reserve a pendingTunnel placeholder synchronously on the
				// receive loop so any follow-on TunnelData/Ack/Close frame
				// that arrives before the dial goroutine completes is
				// buffered in arrival order instead of dropped. The dial
				// itself goes to a goroutine (HandleTunnelOpen may block
				// on slow upstreams). When dial completes, register()
				// activates the placeholder and flushes buffered frames.
				if r.term.tunnels.reserve(open.GetTunnelId()) == nil {
					// Duplicate tunnel_id — reject synchronously.
					_ = transport.SendTunnelClose(&pb.TunnelClose{
						TunnelId: open.GetTunnelId(),
						Reason:   pb.TunnelClose_ERROR,
						Detail:   "duplicate tunnel_id",
					})
					return nil
				}
				go runTunnelOpenDispatch(r.term, open, transport)
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

	// Raw downstream tap: route correlated responses for relay-issued KV /
	// task / workspace / checkpoint ops back to the originating session. The
	// shared runtime forwards these upstream verbatim (sharedRuntimeSession.
	// Send), so the gateway's response lands here with no pending correlation
	// in this Go client — without the tap it would be dropped and the
	// in-sandbox caller would stall until its client-side timeout (the 5s
	// per-turn lag the workclaw bridge saw on kv_put). Errors that correlate
	// to a relay request (DownstreamMessage_Error with a request_id) route
	// the same way so a failed op surfaces promptly instead of timing out.
	if r.relay != nil {
		relay := r.relay
		client.SetRawDownstreamTap(func(msg *pb.DownstreamMessage) bool {
			rid := downstreamResponseRequestID(msg)
			if rid == "" {
				return false
			}
			return relay.routeResponseToOwner(rid, msg)
		})
	}
}

// downstreamResponseRequestID returns the correlation request_id carried by a
// response-class downstream message, or "" for message types that are not
// correlated responses (or carry no request_id). Used by the rawDownstreamTap
// to decide whether a message might belong to a relay session. The set mirrors
// the request-class ops registered in sharedRuntimeSession.Send plus the
// generic Error envelope, which the gateway stamps with the originating
// request_id when an op fails.
func downstreamResponseRequestID(msg *pb.DownstreamMessage) string {
	switch p := msg.GetPayload().(type) {
	case *pb.DownstreamMessage_Kv:
		return p.Kv.GetRequestId()
	case *pb.DownstreamMessage_TaskOp:
		return p.TaskOp.GetRequestId()
	case *pb.DownstreamMessage_TaskQuery:
		return p.TaskQuery.GetRequestId()
	case *pb.DownstreamMessage_Checkpoint:
		return p.Checkpoint.GetRequestId()
	case *pb.DownstreamMessage_Workspace:
		return p.Workspace.GetRequestId()
	case *pb.DownstreamMessage_Error:
		return p.Error.GetRequestId()
	default:
		return ""
	}
}

// =============================================================================
// sharedRelaySink — relay's upstream view of the shared runtime.
// =============================================================================

// sharedRelaySink is the bridge between the relay surface and the shared
// gateway runtime. The relay sees a per-session AetherGatewayClient (via
// dial); under the hood, every accepted sandbox session is funnelled through
// the same gateway connection that the terminator surface uses.
//
// Up to MaxSessions sandbox sessions can be attached concurrently. Their
// upstream envelopes all go through the runtime's single send queue (gRPC
// streams already multiplex). Downstream envelopes are routed back to the
// originating session via per-request_id and per-tunnel_id tables populated
// when the session emits the corresponding ProxyHttpRequest or TunnelOpen.
// Envelopes without a routable id (signals, config) are broadcast to all
// sessions.
type sharedRelaySink struct {
	runtime     *gatewayRuntime
	maxSessions int

	mu             sync.Mutex
	activeSessions map[*sharedRuntimeSession]struct{}
	// requestRoutes maps a ProxyHttpRequest's request_id to the session
	// that originated it. Populated on Send(ProxyHttpRequest), consulted
	// on incoming ProxyHttpResponse / ProxyHttpBodyChunk(!is_request),
	// released on the terminal frame (non-chunked response, or fin chunk).
	requestRoutes map[string]*sharedRuntimeSession
	// tunnelRoutes maps a TunnelOpen's tunnel_id to the originating
	// session. Released on TunnelClose or on session detach.
	tunnelRoutes map[string]*sharedRuntimeSession
}

func newSharedRelaySink(runtime *gatewayRuntime, maxSessions int) *sharedRelaySink {
	if maxSessions < 1 {
		// Validation in config.go enforces this, but guard direct
		// test callers that may pass 0.
		maxSessions = 1
	}
	return &sharedRelaySink{
		runtime:        runtime,
		maxSessions:    maxSessions,
		activeSessions: make(map[*sharedRuntimeSession]struct{}),
		requestRoutes:  make(map[string]*sharedRuntimeSession),
		tunnelRoutes:   make(map[string]*sharedRuntimeSession),
	}
}

// dial is the relay's upstreamDialer in the shared-runtime configuration.
// Each invocation returns a new fake AetherGatewayClient whose Connect()
// returns a session-scoped stream wired to the shared runtime's send queue
// and a per-session inbox of downstream envelopes selected by the
// downstreamRouter.
func (s *sharedRelaySink) dial(_ context.Context) (pb.AetherGatewayClient, func() error, error) {
	return &sharedRuntimeClient{owner: s}, func() error { return nil }, nil
}

// routeMessage delivers msg to whichever session owns the request_id or
// tunnel_id it references. Envelopes without a routable id (signals,
// config, errors without request_id) broadcast to every active session.
// On terminal frames (non-chunked ProxyHttpResponse, fin ProxyHttpBodyChunk,
// TunnelClose) the corresponding route entry is released so the table
// doesn't grow unbounded.
func (s *sharedRelaySink) routeMessage(msg *pb.DownstreamMessage) {
	var (
		sess     *sharedRuntimeSession
		fanout   []*sharedRuntimeSession
		category string
	)

	s.mu.Lock()
	switch p := msg.GetPayload().(type) {
	case *pb.DownstreamMessage_ProxyHttpResponse:
		rid := p.ProxyHttpResponse.GetRequestId()
		sess = s.requestRoutes[rid]
		// Non-chunked response carries the body inline and ends the
		// exchange; chunked responses stay routed until a body chunk
		// arrives with fin=true.
		if !p.ProxyHttpResponse.GetBodyChunked() {
			delete(s.requestRoutes, rid)
		}
		category = "request_id"
	case *pb.DownstreamMessage_ProxyHttpBodyChunk:
		rid := p.ProxyHttpBodyChunk.GetRequestId()
		sess = s.requestRoutes[rid]
		if p.ProxyHttpBodyChunk.GetFin() {
			delete(s.requestRoutes, rid)
		}
		category = "request_id"
	case *pb.DownstreamMessage_TunnelData:
		sess = s.tunnelRoutes[p.TunnelData.GetTunnelId()]
		category = "tunnel_id"
	case *pb.DownstreamMessage_TunnelAck:
		sess = s.tunnelRoutes[p.TunnelAck.GetTunnelId()]
		category = "tunnel_id"
	case *pb.DownstreamMessage_TunnelClose:
		tid := p.TunnelClose.GetTunnelId()
		sess = s.tunnelRoutes[tid]
		delete(s.tunnelRoutes, tid)
		category = "tunnel_id"
	default:
		// Session-level envelopes (Signal/Config/ConnectionAck/...) —
		// broadcast so every attached session sees things like
		// FORCE_DISCONNECT.
		fanout = make([]*sharedRuntimeSession, 0, len(s.activeSessions))
		for ss := range s.activeSessions {
			fanout = append(fanout, ss)
		}
	}
	s.mu.Unlock()

	if len(fanout) > 0 {
		for _, ss := range fanout {
			ss.deliver(msg)
		}
		return
	}
	if sess == nil {
		log.Debug().
			Str("payload_type", fmt.Sprintf("%T", msg.GetPayload())).
			Str("route_key", category).
			Msg("runner: dropping downstream envelope, no matching session")
		return
	}
	sess.deliver(msg)
}

// attachSession registers sess as an active relay session, capped at
// MaxSessions. A false return tells the caller to reject the open so the
// sandbox SDK's auto_reconnect surfaces a real error rather than
// spinning (the storm we saw with the old single-slot design).
func (s *sharedRelaySink) attachSession(sess *sharedRuntimeSession) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.activeSessions) >= s.maxSessions {
		return false
	}
	s.activeSessions[sess] = struct{}{}
	return true
}

// detachSession removes the session from the active set and drops any
// in-flight route entries it still owned. Without this cleanup a long-
// lived sidecar would accumulate stale request_id / tunnel_id entries
// pointing at freed sessions — deliver() no-ops on a closed session, but
// the maps would grow unbounded.
func (s *sharedRelaySink) detachSession(sess *sharedRuntimeSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.activeSessions, sess)
	for rid, owner := range s.requestRoutes {
		if owner == sess {
			delete(s.requestRoutes, rid)
		}
	}
	for tid, owner := range s.tunnelRoutes {
		if owner == sess {
			delete(s.tunnelRoutes, tid)
		}
	}
}

// registerRequest claims request_id for sess so downstream
// ProxyHttpResponse / ProxyHttpBodyChunk frames for that id route back to
// it. Called from sharedRuntimeSession.Send on the originating envelope.
func (s *sharedRelaySink) registerRequest(sess *sharedRuntimeSession, requestID string) {
	if requestID == "" {
		return
	}
	s.mu.Lock()
	s.requestRoutes[requestID] = sess
	s.mu.Unlock()
}

// registerTunnel mirrors registerRequest for TunnelOpen-initiated tunnels.
func (s *sharedRelaySink) registerTunnel(sess *sharedRuntimeSession, tunnelID string) {
	if tunnelID == "" {
		return
	}
	s.mu.Lock()
	s.tunnelRoutes[tunnelID] = sess
	s.mu.Unlock()
}

// routeResponseToOwner delivers a correlated response-class downstream
// message to the relay session that issued the matching request, keyed by
// request_id. Returns true when a session owned the id (message delivered,
// route released so the table stays bounded); false when no relay session
// owns it — in which case the caller must let the SDK handle the message
// normally (e.g. a response to an op the terminator surface issued itself).
func (s *sharedRelaySink) routeResponseToOwner(requestID string, msg *pb.DownstreamMessage) bool {
	if requestID == "" {
		return false
	}
	s.mu.Lock()
	sess := s.requestRoutes[requestID]
	if sess != nil {
		delete(s.requestRoutes, requestID)
	}
	s.mu.Unlock()
	if sess == nil {
		return false
	}
	sess.deliver(msg)
	return true
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
		deliverSem: backpressure.NewSemaphore(
			5, // 5 priorities (PriorityControl..PriorityBestEffort)
			sharedRuntimeSessionDeliverCapacity,
			backpressure.SemaphoreShortTimeout(sharedRuntimeSessionDeliverShortTimeout),
			backpressure.SemaphoreLongTimeout(sharedRuntimeSessionDeliverLongTimeout),
		),
	}
	if !c.owner.attachSession(sess) {
		cancel()
		sess.deliverSem.Close()
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

	// deliverSem gates downstream envelope admission onto the inbox with
	// priority-aware CoDel shedding. Without this, a wedged in-sandbox SDK
	// reader could fill the inbox and the runtime's downstream dispatcher
	// would either block (stalling every other surface) or drop blindly
	// (corrupting whichever envelope happens to arrive next, regardless of
	// importance).
	deliverSem *backpressure.Semaphore

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
//
// For outbound proxy/tunnel envelopes we also record session-ownership of
// the request_id / tunnel_id on the sink's routing tables so the matching
// downstream response (which arrives on the runtime's single gateway
// connection, shared across N sessions) finds its way back to THIS
// session's inbox rather than getting broadcast or dropped.
func (s *sharedRuntimeSession) Send(msg *pb.UpstreamMessage) error {
	if s.closed.Load() {
		return io.ErrClosedPipe
	}
	switch p := msg.GetPayload().(type) {
	case *pb.UpstreamMessage_Init:
		log.Debug().Msg("runner: dropping relay-rewritten Init (runtime owns identity)")
		return nil
	case *pb.UpstreamMessage_ProxyHttpRequest:
		s.owner.registerRequest(s, p.ProxyHttpRequest.GetRequestId())
	case *pb.UpstreamMessage_TunnelOpen:
		s.owner.registerTunnel(s, p.TunnelOpen.GetTunnelId())
	// Correlated request/response ops the in-sandbox SDK awaits. The shared
	// runtime forwards these upstream verbatim, so the gateway's response
	// arrives on the runtime connection with no pending correlation here —
	// register the request_id so the rawDownstreamTap can route the matching
	// response back to THIS session instead of letting it get dropped (which
	// stalls the in-sandbox caller until its client-side timeout).
	case *pb.UpstreamMessage_KvOp:
		s.owner.registerRequest(s, p.KvOp.GetRequestId())
	case *pb.UpstreamMessage_TaskOp:
		s.owner.registerRequest(s, p.TaskOp.GetRequestId())
	case *pb.UpstreamMessage_TaskQuery:
		s.owner.registerRequest(s, p.TaskQuery.GetRequestId())
	case *pb.UpstreamMessage_CheckpointOp:
		s.owner.registerRequest(s, p.CheckpointOp.GetRequestId())
	case *pb.UpstreamMessage_WorkspaceOp:
		s.owner.registerRequest(s, p.WorkspaceOp.GetRequestId())
	}
	prio := priorityForSharedRelayUpstream(msg)
	// SendWithPriority feeds the SDK's CoDel-managed admission queue.
	// Relay-mediated envelopes share the runtime's upstream send path
	// with terminator chunked-response writes and sidecar admin traffic;
	// per-envelope priority lets the SDK shed bulk best-effort traffic
	// first when persistent latency exceeds the CoDel target, rather
	// than letting every blocked send fail at the 30s deadline at once.
	ctx, cancel := context.WithTimeout(s.ctx, sendUpstreamTimeout)
	defer cancel()
	return s.owner.runtime.Client().SendWithPriority(ctx, prio, msg)
}

// priorityForSharedRelayUpstream classifies an UpstreamMessage emitted by a
// relay-mediated sandbox session. The mapping intentionally mirrors
// tunnel_transport.go's terminator-side classification so the SDK's
// admission queue treats both surfaces consistently.
//
// Extracted as a free function so the test in runner_test.go can pin the
// per-payload-type mapping without spinning up a full session.
func priorityForSharedRelayUpstream(msg *pb.UpstreamMessage) backpressure.Priority {
	switch p := msg.GetPayload().(type) {
	case *pb.UpstreamMessage_ProxyHttpRequest:
		return aether.PriorityRequest
	case *pb.UpstreamMessage_ProxyHttpBodyChunk:
		// Relay sessions typically only emit request-direction chunks
		// (sandbox sending a chunked body to a downstream HTTP target).
		// Response-direction chunks are emitted by the terminator
		// surface, not the relay — defensive case for unexpected
		// envelopes that shouldn't occur in practice.
		if p.ProxyHttpBodyChunk.GetIsRequest() {
			return aether.PriorityRequest
		}
		return aether.PriorityResponseChunk
	case *pb.UpstreamMessage_TunnelOpen:
		return aether.PriorityRequest
	case *pb.UpstreamMessage_TunnelData:
		return aether.PriorityResponseChunk
	case *pb.UpstreamMessage_TunnelClose:
		return aether.PriorityControl
	case *pb.UpstreamMessage_TunnelAck:
		return aether.PriorityResponseHeader
	case *pb.UpstreamMessage_Send,
		*pb.UpstreamMessage_Progress,
		*pb.UpstreamMessage_KvOp,
		*pb.UpstreamMessage_SubmitAuditEvent:
		return aether.PriorityBestEffort
	default:
		// Unknown / new payload types default to PriorityRequest:
		// safer than best-effort (won't be shed first) but doesn't
		// claim a higher priority than the request payload type
		// without an explicit classification.
		return aether.PriorityRequest
	}
}

// priorityForSharedRelayDownstream classifies a DownstreamMessage destined for
// the in-sandbox SDK reader on this session. Used by deliver() to gate
// admission via the session's deliverSem; on shed, deliver synthesizes a
// BACKPRESSURE error frame into the inbox so the in-sandbox SDK observes the
// failure (instead of silent drop).
func priorityForSharedRelayDownstream(msg *pb.DownstreamMessage) backpressure.Priority {
	switch p := msg.GetPayload().(type) {
	case *pb.DownstreamMessage_Error:
		return aether.PriorityControl
	case *pb.DownstreamMessage_ProxyHttpResponse:
		return aether.PriorityResponseHeader
	case *pb.DownstreamMessage_ProxyHttpRequest:
		// Relay's perspective: the gateway is asking the in-sandbox
		// SDK to honor an inbound HTTP request. Caller has a deadline.
		return aether.PriorityRequest
	case *pb.DownstreamMessage_ProxyHttpBodyChunk:
		if p.ProxyHttpBodyChunk.GetIsRequest() {
			return aether.PriorityRequest
		}
		return aether.PriorityResponseChunk
	case *pb.DownstreamMessage_TunnelData:
		return aether.PriorityResponseChunk
	case *pb.DownstreamMessage_TunnelClose:
		return aether.PriorityControl
	case *pb.DownstreamMessage_TunnelAck:
		return aether.PriorityResponseHeader
	case *pb.DownstreamMessage_ConnectionAck,
		*pb.DownstreamMessage_Signal,
		*pb.DownstreamMessage_Config:
		return aether.PriorityControl
	case *pb.DownstreamMessage_ProgressUpdate,
		*pb.DownstreamMessage_Msg,
		*pb.DownstreamMessage_Kv:
		// Progress/best-effort delivery: callers don't block on these
		// and lost events are recoverable from the next update / KV
		// read. First class to be shed under sustained inbox pressure.
		return aether.PriorityBestEffort
	default:
		return aether.PriorityRequest
	}
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
// Releases the deliverSem's background reaper so the session doesn't leak
// goroutines under churn (sandbox reconnect storms, idle session reaping).
func (s *sharedRuntimeSession) CloseSend() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		s.owner.detachSession(s)
		if s.deliverSem != nil {
			s.deliverSem.Close()
		}
	})
	return nil
}

// deliver admits msg onto the inbox via the session's priority-aware
// deliverSem. On admission the envelope is forwarded; on CoDel-driven shed
// or acquire-timeout the original envelope is dropped and a BACKPRESSURE
// DownstreamMessage_Error frame is synthesized in its place so the
// in-sandbox SDK observes a clean signal instead of a silent drop.
//
// The deliverSem keeps a wedged in-sandbox reader from permanently blocking
// the runtime's downstream dispatcher: bounded acquire timeout (30s) +
// non-blocking error-frame enqueue means deliver always returns promptly.
func (s *sharedRuntimeSession) deliver(msg *pb.DownstreamMessage) {
	if s.closed.Load() {
		return
	}
	if s.deliverSem == nil {
		// Defensive: sessions constructed via the production Connect path
		// always wire a Semaphore. Older callers (or tests bypassing
		// Connect) fall back to the legacy drop-on-full behaviour so
		// behaviour stays predictable.
		select {
		case s.inbox <- msg:
		default:
			log.Warn().
				Str("payload_type", fmt.Sprintf("%T", msg.GetPayload())).
				Msg("runner: relay session inbox full (no semaphore), dropping downstream envelope")
		}
		return
	}

	prio := priorityForSharedRelayDownstream(msg)
	acqCtx, cancel := context.WithTimeout(s.ctx, sharedRuntimeSessionDeliverAcquireTimeout)
	defer cancel()
	if err := s.deliverSem.Acquire(acqCtx, prio, 1); err != nil {
		// CoDel-shed or ctx deadline.
		s.emitDeliverBackpressureNotice(msg, prio, err)
		return
	}
	// Non-blocking enqueue with immediate token release. Holding the
	// Semaphore token while waiting on a blocked inbox push (the previous
	// shape) caused tokens to wedge whenever the relay consumer fell
	// behind: held tokens exhausted the Semaphore, every subsequent
	// Acquire CoDel-shed, and the synthesised BACKPRESSURE notice then
	// hit the same full inbox and got silently dropped. Releasing the
	// token immediately after the push attempt — success or fail —
	// keeps the Semaphore healthy and gives CoDel a clean dwell-time
	// signal driven by Acquire contention rather than chan back-pressure.
	pushed := false
	select {
	case s.inbox <- msg:
		pushed = true
	default:
		// Inbox full despite admission. Treat as shed.
	}
	s.deliverSem.Release(1)
	if !pushed {
		s.emitDeliverBackpressureNotice(msg, prio, nil)
	}
}

// emitDeliverBackpressureNotice synthesises a BACKPRESSURE error frame
// into the inbox in place of a shed envelope so the in-sandbox SDK sees
// a clean failure on its next Recv. cause is non-nil when the shed came
// from Acquire (CoDel or acquire-deadline) and nil when the inbox push
// failed after admission. Best-effort non-blocking push — if the inbox
// is itself wedged, both the original envelope and the notice are
// dropped and we warn so operators see the cascade.
func (s *sharedRuntimeSession) emitDeliverBackpressureNotice(orig *pb.DownstreamMessage, prio backpressure.Priority, cause error) {
	if log.Debug().Enabled() {
		evt := log.Debug().
			Str("payload_type", fmt.Sprintf("%T", orig.GetPayload())).
			Int("priority", int(prio))
		if cause != nil {
			evt = evt.Err(cause).Str("trigger", "acquire-shed")
		} else {
			evt = evt.Str("trigger", "inbox-full")
		}
		evt.Msg("runner: relay deliver shed, synthesising BACKPRESSURE error")
	}
	notice := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Error{
			Error: &pb.ErrorResponse{
				Code:    "BACKPRESSURE",
				Message: "relay session inbox shed by backpressure; reduce send rate or process messages faster",
			},
		},
	}
	select {
	case s.inbox <- notice:
	default:
		log.Warn().
			Str("payload_type", fmt.Sprintf("%T", orig.GetPayload())).
			Msg("runner: relay session inbox full, dropping both envelope and BACKPRESSURE notice")
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
