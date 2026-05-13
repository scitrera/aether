package proxysidecar

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/identityheaders"
	"github.com/scitrera/aether/sdk/go/aether"
	"google.golang.org/protobuf/proto"
)

// proxyResponseChunkSize is the maximum body size emitted inline in the
// header ProxyHttpResponse frame. Larger bodies are streamed as
// ProxyHttpBodyChunk frames. Mirrors the SDK's caller-side proxyChunkSize so
// the round-trip is symmetric.
const proxyResponseChunkSize = 256 * 1024

// pendingChunkedRequest accumulates ProxyHttpBodyChunk frames for a single
// in-flight chunked-request. It is keyed by request_id in the terminator's
// pending map and torn down when fin arrives or the request hits the backend
// max body cap.
type pendingChunkedRequest struct {
	req       *pb.ProxyHttpRequest
	body      []byte
	maxBody   int64
	transport tunnelTransport
}

// Terminator runs the sidecar in terminator mode. It owns one ServiceClient
// connection to the gateway (via gatewayRuntime) and dispatches inbound
// proxy envelopes to the configured backends.
type Terminator struct {
	cfg         *Config
	cfgPath     string // path from which cfg was loaded; empty if loaded from defaults
	runtime     *gatewayRuntime
	backends    []*httpBackend
	tcpBackends []*tcpBackend
	wsBackends  []*wsBackend
	udpBackends []*udpBackend
	tunnels     *tunnelManager

	// backendMu guards backends, tcpBackends, wsBackends, udpBackends.
	// selectBackend and its siblings take a read lock; Reload takes a write lock.
	backendMu sync.RWMutex

	// reloadMu ensures at most one reload runs at a time. A second SIGHUP
	// while a reload is in progress is silently dropped.
	reloadMu sync.Mutex

	// pending tracks chunked-request bodies currently being assembled,
	// keyed by request_id. Entries are removed on fin, on dispatch failure,
	// or when the accumulated size exceeds the matching backend's
	// MaxBodyBytes.
	pendingMu sync.Mutex
	pending   map[string]*pendingChunkedRequest

	// resolver is reserved for future OBO authority resolution from a
	// remote service. Set via WithAuthorityResolver; nil in v1.
	resolver identityheaders.AuthorityResolver
}

// WithAuthorityResolver attaches an OBO authority resolver. Useful for tests
// and a future v2 mode where the sidecar talks to a remote authority store.
func (t *Terminator) WithAuthorityResolver(r identityheaders.AuthorityResolver) {
	t.resolver = r
	for _, b := range t.backends {
		b.resolver = r
	}
}

// NewTerminator constructs a terminator from the given config. The gateway
// connection is not opened until Run is invoked. Use NewTerminatorFromPath
// when you need SIGHUP reload support.
func NewTerminator(cfg *Config) (*Terminator, error) {
	return NewTerminatorFromPath(cfg, "")
}

// NewTerminatorFromPath is like NewTerminator but also records cfgPath so that
// Reload() knows where to re-read the configuration file.
func NewTerminatorFromPath(cfg *Config, cfgPath string) (*Terminator, error) {
	return newTerminatorInternal(cfg, cfgPath, newGatewayRuntime(cfg))
}

func newTerminatorInternal(cfg *Config, cfgPath string, runtime *gatewayRuntime) (*Terminator, error) {
	httpBackends, tcpBackends, wsBackends, udpBackends := buildBackends(cfg)

	t := &Terminator{
		cfg:         cfg,
		cfgPath:     cfgPath,
		runtime:     runtime,
		backends:    httpBackends,
		tcpBackends: tcpBackends,
		wsBackends:  wsBackends,
		udpBackends: udpBackends,
		tunnels:     newTunnelManager(),
		pending:     make(map[string]*pendingChunkedRequest),
	}
	return t, nil
}

// buildBackends constructs backend slices from cfg.Terminator.Backends.
func buildBackends(cfg *Config) ([]*httpBackend, []*tcpBackend, []*wsBackend, []*udpBackend) {
	httpBackends := make([]*httpBackend, 0)
	tcpBackends := make([]*tcpBackend, 0)
	wsBackends := make([]*wsBackend, 0)
	udpBackends := make([]*udpBackend, 0)
	for _, bcfg := range cfg.Terminator.Backends {
		switch bcfg.Kind {
		case BackendKindHTTP:
			httpBackends = append(httpBackends, newHTTPBackend(bcfg, cfg.TenantID, nil))
		case BackendKindTCP:
			tcpBackends = append(tcpBackends, newTCPBackend(bcfg, nil))
		case BackendKindWS:
			wsBackends = append(wsBackends, newWSBackend(bcfg, nil))
		case BackendKindUDP:
			udpBackends = append(udpBackends, newUDPBackend(bcfg, nil))
		}
	}
	return httpBackends, tcpBackends, wsBackends, udpBackends
}

// Run connects to the gateway and processes inbound envelopes until ctx is
// cancelled or a non-recoverable error occurs.
func (t *Terminator) Run(ctx context.Context) error {
	if err := t.runtime.init(); err != nil {
		return fmt.Errorf("terminator: build client: %w", err)
	}

	t.RegisterHandlers(t.runtime.Client(), t.runtime.Transport())

	go t.runtime.runConnectionLoop(ctx)

	log.Info().
		Str("gateway", t.cfg.Gateway.Address).
		Str("implementation", t.cfg.Service.Implementation).
		Str("specifier", t.cfg.Service.Specifier).
		Int("backends", len(t.backends)).
		Msg("proxy sidecar terminator running")

	<-ctx.Done()
	log.Info().Msg("proxy sidecar terminator shutting down")
	return nil
}

// beginChunkedRequest registers an accumulator for a chunked-request body.
// The terminator does not call the HTTP backend until fin arrives. Callers
// that already passed the parent ProxyHttpRequest envelope SHOULD NOT also
// pass body bytes inline — body_chunked=true means the body is streamed.
func (t *Terminator) beginChunkedRequest(req *pb.ProxyHttpRequest, transport tunnelTransport) error {
	if req == nil || req.GetRequestId() == "" {
		return fmt.Errorf("chunked request requires a non-empty request_id")
	}
	backend, perr := t.selectBackend(req)
	if perr != nil {
		// Reject up front; the caller doesn't need to ship any chunks.
		if transport != nil {
			_ = transport.SendProxyHttpResponse(errorResponse(req.GetRequestId(), perr.Kind, perr.Message))
		}
		return nil
	}
	t.pendingMu.Lock()
	t.pending[req.GetRequestId()] = &pendingChunkedRequest{
		req:       req,
		body:      make([]byte, 0, 64<<10),
		maxBody:   backend.cfg.MaxBodyBytes,
		transport: transport,
	}
	t.pendingMu.Unlock()
	return nil
}

// handleChunkedRequestFrame appends data to a pending chunked-request and, on
// fin, dispatches the assembled request to the matching backend. Oversize
// uploads are short-circuited with a PAYLOAD_TOO_LARGE response and the
// accumulator is freed; the terminator does NOT continue buffering past the
// cap.
func (t *Terminator) handleChunkedRequestFrame(ctx context.Context, chunk *pb.ProxyHttpBodyChunk, transport tunnelTransport) error {
	requestID := chunk.GetRequestId()
	if requestID == "" {
		return nil
	}
	t.pendingMu.Lock()
	pending, ok := t.pending[requestID]
	if !ok {
		t.pendingMu.Unlock()
		// Frame for a request we never accepted — drop quietly. The caller
		// will time out waiting for a header response.
		return nil
	}
	if int64(len(pending.body)+len(chunk.GetData())) > pending.maxBody {
		delete(t.pending, requestID)
		t.pendingMu.Unlock()
		if transport != nil {
			_ = transport.SendProxyHttpResponse(errorResponse(requestID, pb.ProxyError_PAYLOAD_TOO_LARGE,
				fmt.Sprintf("chunked request body exceeds backend limit %d", pending.maxBody)))
		}
		return nil
	}
	pending.body = append(pending.body, chunk.GetData()...)
	if !chunk.GetFin() {
		t.pendingMu.Unlock()
		return nil
	}
	delete(t.pending, requestID)
	t.pendingMu.Unlock()

	body := pending.body
	req := pending.req
	// Mark the request as having an inline body now that we've reassembled
	// it, so downstream backend dispatch sees a self-contained envelope.
	req.BodyChunked = false
	return t.dispatchAndRespond(ctx, req, body, transport)
}

// dispatchAndRespond runs the configured backend for a fully-assembled
// request and emits the response upstream. When the response body fits in
// proxyResponseChunkSize it is sent inline in a single ProxyHttpResponse;
// otherwise the header carries body_chunked=true and the body is streamed in
// ProxyHttpBodyChunk frames terminated by fin=true.
//
// When req.StreamResponseIndefinitely is true the bounded path is replaced
// by a streaming path: timeout_ms is used for time-to-first-byte only, body
// chunks are emitted as they arrive from the backend, and the stream closes
// on EOF / idle timeout / max-bytes overflow / caller-cancel.
func (t *Terminator) dispatchAndRespond(ctx context.Context, req *pb.ProxyHttpRequest, body []byte, transport tunnelTransport) error {
	if req.GetStreamResponseIndefinitely() {
		return t.dispatchStreamingAndRespond(ctx, req, body, transport)
	}
	resp, respBody := t.HandleProxyRequest(ctx, req, body)
	if resp == nil {
		return nil
	}
	if transport == nil {
		return nil
	}
	// Errors and small bodies travel inline.
	if resp.GetError() != nil || len(respBody) <= proxyResponseChunkSize {
		return transport.SendProxyHttpResponse(resp)
	}
	// Chunked response: header without body, then body chunks. Mutate
	// resp.Body to nil and set body_chunked=true so the caller's accumulator
	// engages.
	resp.Body = nil
	resp.BodyChunked = true
	if err := transport.SendProxyHttpResponse(resp); err != nil {
		return err
	}
	requestID := resp.GetRequestId()
	for seq, offset := uint32(0), 0; offset < len(respBody); seq++ {
		end := offset + proxyResponseChunkSize
		if end > len(respBody) {
			end = len(respBody)
		}
		fin := end == len(respBody)
		chunk := &pb.ProxyHttpBodyChunk{
			RequestId: requestID,
			IsRequest: false,
			Seq:       seq,
			Data:      respBody[offset:end],
			Fin:       fin,
		}
		if err := transport.SendProxyHttpBodyChunk(chunk); err != nil {
			return err
		}
		offset = end
	}
	return nil
}

// streamIdleTimeoutDefault is the idle-byte deadline applied to streaming
// responses when stream_idle_timeout_ms is unset on the request.
const streamIdleTimeoutDefault = 30 * time.Second

// streamReadBufSize is the size of the read buffer used when draining a
// streaming backend response. Tuned to be small enough to surface SSE events
// promptly without paying a syscall-per-byte tax for high-throughput streams.
const streamReadBufSize = 16 * 1024

// dispatchStreamingAndRespond runs the streaming response path. The body is
// emitted as ProxyHttpBodyChunk frames that arrive in lockstep with the
// backend; the call returns only when one of:
//
//   - the backend returns EOF (clean fin=true close)
//   - no body bytes flow for stream_idle_timeout_ms (TIMEOUT mid-stream)
//   - total bytes exceed max_response_body_bytes (PAYLOAD_TOO_LARGE mid-stream)
//   - the caller (gateway / SDK) cancels ctx
//
// Audit emission for OpProxyHttpStreamClosed happens in the gateway routing
// layer when the final frame lands; the sidecar only needs to ensure the
// closing frame carries the correct kind so that audit can be derived.
func (t *Terminator) dispatchStreamingAndRespond(ctx context.Context, req *pb.ProxyHttpRequest, body []byte, transport tunnelTransport) error {
	if transport == nil {
		return nil
	}

	backend, perr := t.selectBackend(req)
	if perr != nil {
		return transport.SendProxyHttpResponse(errorResponse(req.GetRequestId(), perr.Kind, perr.Message))
	}

	resp, cancel, perr := backend.dispatchStreaming(ctx, req, body)
	if perr != nil {
		return transport.SendProxyHttpResponse(errorResponse(req.GetRequestId(), perr.Kind, perr.Message))
	}
	defer cancel()
	defer resp.Body.Close()

	// Resolve effective limits.
	idleTimeout := streamIdleTimeoutDefault
	if v := req.GetStreamIdleTimeoutMs(); v > 0 {
		idleTimeout = time.Duration(v) * time.Millisecond
	}
	maxBytes := req.GetMaxResponseBodyBytes()
	if maxBytes <= 0 {
		maxBytes = backend.cfg.MaxBodyBytes
	}

	headers := make(map[string]string, len(resp.Header))
	for k := range resp.Header {
		headers[k] = resp.Header.Get(k)
	}

	requestID := req.GetRequestId()
	header := &pb.ProxyHttpResponse{
		RequestId:   requestID,
		StatusCode:  int32(resp.StatusCode),
		Headers:     headers,
		BodyChunked: true,
	}
	if err := transport.SendProxyHttpResponse(header); err != nil {
		return err
	}

	// Stream body chunks. We use a goroutine + channel to apply the idle
	// deadline without leaning on Read deadlines (which the http response
	// body does not support directly).
	type readChunk struct {
		data []byte
		err  error
	}
	chunkCh := make(chan readChunk, 1)
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	go func() {
		defer close(chunkCh)
		buf := make([]byte, streamReadBufSize)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				out := make([]byte, n)
				copy(out, buf[:n])
				select {
				case chunkCh <- readChunk{data: out}:
				case <-streamCtx.Done():
					return
				}
			}
			if err != nil {
				select {
				case chunkCh <- readChunk{err: err}:
				case <-streamCtx.Done():
				}
				return
			}
		}
	}()

	var (
		seq        uint32
		totalBytes int64
		idleTimer  = time.NewTimer(idleTimeout)
	)
	defer idleTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			// Caller cancelled — close the stream with a fin chunk so the
			// peer knows we're done. Treat as clean close.
			_ = transport.SendProxyHttpBodyChunk(&pb.ProxyHttpBodyChunk{
				RequestId: requestID,
				IsRequest: false,
				Seq:       seq,
				Fin:       true,
			})
			return nil
		case <-idleTimer.C:
			// No bytes for idleTimeout — close with TIMEOUT.
			_ = transport.SendProxyHttpResponse(errorResponse(requestID, pb.ProxyError_TIMEOUT,
				fmt.Sprintf("backend %q stream idle for %s", backend.cfg.Name, idleTimeout)))
			return nil
		case rc, ok := <-chunkCh:
			if !ok {
				// Reader goroutine already signalled completion in a prior
				// iteration; nothing left to do.
				return nil
			}
			if rc.err != nil {
				// io.EOF is a clean close → emit fin=true.
				if rc.err == io.EOF {
					_ = transport.SendProxyHttpBodyChunk(&pb.ProxyHttpBodyChunk{
						RequestId: requestID,
						IsRequest: false,
						Seq:       seq,
						Fin:       true,
					})
					return nil
				}
				// Any other error is an upstream failure.
				_ = transport.SendProxyHttpResponse(errorResponse(requestID, pb.ProxyError_UPSTREAM_RESET,
					fmt.Sprintf("backend %q stream read: %v", backend.cfg.Name, rc.err)))
				return nil
			}
			// Reset idle timer on data; bytes accounting next.
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(idleTimeout)

			data := rc.data
			if maxBytes > 0 && totalBytes+int64(len(data)) > maxBytes {
				// Send the bytes we still legally can, then close with
				// PAYLOAD_TOO_LARGE so the caller sees the partial body.
				room := maxBytes - totalBytes
				if room > 0 {
					_ = transport.SendProxyHttpBodyChunk(&pb.ProxyHttpBodyChunk{
						RequestId: requestID,
						IsRequest: false,
						Seq:       seq,
						Data:      data[:room],
						Fin:       false,
					})
					// seq not incremented: the next statement returns and the
					// terminating PAYLOAD_TOO_LARGE response carries no body
					// chunk that would consume another sequence number.
				}
				_ = transport.SendProxyHttpResponse(errorResponse(requestID, pb.ProxyError_PAYLOAD_TOO_LARGE,
					fmt.Sprintf("backend %q response body exceeds limit %d", backend.cfg.Name, maxBytes)))
				return nil
			}
			totalBytes += int64(len(data))
			if err := transport.SendProxyHttpBodyChunk(&pb.ProxyHttpBodyChunk{
				RequestId: requestID,
				IsRequest: false,
				Seq:       seq,
				Data:      data,
				Fin:       false,
			}); err != nil {
				return err
			}
			seq++
		}
	}
}

// RegisterHandlers installs the terminator's inbound dispatcher hooks on the
// supplied ServiceClient. This is the work `Run` performs after building its
// own runtime; composite-mode callers reuse a shared runtime and call this
// directly so the same connection can serve both terminator and relay
// surfaces.
//
// The supplied transport ships outbound proxy/tunnel envelopes (responses,
// tunnel acks, etc.) upstream. Composite-mode callers may wrap the transport
// to add side-effects (e.g. routing to multiple consumers) but production
// terminators pass the runtime's bare ServiceClient transport.
func (t *Terminator) RegisterHandlers(client *aether.ServiceClient, transport tunnelTransport) {
	client.OnMessage(func(msgCtx context.Context, msg *aether.Message) error {
		// Plain SendMessage delivery path — terminators don't expect
		// peer-to-peer messages, so this is a fall-through log.
		log.Debug().
			Str("source", msg.SourceTopic).
			Int("payload_bytes", len(msg.Payload)).
			Msg("terminator: received message via OnMessage path")
		return nil
	})

	// Wire the service-side proxy/tunnel hooks so envelopes the gateway
	// publishes to our sv:: topic land in the right backend dispatcher.
	client.OnProxyHttpRequest(func(reqCtx context.Context, req *pb.ProxyHttpRequest) error {
		// Chunked-request: register accumulator, wait for body chunks. Dispatch
		// happens on fin. The header envelope carries no body bytes by spec.
		if req.GetBodyChunked() {
			return t.beginChunkedRequest(req, transport)
		}
		// Inline body: dispatch immediately, stream large responses.
		return t.dispatchAndRespond(reqCtx, req, req.GetBody(), transport)
	})
	client.OnProxyHttpBodyChunk(func(chunkCtx context.Context, chunk *pb.ProxyHttpBodyChunk) error {
		if !chunk.GetIsRequest() {
			// Service principals only receive request-direction chunks
			// (caller→service). Response-direction chunks are emitted, not
			// consumed, by the terminator. Drop quietly.
			return nil
		}
		return t.handleChunkedRequestFrame(chunkCtx, chunk, transport)
	})
	client.OnTunnelDataIn(func(dataCtx context.Context, frame *pb.TunnelData) error {
		// Seq=0 with a payload that decodes as a TunnelOpen is the
		// gateway's "open" signal (T4 wire format); all other frames are
		// follow-on data.
		if frame.GetSeq() == 0 && len(frame.GetData()) > 0 {
			open := &pb.TunnelOpen{}
			if err := proto.Unmarshal(frame.GetData(), open); err == nil && open.GetTunnelId() != "" {
				if cm := t.HandleTunnelOpen(dataCtx, open, transport); cm != nil {
					_ = transport.SendTunnelClose(cm)
				}
				return nil
			}
		}
		t.HandleTunnelData(frame, transport)
		return nil
	})
	client.OnTunnelAckIn(func(_ context.Context, ack *pb.TunnelAck) error {
		t.HandleTunnelAck(ack)
		return nil
	})
	client.OnTunnelCloseIn(func(_ context.Context, cm *pb.TunnelClose) error {
		t.HandleTunnelClose(cm)
		return nil
	})
}

// BeginChunkedRequestForTest exposes beginChunkedRequest for cross-package
// integration tests. The transport parameter is typed as `any` to avoid
// leaking the unexported tunnelTransport interface; the runtime checks that
// the value implements the required methods.
func (t *Terminator) BeginChunkedRequestForTest(req *pb.ProxyHttpRequest, transport any) error {
	tr, ok := transport.(tunnelTransport)
	if !ok {
		return fmt.Errorf("transport %T does not implement the required methods", transport)
	}
	return t.beginChunkedRequest(req, tr)
}

// HandleChunkedRequestFrameForTest exposes handleChunkedRequestFrame for
// cross-package integration tests.
func (t *Terminator) HandleChunkedRequestFrameForTest(ctx context.Context, chunk *pb.ProxyHttpBodyChunk, transport any) error {
	tr, ok := transport.(tunnelTransport)
	if !ok {
		return fmt.Errorf("transport %T does not implement the required methods", transport)
	}
	return t.handleChunkedRequestFrame(ctx, chunk, tr)
}

// DispatchStreamingForTest exposes the streaming response path for
// cross-package integration tests. The caller drives the dispatch end-to-end
// with a synchronous transport and reads emitted frames off the transport
// once it returns.
func (t *Terminator) DispatchStreamingForTest(ctx context.Context, req *pb.ProxyHttpRequest, body []byte, transport any) error {
	tr, ok := transport.(tunnelTransport)
	if !ok {
		return fmt.Errorf("transport %T does not implement the required methods", transport)
	}
	return t.dispatchStreamingAndRespond(ctx, req, body, tr)
}

// HandleProxyRequest dispatches a single ProxyHttpRequest envelope to the
// configured backend, returning the response envelope (header) and body bytes.
// This is the primary unit-testable entry point.
//
// When the request body arrives chunked, callers must first invoke
// AppendBodyChunk repeatedly and then call HandleProxyRequest with body=nil
// to flush. The terminator drops accumulated state for request_id once
// dispatch completes.
func (t *Terminator) HandleProxyRequest(ctx context.Context, req *pb.ProxyHttpRequest, body []byte) (*pb.ProxyHttpResponse, []byte) {
	if req == nil {
		return errorResponse("", pb.ProxyError_DECODE_FAILED, "nil request"), nil
	}

	backend, perr := t.selectBackend(req)
	if perr != nil {
		return errorResponse(req.GetRequestId(), perr.Kind, perr.Message), nil
	}

	resp, perr := backend.dispatch(ctx, req, body)
	if perr != nil {
		return errorResponse(req.GetRequestId(), perr.Kind, perr.Message), nil
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return errorResponse(req.GetRequestId(), pb.ProxyError_UPSTREAM_RESET, fmt.Sprintf("read backend body: %v", err)), nil
	}

	headers := make(map[string]string, len(resp.Header))
	for k := range resp.Header {
		headers[k] = resp.Header.Get(k)
	}

	return &pb.ProxyHttpResponse{
		RequestId:  req.GetRequestId(),
		StatusCode: int32(resp.StatusCode),
		Headers:    headers,
		Body:       respBody,
	}, respBody
}

// HandleTunnelOpen routes a TunnelOpen envelope from the gateway to the
// matching backend. Returns a non-nil TunnelClose only when the tunnel is
// rejected synchronously (no backend, unsupported protocol, validation
// failure); when the tunnel is accepted the TCP backend takes ownership and
// emits frames through transport for the lifetime of the tunnel.
//
// Note: in the wire format, the gateway delivers TunnelOpen as a downstream
// TunnelData{seq:0} carrying a marshaled TunnelOpen body. Callers that
// receive that envelope should decode and pass the decoded TunnelOpen here.
func (t *Terminator) HandleTunnelOpen(ctx context.Context, open *pb.TunnelOpen, transport tunnelTransport) *pb.TunnelClose {
	if open == nil {
		return &pb.TunnelClose{Reason: pb.TunnelClose_ERROR, Detail: "nil tunnel_open"}
	}

	// Snapshot backend slices under read lock so Reload cannot race with
	// backend selection and tunnel open.
	t.backendMu.RLock()
	tcpBackends := t.tcpBackends
	wsBackends := t.wsBackends
	udpBackends := t.udpBackends
	t.backendMu.RUnlock()

	switch open.GetProtocol() {
	case pb.TunnelOpen_TCP:
		if len(tcpBackends) == 0 {
			return &pb.TunnelClose{
				TunnelId: open.GetTunnelId(),
				Reason:   pb.TunnelClose_ERROR,
				Detail:   "tunnel kind TCP not implemented in this build (no TCP backend configured)",
			}
		}
		backend := selectTCPBackendFrom(tcpBackends, open)
		if backend == nil {
			return &pb.TunnelClose{
				TunnelId: open.GetTunnelId(),
				Reason:   pb.TunnelClose_ERROR,
				Detail:   "ACL_DENIED: no TCP backend permits this tunnel",
			}
		}
		tun, err := backend.open(ctx, open, transport)
		if err != nil {
			return &pb.TunnelClose{
				TunnelId: open.GetTunnelId(),
				Reason:   pb.TunnelClose_ERROR,
				Detail:   fmt.Sprintf("backend open failed: %v", err),
			}
		}
		if !t.tunnels.register(tun) {
			tun.stop()
			return &pb.TunnelClose{
				TunnelId: open.GetTunnelId(),
				Reason:   pb.TunnelClose_ERROR,
				Detail:   "duplicate tunnel_id",
			}
		}
		go func() {
			<-tun.ctx.Done()
			t.tunnels.unregister(open.GetTunnelId())
		}()
		return nil
	case pb.TunnelOpen_WEBSOCKET:
		if len(wsBackends) == 0 {
			return &pb.TunnelClose{
				TunnelId: open.GetTunnelId(),
				Reason:   pb.TunnelClose_ERROR,
				Detail:   "tunnel kind WEBSOCKET not implemented in this build (no WS backend configured)",
			}
		}
		backend := selectWSBackendFrom(wsBackends, open)
		if backend == nil {
			return &pb.TunnelClose{
				TunnelId: open.GetTunnelId(),
				Reason:   pb.TunnelClose_ERROR,
				Detail:   "ACL_DENIED: no WS backend permits this tunnel",
			}
		}
		tun, err := backend.open(ctx, open, transport)
		if err != nil {
			return &pb.TunnelClose{
				TunnelId: open.GetTunnelId(),
				Reason:   pb.TunnelClose_ERROR,
				Detail:   fmt.Sprintf("backend open failed: %v", err),
			}
		}
		if !t.tunnels.register(tun) {
			tun.stop()
			return &pb.TunnelClose{
				TunnelId: open.GetTunnelId(),
				Reason:   pb.TunnelClose_ERROR,
				Detail:   "duplicate tunnel_id",
			}
		}
		go func() {
			<-tun.ctx.Done()
			t.tunnels.unregister(open.GetTunnelId())
		}()
		return nil
	case pb.TunnelOpen_UDP:
		if len(udpBackends) == 0 {
			return &pb.TunnelClose{
				TunnelId: open.GetTunnelId(),
				Reason:   pb.TunnelClose_ERROR,
				Detail:   "tunnel kind UDP not implemented in this build (no UDP backend configured)",
			}
		}
		backend := selectUDPBackendFrom(udpBackends, open)
		if backend == nil {
			return &pb.TunnelClose{
				TunnelId: open.GetTunnelId(),
				Reason:   pb.TunnelClose_ERROR,
				Detail:   "ACL_DENIED: no UDP backend permits this tunnel",
			}
		}
		tun, err := backend.open(ctx, open, transport)
		if err != nil {
			return &pb.TunnelClose{
				TunnelId: open.GetTunnelId(),
				Reason:   pb.TunnelClose_ERROR,
				Detail:   fmt.Sprintf("backend open failed: %v", err),
			}
		}
		if !t.tunnels.register(tun) {
			tun.stop()
			return &pb.TunnelClose{
				TunnelId: open.GetTunnelId(),
				Reason:   pb.TunnelClose_ERROR,
				Detail:   "duplicate tunnel_id",
			}
		}
		go func() {
			<-tun.ctx.Done()
			t.tunnels.unregister(open.GetTunnelId())
		}()
		return nil
	default:
		return &pb.TunnelClose{
			TunnelId: open.GetTunnelId(),
			Reason:   pb.TunnelClose_ERROR,
			Detail:   fmt.Sprintf("tunnel kind %s not implemented in this build", open.GetProtocol().String()),
		}
	}
}

// HandleTunnelData routes a TunnelData frame from the gateway to the matching
// open tunnel. Frames for unknown tunnels prompt a TunnelClose{PEER_RESET} so
// the caller stops sending.
func (t *Terminator) HandleTunnelData(data *pb.TunnelData, transport tunnelTransport) {
	if data == nil {
		return
	}
	tun := t.tunnels.get(data.GetTunnelId())
	if tun == nil {
		if transport != nil {
			_ = transport.SendTunnelClose(&pb.TunnelClose{
				TunnelId: data.GetTunnelId(),
				Reason:   pb.TunnelClose_PEER_RESET,
				Detail:   "no active tunnel for id",
			})
		}
		return
	}
	tun.storeInboundSeq(data.GetSeq())
	tun.handleData(data)
}

// HandleTunnelAck applies an inbound flow-control credit grant. Acks for
// unknown tunnels are silently dropped — a stale ack is harmless.
func (t *Terminator) HandleTunnelAck(ack *pb.TunnelAck) {
	if ack == nil {
		return
	}
	tun := t.tunnels.get(ack.GetTunnelId())
	if tun == nil {
		return
	}
	tun.handleAck(ack)
}

// HandleTunnelClose terminates a live tunnel. Closes for unknown tunnels are
// idempotent no-ops.
func (t *Terminator) HandleTunnelClose(closeMsg *pb.TunnelClose) {
	if closeMsg == nil {
		return
	}
	tun := t.tunnels.get(closeMsg.GetTunnelId())
	if tun == nil {
		return
	}
	tun.handleClose(closeMsg)
}

// StopAllTunnels forces every active tunnel down. Intended for shutdown.
func (t *Terminator) StopAllTunnels() {
	if t.tunnels != nil {
		t.tunnels.stopAll()
	}
}

// selectBackend picks the backend that should handle req. When req carries an
// explicit BackendName the named backend is consulted directly — but its
// method/path allowlist still applies, so the explicit name does not bypass
// ACL. When BackendName is empty the first backend whose allowlist accepts
// the request wins. Returns ACL_DENIED when the named backend is missing or
// rejects the request, or when no backend matches.
func (t *Terminator) selectBackend(req *pb.ProxyHttpRequest) (*httpBackend, *proxyResponseError) {
	t.backendMu.RLock()
	backends := t.backends
	t.backendMu.RUnlock()
	if name := req.GetBackendName(); name != "" {
		for _, b := range backends {
			if b.cfg.Name != name {
				continue
			}
			if !b.matches(req.GetMethod(), req.GetPath()) {
				return nil, newProxyError(pb.ProxyError_ACL_DENIED,
					"backend %q does not permit %s %s", name, req.GetMethod(), req.GetPath())
			}
			return b, nil
		}
		return nil, newProxyError(pb.ProxyError_ACL_DENIED,
			"no backend named %q", name)
	}
	for _, b := range backends {
		if b.matches(req.GetMethod(), req.GetPath()) {
			return b, nil
		}
	}
	return nil, newProxyError(pb.ProxyError_ACL_DENIED,
		"no backend permits %s %s", req.GetMethod(), req.GetPath())
}

// Reload re-reads the config file and atomically swaps the backend slices.
// It is safe to call from a signal handler goroutine. A second concurrent
// Reload call is a no-op (returns immediately without blocking).
func (t *Terminator) Reload() {
	if t.cfgPath == "" {
		log.Error().Msg("terminator: reload requested but no config path recorded (dev-defaults mode); skipping")
		return
	}

	// Drop the second SIGHUP if a reload is already in progress.
	if !t.reloadMu.TryLock() {
		log.Warn().Msg("terminator: reload already in progress; dropping duplicate SIGHUP")
		return
	}
	defer t.reloadMu.Unlock()

	newCfg, err := LoadConfig(t.cfgPath)
	if err != nil {
		log.Error().Err(err).Str("path", t.cfgPath).Msg("terminator: reload failed: cannot read config file; keeping old config")
		return
	}
	if err := newCfg.Validate(); err != nil {
		log.Error().Err(err).Str("path", t.cfgPath).Msg("terminator: reload failed: invalid config; keeping old config")
		return
	}
	if !newCfg.Terminator.Enabled {
		log.Error().
			Msg("terminator: reload rejected: terminator.enabled flipped to false; surface enable/disable is not reloadable")
		return
	}

	newHTTP, newTCP, newWS, newUDP := buildBackends(newCfg)

	// Preserve the resolver on newly-built HTTP backends if one was set.
	if t.resolver != nil {
		for _, b := range newHTTP {
			b.resolver = t.resolver
		}
	}

	t.backendMu.Lock()
	oldHTTPCount := len(t.backends)
	t.backends = newHTTP
	t.tcpBackends = newTCP
	t.wsBackends = newWS
	t.udpBackends = newUDP
	t.cfg = newCfg
	t.backendMu.Unlock()

	log.Info().
		Str("path", t.cfgPath).
		Int("old_http_backends", oldHTTPCount).
		Int("new_http_backends", len(newHTTP)).
		Int("new_tcp_backends", len(newTCP)).
		Int("new_ws_backends", len(newWS)).
		Int("new_udp_backends", len(newUDP)).
		Msg("terminator: config reloaded")
}

// selectTCPBackendFrom returns the first backend from the provided slice that
// admits the tunnel open frame, mirroring the logic in selectTCPBackend but
// operating on an externally-snapshotted slice so it is safe to call after
// releasing the backendMu read lock.
func selectTCPBackendFrom(backends []*tcpBackend, open *pb.TunnelOpen) *tcpBackend {
	hint := open.GetRemoteHint()
	for _, b := range backends {
		if _, err := resolveTCPAddress(b.cfg, hint); err == nil {
			return b
		}
	}
	return nil
}

// selectWSBackendFrom mirrors selectTCPBackendFrom for WS backends.
func selectWSBackendFrom(backends []*wsBackend, open *pb.TunnelOpen) *wsBackend {
	hint := open.GetRemoteHint()
	for _, b := range backends {
		if _, err := resolveWSAddress(b.cfg, hint); err == nil {
			return b
		}
	}
	return nil
}

// selectUDPBackendFrom mirrors selectTCPBackendFrom for UDP backends.
func selectUDPBackendFrom(backends []*udpBackend, open *pb.TunnelOpen) *udpBackend {
	hint := open.GetRemoteHint()
	for _, b := range backends {
		if _, err := resolveUDPAddress(b.cfg, hint); err == nil {
			return b
		}
	}
	return nil
}

// errorResponse builds a ProxyHttpResponse carrying the given proxy error.
func errorResponse(requestID string, kind pb.ProxyError_Kind, msg string) *pb.ProxyHttpResponse {
	return &pb.ProxyHttpResponse{
		RequestId: requestID,
		Error: &pb.ProxyError{
			Kind:    kind,
			Message: msg,
		},
	}
}
