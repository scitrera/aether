// Package aether proxy HTTP support for the Go SDK.
//
// This file provides ProxyHTTP and AetherRoundTripper, allowing callers to
// tunnel HTTP requests through an Aether connection to a target service
// (e.g. "sv::memorylayer::default" or wildcard "sv::memorylayer").

package aether

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// proxyChunkSize is the maximum body size sent inline. Bodies larger than this
// are split into ProxyHttpBodyChunk frames.
const proxyChunkSize = 256 * 1024

// =============================================================================
// Pending proxy request state
// =============================================================================

// proxyInflight holds the state for one in-flight ProxyHTTP call.
type proxyInflight struct {
	headerCh chan *pb.ProxyHttpResponse // receives the initial response header frame
	chunks   [][]byte                   // accumulates response body chunks (if chunked)
	mu       sync.Mutex
	done     chan struct{} // closed when fin chunk arrives

	// streaming, when non-nil, receives chunks as they arrive instead of
	// being accumulated in chunks. Used by ProxyHTTP callers that opted into
	// stream_response_indefinitely so a backend SSE / long-poll response can
	// be drained incrementally.
	streaming *streamingBody
}

// streamingBody is a thread-safe pipe-like buffer fed by inbound
// ProxyHttpBodyChunk frames and drained by the SDK caller via the io.Reader
// returned in *http.Response.Body.
type streamingBody struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    []byte
	closed bool
	err    error // non-nil terminal error (mid-stream ProxyError)
	doneCh chan struct{}
}

func newStreamingBody() *streamingBody {
	s := &streamingBody{doneCh: make(chan struct{})}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// push appends bytes to the buffer; safe to call concurrently with Read.
func (s *streamingBody) push(data []byte) {
	if len(data) == 0 {
		return
	}
	s.mu.Lock()
	if !s.closed {
		s.buf = append(s.buf, data...)
		s.cond.Broadcast()
	}
	s.mu.Unlock()
}

// closeWithErr marks the stream done; subsequent reads drain remaining bytes
// then return err (or io.EOF when err is nil).
func (s *streamingBody) closeWithErr(err error) {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.err = err
		close(s.doneCh)
		s.cond.Broadcast()
	}
	s.mu.Unlock()
}

// Read implements io.Reader.
func (s *streamingBody) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.buf) == 0 && !s.closed {
		s.cond.Wait()
	}
	if len(s.buf) > 0 {
		n := copy(p, s.buf)
		s.buf = s.buf[n:]
		return n, nil
	}
	// closed && empty
	if s.err != nil {
		return 0, s.err
	}
	return 0, io.EOF
}

// Close marks the body done so the consumer can stop reading. Safe to call
// multiple times.
func (s *streamingBody) Close() error {
	s.closeWithErr(nil)
	return nil
}

// =============================================================================
// BaseClient additions — pending proxy request registry
// =============================================================================

// pendingProxyRequests is the registry for in-flight proxy requests.
// It is a field on BaseClient (added via composition at compile time via the
// pendingRequests generic type), but because we need the multi-frame chunk
// accumulation we maintain a separate sync.Map here keyed by request_id.

// proxyInflights holds in-flight proxy inflight state keyed by request_id.
var globalProxyInflights sync.Map

// registerProxyInflight stores inflight state for a new proxy call.
func registerProxyInflight(requestID string) *proxyInflight {
	inf := &proxyInflight{
		headerCh: make(chan *pb.ProxyHttpResponse, 1),
		done:     make(chan struct{}),
	}
	globalProxyInflights.Store(requestID, inf)
	return inf
}

// registerStreamingProxyInflight is like registerProxyInflight but wires a
// streamingBody so chunks flow into the SDK reader as they arrive.
func registerStreamingProxyInflight(requestID string) *proxyInflight {
	inf := registerProxyInflight(requestID)
	inf.streaming = newStreamingBody()
	return inf
}

// deleteProxyInflight removes inflight state for a completed/aborted proxy call.
func deleteProxyInflight(requestID string) {
	globalProxyInflights.Delete(requestID)
}

// resolveProxyResponse delivers a response header frame to the waiting
// ProxyHTTP call. Returns true if the request was found. Streaming inflights
// can receive a second header frame mid-stream (carrying a terminal
// ProxyError); in that case the streaming body is closed with the error.
func resolveProxyResponse(requestID string, resp *pb.ProxyHttpResponse) bool {
	val, ok := globalProxyInflights.Load(requestID)
	if !ok {
		return false
	}
	inf := val.(*proxyInflight)

	// For streaming inflights, treat any post-header header frame as a
	// terminal mid-stream error (TIMEOUT, PAYLOAD_TOO_LARGE, UPSTREAM_RESET).
	if inf.streaming != nil {
		if pe := resp.GetError(); pe != nil {
			select {
			case <-inf.done:
			default:
				close(inf.done)
			}
			inf.streaming.closeWithErr(&ProxyTransportError{
				Kind:    pe.GetKind().String(),
				Message: pe.GetMessage(),
			})
			return true
		}
	}

	// Non-blocking send; if the channel is already full the caller timed out.
	select {
	case inf.headerCh <- resp:
	default:
	}
	// If the body is NOT chunked, close done immediately so the waiter proceeds.
	if !resp.GetBodyChunked() {
		select {
		case <-inf.done:
		default:
			close(inf.done)
		}
	}
	return true
}

// appendProxyChunk appends a response body chunk to the inflight state.
// When fin=true it also signals that all chunks have arrived.
func appendProxyChunk(requestID string, data []byte, fin bool) {
	val, ok := globalProxyInflights.Load(requestID)
	if !ok {
		return
	}
	inf := val.(*proxyInflight)
	if inf.streaming != nil {
		inf.streaming.push(data)
		if fin {
			inf.streaming.closeWithErr(nil)
			select {
			case <-inf.done:
			default:
				close(inf.done)
			}
		}
		return
	}
	inf.mu.Lock()
	inf.chunks = append(inf.chunks, data)
	inf.mu.Unlock()
	if fin {
		select {
		case <-inf.done:
		default:
			close(inf.done)
		}
	}
}

// =============================================================================
// dispatchResponse handlers (called from client.go's dispatchResponse switch)
// =============================================================================

// handleProxyHttpResponse processes a ProxyHttpResponse from the gateway.
func (c *BaseClient) handleProxyHttpResponse(resp *pb.ProxyHttpResponse) {
	resolveProxyResponse(resp.GetRequestId(), resp)
}

// handleProxyHttpBodyChunk processes a ProxyHttpBodyChunk from the gateway.
func (c *BaseClient) handleProxyHttpBodyChunk(chunk *pb.ProxyHttpBodyChunk) {
	if chunk.GetIsRequest() {
		return // request-direction chunks are sent by us, not received
	}
	appendProxyChunk(chunk.GetRequestId(), chunk.GetData(), chunk.GetFin())
}

// =============================================================================
// ProxyHTTP options
// =============================================================================

// proxyOptions collects optional ProxyHTTP parameters.
type proxyOptions struct {
	backend        string
	streamResponse bool
	streamIdleMs   int64
	streamMaxBytes int64
}

// ProxyOpt configures a ProxyHTTP call.
type ProxyOpt func(*proxyOptions)

// WithBackend selects a named backend on the terminator side. Empty string
// (the default) lets the terminator pick the first backend whose allow-list
// admits the request. The backend's allow-list still applies even when an
// explicit name is supplied.
func WithBackend(name string) ProxyOpt {
	return func(o *proxyOptions) { o.backend = name }
}

// WithStreamResponse opts into unbounded response streaming (SSE / log tails
// / model token streams). When enabled, the request's context deadline is
// the time-to-first-byte deadline only; subsequent body bytes are governed
// by idleTimeoutMs (default 30s when 0), and the response stops with
// PAYLOAD_TOO_LARGE if total bytes exceed maxBytes (0 = use the backend's
// configured per-backend cap).
//
// The returned *http.Response.Body is an io.ReadCloser that yields bytes as
// chunks arrive from the backend; close it when done.
func WithStreamResponse(idleTimeoutMs int64, maxBytes int64) ProxyOpt {
	return func(o *proxyOptions) {
		o.streamResponse = true
		o.streamIdleMs = idleTimeoutMs
		o.streamMaxBytes = maxBytes
	}
}

// =============================================================================
// ProxyHTTP — main entry point
// =============================================================================

// ProxyHTTP sends an HTTP request to target through the Aether connection and
// returns the HTTP response. The target may be a fully-qualified service topic
// ("sv::impl::spec") or a wildcard form ("sv::impl") for load-balanced
// dispatch.
//
// The req body (if any) is read and forwarded. Bodies larger than 256 KB are
// split into ProxyHttpBodyChunk frames automatically.
//
// A deadline may be communicated via ctx; an additional per-call timeout can
// also be encoded in the request context. When both are present the shorter one
// wins at the gateway side.
func (c *BaseClient) ProxyHTTP(ctx context.Context, target string, req *http.Request, opts ...ProxyOpt) (*http.Response, error) {
	var o proxyOptions
	for _, opt := range opts {
		opt(&o)
	}
	if target == "" {
		return nil, fmt.Errorf("aether proxy: target topic is required")
	}

	// Read the request body.
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("aether proxy: reading request body: %w", err)
		}
		req.Body.Close()
	}

	// Build request headers map (flatten multi-value headers).
	headers := make(map[string]string, len(req.Header))
	for k, vs := range req.Header {
		headers[k] = strings.Join(vs, ", ")
	}

	// Compute timeout_ms from context deadline if present.
	var timeoutMs int64
	if dl, ok := ctx.Deadline(); ok {
		remaining := time.Until(dl)
		if remaining > 0 {
			timeoutMs = remaining.Milliseconds()
		}
	}

	// Build the path + query string.
	path := req.URL.RequestURI()

	requestID := c.NextRequestID()
	var inf *proxyInflight
	if o.streamResponse {
		inf = registerStreamingProxyInflight(requestID)
	} else {
		inf = registerProxyInflight(requestID)
	}
	// For streaming responses we cannot delete the inflight on return: the
	// caller still owns the body reader. The streaming body's Close() path
	// arranges for cleanup on completion (see buildHTTPResponse below).
	if !o.streamResponse {
		defer deleteProxyInflight(requestID)
	}

	chunked := len(body) > proxyChunkSize

	proxyReq := &pb.ProxyHttpRequest{
		RequestId:                  requestID,
		TargetTopic:                target,
		Method:                     req.Method,
		Path:                       path,
		Headers:                    headers,
		TimeoutMs:                  timeoutMs,
		FollowRedirects:            true,
		BackendName:                o.backend,
		StreamResponseIndefinitely: o.streamResponse,
		StreamIdleTimeoutMs:        o.streamIdleMs,
		MaxResponseBodyBytes:       o.streamMaxBytes,
	}

	if !chunked {
		proxyReq.Body = body
	} else {
		proxyReq.BodyChunked = true
	}

	// Inject OBO authorization from request context if present.
	if auth := oboFromContext(req.Context()); auth != nil {
		proxyReq.Authorization = auth
	}

	// Send the initial request frame.
	if err := c.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_ProxyHttpRequest{ProxyHttpRequest: proxyReq},
	}); err != nil {
		return nil, fmt.Errorf("aether proxy: sending request: %w", err)
	}

	// Send body chunks if needed.
	if chunked {
		for seq, offset := uint32(0), 0; offset < len(body); seq++ {
			end := offset + proxyChunkSize
			if end > len(body) {
				end = len(body)
			}
			fin := end == len(body)
			chunk := &pb.ProxyHttpBodyChunk{
				RequestId: requestID,
				IsRequest: true,
				Seq:       seq,
				Data:      body[offset:end],
				Fin:       fin,
			}
			if err := c.Send(&pb.UpstreamMessage{
				Payload: &pb.UpstreamMessage_ProxyHttpBodyChunk{ProxyHttpBodyChunk: chunk},
			}); err != nil {
				return nil, fmt.Errorf("aether proxy: sending body chunk %d: %w", seq, err)
			}
			offset = end
		}
	}

	// Wait for the response header.
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("aether proxy: context canceled waiting for response: %w", ctx.Err())
	case hdr := <-inf.headerCh:
		return c.buildHTTPResponse(ctx, req, hdr, inf)
	}
}

// buildHTTPResponse assembles an *http.Response from the header frame and any
// accumulated body chunks.
func (c *BaseClient) buildHTTPResponse(ctx context.Context, req *http.Request, hdr *pb.ProxyHttpResponse, inf *proxyInflight) (*http.Response, error) {
	// Surface transport-layer errors.
	if pe := hdr.GetError(); pe != nil && pe.GetKind() != pb.ProxyError_UNKNOWN {
		if inf.streaming != nil {
			inf.streaming.closeWithErr(nil)
			deleteProxyInflight(hdr.GetRequestId())
		}
		return nil, &ProxyTransportError{Kind: pe.GetKind().String(), Message: pe.GetMessage()}
	}

	statusCode := int(hdr.GetStatusCode())
	if statusCode == 0 {
		statusCode = 200
	}

	// Streaming path: hand back an http.Response whose Body is the
	// streamingBody pipe; chunks flow in via appendProxyChunk as the gateway
	// delivers ProxyHttpBodyChunk frames. We do NOT wait for fin here.
	if inf.streaming != nil {
		requestID := hdr.GetRequestId()
		body := &streamingResponseReader{
			body:      inf.streaming,
			requestID: requestID,
		}
		httpResp := &http.Response{
			StatusCode: statusCode,
			Status:     fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
			Header:     make(http.Header),
			Body:       body,
			Request:    req,
		}
		for k, v := range hdr.GetHeaders() {
			httpResp.Header.Set(k, v)
		}
		return httpResp, nil
	}

	// If body is chunked, wait for all chunks to arrive.
	if hdr.GetBodyChunked() {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("aether proxy: context canceled waiting for body chunks: %w", ctx.Err())
		case <-inf.done:
		}
	}

	// Assemble body.
	var responseBody []byte
	if hdr.GetBodyChunked() {
		inf.mu.Lock()
		var total int
		for _, ch := range inf.chunks {
			total += len(ch)
		}
		responseBody = make([]byte, 0, total)
		for _, ch := range inf.chunks {
			responseBody = append(responseBody, ch...)
		}
		inf.mu.Unlock()
	} else {
		responseBody = hdr.GetBody()
	}

	// Build http.Response.
	httpResp := &http.Response{
		StatusCode: statusCode,
		Status:     fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(responseBody)),
		Request:    req,
	}
	for k, v := range hdr.GetHeaders() {
		httpResp.Header.Set(k, v)
	}
	return httpResp, nil
}

// streamingResponseReader wraps a streamingBody and removes the proxy
// inflight registration once the caller closes the body. This is the only
// place where the streaming inflight is deleted from the global registry —
// dropping it earlier would lose chunks still in transit.
type streamingResponseReader struct {
	body      *streamingBody
	requestID string
}

func (r *streamingResponseReader) Read(p []byte) (int, error) {
	return r.body.Read(p)
}

func (r *streamingResponseReader) Close() error {
	err := r.body.Close()
	deleteProxyInflight(r.requestID)
	return err
}

// =============================================================================
// ProxyTransportError
// =============================================================================

// ProxyTransportError is returned when the gateway reports a transport-layer
// failure (e.g. dial failed, sidecar unavailable, timeout).
type ProxyTransportError struct {
	Kind    string
	Message string
}

func (e *ProxyTransportError) Error() string {
	return fmt.Sprintf("aether proxy transport error: %s: %s", e.Kind, e.Message)
}

// =============================================================================
// AetherRoundTripper — implements http.RoundTripper
// =============================================================================

// AetherRoundTripper implements http.RoundTripper, allowing an *http.Client to
// route requests through an Aether connection to a target service.
//
// Example:
//
//	rt := &AetherRoundTripper{Client: agentClient, Target: "sv::memorylayer::default"}
//	httpClient := &http.Client{Transport: rt}
//	resp, err := httpClient.Get("http://ignored/v1/memories/abc")
type AetherRoundTripper struct {
	// Client is the connected Aether client used for transport.
	Client *BaseClient

	// Target is the Aether service topic (e.g. "sv::memorylayer::default" or
	// wildcard "sv::memorylayer").
	Target string

	// Backend optionally pins requests to a named terminator backend. The
	// backend's allow-list still applies; this field selects which backend's
	// ACL is consulted, not whether the request is allowed.
	Backend string
}

// RoundTrip executes the HTTP request via the Aether proxy and returns the
// response.
func (rt *AetherRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var opts []ProxyOpt
	if rt.Backend != "" {
		opts = append(opts, WithBackend(rt.Backend))
	}
	return rt.Client.ProxyHTTP(req.Context(), rt.Target, req, opts...)
}

// =============================================================================
// OBO context key and helper
// =============================================================================

type oboContextKey struct{}

// WithOBOAuthorization returns a copy of ctx carrying an OBO AuthorizationContext
// that ProxyHTTP will inject into outgoing proxy requests.
func WithOBOAuthorization(ctx context.Context, auth *pb.AuthorizationContext) context.Context {
	return context.WithValue(ctx, oboContextKey{}, auth)
}

// oboFromContext retrieves an OBO AuthorizationContext from the context, or nil.
func oboFromContext(ctx context.Context) *pb.AuthorizationContext {
	if ctx == nil {
		return nil
	}
	v := ctx.Value(oboContextKey{})
	if v == nil {
		return nil
	}
	return v.(*pb.AuthorizationContext)
}
