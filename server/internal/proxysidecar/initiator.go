package proxysidecar

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
)

// proxyDispatcher abstracts the gateway-side ProxyHTTP call so initiator
// tests can use a fake without spinning up a real ServiceClient.
type proxyDispatcher interface {
	ProxyHTTP(ctx context.Context, target string, req *http.Request) (*http.Response, error)
}

// Initiator runs the sidecar in initiator mode. It exposes a local HTTP
// listener that forwards each inbound request through the Aether gateway
// to a configured target service topic.
//
// In v1 the initiator does not own a gateway connection — the caller injects
// a proxyDispatcher via SetDispatcher. Future relay/embedded modes can
// construct a gatewayRuntime alongside the initiator and route the dispatcher
// through it; see gateway_client.go.
type Initiator struct {
	cfg        *Config
	dispatcher proxyDispatcher
	srv        *http.Server
}

// NewInitiator builds an initiator from cfg. Use SetDispatcher to swap in
// a fake transport for tests; production callers wire a real
// AgentClient/UserClient via Run.
func NewInitiator(cfg *Config) (*Initiator, error) {
	if cfg.Initiator.Listen.Bind == "" {
		cfg.Initiator.Listen.Bind = "localhost:8888"
	}
	return &Initiator{cfg: cfg}, nil
}

// SetDispatcher swaps the proxy dispatcher. Tests use this to inject a fake
// that captures outgoing ProxyHttpRequest envelopes; production callers
// pass a connected SDK client.
func (i *Initiator) SetDispatcher(d proxyDispatcher) {
	i.dispatcher = d
}

// Run starts the local HTTP listener and blocks until ctx is cancelled.
//
// In v1 the production wiring requires the caller to inject a dispatcher via
// SetDispatcher first — the initiator does not yet open its own gateway
// connection because there is no client identity to use. A future revision
// can spawn an internal AgentClient or hook into a host process's existing
// connection.
func (i *Initiator) Run(ctx context.Context) error {
	if i.dispatcher == nil {
		return fmt.Errorf("initiator: no dispatcher configured (call SetDispatcher first)")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", i.handle)

	i.srv = &http.Server{
		Addr:              i.cfg.Initiator.Listen.Bind,
		Handler:           mux,
		ReadHeaderTimeout: 30 * time.Second,
	}

	listener, err := net.Listen("tcp", i.cfg.Initiator.Listen.Bind)
	if err != nil {
		return fmt.Errorf("initiator: listen on %s: %w", i.cfg.Initiator.Listen.Bind, err)
	}

	log.Info().
		Str("bind", i.cfg.Initiator.Listen.Bind).
		Str("target", i.cfg.Initiator.Target.Topic).
		Msg("proxy sidecar initiator listening")

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = i.srv.Shutdown(shutdownCtx)
	}()

	if err := i.srv.Serve(listener); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// ServeHTTP exposes the request handler for tests via httptest.NewServer.
func (i *Initiator) ServeHTTP(w http.ResponseWriter, r *http.Request) { i.handle(w, r) }

// handle processes a single incoming HTTP request: it copies headers and
// body, dispatches the request through the gateway, and writes the response
// back to the local client.
func (i *Initiator) handle(w http.ResponseWriter, r *http.Request) {
	if i.dispatcher == nil {
		http.Error(w, "initiator: no dispatcher configured", http.StatusServiceUnavailable)
		return
	}

	// Read the request body fully so it can be safely passed downstream.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("initiator: read body: %v", err), http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	// Build a fresh outbound request that the dispatcher reads. The host
	// in the URL is irrelevant — the dispatcher only cares about
	// method/path/headers/body — but it must be a syntactically valid URL
	// so http.NewRequest does not reject it.
	outboundURL := "http://aether-proxy" + r.URL.RequestURI()
	outbound, err := http.NewRequestWithContext(r.Context(), r.Method, outboundURL, strings.NewReader(string(body)))
	if err != nil {
		http.Error(w, fmt.Sprintf("initiator: build outbound: %v", err), http.StatusInternalServerError)
		return
	}
	for k, vs := range r.Header {
		// Drop hop-by-hop and rewrite headers; the proxy will re-mint
		// trusted X-Auth-* values on the terminator side.
		if isHopByHop(k) {
			continue
		}
		for _, v := range vs {
			outbound.Header.Add(k, v)
		}
	}

	resp, err := i.dispatcher.ProxyHTTP(r.Context(), i.cfg.Initiator.Target.Topic, outbound)
	if err != nil {
		http.Error(w, fmt.Sprintf("initiator: proxy: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Warn().Err(err).Msg("initiator: copy response body")
	}
}

// captureDispatcher is a test-only proxyDispatcher that records the most
// recent outbound proxy request and returns a configured response.
type captureDispatcher struct {
	target  string
	req     *http.Request
	body    []byte
	respFn  func(*http.Request) (*http.Response, error)
	calls   int
	lastErr error
}

func (c *captureDispatcher) ProxyHTTP(_ context.Context, target string, req *http.Request) (*http.Response, error) {
	c.calls++
	c.target = target
	c.req = req
	if req != nil && req.Body != nil {
		body, _ := io.ReadAll(req.Body)
		c.body = body
		req.Body = io.NopCloser(strings.NewReader(string(body)))
	}
	if c.respFn != nil {
		resp, err := c.respFn(req)
		c.lastErr = err
		return resp, err
	}
	return &http.Response{
		StatusCode: 200,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader("ok")),
	}, nil
}

// Encode_ProxyHttpRequest_FromHTTP is exposed for test parity: it builds a
// canonical pb.ProxyHttpRequest from an *http.Request the same way
// SDK ProxyHTTP does. Used in initiator_test.go to assert envelope shape.
func Encode_ProxyHttpRequest_FromHTTP(r *http.Request, target, requestID string, body []byte) *pb.ProxyHttpRequest {
	headers := make(map[string]string, len(r.Header))
	for k, vs := range r.Header {
		headers[k] = strings.Join(vs, ", ")
	}
	return &pb.ProxyHttpRequest{
		RequestId:   requestID,
		TargetTopic: target,
		Method:      r.Method,
		Path:        r.URL.RequestURI(),
		Headers:     headers,
		Body:        body,
	}
}
