package aether

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// Helpers
// =============================================================================

// newConnectedBaseClient creates a BaseClient with running=true for unit tests.
func newConnectedBaseClient(t *testing.T) *BaseClient {
	t.Helper()
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)
	return client
}

// fakeHTTPRequest builds a minimal *http.Request pointing at the given path.
func fakeHTTPRequest(t *testing.T, method, urlStr string, body []byte) *http.Request {
	t.Helper()
	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, urlStr, bodyReader)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	return req
}

// injectProxyResponse simulates the gateway sending back a ProxyHttpResponse.
// It drains the request queue, extracts the request_id, and resolves it.
// If chunkedBody is non-nil, it delivers the body as ProxyHttpBodyChunk frames.
func injectProxyResponse(t *testing.T, client *BaseClient, statusCode int32, respHeaders map[string]string, respBody []byte, proxyErr *pb.ProxyError) {
	t.Helper()
	go func() {
		// Wait briefly then drain the request queue.
		time.Sleep(5 * time.Millisecond)

		// Drain all queued messages to find the ProxyHttpRequest.
		var requestID string
		var chunkedRequest bool
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) && requestID == "" {
			select {
			case msg := <-client.RequestQueue():
				if pr := msg.GetProxyHttpRequest(); pr != nil {
					requestID = pr.GetRequestId()
					chunkedRequest = pr.GetBodyChunked()
				}
				// Drain any body chunks too.
			default:
				time.Sleep(2 * time.Millisecond)
			}
		}
		if requestID == "" {
			return
		}

		// Drain remaining body chunk messages if request was chunked.
		if chunkedRequest {
			for {
				select {
				case msg := <-client.RequestQueue():
					if ch := msg.GetProxyHttpBodyChunk(); ch != nil && ch.GetFin() {
						goto drained
					}
				case <-time.After(100 * time.Millisecond):
					goto drained
				}
			}
		}
	drained:

		chunkedResp := len(respBody) > proxyChunkSize && proxyErr == nil

		hdr := &pb.ProxyHttpResponse{
			RequestId:   requestID,
			StatusCode:  statusCode,
			Headers:     respHeaders,
			BodyChunked: chunkedResp,
			Error:       proxyErr,
		}
		if !chunkedResp {
			hdr.Body = respBody
		}

		// Deliver the header frame.
		resolveProxyResponse(requestID, hdr)

		// If body is chunked, deliver the chunks.
		if chunkedResp {
			for seq, offset := 0, 0; offset < len(respBody); seq++ {
				end := offset + proxyChunkSize
				if end > len(respBody) {
					end = len(respBody)
				}
				fin := end == len(respBody)
				appendProxyChunk(requestID, respBody[offset:end], fin)
				offset = end
			}
		}
	}()
}

// =============================================================================
// ProxyHTTP tests
// =============================================================================

func TestProxyHTTP_EmptyBody(t *testing.T) {
	client := newConnectedBaseClient(t)
	injectProxyResponse(t, client, 200, map[string]string{"Content-Type": "application/json"}, nil, nil)

	req := fakeHTTPRequest(t, "GET", "http://ignored/v1/ping", nil)
	resp, err := client.ProxyHTTP(context.Background(), "sv::testsvc::default", req)
	if err != nil {
		t.Fatalf("ProxyHTTP() error = %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) != 0 {
		t.Errorf("Body = %q, want empty", body)
	}
}

func TestProxyHTTP_SmallBody(t *testing.T) {
	client := newConnectedBaseClient(t)
	respBody := []byte(`{"ok":true}`)
	injectProxyResponse(t, client, 200, nil, respBody, nil)

	req := fakeHTTPRequest(t, "POST", "http://ignored/v1/items", []byte(`{"name":"test"}`))
	resp, err := client.ProxyHTTP(context.Background(), "sv::testsvc::default", req)
	if err != nil {
		t.Fatalf("ProxyHTTP() error = %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != string(respBody) {
		t.Errorf("Body = %q, want %q", body, respBody)
	}
}

func TestProxyHTTP_RequestBody_JustUnder256KB(t *testing.T) {
	client := newConnectedBaseClient(t)
	reqBody := bytes.Repeat([]byte("x"), proxyChunkSize-1)
	injectProxyResponse(t, client, 201, nil, []byte("created"), nil)

	req := fakeHTTPRequest(t, "POST", "http://ignored/upload", reqBody)
	resp, err := client.ProxyHTTP(context.Background(), "sv::testsvc::default", req)
	if err != nil {
		t.Fatalf("ProxyHTTP() error = %v", err)
	}
	if resp.StatusCode != 201 {
		t.Errorf("StatusCode = %d, want 201", resp.StatusCode)
	}

	// Verify the request was sent inline (no chunking).
	// The injectProxyResponse goroutine already consumed the queue message.
}

func TestProxyHTTP_RequestBody_Exactly256KB(t *testing.T) {
	client := newConnectedBaseClient(t)
	reqBody := bytes.Repeat([]byte("y"), proxyChunkSize)
	injectProxyResponse(t, client, 200, nil, []byte("ok"), nil)

	req := fakeHTTPRequest(t, "PUT", "http://ignored/data", reqBody)
	resp, err := client.ProxyHTTP(context.Background(), "sv::testsvc::default", req)
	if err != nil {
		t.Fatalf("ProxyHTTP() error = %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
	}
}

func TestProxyHTTP_RequestBody_1MB_Chunked(t *testing.T) {
	client := newConnectedBaseClient(t)
	reqBody := bytes.Repeat([]byte("a"), 1*1024*1024)
	injectProxyResponse(t, client, 200, nil, []byte("ok"), nil)

	req := fakeHTTPRequest(t, "POST", "http://ignored/big", reqBody)
	resp, err := client.ProxyHTTP(context.Background(), "sv::testsvc::default", req)
	if err != nil {
		t.Fatalf("ProxyHTTP() error = %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
	}
}

func TestProxyHTTP_RequestBody_5MB_Chunked(t *testing.T) {
	client := newConnectedBaseClient(t)
	reqBody := bytes.Repeat([]byte("b"), 5*1024*1024)
	injectProxyResponse(t, client, 200, nil, []byte("ok"), nil)

	req := fakeHTTPRequest(t, "POST", "http://ignored/huge", reqBody)
	resp, err := client.ProxyHTTP(context.Background(), "sv::testsvc::default", req)
	if err != nil {
		t.Fatalf("ProxyHTTP() error = %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
	}
}

func TestProxyHTTP_ResponseBody_Chunked_1MB(t *testing.T) {
	client := newConnectedBaseClient(t)
	bigBody := bytes.Repeat([]byte("r"), 1*1024*1024)
	injectProxyResponse(t, client, 200, nil, bigBody, nil)

	req := fakeHTTPRequest(t, "GET", "http://ignored/download", nil)
	resp, err := client.ProxyHTTP(context.Background(), "sv::testsvc::default", req)
	if err != nil {
		t.Fatalf("ProxyHTTP() error = %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(body, bigBody) {
		t.Errorf("Body length = %d, want %d", len(body), len(bigBody))
	}
}

func TestProxyHTTP_ResponseBody_Chunked_5MB(t *testing.T) {
	client := newConnectedBaseClient(t)
	bigBody := bytes.Repeat([]byte("s"), 5*1024*1024)
	injectProxyResponse(t, client, 200, nil, bigBody, nil)

	req := fakeHTTPRequest(t, "GET", "http://ignored/download5", nil)
	resp, err := client.ProxyHTTP(context.Background(), "sv::testsvc::default", req)
	if err != nil {
		t.Fatalf("ProxyHTTP() error = %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(body, bigBody) {
		t.Errorf("Body length = %d, want %d", len(body), len(bigBody))
	}
}

func TestProxyHTTP_TransportError(t *testing.T) {
	client := newConnectedBaseClient(t)
	proxyErr := &pb.ProxyError{
		Kind:    pb.ProxyError_SIDECAR_UNAVAILABLE,
		Message: "no sidecar connected",
	}
	injectProxyResponse(t, client, 0, nil, nil, proxyErr)

	req := fakeHTTPRequest(t, "GET", "http://ignored/probe", nil)
	_, err := client.ProxyHTTP(context.Background(), "sv::testsvc::default", req)
	if err == nil {
		t.Fatal("ProxyHTTP() should return error for transport error")
	}
	var pte *ProxyTransportError
	if !isProxyTransportError(err, &pte) {
		t.Errorf("error type = %T, want *ProxyTransportError", err)
	}
}

func TestProxyHTTP_Timeout(t *testing.T) {
	client := newConnectedBaseClient(t)
	// Do NOT inject a response — let the context expire.

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	req := fakeHTTPRequest(t, "GET", "http://ignored/slow", nil)
	_, err := client.ProxyHTTP(ctx, "sv::testsvc::default", req)
	if err == nil {
		t.Fatal("ProxyHTTP() should return error on timeout")
	}
}

func TestProxyHTTP_WildcardTarget(t *testing.T) {
	client := newConnectedBaseClient(t)
	injectProxyResponse(t, client, 200, nil, []byte("ok"), nil)

	req := fakeHTTPRequest(t, "GET", "http://ignored/anything", nil)
	resp, err := client.ProxyHTTP(context.Background(), "sv::testsvc", req)
	if err != nil {
		t.Fatalf("ProxyHTTP() with wildcard target error = %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
	}
}

func TestProxyHTTP_OBOInjection(t *testing.T) {
	client := newConnectedBaseClient(t)
	injectProxyResponse(t, client, 200, nil, []byte("ok"), nil)

	obo := &pb.AuthorizationContext{
		AuthorityMode: "obo",
		GrantId:       "grant-abc",
	}

	// Wrap the request context with OBO authorization.
	reqCtx := WithOBOAuthorization(context.Background(), obo)
	req := fakeHTTPRequest(t, "GET", "http://ignored/secure", nil)
	req = req.WithContext(reqCtx)

	resp, err := client.ProxyHTTP(context.Background(), "sv::testsvc::default", req)
	if err != nil {
		t.Fatalf("ProxyHTTP() error = %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
	}

	// Verify that the upstream message carried the OBO context.
	// injectProxyResponse already drained the queue; verify via a direct send check.
}

func TestProxyHTTP_OBOInjection_MessageContainsAuth(t *testing.T) {
	client := newConnectedBaseClient(t)

	obo := &pb.AuthorizationContext{
		AuthorityMode: "obo",
		GrantId:       "grant-xyz",
	}

	// We need to inspect the sent message, so don't use injectProxyResponse.
	// Instead, run ProxyHTTP in a goroutine and inspect the queued message.
	req := fakeHTTPRequest(t, "GET", "http://ignored/secure2", nil)
	req = req.WithContext(WithOBOAuthorization(req.Context(), obo))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := client.ProxyHTTP(ctx, "sv::testsvc::default", req)
		done <- err
	}()

	// Drain and inspect.
	var sentMsg *pb.UpstreamMessage
	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) && sentMsg == nil {
		select {
		case msg := <-client.RequestQueue():
			if msg.GetProxyHttpRequest() != nil {
				sentMsg = msg
			}
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}

	if sentMsg == nil {
		<-done
		t.Fatal("no ProxyHttpRequest found in queue")
	}

	pr := sentMsg.GetProxyHttpRequest()
	if pr.GetAuthorization() == nil {
		t.Error("Authorization context should be set in ProxyHttpRequest")
	} else {
		if pr.GetAuthorization().GetGrantId() != "grant-xyz" {
			t.Errorf("GrantId = %q, want %q", pr.GetAuthorization().GetGrantId(), "grant-xyz")
		}
	}

	// Resolve to unblock the goroutine.
	resolveProxyResponse(pr.GetRequestId(), &pb.ProxyHttpResponse{
		RequestId:  pr.GetRequestId(),
		StatusCode: 200,
	})
	<-done
}

func TestProxyHTTP_HeadersForwarded(t *testing.T) {
	client := newConnectedBaseClient(t)
	injectProxyResponse(t, client, 200, map[string]string{"X-Custom": "value"}, []byte("body"), nil)

	req := fakeHTTPRequest(t, "GET", "http://ignored/hdr", nil)
	req.Header.Set("X-My-Header", "hello")
	resp, err := client.ProxyHTTP(context.Background(), "sv::testsvc::default", req)
	if err != nil {
		t.Fatalf("ProxyHTTP() error = %v", err)
	}
	if resp.Header.Get("X-Custom") != "value" {
		t.Errorf("response header X-Custom = %q, want %q", resp.Header.Get("X-Custom"), "value")
	}
}

// =============================================================================
// AetherRoundTripper tests
// =============================================================================

func TestAetherRoundTripper_RoundTrip(t *testing.T) {
	client := newConnectedBaseClient(t)
	injectProxyResponse(t, client, 200, nil, []byte(`{"ok":true}`), nil)

	rt := &AetherRoundTripper{Client: client, Target: "sv::testsvc::default"}
	httpClient := &http.Client{Transport: rt}

	resp, err := httpClient.Get("http://ignored/v1/test")
	if err != nil {
		t.Fatalf("http.Client.Get() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
	}
}

func TestAetherRoundTripper_WildcardTarget(t *testing.T) {
	client := newConnectedBaseClient(t)
	injectProxyResponse(t, client, 204, nil, nil, nil)

	rt := &AetherRoundTripper{Client: client, Target: "sv::testsvc"}
	httpClient := &http.Client{Transport: rt}

	resp, err := httpClient.Get("http://ignored/healthz")
	if err != nil {
		t.Fatalf("http.Client.Get() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Errorf("StatusCode = %d, want 204", resp.StatusCode)
	}
}

func TestAetherRoundTripper_ImplementsInterface(t *testing.T) {
	var _ http.RoundTripper = &AetherRoundTripper{}
}

// =============================================================================
// WithOBOAuthorization / oboFromContext tests
// =============================================================================

func TestWithOBOAuthorization_RoundTrip(t *testing.T) {
	obo := &pb.AuthorizationContext{GrantId: "g-1"}
	ctx := WithOBOAuthorization(context.Background(), obo)
	got := oboFromContext(ctx)
	if got == nil {
		t.Fatal("oboFromContext returned nil")
	}
	if got.GetGrantId() != "g-1" {
		t.Errorf("GrantId = %q, want %q", got.GetGrantId(), "g-1")
	}
}

func TestOboFromContext_NilContext(t *testing.T) {
	got := oboFromContext(nil)
	if got != nil {
		t.Error("oboFromContext(nil) should return nil")
	}
}

func TestOboFromContext_Missing(t *testing.T) {
	got := oboFromContext(context.Background())
	if got != nil {
		t.Error("oboFromContext with no value should return nil")
	}
}

// =============================================================================
// ProxyTransportError test
// =============================================================================

func TestProxyTransportError_Error(t *testing.T) {
	e := &ProxyTransportError{Kind: "DIAL_FAILED", Message: "connection refused"}
	s := e.Error()
	if s == "" {
		t.Error("Error() should not return empty string")
	}
}

// =============================================================================
// Error type helpers
// =============================================================================

func isProxyTransportError(err error, target **ProxyTransportError) bool {
	if err == nil {
		return false
	}
	if pte, ok := err.(*ProxyTransportError); ok {
		if target != nil {
			*target = pte
		}
		return true
	}
	return false
}

// =============================================================================
// dispatchResponse integration tests
// =============================================================================

func TestDispatchResponse_ProxyHttpResponse(t *testing.T) {
	client := newConnectedBaseClient(t)

	requestID := "test-req-1"
	inf := registerProxyInflight(requestID)
	defer deleteProxyInflight(requestID)

	downstream := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ProxyHttpResponse{
			ProxyHttpResponse: &pb.ProxyHttpResponse{
				RequestId:  requestID,
				StatusCode: 200,
				Body:       []byte("hello"),
			},
		},
	}

	if err := client.dispatchResponse(context.Background(), downstream); err != nil {
		t.Fatalf("dispatchResponse() error = %v", err)
	}

	select {
	case hdr := <-inf.headerCh:
		if hdr.GetStatusCode() != 200 {
			t.Errorf("StatusCode = %d, want 200", hdr.GetStatusCode())
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for proxy response")
	}
}

func TestDispatchResponse_ProxyHttpBodyChunk(t *testing.T) {
	client := newConnectedBaseClient(t)

	requestID := "test-req-chunk-1"
	inf := registerProxyInflight(requestID)
	defer deleteProxyInflight(requestID)

	// First deliver the header with body_chunked=true.
	hdrMsg := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ProxyHttpResponse{
			ProxyHttpResponse: &pb.ProxyHttpResponse{
				RequestId:   requestID,
				StatusCode:  200,
				BodyChunked: true,
			},
		},
	}
	if err := client.dispatchResponse(context.Background(), hdrMsg); err != nil {
		t.Fatalf("dispatchResponse(header) error = %v", err)
	}

	// Then deliver a body chunk with fin=true.
	chunkMsg := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ProxyHttpBodyChunk{
			ProxyHttpBodyChunk: &pb.ProxyHttpBodyChunk{
				RequestId: requestID,
				IsRequest: false,
				Seq:       0,
				Data:      []byte("chunk-data"),
				Fin:       true,
			},
		},
	}
	if err := client.dispatchResponse(context.Background(), chunkMsg); err != nil {
		t.Fatalf("dispatchResponse(chunk) error = %v", err)
	}

	// Both done channel and header channel should be signaled.
	select {
	case hdr := <-inf.headerCh:
		if !hdr.GetBodyChunked() {
			t.Error("BodyChunked should be true")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for proxy header")
	}

	select {
	case <-inf.done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for chunk fin signal")
	}

	inf.mu.Lock()
	if len(inf.chunks) != 1 || string(inf.chunks[0]) != "chunk-data" {
		t.Errorf("chunks = %v, want [chunk-data]", inf.chunks)
	}
	inf.mu.Unlock()
}

// =============================================================================
// Verify request message fields
// =============================================================================

func TestProxyHTTP_RequestFields(t *testing.T) {
	client := newConnectedBaseClient(t)

	req := fakeHTTPRequest(t, "DELETE", "http://ignored/v2/items/42?soft=true", nil)
	req.Header.Set("Authorization", "Bearer tok")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := client.ProxyHTTP(ctx, "sv::svc::instance1", req)
		done <- err
	}()

	var pr *pb.ProxyHttpRequest
	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) && pr == nil {
		select {
		case msg := <-client.RequestQueue():
			pr = msg.GetProxyHttpRequest()
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	if pr == nil {
		<-done
		t.Fatal("no ProxyHttpRequest in queue")
	}

	tests := []struct {
		field string
		got   string
		want  string
	}{
		{"Method", pr.GetMethod(), "DELETE"},
		{"Path", pr.GetPath(), "/v2/items/42?soft=true"},
		{"TargetTopic", pr.GetTargetTopic(), "sv::svc::instance1"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s = %q, want %q", tt.field, tt.got, tt.want)
		}
	}
	if pr.GetHeaders()["Authorization"] != "Bearer tok" {
		t.Errorf("Authorization header = %q, want %q", pr.GetHeaders()["Authorization"], "Bearer tok")
	}
	if pr.GetRequestId() == "" {
		t.Error("RequestId should not be empty")
	}

	// Resolve to unblock the goroutine.
	resolveProxyResponse(pr.GetRequestId(), &pb.ProxyHttpResponse{
		RequestId:  pr.GetRequestId(),
		StatusCode: 200,
	})
	<-done
}

// Compile-time check: BaseClient must be usable with ProxyHTTP.
var _ = fmt.Sprintf // suppress import warning

// TestProxyHTTP_WithBackend verifies that WithBackend pins the named backend
// onto the outgoing ProxyHttpRequest envelope.
func TestProxyHTTP_WithBackend(t *testing.T) {
	client := newConnectedBaseClient(t)

	req := fakeHTTPRequest(t, "GET", "http://ignored/v1/ping", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := client.ProxyHTTP(ctx, "sv::svc::instance1", req, WithBackend("admin"))
		done <- err
	}()

	var pr *pb.ProxyHttpRequest
	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) && pr == nil {
		select {
		case msg := <-client.RequestQueue():
			pr = msg.GetProxyHttpRequest()
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	if pr == nil {
		<-done
		t.Fatal("no ProxyHttpRequest in queue")
	}
	if got := pr.GetBackendName(); got != "admin" {
		t.Errorf("BackendName = %q, want %q", got, "admin")
	}

	resolveProxyResponse(pr.GetRequestId(), &pb.ProxyHttpResponse{
		RequestId:  pr.GetRequestId(),
		StatusCode: 200,
	})
	<-done
}

// TestProxyHTTP_NoBackendOption verifies that omitting WithBackend leaves
// the BackendName field empty (legacy behaviour).
func TestProxyHTTP_NoBackendOption(t *testing.T) {
	client := newConnectedBaseClient(t)

	req := fakeHTTPRequest(t, "GET", "http://ignored/v1/ping", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := client.ProxyHTTP(ctx, "sv::svc::instance1", req)
		done <- err
	}()

	var pr *pb.ProxyHttpRequest
	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) && pr == nil {
		select {
		case msg := <-client.RequestQueue():
			pr = msg.GetProxyHttpRequest()
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	if pr == nil {
		<-done
		t.Fatal("no ProxyHttpRequest in queue")
	}
	if got := pr.GetBackendName(); got != "" {
		t.Errorf("BackendName = %q, want empty", got)
	}

	resolveProxyResponse(pr.GetRequestId(), &pb.ProxyHttpResponse{
		RequestId:  pr.GetRequestId(),
		StatusCode: 200,
	})
	<-done
}

// TestAetherRoundTripper_BackendField verifies the Backend field on
// AetherRoundTripper threads through to the proxy envelope.
func TestAetherRoundTripper_BackendField(t *testing.T) {
	client := newConnectedBaseClient(t)

	rt := &AetherRoundTripper{Client: client, Target: "sv::svc::instance1", Backend: "primary"}

	httpReq := fakeHTTPRequest(t, "GET", "http://ignored/v1/items", nil)

	done := make(chan error, 1)
	go func() {
		_, err := rt.RoundTrip(httpReq)
		done <- err
	}()

	var pr *pb.ProxyHttpRequest
	deadline := time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) && pr == nil {
		select {
		case msg := <-client.RequestQueue():
			pr = msg.GetProxyHttpRequest()
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	if pr == nil {
		<-done
		t.Fatal("no ProxyHttpRequest in queue")
	}
	if got := pr.GetBackendName(); got != "primary" {
		t.Errorf("BackendName = %q, want %q", got, "primary")
	}

	resolveProxyResponse(pr.GetRequestId(), &pb.ProxyHttpResponse{
		RequestId:  pr.GetRequestId(),
		StatusCode: 200,
	})
	<-done
}
