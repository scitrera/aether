package proxysidecar

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// TestTerminator_StreamResponse_DrainsAllChunks_OnEOF asserts the streaming
// path delivers every byte the backend writes and emits a terminating fin
// chunk when the backend body closes cleanly.
func TestTerminator_StreamResponse_DrainsAllChunks_OnEOF(t *testing.T) {
	t.Parallel()

	const eventCount = 100
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < eventCount; i++ {
			fmt.Fprintf(w, "event-%d\n", i)
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(2 * time.Millisecond)
		}
	}))
	defer mock.Close()

	cfg := terminatorTestConfig()
	cfg.Terminator.Backends[0].URL = mock.URL
	cfg.Terminator.Backends[0].MaxBodyBytes = 10 << 20

	t1, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	transport := newFakeTransport()
	req := &pb.ProxyHttpRequest{
		RequestId:                  "rid-stream-eof",
		Method:                     "GET",
		Path:                       "/events",
		StreamResponseIndefinitely: true,
		StreamIdleTimeoutMs:        5000,
	}
	if err := t1.dispatchAndRespond(context.Background(), req, nil, transport); err != nil {
		t.Fatalf("dispatchAndRespond: %v", err)
	}

	transport.mu.Lock()
	resps := append([]*pb.ProxyHttpResponse(nil), transport.httpResps...)
	chunks := append([]*pb.ProxyHttpBodyChunk(nil), transport.httpChunks...)
	transport.mu.Unlock()

	if len(resps) != 1 {
		t.Fatalf("expected exactly 1 header response, got %d", len(resps))
	}
	if !resps[0].GetBodyChunked() {
		t.Errorf("expected body_chunked=true on streaming header")
	}
	if resps[0].GetError() != nil {
		t.Errorf("unexpected ProxyError on streaming header: %s", resps[0].GetError().GetMessage())
	}
	if len(chunks) == 0 {
		t.Fatal("expected at least one body chunk")
	}
	if !chunks[len(chunks)-1].GetFin() {
		t.Errorf("expected final chunk to set fin=true on EOF")
	}
	var assembled []byte
	for _, c := range chunks {
		assembled = append(assembled, c.GetData()...)
	}
	for i := 0; i < eventCount; i++ {
		needle := []byte(fmt.Sprintf("event-%d\n", i))
		if !bytes.Contains(assembled, needle) {
			t.Errorf("missing event %d in stream", i)
			break
		}
	}
}

// TestTerminator_StreamResponse_IdleTimeout_ClosesWithTimeout asserts that a
// stalled backend closes the stream with ProxyError{TIMEOUT} after
// stream_idle_timeout_ms with no bytes flowing.
func TestTerminator_StreamResponse_IdleTimeout_ClosesWithTimeout(t *testing.T) {
	t.Parallel()

	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		// Send one byte so TTFB resolves, then stall.
		fmt.Fprint(w, "x")
		if flusher != nil {
			flusher.Flush()
		}
		time.Sleep(2 * time.Second)
	}))
	defer mock.Close()

	cfg := terminatorTestConfig()
	cfg.Terminator.Backends[0].URL = mock.URL
	cfg.Terminator.Backends[0].MaxBodyBytes = 10 << 20

	t1, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	transport := newFakeTransport()
	req := &pb.ProxyHttpRequest{
		RequestId:                  "rid-stream-idle",
		Method:                     "GET",
		Path:                       "/events",
		StreamResponseIndefinitely: true,
		StreamIdleTimeoutMs:        100, // 100ms idle deadline
	}
	start := time.Now()
	if err := t1.dispatchAndRespond(context.Background(), req, nil, transport); err != nil {
		t.Fatalf("dispatchAndRespond: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 1500*time.Millisecond {
		t.Errorf("idle close took too long: %v", elapsed)
	}

	transport.mu.Lock()
	resps := append([]*pb.ProxyHttpResponse(nil), transport.httpResps...)
	transport.mu.Unlock()

	if len(resps) < 2 {
		t.Fatalf("expected header + terminal error response, got %d", len(resps))
	}
	terminal := resps[len(resps)-1]
	if terminal.GetError() == nil {
		t.Fatalf("expected terminal ProxyError, got status=%d", terminal.GetStatusCode())
	}
	if got := terminal.GetError().GetKind(); got != pb.ProxyError_TIMEOUT {
		t.Errorf("expected TIMEOUT, got %s", got)
	}
}

// TestTerminator_StreamResponse_MaxBytes_ClosesWithPayloadTooLarge asserts
// that a backend exceeding max_response_body_bytes mid-stream is closed with
// PAYLOAD_TOO_LARGE and that bytes received before the cap survive.
func TestTerminator_StreamResponse_MaxBytes_ClosesWithPayloadTooLarge(t *testing.T) {
	t.Parallel()

	const totalBytes = 64 << 10 // 64 KiB
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		buf := bytes.Repeat([]byte("A"), 4096)
		var written int
		for written < totalBytes {
			n, err := w.Write(buf)
			if err != nil {
				return
			}
			written += n
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(1 * time.Millisecond)
		}
	}))
	defer mock.Close()

	cfg := terminatorTestConfig()
	cfg.Terminator.Backends[0].URL = mock.URL
	cfg.Terminator.Backends[0].MaxBodyBytes = 10 << 20

	t1, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	transport := newFakeTransport()
	req := &pb.ProxyHttpRequest{
		RequestId:                  "rid-stream-cap",
		Method:                     "GET",
		Path:                       "/big",
		StreamResponseIndefinitely: true,
		StreamIdleTimeoutMs:        5_000,
		MaxResponseBodyBytes:       8 * 1024, // 8 KiB cap
	}
	if err := t1.dispatchAndRespond(context.Background(), req, nil, transport); err != nil {
		t.Fatalf("dispatchAndRespond: %v", err)
	}

	transport.mu.Lock()
	resps := append([]*pb.ProxyHttpResponse(nil), transport.httpResps...)
	chunks := append([]*pb.ProxyHttpBodyChunk(nil), transport.httpChunks...)
	transport.mu.Unlock()

	if len(resps) < 2 {
		t.Fatalf("expected header + terminal PAYLOAD_TOO_LARGE response, got %d", len(resps))
	}
	terminal := resps[len(resps)-1]
	if terminal.GetError() == nil {
		t.Fatal("expected terminal ProxyError")
	}
	if got := terminal.GetError().GetKind(); got != pb.ProxyError_PAYLOAD_TOO_LARGE {
		t.Errorf("expected PAYLOAD_TOO_LARGE, got %s", got)
	}

	// Bytes received before the cap must be preserved.
	var received int
	for _, c := range chunks {
		received += len(c.GetData())
	}
	if int64(received) > req.GetMaxResponseBodyBytes() {
		t.Errorf("emitted %d bytes, exceeds cap %d", received, req.GetMaxResponseBodyBytes())
	}
	if received == 0 {
		t.Errorf("expected partial body to be preserved, received 0 bytes")
	}
}

// TestTerminator_StreamResponse_TTFBTimeout asserts that timeout_ms governs
// time-to-first-byte only when stream_response_indefinitely=true: a backend
// that delays its first byte beyond timeout_ms is closed with TIMEOUT.
func TestTerminator_StreamResponse_TTFBTimeout(t *testing.T) {
	t.Parallel()

	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Stall before writing anything, exceeding timeout_ms.
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer mock.Close()

	cfg := terminatorTestConfig()
	cfg.Terminator.Backends[0].URL = mock.URL
	cfg.Terminator.Backends[0].MaxBodyBytes = 1 << 20

	t1, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	transport := newFakeTransport()
	req := &pb.ProxyHttpRequest{
		RequestId:                  "rid-stream-ttfb",
		Method:                     "GET",
		Path:                       "/slow",
		TimeoutMs:                  100,
		StreamResponseIndefinitely: true,
		StreamIdleTimeoutMs:        5_000,
	}
	if err := t1.dispatchAndRespond(context.Background(), req, nil, transport); err != nil {
		t.Fatalf("dispatchAndRespond: %v", err)
	}

	transport.mu.Lock()
	resps := append([]*pb.ProxyHttpResponse(nil), transport.httpResps...)
	transport.mu.Unlock()
	if len(resps) != 1 {
		t.Fatalf("expected exactly 1 (TTFB error) response, got %d", len(resps))
	}
	if resps[0].GetError() == nil {
		t.Fatal("expected ProxyError on TTFB timeout")
	}
	if got := resps[0].GetError().GetKind(); got != pb.ProxyError_TIMEOUT {
		t.Errorf("expected TIMEOUT, got %s", got)
	}
}

// TestTerminator_StreamResponse_BoundedPathUntouched asserts that without
// stream_response_indefinitely the existing buffered/chunked path is taken.
// (Regression guard for non-streaming behaviour.)
func TestTerminator_StreamResponse_BoundedPathUntouched(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer mock.Close()

	cfg := terminatorTestConfig()
	cfg.Terminator.Backends[0].URL = mock.URL

	t1, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	transport := newFakeTransport()
	req := &pb.ProxyHttpRequest{
		RequestId: "rid-bounded",
		Method:    "GET",
		Path:      "/v1/ping",
	}
	if err := t1.dispatchAndRespond(context.Background(), req, nil, transport); err != nil {
		t.Fatalf("dispatchAndRespond: %v", err)
	}

	if hits.Load() != 1 {
		t.Errorf("expected backend to be hit exactly once, got %d", hits.Load())
	}

	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.httpResps) != 1 {
		t.Fatalf("expected exactly 1 response (inline), got %d", len(transport.httpResps))
	}
	if transport.httpResps[0].GetBodyChunked() {
		t.Errorf("bounded path should not set body_chunked for small responses")
	}
	if got := string(transport.httpResps[0].GetBody()); got != `{"ok":true}` {
		t.Errorf("body: got %q, want %q", got, `{"ok":true}`)
	}
}
