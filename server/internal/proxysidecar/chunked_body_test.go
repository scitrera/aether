package proxysidecar

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// waitForFinChunkedResponse polls transport until the assembled chunked-response
// arrives — i.e. at least one ProxyHttpResponse header has landed and either
// the header carries an inline error or the last body chunk has fin=true.
// Returns true on completion within deadline, false on timeout. Used by tests
// that drive handleChunkedRequestFrame to wait for the async fin-dispatch
// goroutine spawned by terminator.go.
func waitForFinChunkedResponse(transport *fakeTransport, deadline time.Duration) bool {
	until := time.Now().Add(deadline)
	for time.Now().Before(until) {
		transport.mu.Lock()
		done := false
		if len(transport.httpResps) > 0 {
			header := transport.httpResps[0]
			if header.GetError() != nil || !header.GetBodyChunked() {
				done = true
			} else if len(transport.httpChunks) > 0 && transport.httpChunks[len(transport.httpChunks)-1].GetFin() {
				done = true
			}
		}
		transport.mu.Unlock()
		if done {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// TestTerminator_ChunkedRequest_Reassembly_RoundTrip asserts a chunked-body
// request (header + N chunks with fin) reaches the backend with the body
// reconstructed byte-equal to the source.
func TestTerminator_ChunkedRequest_Reassembly_RoundTrip(t *testing.T) {
	t.Parallel()

	var received atomic.Pointer[[]byte]
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bcopy := make([]byte, len(body))
		copy(bcopy, body)
		received.Store(&bcopy)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(200)
		_, _ = w.Write(body)
	}))
	defer mock.Close()

	cfg := terminatorTestConfig()
	cfg.Terminator.Backends[0].URL = mock.URL
	cfg.Terminator.Backends[0].MaxBodyBytes = 4 << 20 // 4 MiB

	t1, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	const total = 1 << 20 // 1 MiB
	source := make([]byte, total)
	if _, err := rand.Read(source); err != nil {
		t.Fatalf("rand: %v", err)
	}

	transport := newFakeTransport()
	req := &pb.ProxyHttpRequest{
		RequestId:   "rid-chunked-up",
		Method:      "POST",
		Path:        "/ingest",
		BodyChunked: true,
	}
	if err := t1.beginChunkedRequest(req, transport); err != nil {
		t.Fatalf("beginChunkedRequest: %v", err)
	}

	const chunkSize = 200 * 1024
	for offset, seq := 0, uint32(0); offset < len(source); seq++ {
		end := offset + chunkSize
		if end > len(source) {
			end = len(source)
		}
		fin := end == len(source)
		chunk := &pb.ProxyHttpBodyChunk{
			RequestId: "rid-chunked-up",
			IsRequest: true,
			Seq:       seq,
			Data:      source[offset:end],
			Fin:       fin,
		}
		if err := t1.handleChunkedRequestFrame(context.Background(), chunk, transport); err != nil {
			t.Fatalf("handleChunkedRequestFrame: %v", err)
		}
		offset = end
	}

	// handleChunkedRequestFrame on a fin chunk now hands the assembled-body
	// dispatch to a goroutine so the SDK receive loop isn't held for the
	// backend response lifetime. Wait for the dispatch to land + the
	// response chunks to fully assemble before asserting.
	if !waitForFinChunkedResponse(transport, 5*time.Second) {
		t.Fatal("fin-dispatch did not produce a complete chunked response within 5s")
	}

	got := received.Load()
	if got == nil {
		t.Fatal("backend never received the request")
	}
	if !bytes.Equal(*got, source) {
		t.Fatalf("backend body length=%d does not byte-match source length=%d", len(*got), len(source))
	}

	// Response: small body should ride inline, no chunk frames emitted.
	transport.mu.Lock()
	resps := append([]*pb.ProxyHttpResponse(nil), transport.httpResps...)
	chunks := append([]*pb.ProxyHttpBodyChunk(nil), transport.httpChunks...)
	transport.mu.Unlock()
	if len(resps) != 1 {
		t.Fatalf("expected 1 response header frame, got %d", len(resps))
	}
	// 1 MiB > 256 KiB, so response should be chunked.
	if !resps[0].GetBodyChunked() {
		t.Errorf("expected chunked response (body=%d > %d), got inline", total, proxyResponseChunkSize)
	}
	if len(chunks) == 0 {
		t.Fatal("expected response chunk frames for >256 KiB body")
	}
	// Concatenate response chunks and assert byte-equal.
	var assembled []byte
	for _, c := range chunks {
		assembled = append(assembled, c.GetData()...)
	}
	if !bytes.Equal(assembled, source) {
		t.Errorf("response body did not survive round-trip")
	}
	if !chunks[len(chunks)-1].GetFin() {
		t.Error("expected final chunk to set fin=true")
	}
}

// TestTerminator_ChunkedRequest_OversizeBody_PayloadTooLarge asserts that
// a chunked upload exceeding the backend's MaxBodyBytes is short-circuited
// with a PAYLOAD_TOO_LARGE response and no further accumulation.
func TestTerminator_ChunkedRequest_OversizeBody_PayloadTooLarge(t *testing.T) {
	t.Parallel()

	cfg := terminatorTestConfig()
	cfg.Terminator.Backends[0].MaxBodyBytes = 1024

	t1, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	transport := newFakeTransport()
	req := &pb.ProxyHttpRequest{
		RequestId:   "rid-too-big",
		Method:      "POST",
		Path:        "/ingest",
		BodyChunked: true,
	}
	if err := t1.beginChunkedRequest(req, transport); err != nil {
		t.Fatalf("beginChunkedRequest: %v", err)
	}
	// Single chunk that exceeds the cap.
	chunk := &pb.ProxyHttpBodyChunk{
		RequestId: "rid-too-big",
		IsRequest: true,
		Data:      make([]byte, 4096),
		Fin:       true,
	}
	if err := t1.handleChunkedRequestFrame(context.Background(), chunk, transport); err != nil {
		t.Fatalf("handleChunkedRequestFrame: %v", err)
	}

	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.httpResps) != 1 {
		t.Fatalf("expected exactly 1 ProxyHttpResponse, got %d", len(transport.httpResps))
	}
	if got := transport.httpResps[0].GetError().GetKind(); got != pb.ProxyError_PAYLOAD_TOO_LARGE {
		t.Errorf("expected PAYLOAD_TOO_LARGE, got %v", got)
	}
}

// TestTerminator_ChunkedRequest_BeginRejectsUnknownPath asserts that
// beginChunkedRequest emits an ACL_DENIED ProxyHttpResponse when the
// caller's method/path does not match any backend allow-list.
func TestTerminator_ChunkedRequest_BeginRejectsUnknownPath(t *testing.T) {
	t.Parallel()

	cfg := terminatorTestConfig()
	cfg.Terminator.Backends[0].AllowPaths = []string{"/v1/*"}

	t1, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	transport := newFakeTransport()
	req := &pb.ProxyHttpRequest{
		RequestId:   "rid-rej",
		Method:      "POST",
		Path:        "/admin/secrets",
		BodyChunked: true,
	}
	if err := t1.beginChunkedRequest(req, transport); err != nil {
		t.Fatalf("beginChunkedRequest: %v", err)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.httpResps) != 1 {
		t.Fatalf("expected 1 ProxyHttpResponse, got %d", len(transport.httpResps))
	}
	if got := transport.httpResps[0].GetError().GetKind(); got != pb.ProxyError_ACL_DENIED {
		t.Errorf("expected ACL_DENIED, got %v", got)
	}
}

// TestTerminator_ChunkedFin_HandlerReturnsQuickly proves that the fin-chunk
// branch of handleChunkedRequestFrame is goroutine-isolated: even when the
// backend response takes a long time, the handler returns to the SDK
// receive loop promptly. This is the regression guard for the receive-loop
// wedge — before the goroutine hand-off, a slow chunked-upload dispatch
// blocked the loop and starved every other envelope on the connection.
func TestTerminator_ChunkedFin_HandlerReturnsQuickly(t *testing.T) {
	t.Parallel()

	const backendDelay = 500 * time.Millisecond
	const handlerBudget = 50 * time.Millisecond

	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Drain body to avoid client-side block while we sleep, then
		// stall before producing a response. The handler should NOT
		// observe this sleep.
		_, _ = io.ReadAll(r.Body)
		time.Sleep(backendDelay)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
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
		RequestId:   "rid-fast-return",
		Method:      "POST",
		Path:        "/ingest",
		BodyChunked: true,
	}
	if err := t1.beginChunkedRequest(req, transport); err != nil {
		t.Fatalf("beginChunkedRequest: %v", err)
	}

	// Single fin chunk — the handler call below is the one whose return
	// latency we care about.
	chunk := &pb.ProxyHttpBodyChunk{
		RequestId: "rid-fast-return",
		IsRequest: true,
		Seq:       0,
		Data:      []byte("payload"),
		Fin:       true,
	}
	start := time.Now()
	if err := t1.handleChunkedRequestFrame(context.Background(), chunk, transport); err != nil {
		t.Fatalf("handleChunkedRequestFrame: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > handlerBudget {
		t.Fatalf("handleChunkedRequestFrame on fin took %s, want <%s — fin-dispatch is wedging the receive loop",
			elapsed, handlerBudget)
	}

	// The response itself should still land (eventually) — verify the
	// async dispatch is actually doing the work, not silently dropping it.
	if !waitForFinChunkedResponse(transport, backendDelay+2*time.Second) {
		t.Fatal("async fin-dispatch never produced a response — dispatch lost?")
	}
}
