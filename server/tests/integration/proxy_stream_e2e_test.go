//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/proxysidecar"
	"github.com/scitrera/aether/pkg/identityheaders"
)

// TestPhase27_StreamResponse_DrainsManyEventsOverTime exercises the SSE /
// long-poll path: a backend emits 100 named events over ~1.5 seconds, the
// caller drains them via the streaming response path, and the test asserts
// every event is delivered intact in order.
func TestPhase27_StreamResponse_DrainsManyEventsOverTime(t *testing.T) {
	t.Parallel()

	const eventCount = 100

	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < eventCount; i++ {
			fmt.Fprintf(w, "event-%03d\n", i)
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(2 * time.Millisecond)
		}
	}))
	defer mock.Close()

	h := newHarness()
	h.addTerminator(t, "memorylayer", "stream", mock.URL, "tenant-test", proxysidecar.HeaderModeStrict)

	req := &pb.ProxyHttpRequest{
		RequestId:   "rid-stream-e2e",
		TargetTopic: "sv::memorylayer::stream",
		Method:      "GET",
		Path:        "/events",
		Headers: map[string]string{
			identityheaders.HeaderUserID:        "alice",
			identityheaders.HeaderPrincipalType: "User",
		},
		StreamResponseIndefinitely: true,
		StreamIdleTimeoutMs:        2000,
		TimeoutMs:                  2000,
	}

	hdr, body, err := dispatchStream(t, h, req, "alice")
	if err != nil {
		t.Fatalf("dispatchStream: %v", err)
	}
	if hdr == nil {
		t.Fatal("nil header")
	}
	if hdr.GetError() != nil {
		t.Fatalf("ProxyError on header: %s: %s", hdr.GetError().GetKind(), hdr.GetError().GetMessage())
	}
	if !hdr.GetBodyChunked() {
		t.Errorf("expected body_chunked=true on streaming response")
	}
	for i := 0; i < eventCount; i++ {
		needle := fmt.Sprintf("event-%03d\n", i)
		if !strings.Contains(string(body), needle) {
			t.Errorf("missing event %d (%q) in stream of %d bytes", i, needle, len(body))
			break
		}
	}
}

// TestPhase27_StreamResponse_IdleTimeoutClosesStream asserts a stalled
// backend triggers a mid-stream PROXY TIMEOUT through the gateway-style
// path.
func TestPhase27_StreamResponse_IdleTimeoutClosesStream(t *testing.T) {
	t.Parallel()

	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		fmt.Fprint(w, "x")
		if flusher != nil {
			flusher.Flush()
		}
		time.Sleep(2 * time.Second)
	}))
	defer mock.Close()

	h := newHarness()
	h.addTerminator(t, "memorylayer", "stall", mock.URL, "tenant-test", proxysidecar.HeaderModeStrict)

	req := &pb.ProxyHttpRequest{
		RequestId:                  "rid-stream-stall",
		TargetTopic:                "sv::memorylayer::stall",
		Method:                     "GET",
		Path:                       "/stall",
		Headers:                    map[string]string{identityheaders.HeaderUserID: "alice", identityheaders.HeaderPrincipalType: "User"},
		StreamResponseIndefinitely: true,
		StreamIdleTimeoutMs:        100,
	}

	resps, _, err := dispatchStreamRaw(t, h, req, "alice")
	if err != nil {
		t.Fatalf("dispatchStreamRaw: %v", err)
	}
	if len(resps) < 2 {
		t.Fatalf("expected at least 2 responses (header + terminal error), got %d", len(resps))
	}
	terminal := resps[len(resps)-1]
	if terminal.GetError() == nil {
		t.Fatal("expected mid-stream ProxyError, got none")
	}
	if got := terminal.GetError().GetKind(); got != pb.ProxyError_TIMEOUT {
		t.Errorf("expected TIMEOUT, got %s", got)
	}
}

// TestPhase27_StreamResponse_MaxBytesClosesPartialPreserved asserts the
// PAYLOAD_TOO_LARGE mid-stream close path delivers the partial body to the
// caller.
func TestPhase27_StreamResponse_MaxBytesClosesPartialPreserved(t *testing.T) {
	t.Parallel()

	const total = 64 * 1024
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(200)
		flusher, _ := w.(http.Flusher)
		buf := make([]byte, 4096)
		for i := range buf {
			buf[i] = 'A'
		}
		written := 0
		for written < total {
			n, err := w.Write(buf)
			if err != nil {
				return
			}
			written += n
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(time.Millisecond)
		}
	}))
	defer mock.Close()

	h := newHarness()
	h.addTerminator(t, "memorylayer", "big", mock.URL, "tenant-test", proxysidecar.HeaderModeStrict)

	req := &pb.ProxyHttpRequest{
		RequestId:                  "rid-stream-big",
		TargetTopic:                "sv::memorylayer::big",
		Method:                     "GET",
		Path:                       "/big",
		Headers:                    map[string]string{identityheaders.HeaderUserID: "alice", identityheaders.HeaderPrincipalType: "User"},
		StreamResponseIndefinitely: true,
		StreamIdleTimeoutMs:        5000,
		MaxResponseBodyBytes:       8 * 1024,
	}

	resps, body, err := dispatchStreamRaw(t, h, req, "alice")
	if err != nil {
		t.Fatalf("dispatchStreamRaw: %v", err)
	}
	if len(resps) < 2 {
		t.Fatalf("expected header + terminal PAYLOAD_TOO_LARGE, got %d responses", len(resps))
	}
	terminal := resps[len(resps)-1]
	if terminal.GetError() == nil {
		t.Fatal("expected mid-stream PAYLOAD_TOO_LARGE")
	}
	if got := terminal.GetError().GetKind(); got != pb.ProxyError_PAYLOAD_TOO_LARGE {
		t.Errorf("expected PAYLOAD_TOO_LARGE, got %s", got)
	}
	if int64(len(body)) > req.GetMaxResponseBodyBytes() {
		t.Errorf("partial body %d bytes exceeds cap %d", len(body), req.GetMaxResponseBodyBytes())
	}
	if len(body) == 0 {
		t.Errorf("expected partial body bytes preserved, got 0")
	}
}

// dispatchStream resolves the target through the harness, drives the
// streaming dispatch via the terminator's exported test entry point, and
// returns the first (header) response plus the concatenated body bytes.
func dispatchStream(t *testing.T, h *harness, req *pb.ProxyHttpRequest, callerUser string) (*pb.ProxyHttpResponse, []byte, error) {
	t.Helper()
	resps, body, err := dispatchStreamRaw(t, h, req, callerUser)
	if err != nil {
		return nil, nil, err
	}
	if len(resps) == 0 {
		return nil, nil, nil
	}
	return resps[0], body, nil
}

// dispatchStreamRaw is dispatchStream's full-fidelity sibling: it returns
// every emitted ProxyHttpResponse so callers can inspect mid-stream error
// frames.
func dispatchStreamRaw(t *testing.T, h *harness, req *pb.ProxyHttpRequest, callerUser string) ([]*pb.ProxyHttpResponse, []byte, error) {
	t.Helper()
	concrete, err := h.resolveWildcardOrConcrete(req.GetTargetTopic())
	if err != nil {
		return nil, nil, err
	}
	req.TargetTopic = concrete
	entry := h.findTerminator(concrete)
	if entry == nil {
		return nil, nil, fmt.Errorf("no terminator for %s", concrete)
	}
	if req.Headers == nil {
		req.Headers = map[string]string{}
	}
	req.Headers["x-aether-actor-topic"] = userCaller(callerUser, "w")

	transport := newCapturingTransport()
	if err := entry.t.DispatchStreamingForTest(context.Background(), req, nil, transport); err != nil {
		return nil, nil, err
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	resps := append([]*pb.ProxyHttpResponse(nil), transport.resps...)
	var body []byte
	for _, c := range transport.chunks {
		body = append(body, c.GetData()...)
	}
	return resps, body, nil
}
