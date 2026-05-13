package aether

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// TestProxyHTTP_StreamResponse_DrainsChunksIncrementally asserts that opting
// into stream_response_indefinitely makes the SDK return an *http.Response
// whose Body yields bytes as ProxyHttpBodyChunk frames arrive — without
// buffering the full response.
func TestProxyHTTP_StreamResponse_DrainsChunksIncrementally(t *testing.T) {
	client := newConnectedBaseClient(t)

	req := fakeHTTPRequest(t, "GET", "http://ignored/events", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := client.ProxyHTTP(ctx, "sv::testsvc::default", req,
			WithStreamResponse(0, 0))
		done <- result{resp: resp, err: err}
	}()

	// Pull the upstream envelope and assert the streaming opt-in fields are set.
	var pr *pb.ProxyHttpRequest
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && pr == nil {
		select {
		case msg := <-client.RequestQueue():
			pr = msg.GetProxyHttpRequest()
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	if pr == nil {
		t.Fatal("no ProxyHttpRequest in queue")
	}
	if !pr.GetStreamResponseIndefinitely() {
		t.Errorf("StreamResponseIndefinitely not set on outgoing request")
	}

	// Send the streaming header (body_chunked=true).
	requestID := pr.GetRequestId()
	resolveProxyResponse(requestID, &pb.ProxyHttpResponse{
		RequestId:   requestID,
		StatusCode:  200,
		Headers:     map[string]string{"Content-Type": "text/event-stream"},
		BodyChunked: true,
	})

	// Wait for the SDK to deliver the *http.Response.
	var got result
	select {
	case got = <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("SDK never resolved the streaming response header")
	}
	if got.err != nil {
		t.Fatalf("ProxyHTTP error: %v", got.err)
	}
	if got.resp.StatusCode != 200 {
		t.Errorf("status: got %d, want 200", got.resp.StatusCode)
	}

	// Push three chunks and a fin frame; iterate the body and assert each
	// chunk appears in order.
	go func() {
		appendProxyChunk(requestID, []byte("hello "), false)
		time.Sleep(5 * time.Millisecond)
		appendProxyChunk(requestID, []byte("world "), false)
		time.Sleep(5 * time.Millisecond)
		appendProxyChunk(requestID, []byte("!"), true)
	}()

	body, err := io.ReadAll(got.resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "hello world !" {
		t.Errorf("body: got %q, want %q", string(body), "hello world !")
	}
	_ = got.resp.Body.Close()
}

// TestProxyHTTP_StreamResponse_MidStreamError asserts that a terminal
// ProxyError frame mid-stream is surfaced as an io error from the body
// reader.
func TestProxyHTTP_StreamResponse_MidStreamError(t *testing.T) {
	client := newConnectedBaseClient(t)

	req := fakeHTTPRequest(t, "GET", "http://ignored/events", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct {
		resp *http.Response
		err  error
	}, 1)
	go func() {
		resp, err := client.ProxyHTTP(ctx, "sv::testsvc::default", req,
			WithStreamResponse(30_000, 0))
		done <- struct {
			resp *http.Response
			err  error
		}{resp, err}
	}()

	var pr *pb.ProxyHttpRequest
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && pr == nil {
		select {
		case msg := <-client.RequestQueue():
			pr = msg.GetProxyHttpRequest()
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	if pr == nil {
		t.Fatal("no ProxyHttpRequest in queue")
	}

	requestID := pr.GetRequestId()
	resolveProxyResponse(requestID, &pb.ProxyHttpResponse{
		RequestId:   requestID,
		StatusCode:  200,
		BodyChunked: true,
	})

	r := <-done
	if r.err != nil {
		t.Fatalf("unexpected error from header phase: %v", r.err)
	}

	// Deliver one chunk, then a mid-stream PAYLOAD_TOO_LARGE.
	go func() {
		appendProxyChunk(requestID, []byte("partial"), false)
		time.Sleep(5 * time.Millisecond)
		resolveProxyResponse(requestID, &pb.ProxyHttpResponse{
			RequestId: requestID,
			Error: &pb.ProxyError{
				Kind:    pb.ProxyError_PAYLOAD_TOO_LARGE,
				Message: "exceeded cap",
			},
		})
	}()

	buf := make([]byte, 16)
	n, err := r.resp.Body.Read(buf)
	if err != nil || n == 0 {
		t.Fatalf("expected first chunk, got n=%d err=%v", n, err)
	}
	if !bytes.Equal(buf[:n], []byte("partial")) {
		t.Errorf("first chunk: got %q, want %q", string(buf[:n]), "partial")
	}
	// Subsequent read must surface the mid-stream error.
	_, err = r.resp.Body.Read(buf)
	if err == nil {
		t.Fatal("expected error after mid-stream PAYLOAD_TOO_LARGE, got nil")
	}
	pe, ok := err.(*ProxyTransportError)
	if !ok {
		t.Fatalf("expected *ProxyTransportError, got %T: %v", err, err)
	}
	if pe.Kind != pb.ProxyError_PAYLOAD_TOO_LARGE.String() {
		t.Errorf("ProxyTransportError kind: got %q, want %q", pe.Kind, pb.ProxyError_PAYLOAD_TOO_LARGE.String())
	}
	_ = r.resp.Body.Close()
}

// TestProxyHTTP_StreamResponse_PassesProtoFields asserts the SDK threads the
// streaming opt-in fields onto the ProxyHttpRequest envelope.
func TestProxyHTTP_StreamResponse_PassesProtoFields(t *testing.T) {
	client := newConnectedBaseClient(t)
	req := fakeHTTPRequest(t, "GET", "http://ignored/probe", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go func() { _, _ = client.ProxyHTTP(ctx, "sv::svc::inst", req, WithStreamResponse(15000, 1024)) }()

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
		t.Fatal("no ProxyHttpRequest in queue")
	}
	if !pr.GetStreamResponseIndefinitely() {
		t.Errorf("StreamResponseIndefinitely not set")
	}
	if pr.GetStreamIdleTimeoutMs() != 15000 {
		t.Errorf("StreamIdleTimeoutMs: got %d, want 15000", pr.GetStreamIdleTimeoutMs())
	}
	if pr.GetMaxResponseBodyBytes() != 1024 {
		t.Errorf("MaxResponseBodyBytes: got %d, want 1024", pr.GetMaxResponseBodyBytes())
	}
}
