//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/proxysidecar"
	"github.com/scitrera/aether/pkg/identityheaders"
)

// TestPhase21_POST_LargeChunkedBody_RoundTrip exercises the full chunked
// request body path: caller streams a 10 MiB body in 256 KiB chunks, the
// terminator reassembles, the backend echoes, and the (large) response
// returns chunked too. Asserts byte-equality both directions.
func TestPhase21_POST_LargeChunkedBody_RoundTrip(t *testing.T) {
	t.Parallel()

	be := newEchoBackend(t, "be-chunk-up")
	h := newHarness()
	h.addTerminator(t, "memorylayer", "chunked", be.srv.URL, "tenant-test", proxysidecar.HeaderModeStrict)

	// 10 MiB random payload; well above the 256 KiB chunk threshold.
	body := make([]byte, 10<<20)
	if _, err := rand.Read(body); err != nil {
		t.Fatalf("rand: %v", err)
	}

	req := &pb.ProxyHttpRequest{
		RequestId:   "r-chunk-up",
		TargetTopic: "sv::memorylayer::chunked",
		Method:      "POST",
		Path:        "/echo-bytes",
		Headers: map[string]string{
			"Content-Type":                      "application/octet-stream",
			identityheaders.HeaderUserID:        "alice",
			identityheaders.HeaderPrincipalType: "User",
		},
		BodyChunked: true,
	}

	resp, respBody, err := h.proxyHTTPChunked(context.Background(), userCaller("alice", "w"), req, body, 256*1024)
	if err != nil {
		t.Fatalf("proxyHTTPChunked: %v", err)
	}
	if status := expectOK(t, resp); status != 200 {
		t.Fatalf("status: got %d, want 200", status)
	}
	if len(respBody) != len(body) {
		t.Fatalf("response length: got %d, want %d", len(respBody), len(body))
	}
	if !bytes.Equal(respBody, body) {
		t.Error("response body did not survive chunked round-trip")
	}

	rec := be.lastRequest()
	if rec == nil {
		t.Fatal("backend never received the chunked request")
	}
	if !bytes.Equal(rec.Body, body) {
		t.Errorf("backend body mismatch: backend got %d bytes, source had %d", len(rec.Body), len(body))
	}
}
