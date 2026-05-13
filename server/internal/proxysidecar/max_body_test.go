package proxysidecar

import (
	"bytes"
	"context"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
)

func TestTerminator_RejectsOversizedBody(t *testing.T) {
	t.Parallel()

	cfg := terminatorTestConfig()
	cfg.Terminator.Backends[0].MaxBodyBytes = 16 // tiny limit so we exceed easily

	t1, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	req := &pb.ProxyHttpRequest{
		RequestId: "r-too-big",
		Method:    "POST",
		Path:      "/v1/upload",
	}
	body := bytes.Repeat([]byte("A"), 1024) // 1024 > 16
	resp, _ := t1.HandleProxyRequest(context.Background(), req, body)
	assertProxyError(t, resp, pb.ProxyError_PAYLOAD_TOO_LARGE)
}
