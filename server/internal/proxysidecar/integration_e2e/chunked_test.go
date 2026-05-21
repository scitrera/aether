//go:build e2e

package integration_e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/aether/sdk/go/aether"
)

// TestE2E_LargeChunkedUpload_UnderStreamLoad opens 2 concurrent /slow
// SSE streams and in parallel POSTs ~5 MiB of random bytes through
// /echo as a chunked-body request. Asserts the upload completes within
// 10s and the echoed body matches what was sent.
//
// This validates task 15 (async-wrap remaining sidecar handlers):
// chunked-request fin dispatch and the multi-frame request assembler
// must not wedge the receive loop while streams hold goroutines on
// /slow drips. If the sibling executor for task 15 has not landed
// their fix yet, this test may time out — that is documented in the
// task spec.
func TestE2E_LargeChunkedUpload_UnderStreamLoad(t *testing.T) {
	t.Parallel()

	const (
		streamFanout = 2
		uploadSize   = 5 * 1024 * 1024 // 5 MiB
		// The upload deadline doubles as the test's effective wall-time
		// when the chunked-request fin path wedges the receive loop —
		// keep it tight so the package timeout has headroom for the
		// rest of the suite. A successful upload completes in well
		// under 5s; the deadline is the regression budget, not the
		// happy-path budget.
		uploadCtxDur = 15 * time.Second
		uploadHard   = 10 * time.Second
		slowDur      = 8 * time.Second
	)

	h := NewE2EHarness(t, E2EHarnessOptions{SlowStreamDuration: slowDur})

	// Pre-build the payload.
	payload := make([]byte, uploadSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	rootCtx, rootCancel := context.WithTimeout(context.Background(), slowDur+15*time.Second)
	defer rootCancel()

	streamClient := dialAgentClient(t, h, "chunked-streams")
	uploadClient := dialAgentClient(t, h, "chunked-upload")

	var (
		wg           sync.WaitGroup
		streamErrors atomic.Int32
		streamCompl  atomic.Int32
	)

	// Background streams to load the runtime.
	for i := 0; i < streamFanout; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := driveStream(rootCtx, streamClient, h.ServiceTopic, "/slow", slowDur+3*time.Second)
			if err != nil {
				streamErrors.Add(1)
				t.Logf("background stream %d error: %v", i, err)
				return
			}
			streamCompl.Add(1)
		}(i)
	}

	// Give the streams a head start so their /slow drips are in flight
	// when the upload begins.
	time.Sleep(250 * time.Millisecond)

	// Issue the chunked upload.
	uploadCtx, uploadCancel := context.WithTimeout(rootCtx, uploadCtxDur)
	defer uploadCancel()
	uploadStart := time.Now()

	gotBody, err := driveChunkedUpload(uploadCtx, uploadClient, h.ServiceTopic, "/echo", payload, uploadHard)
	uploadElapsed := time.Since(uploadStart)

	if err != nil {
		t.Errorf("chunked upload failed after %s: %v", uploadElapsed, err)
	} else {
		if !bytes.Equal(gotBody, payload) {
			t.Errorf("chunked echo body mismatch: got %d bytes, want %d bytes",
				len(gotBody), len(payload))
			if len(gotBody) > 0 && len(payload) > 0 {
				t.Logf("first 32 bytes: got=%x want=%x", gotBody[:32], payload[:32])
			}
		}
		if uploadElapsed > uploadHard {
			t.Errorf("chunked upload took %s, hard budget %s", uploadElapsed, uploadHard)
		}
	}

	wg.Wait()

	t.Logf("chunked upload: %d bytes in %s; streams: completed=%d errors=%d",
		len(payload), uploadElapsed, streamCompl.Load(), streamErrors.Load())
}

// driveChunkedUpload POSTs body to path through the SDK's proxy. The
// SDK auto-splits requests larger than its proxyChunkSize (256 KiB)
// into ProxyHttpBodyChunk frames; we just hand the whole body in.
// timeout is the per-call deadline; the response body is read fully
// and returned to the caller.
func driveChunkedUpload(ctx context.Context, client *aether.AgentClient, target, path string, body []byte, timeout time.Duration) ([]byte, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(callCtx, "POST", "http://ignored"+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(body))

	resp, err := client.ProxyHTTP(callCtx, target, req, aether.WithBackend("local"))
	if err != nil {
		return nil, fmt.Errorf("ProxyHTTP: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status=%d", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		return got, fmt.Errorf("ReadAll: %w", err)
	}
	return got, nil
}
