package proxysidecar

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/sdk/go/aether"
	"github.com/scitrera/go-backpressure"
	"google.golang.org/protobuf/proto"
)

// newDeliverTestSession builds a sharedRuntimeSession with the smallest
// viable deliverSem (capacity=1) for shed-behaviour tests, attached to a
// no-op sink. The caller is responsible for cancelling sessCancel and
// closing sess.deliverSem.
func newDeliverTestSession(t *testing.T, sinkCap int) (*sharedRuntimeSession, context.CancelFunc) {
	t.Helper()
	sink := newSharedRelaySink(nil, sinkCap)
	sessCtx, cancel := context.WithCancel(context.Background())
	sess := &sharedRuntimeSession{
		owner:  sink,
		ctx:    sessCtx,
		cancel: cancel,
		inbox:  make(chan *pb.DownstreamMessage, 4),
		deliverSem: backpressure.NewSemaphore(
			5, // 5 priorities to match production
			1, // capacity=1 so a single hold saturates the queue
			backpressure.SemaphoreShortTimeout(5*time.Millisecond),
			backpressure.SemaphoreLongTimeout(10*time.Millisecond),
		),
	}
	if !sink.attachSession(sess) {
		cancel()
		sess.deliverSem.Close()
		t.Fatalf("attachSession failed unexpectedly")
	}
	return sess, func() {
		cancel()
		sess.deliverSem.Close()
	}
}

// TestSharedRuntimeSessionDeliver_ShedsBestEffortFirst pins the per-session
// deliver path's priority-aware shedding. With the sole Semaphore token
// pinned at PriorityControl, a subsequent PriorityBestEffort deliver should
// be shed by CoDel; the original envelope must be discarded and a
// BACKPRESSURE DownstreamMessage_Error must appear on the inbox in its
// place so the in-sandbox SDK observes a clean failure signal.
func TestSharedRuntimeSessionDeliver_ShedsBestEffortFirst(t *testing.T) {
	sess, cleanup := newDeliverTestSession(t, 4)
	defer cleanup()

	// Hold the single Semaphore token at PriorityControl so any
	// lower-priority Acquire (BestEffort/Request/etc.) cannot proceed
	// until either the timeout fires or CoDel sheds.
	holderCtx, holderCancel := context.WithCancel(context.Background())
	defer holderCancel()

	holderReady := make(chan struct{})
	go func() {
		if err := sess.deliverSem.Acquire(holderCtx, aether.PriorityControl, 1); err != nil {
			// holderCtx was cancelled — nothing to release.
			return
		}
		close(holderReady)
		<-holderCtx.Done()
		sess.deliverSem.Release(1)
	}()

	select {
	case <-holderReady:
	case <-time.After(2 * time.Second):
		t.Fatal("holder failed to acquire PriorityControl token within 2s")
	}

	// Override the acquire timeout for this test by cancelling sess.ctx
	// after a short wait so deliver's Acquire returns deadline exceeded
	// fast. Production uses sharedRuntimeSessionDeliverAcquireTimeout
	// (30s) which is too slow for a unit test.
	//
	// We avoid mutating the const; instead, use a wrapper goroutine that
	// kicks the in-flight deliver by closing the sem via a brief cancel
	// of holderCtx after detecting the deliver has started — but the
	// simpler approach is: trigger a deliver with a sentinel BestEffort
	// payload and synchronously check the inbox after the
	// CoDel-short-timeout window (10ms) elapses.

	// Wrap deliver in a goroutine so the test thread can observe the
	// inbox without blocking on deliver's 30s ceiling — the CoDel queue
	// will shed within ~10ms (sharedRuntimeSessionDeliverLongTimeout in
	// the test session above), and the BACKPRESSURE notice should land
	// promptly. We give a generous wall-clock budget for shedding.
	bestEffortMsg := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ProgressUpdate{
			ProgressUpdate: &pb.ProgressUpdate{Summary: "best-effort sentinel"},
		},
	}

	deliverDone := make(chan struct{})
	go func() {
		defer close(deliverDone)
		sess.deliver(bestEffortMsg)
	}()

	// Pull the first inbox frame. With holderCtx pinning the token, we
	// expect the BACKPRESSURE error frame (not the original) once
	// CoDel sheds. Allow up to 5s in case the CI host is slow; CoDel's
	// long timeout in this session is 10ms so this is generous.
	var got *pb.DownstreamMessage
	select {
	case got = <-sess.inbox:
	case <-time.After(5 * time.Second):
		t.Fatal("no inbox frame within 5s — deliver did not shed or synthesise BACKPRESSURE notice")
	}

	if got == bestEffortMsg {
		t.Fatalf("inbox contains the original BestEffort envelope; deliver should have shed it under CoDel pressure")
	}
	errPayload, ok := got.GetPayload().(*pb.DownstreamMessage_Error)
	if !ok {
		t.Fatalf("inbox payload = %T, want *pb.DownstreamMessage_Error (BACKPRESSURE synthesis)", got.GetPayload())
	}
	if got, want := errPayload.Error.GetCode(), "BACKPRESSURE"; got != want {
		t.Errorf("error code = %q, want %q", got, want)
	}

	// Release the holder so the deliver goroutine can finish if it's
	// still mid-acquire (it shouldn't be — shed already returned — but
	// don't leak the goroutine).
	holderCancel()
	select {
	case <-deliverDone:
	case <-time.After(2 * time.Second):
		t.Fatal("deliver goroutine did not return after release")
	}
}

// TestPriorityForSharedRelayUpstream pins the per-payload priority mapping
// the SDK uses to admit relay-mediated upstream envelopes. Failing this
// test means the runner sends are now routing envelopes through the wrong
// CoDel queue — a behaviour regression even when no test downstream of it
// notices.
// TestRouteResponseToOwner_DeliversAndReleases verifies the correlated
// response routing the rawDownstreamTap relies on: a registered request_id
// routes its response to the owning session's inbox and releases the route;
// an unregistered id returns false (so the SDK handles it normally) and
// delivers nothing.
func TestRouteResponseToOwner_DeliversAndReleases(t *testing.T) {
	sess, cleanup := newDeliverTestSession(t, 1)
	defer cleanup()
	sink := sess.owner

	const reqID = "kv-req-1"
	sink.registerRequest(sess, reqID)

	kvResp := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Kv{Kv: &pb.KVResponse{RequestId: reqID, Success: true}},
	}
	if !sink.routeResponseToOwner(reqID, kvResp) {
		t.Fatal("routeResponseToOwner returned false for a registered request_id")
	}
	select {
	case got := <-sess.inbox:
		if _, ok := got.GetPayload().(*pb.DownstreamMessage_Kv); !ok {
			t.Fatalf("inbox got %T, want *DownstreamMessage_Kv", got.GetPayload())
		}
	default:
		t.Fatal("expected the KV response on the session inbox")
	}

	// Route was released — a second delivery for the same id must miss.
	if sink.routeResponseToOwner(reqID, kvResp) {
		t.Error("routeResponseToOwner returned true after the route was released")
	}
	// Unknown id never claims.
	if sink.routeResponseToOwner("no-such-id", kvResp) {
		t.Error("routeResponseToOwner returned true for an unregistered request_id")
	}
}

// TestDownstreamResponseRequestID pins the correlation-id extraction the tap
// uses to decide whether a downstream message might belong to a relay session.
func TestDownstreamResponseRequestID(t *testing.T) {
	cases := []struct {
		name string
		msg  *pb.DownstreamMessage
		want string
	}{
		{
			name: "Kv",
			msg:  &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_Kv{Kv: &pb.KVResponse{RequestId: "r1"}}},
			want: "r1",
		},
		{
			name: "TaskOp",
			msg:  &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_TaskOp{TaskOp: &pb.TaskOperationResponse{RequestId: "r2"}}},
			want: "r2",
		},
		{
			name: "TaskQuery",
			msg:  &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_TaskQuery{TaskQuery: &pb.TaskQueryResponse{RequestId: "r3"}}},
			want: "r3",
		},
		{
			name: "Error_with_request_id",
			msg:  &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_Error{Error: &pb.ErrorResponse{RequestId: "r4"}}},
			want: "r4",
		},
		{
			name: "Msg_is_not_a_correlated_response",
			msg:  &pb.DownstreamMessage{Payload: &pb.DownstreamMessage_Msg{Msg: &pb.IncomingMessage{}}},
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := downstreamResponseRequestID(tc.msg); got != tc.want {
				t.Errorf("downstreamResponseRequestID() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPriorityForSharedRelayUpstream(t *testing.T) {
	cases := []struct {
		name string
		msg  *pb.UpstreamMessage
		want backpressure.Priority
	}{
		{
			name: "ProxyHttpRequest",
			msg:  &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_ProxyHttpRequest{ProxyHttpRequest: &pb.ProxyHttpRequest{}}},
			want: aether.PriorityRequest,
		},
		{
			name: "ProxyHttpBodyChunk_request_direction",
			msg:  &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_ProxyHttpBodyChunk{ProxyHttpBodyChunk: &pb.ProxyHttpBodyChunk{IsRequest: true}}},
			want: aether.PriorityRequest,
		},
		{
			name: "ProxyHttpBodyChunk_response_direction",
			msg:  &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_ProxyHttpBodyChunk{ProxyHttpBodyChunk: &pb.ProxyHttpBodyChunk{IsRequest: false}}},
			want: aether.PriorityResponseChunk,
		},
		{
			name: "TunnelOpen",
			msg:  &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_TunnelOpen{TunnelOpen: &pb.TunnelOpen{}}},
			want: aether.PriorityRequest,
		},
		{
			name: "TunnelData",
			msg:  &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_TunnelData{TunnelData: &pb.TunnelData{}}},
			want: aether.PriorityResponseChunk,
		},
		{
			name: "TunnelClose",
			msg:  &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_TunnelClose{TunnelClose: &pb.TunnelClose{}}},
			want: aether.PriorityControl,
		},
		{
			name: "TunnelAck",
			msg:  &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_TunnelAck{TunnelAck: &pb.TunnelAck{}}},
			want: aether.PriorityResponseHeader,
		},
		{
			name: "Send_best_effort",
			msg:  &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_Send{Send: &pb.SendMessage{}}},
			want: aether.PriorityBestEffort,
		},
		{
			name: "Progress_best_effort",
			msg:  &pb.UpstreamMessage{Payload: &pb.UpstreamMessage_Progress{Progress: &pb.ProgressReport{}}},
			want: aether.PriorityBestEffort,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := priorityForSharedRelayUpstream(tc.msg); got != tc.want {
				t.Errorf("priorityForSharedRelayUpstream(%T) = %d, want %d", tc.msg.GetPayload(), got, tc.want)
			}
		})
	}
}

// touchDeliver exercises deliver from multiple goroutines under a relaxed
// capacity so the race detector can catch missed locking in the shed path.
// Not a behavioural assertion; the test passes if it completes without
// `go test -race` complaints and without deadlock.
func TestSharedRuntimeSessionDeliver_NoRaces(t *testing.T) {
	sess, cleanup := newDeliverTestSession(t, 4)
	defer cleanup()

	// Loosen capacity for this exercise so it doesn't degenerate into
	// the always-shed test above.
	sess.deliverSem.SetCapacity(4)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 16; j++ {
				sess.deliver(&pb.DownstreamMessage{
					Payload: &pb.DownstreamMessage_Error{
						Error: &pb.ErrorResponse{Code: "TEST"},
					},
				})
			}
		}()
	}

	// Drain whatever lands on the inbox so deliver doesn't wedge waiting
	// for the staging chan.
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for {
			select {
			case <-sess.inbox:
			case <-time.After(200 * time.Millisecond):
				return
			}
		}
	}()
	wg.Wait()
	<-drainDone
}

// TestTunnelOpen_HandlerReturnsQuickly proves the OnTunnelDataIn
// TunnelOpen branch is goroutine-isolated: a slow upstream dial does not
// block the simulated receive-loop call. Mirrors the chunked-fin test in
// chunked_body_test.go — the regression site is the SDK's single-goroutine
// receiveLoop, and the contract is "handler returns promptly so other
// envelopes can be dispatched even when one TunnelOpen's dial stalls".
func TestTunnelOpen_HandlerReturnsQuickly(t *testing.T) {
	t.Parallel()

	const dialDelay = 500 * time.Millisecond
	const handlerBudget = 50 * time.Millisecond

	// Stand up a listener so resolveTCPAddress has a real target, then
	// inject a slow custom dialer onto the backend that sleeps for
	// dialDelay before returning the conn. The slow dial is the wedge a
	// pre-async receive loop would have hit.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { c.Close() })
		}
	}()

	cfg := &Config{
		Gateway: GatewayConfig{Address: "localhost:0", Insecure: true},
		Service: ServiceConfig{Implementation: "memorylayer", Specifier: "test"},
		Terminator: TerminatorConfig{
			Enabled: true,
			Backends: []BackendConfig{{
				Name:     "tcp-slow",
				Kind:     BackendKindTCP,
				URL:      "tcp://" + ln.Addr().String(),
				MaxBytes: 1 << 20,
			}},
		},
		TenantID: "tenant-test",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config: %v", err)
	}
	term, err := NewTerminator(cfg)
	if err != nil {
		t.Fatalf("NewTerminator: %v", err)
	}

	// Override the dialer so the dial blocks for dialDelay — that's what
	// HandleTunnelOpen will call under the hood when the OnTunnelDataIn
	// handler spawns runTunnelOpenDispatch.
	var dialStarted atomic.Bool
	term.tcpBackends[0].dialer = func(ctx context.Context, address string) (net.Conn, error) {
		dialStarted.Store(true)
		select {
		case <-time.After(dialDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		var d net.Dialer
		return d.DialContext(ctx, "tcp", address)
	}

	// Build the OnTunnelDataIn handler closure that runner.go::installOn
	// would register. This mirrors the production wiring so we test the
	// same shape (TunnelOpen branch → go runTunnelOpenDispatch). We can't
	// trivially spin up an SDK client in a unit test, so we replay the
	// handler body inline.
	router := &downstreamRouter{term: term}
	transport := newFakeTransport()
	handler := func(_ context.Context, frame *pb.TunnelData) error {
		if frame.GetSeq() == 0 && len(frame.GetData()) > 0 {
			open := &pb.TunnelOpen{}
			if err := tunnelDataIsOpen(frame, open); err == nil {
				go runTunnelOpenDispatch(router.term, open, transport)
				return nil
			}
		}
		// Non-open branches are not exercised here; the test only
		// concerns itself with the TunnelOpen fast-return.
		return nil
	}

	// Build a seq=0 TunnelData carrying a TunnelOpen body — the wire
	// shape the gateway uses to deliver an open signal.
	open := &pb.TunnelOpen{
		TunnelId: "tun-fast-return",
		Protocol: pb.TunnelOpen_TCP,
	}
	openBytes, err := proto.Marshal(open)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	frame := &pb.TunnelData{
		TunnelId: open.GetTunnelId(),
		Seq:      0,
		Data:     openBytes,
	}

	start := time.Now()
	if err := handler(context.Background(), frame); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > handlerBudget {
		t.Fatalf("OnTunnelDataIn TunnelOpen branch took %s, want <%s — dispatch is wedging the receive loop",
			elapsed, handlerBudget)
	}

	// Verify the dispatch goroutine actually started (so we're really
	// measuring the spawn pattern, not a no-op). Allow up to the dialDelay
	// window for it to enter the dialer.
	deadline := time.Now().Add(dialDelay + 1*time.Second)
	for time.Now().Before(deadline) {
		if dialStarted.Load() {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !dialStarted.Load() {
		t.Fatal("dispatch goroutine never invoked the backend dialer — TunnelOpen lost?")
	}
}
