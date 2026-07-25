package aether

import (
	"context"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// newTestConnBaseClient builds a BaseClient put into the "running" state so
// Send queues messages instead of failing.
func newTestConnBaseClient(t *testing.T) *BaseClient {
	t.Helper()
	bc, err := NewBaseClient(BaseClientConfig{ServerAddr: TestServerAddr})
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	bc.running.Store(true)
	return bc
}

func TestBaseClient_ConnectionStatus_RoundTrip(t *testing.T) {
	bc := newTestConnBaseClient(t)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-bc.RequestQueue()
		req := msg.GetConnectionStatusRequest()
		if req == nil {
			t.Errorf("expected ConnectionStatusRequest, got %T", msg.GetPayload())
			return
		}
		if req.GetPrincipal().GetPrincipalType() != "service" ||
			req.GetPrincipal().GetPrincipalId() != "sandbox-sidecar::sb-1" {
			t.Errorf("unexpected principal: %+v", req.GetPrincipal())
			return
		}
		bc.pendingConnectionStatusRequests.Range(func(key, val any) bool {
			ch := val.(chan *pb.ConnectionStatusResponse)
			bc.pendingConnectionStatusRequests.Delete(key)
			ch <- &pb.ConnectionStatusResponse{
				RequestId: req.GetRequestId(),
				Ok:        true,
				Connected: true,
			}
			return false
		})
	}()

	resp, err := bc.ConnectionStatus(context.Background(), "service", "sandbox-sidecar::sb-1")
	if err != nil {
		t.Fatalf("ConnectionStatus() error = %v", err)
	}
	if !resp.GetOk() || !resp.GetConnected() {
		t.Errorf("got %+v, want Ok=true Connected=true", resp)
	}
}

func TestBaseClient_ConnectionStatus_Disconnected(t *testing.T) {
	bc := newTestConnBaseClient(t)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-bc.RequestQueue()
		req := msg.GetConnectionStatusRequest()
		if req == nil {
			return
		}
		bc.pendingConnectionStatusRequests.Range(func(key, val any) bool {
			ch := val.(chan *pb.ConnectionStatusResponse)
			bc.pendingConnectionStatusRequests.Delete(key)
			ch <- &pb.ConnectionStatusResponse{RequestId: req.GetRequestId(), Ok: true, Connected: false}
			return false
		})
	}()

	resp, err := bc.ConnectionStatus(context.Background(), "service", "sandbox-sidecar::sb-2")
	if err != nil {
		t.Fatalf("ConnectionStatus() error = %v", err)
	}
	if !resp.GetOk() || resp.GetConnected() {
		t.Errorf("got %+v, want Ok=true Connected=false", resp)
	}
}

func TestBaseClient_ConnectionStatus_Timeout(t *testing.T) {
	bc := newTestConnBaseClient(t)

	// No responder; expect a timeout error.
	_, err := bc.Connection().Status(context.Background(), "service", "sandbox-sidecar::sb-3", 50*time.Millisecond)
	if err == nil {
		t.Fatal("ConnectionStatus should time out when no response arrives")
	}
}

// TestBaseClient_DispatchResponse_ConnectionStatus verifies dispatch wiring:
// an inbound ConnectionStatusResponse with a known request_id resolves the
// pending sync caller.
func TestBaseClient_DispatchResponse_ConnectionStatus(t *testing.T) {
	bc := newTestConnBaseClient(t)

	requestID := bc.NextRequestID()
	ch := bc.RegisterPendingConnectionStatusRequest(requestID)

	protoResp := &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ConnectionStatusResponse{
			ConnectionStatusResponse: &pb.ConnectionStatusResponse{
				RequestId: requestID,
				Ok:        true,
				Connected: true,
			},
		},
	}
	if err := bc.dispatchResponse(context.Background(), protoResp); err != nil {
		t.Errorf("dispatchResponse() error = %v", err)
	}

	select {
	case got := <-ch:
		if !got.GetOk() || !got.GetConnected() {
			t.Errorf("got %+v, want Ok=true Connected=true", got)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatchResponse did not resolve pending connection-status request")
	}
}
