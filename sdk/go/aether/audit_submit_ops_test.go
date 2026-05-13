// Package aether tests for foreign audit-event submission.

package aether

import (
	"context"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// resolveFirstPendingAuditSubmit drains the request queue and resolves the
// first pending audit-submit request with the given response. The mock
// server-side ack mirrors the gateway dispatch path: the response carries
// the client_request_id that was registered with the pending map.
func resolveFirstPendingAuditSubmit(t *testing.T, client *BaseClient, resp *pb.SubmitAuditEventResponse) {
	t.Helper()
	time.Sleep(10 * time.Millisecond)

	// Drain the upstream request and copy the client_request_id so the
	// response correlates correctly.
	select {
	case msg := <-client.RequestQueue():
		req := msg.GetSubmitAuditEvent()
		if req == nil {
			t.Fatalf("expected SubmitAuditEvent on the queue, got %T", msg.Payload)
		}
		if resp.GetClientRequestId() == "" {
			resp.ClientRequestId = req.GetClientRequestId()
		}
	default:
		t.Fatal("no upstream message queued")
	}

	client.pendingAuditSubmitRequests.Range(func(key, val any) bool {
		ch := val.(chan *pb.SubmitAuditEventResponse)
		client.pendingAuditSubmitRequests.Delete(key)
		ch <- resp
		return false
	})
}

func TestSubmitAuditEvent_HappyPath(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go resolveFirstPendingAuditSubmit(t, client, &pb.SubmitAuditEventResponse{
		Success: true,
	})

	resp, err := client.SubmitAuditEvent(context.Background(), SubmitAuditEventOpts{
		EventType: "custom",
		Operation: "ingest",
		Success:   true,
		Metadata:  map[string]string{"trace_id": "t-1"},
	})
	if err != nil {
		t.Fatalf("SubmitAuditEvent() error = %v", err)
	}
	if !resp.Success {
		t.Fatal("expected Success=true")
	}
	if resp.ErrorCode != "" {
		t.Errorf("ErrorCode = %q, want empty", resp.ErrorCode)
	}
	if resp.ClientRequestID == "" {
		t.Error("expected ClientRequestID to be echoed back")
	}
}

func TestSubmitAuditEvent_ServerError(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go resolveFirstPendingAuditSubmit(t, client, &pb.SubmitAuditEventResponse{
		Success:      false,
		ErrorCode:    "ERR_AUDIT_TYPE_FORBIDDEN",
		ErrorMessage: "event_type 'connection' is reserved for the gateway",
	})

	resp, err := client.SubmitAuditEvent(context.Background(), SubmitAuditEventOpts{
		EventType: "connection",
		Operation: "open",
	})
	if err != nil {
		t.Fatalf("SubmitAuditEvent() error = %v", err)
	}
	if resp.Success {
		t.Fatal("expected Success=false for forbidden event_type")
	}
	if resp.ErrorCode != "ERR_AUDIT_TYPE_FORBIDDEN" {
		t.Errorf("ErrorCode = %q, want ERR_AUDIT_TYPE_FORBIDDEN", resp.ErrorCode)
	}
	if resp.ErrorMessage == "" {
		t.Error("expected ErrorMessage to be populated")
	}
}

func TestSubmitAuditEvent_QueuesCorrectRequest(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	// Fire SubmitAuditEvent in a goroutine; assert the upstream payload
	// looks right, then deliver a canned response so the call returns.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := client.SubmitAuditEvent(context.Background(), SubmitAuditEventOpts{
			EventType:    "kv",
			Operation:    "put",
			ResourceType: "kv_key",
			ResourceID:   "my-key",
			Workspace:    "tenant-a",
			Success:      true,
			Metadata:     map[string]string{"size_bytes": "42"},
		})
		if err != nil {
			t.Errorf("SubmitAuditEvent() error = %v", err)
		}
	}()

	// Wait briefly for the request to be queued, then verify it.
	time.Sleep(20 * time.Millisecond)
	select {
	case msg := <-client.RequestQueue():
		req := msg.GetSubmitAuditEvent()
		if req == nil {
			t.Fatal("expected SubmitAuditEventRequest in queue")
		}
		if req.GetEventType() != "kv" {
			t.Errorf("EventType = %q, want kv", req.GetEventType())
		}
		if req.GetOperation() != "put" {
			t.Errorf("Operation = %q, want put", req.GetOperation())
		}
		if req.GetResourceType() != "kv_key" {
			t.Errorf("ResourceType = %q, want kv_key", req.GetResourceType())
		}
		if req.GetWorkspace() != "tenant-a" {
			t.Errorf("Workspace = %q, want tenant-a", req.GetWorkspace())
		}
		if req.GetClientRequestId() == "" {
			t.Error("ClientRequestId should be set")
		}
		if req.GetMetadata()["size_bytes"] != "42" {
			t.Errorf("Metadata[size_bytes] = %q, want 42", req.GetMetadata()["size_bytes"])
		}

		// Deliver the response so the goroutine completes.
		client.pendingAuditSubmitRequests.Range(func(key, val any) bool {
			ch := val.(chan *pb.SubmitAuditEventResponse)
			client.pendingAuditSubmitRequests.Delete(key)
			ch <- &pb.SubmitAuditEventResponse{
				Success:         true,
				ClientRequestId: req.GetClientRequestId(),
			}
			return false
		})
	default:
		t.Fatal("Message should be queued")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SubmitAuditEvent did not return")
	}
}

func TestSubmitAuditEvent_DispatchResolvesPending(t *testing.T) {
	// Exercises the dispatchResponse path: handleSubmitAuditEventResponse
	// must route to the registered pending channel by client_request_id.
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	reqID := "req-test-1"
	ch := client.RegisterPendingAuditSubmitRequest(reqID)

	resp := &pb.SubmitAuditEventResponse{
		ClientRequestId: reqID,
		Success:         true,
	}

	if err := client.dispatchResponse(context.Background(), &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_SubmitAuditEventResponse{SubmitAuditEventResponse: resp},
	}); err != nil {
		t.Fatalf("dispatchResponse() error = %v", err)
	}

	select {
	case got := <-ch:
		if got.GetClientRequestId() != reqID {
			t.Errorf("ClientRequestId = %q, want %q", got.GetClientRequestId(), reqID)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatchResponse did not resolve pending channel")
	}
}
