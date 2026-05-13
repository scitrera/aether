package aether

import (
	"context"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// KV Helper Tests
// =============================================================================

func TestKV_NewKV(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	kv := newKV(client)
	if kv == nil {
		t.Fatal("newKV() should not return nil")
	}
	if kv.client != client {
		t.Error("KV client reference should match")
	}
}

func TestBaseClient_KV(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	kv := client.KV()
	if kv == nil {
		t.Fatal("KV() should not return nil")
	}
}

// =============================================================================
// Async KV Operation Tests
// =============================================================================

func TestKV_Get(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()
	err = kv.Get("test-key", KVScopeGlobal, "", "")
	if err != nil {
		t.Errorf("Get() error = %v", err)
	}

	// Verify the message was queued
	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp == nil {
			t.Fatal("Expected KVOperation in message")
		}
		if kvOp.Op != pb.KVOperation_GET {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_GET)
		}
		if kvOp.Key != "test-key" {
			t.Errorf("Key = %q, want %q", kvOp.Key, "test-key")
		}
		if kvOp.Scope != pb.KVOperation_GLOBAL {
			t.Errorf("Scope = %v, want %v", kvOp.Scope, pb.KVOperation_GLOBAL)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestKV_Get_DefaultScope(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()
	// Empty scope should default to global
	err = kv.Get("test-key", "", "", "")
	if err != nil {
		t.Errorf("Get() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp.Scope != pb.KVOperation_GLOBAL {
			t.Errorf("Scope = %v, want %v (default)", kvOp.Scope, pb.KVOperation_GLOBAL)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestKV_Get_WorkspaceScope(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()
	err = kv.Get("workspace-key", KVScopeWorkspace, "", "my-workspace")
	if err != nil {
		t.Errorf("Get() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp.Scope != pb.KVOperation_WORKSPACE {
			t.Errorf("Scope = %v, want %v", kvOp.Scope, pb.KVOperation_WORKSPACE)
		}
		if kvOp.Workspace != "my-workspace" {
			t.Errorf("Workspace = %q, want %q", kvOp.Workspace, "my-workspace")
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestKV_Put(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()
	value := testKVValue()
	err = kv.Put("test-key", value, KVScopeWorkspace, "", "test-workspace", 3600)
	if err != nil {
		t.Errorf("Put() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp == nil {
			t.Fatal("Expected KVOperation in message")
		}
		if kvOp.Op != pb.KVOperation_PUT {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_PUT)
		}
		if kvOp.Key != "test-key" {
			t.Errorf("Key = %q, want %q", kvOp.Key, "test-key")
		}
		if string(kvOp.Value) != string(value) {
			t.Errorf("Value = %q, want %q", kvOp.Value, value)
		}
		if kvOp.Scope != pb.KVOperation_WORKSPACE {
			t.Errorf("Scope = %v, want %v", kvOp.Scope, pb.KVOperation_WORKSPACE)
		}
		if kvOp.Workspace != "test-workspace" {
			t.Errorf("Workspace = %q, want %q", kvOp.Workspace, "test-workspace")
		}
		if kvOp.Ttl != 3600 {
			t.Errorf("TTL = %d, want 3600", kvOp.Ttl)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestKV_List(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()
	err = kv.List("prefix_", KVScopeUser, "user-123", "")
	if err != nil {
		t.Errorf("List() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp == nil {
			t.Fatal("Expected KVOperation in message")
		}
		if kvOp.Op != pb.KVOperation_LIST {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_LIST)
		}
		if kvOp.Key != "prefix_" {
			t.Errorf("Key = %q, want %q", kvOp.Key, "prefix_")
		}
		if kvOp.Scope != pb.KVOperation_USER {
			t.Errorf("Scope = %v, want %v", kvOp.Scope, pb.KVOperation_USER)
		}
		if kvOp.UserId != "user-123" {
			t.Errorf("UserID = %q, want %q", kvOp.UserId, "user-123")
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestKV_Delete(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()
	err = kv.Delete("delete-key", KVScopeUserWorkspace, "user-123", "test-workspace")
	if err != nil {
		t.Errorf("Delete() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp == nil {
			t.Fatal("Expected KVOperation in message")
		}
		if kvOp.Op != pb.KVOperation_DELETE {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_DELETE)
		}
		if kvOp.Key != "delete-key" {
			t.Errorf("Key = %q, want %q", kvOp.Key, "delete-key")
		}
		if kvOp.Scope != pb.KVOperation_USER_WORKSPACE {
			t.Errorf("Scope = %v, want %v", kvOp.Scope, pb.KVOperation_USER_WORKSPACE)
		}
		if kvOp.UserId != "user-123" {
			t.Errorf("UserID = %q, want %q", kvOp.UserId, "user-123")
		}
		if kvOp.Workspace != "test-workspace" {
			t.Errorf("Workspace = %q, want %q", kvOp.Workspace, "test-workspace")
		}
	default:
		t.Error("Message should be in queue")
	}
}

// =============================================================================
// Convenience Method Tests
// =============================================================================

func TestKV_GetGlobal(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()
	err = kv.GetGlobal("global-key")
	if err != nil {
		t.Errorf("GetGlobal() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp.Scope != pb.KVOperation_GLOBAL {
			t.Errorf("Scope = %v, want %v", kvOp.Scope, pb.KVOperation_GLOBAL)
		}
		if kvOp.Key != "global-key" {
			t.Errorf("Key = %q, want %q", kvOp.Key, "global-key")
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestKV_PutGlobal(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()
	value := []byte("global-value")
	err = kv.PutGlobal("global-key", value)
	if err != nil {
		t.Errorf("PutGlobal() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp.Op != pb.KVOperation_PUT {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_PUT)
		}
		if kvOp.Scope != pb.KVOperation_GLOBAL {
			t.Errorf("Scope = %v, want %v", kvOp.Scope, pb.KVOperation_GLOBAL)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestKV_DeleteGlobal(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()
	err = kv.DeleteGlobal("delete-global-key")
	if err != nil {
		t.Errorf("DeleteGlobal() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp.Op != pb.KVOperation_DELETE {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_DELETE)
		}
		if kvOp.Scope != pb.KVOperation_GLOBAL {
			t.Errorf("Scope = %v, want %v", kvOp.Scope, pb.KVOperation_GLOBAL)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestKV_ListGlobal(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()
	err = kv.ListGlobal("prefix_")
	if err != nil {
		t.Errorf("ListGlobal() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp.Op != pb.KVOperation_LIST {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_LIST)
		}
		if kvOp.Scope != pb.KVOperation_GLOBAL {
			t.Errorf("Scope = %v, want %v", kvOp.Scope, pb.KVOperation_GLOBAL)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestKV_WorkspaceMethods(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()

	tests := []struct {
		name      string
		operation func() error
		checkOp   pb.KVOperation_OpType
	}{
		{
			name:      "GetWorkspace",
			operation: func() error { return kv.GetWorkspace("key", "ws") },
			checkOp:   pb.KVOperation_GET,
		},
		{
			name:      "PutWorkspace",
			operation: func() error { return kv.PutWorkspace("key", []byte("val"), "ws") },
			checkOp:   pb.KVOperation_PUT,
		},
		{
			name:      "DeleteWorkspace",
			operation: func() error { return kv.DeleteWorkspace("key", "ws") },
			checkOp:   pb.KVOperation_DELETE,
		},
		{
			name:      "ListWorkspace",
			operation: func() error { return kv.ListWorkspace("prefix", "ws") },
			checkOp:   pb.KVOperation_LIST,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.operation()
			if err != nil {
				t.Errorf("%s() error = %v", tt.name, err)
			}

			select {
			case msg := <-client.RequestQueue():
				kvOp := msg.GetKvOp()
				if kvOp.Op != tt.checkOp {
					t.Errorf("Op = %v, want %v", kvOp.Op, tt.checkOp)
				}
				if kvOp.Scope != kvScopeToProto(KVScopeWorkspace) {
					t.Errorf("Scope = %v, want %v", kvOp.Scope, kvScopeToProto(KVScopeWorkspace))
				}
				if kvOp.Workspace != "ws" {
					t.Errorf("Workspace = %q, want %q", kvOp.Workspace, "ws")
				}
			default:
				t.Error("Message should be in queue")
			}
		})
	}
}

func TestKV_UserMethods(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()

	tests := []struct {
		name      string
		operation func() error
		checkOp   pb.KVOperation_OpType
	}{
		{
			name:      "GetUser",
			operation: func() error { return kv.GetUser("key", "user-1") },
			checkOp:   pb.KVOperation_GET,
		},
		{
			name:      "PutUser",
			operation: func() error { return kv.PutUser("key", []byte("val"), "user-1") },
			checkOp:   pb.KVOperation_PUT,
		},
		{
			name:      "DeleteUser",
			operation: func() error { return kv.DeleteUser("key", "user-1") },
			checkOp:   pb.KVOperation_DELETE,
		},
		{
			name:      "ListUser",
			operation: func() error { return kv.ListUser("prefix", "user-1") },
			checkOp:   pb.KVOperation_LIST,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.operation()
			if err != nil {
				t.Errorf("%s() error = %v", tt.name, err)
			}

			select {
			case msg := <-client.RequestQueue():
				kvOp := msg.GetKvOp()
				if kvOp.Op != tt.checkOp {
					t.Errorf("Op = %v, want %v", kvOp.Op, tt.checkOp)
				}
				if kvOp.Scope != kvScopeToProto(KVScopeUser) {
					t.Errorf("Scope = %v, want %v", kvOp.Scope, kvScopeToProto(KVScopeUser))
				}
				if kvOp.UserId != "user-1" {
					t.Errorf("UserID = %q, want %q", kvOp.UserId, "user-1")
				}
			default:
				t.Error("Message should be in queue")
			}
		})
	}
}

func TestKV_UserWorkspaceMethods(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()

	tests := []struct {
		name      string
		operation func() error
		checkOp   pb.KVOperation_OpType
	}{
		{
			name:      "GetUserWorkspace",
			operation: func() error { return kv.GetUserWorkspace("key", "user-1", "ws") },
			checkOp:   pb.KVOperation_GET,
		},
		{
			name:      "PutUserWorkspace",
			operation: func() error { return kv.PutUserWorkspace("key", []byte("val"), "user-1", "ws") },
			checkOp:   pb.KVOperation_PUT,
		},
		{
			name:      "DeleteUserWorkspace",
			operation: func() error { return kv.DeleteUserWorkspace("key", "user-1", "ws") },
			checkOp:   pb.KVOperation_DELETE,
		},
		{
			name:      "ListUserWorkspace",
			operation: func() error { return kv.ListUserWorkspace("prefix", "user-1", "ws") },
			checkOp:   pb.KVOperation_LIST,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.operation()
			if err != nil {
				t.Errorf("%s() error = %v", tt.name, err)
			}

			select {
			case msg := <-client.RequestQueue():
				kvOp := msg.GetKvOp()
				if kvOp.Op != tt.checkOp {
					t.Errorf("Op = %v, want %v", kvOp.Op, tt.checkOp)
				}
				if kvOp.Scope != kvScopeToProto(KVScopeUserWorkspace) {
					t.Errorf("Scope = %v, want %v", kvOp.Scope, kvScopeToProto(KVScopeUserWorkspace))
				}
				if kvOp.UserId != "user-1" {
					t.Errorf("UserID = %q, want %q", kvOp.UserId, "user-1")
				}
				if kvOp.Workspace != "ws" {
					t.Errorf("Workspace = %q, want %q", kvOp.Workspace, "ws")
				}
			default:
				t.Error("Message should be in queue")
			}
		})
	}
}

// =============================================================================
// Synchronous KV Operation Tests
// =============================================================================

func TestKV_GetSync_Success(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()

	// Simulate a response being received via correlation
	go func() {
		time.Sleep(10 * time.Millisecond)
		// Drain the request that was sent and extract request_id
		msg := <-client.RequestQueue()
		reqID := msg.GetKvOp().GetRequestId()
		// Resolve via correlation
		client.ResolvePendingKVRequest(reqID, &KVResponse{
			Success:   true,
			Value:     []byte("test-value"),
			RequestId: reqID,
		})
	}()

	ctx := context.Background()
	resp, err := kv.GetSync(ctx, KVGetOptions{
		Key:     "test-key",
		Scope:   KVScopeGlobal,
		Timeout: 1 * time.Second,
	})

	if err != nil {
		t.Errorf("GetSync() error = %v", err)
	}
	if resp == nil {
		t.Fatal("GetSync() response should not be nil")
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
	if string(resp.Value) != "test-value" {
		t.Errorf("Value = %q, want %q", resp.Value, "test-value")
	}
}

func TestKV_GetSync_Timeout(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()

	ctx := context.Background()
	_, err = kv.GetSync(ctx, KVGetOptions{
		Key:     "test-key",
		Scope:   KVScopeGlobal,
		Timeout: 50 * time.Millisecond,
	})

	if err == nil {
		t.Fatal("GetSync() should timeout")
	}

	var timeoutErr *TimeoutError
	if !isTimeoutError(err, &timeoutErr) {
		t.Errorf("GetSync() error type = %T, want *TimeoutError", err)
	}
}

func TestKV_GetSync_ContextCanceled(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	_, err = kv.GetSync(ctx, KVGetOptions{
		Key:     "test-key",
		Scope:   KVScopeGlobal,
		Timeout: 5 * time.Second,
	})

	if err == nil {
		t.Fatal("GetSync() should return error on context cancel")
	}
}

func TestKV_GetSync_DefaultTimeout(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()

	// Simulate a response being received via correlation
	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetKvOp().GetRequestId()
		client.ResolvePendingKVRequest(reqID, &KVResponse{Success: true, RequestId: reqID})
	}()

	ctx := context.Background()
	// Timeout = 0 should use DefaultKVTimeout
	resp, err := kv.GetSync(ctx, KVGetOptions{
		Key:   "test-key",
		Scope: KVScopeGlobal,
	})

	if err != nil {
		t.Errorf("GetSync() error = %v", err)
	}
	if resp == nil {
		t.Fatal("GetSync() response should not be nil")
	}
}

func TestKV_PutSync_Success(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetKvOp().GetRequestId()
		client.ResolvePendingKVRequest(reqID, &KVResponse{Success: true, RequestId: reqID})
	}()

	ctx := context.Background()
	resp, err := kv.PutSync(ctx, KVPutOptions{
		Key:     "test-key",
		Value:   []byte("test-value"),
		Scope:   KVScopeGlobal,
		TTL:     1 * time.Hour,
		Timeout: 1 * time.Second,
	})

	if err != nil {
		t.Errorf("PutSync() error = %v", err)
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
}

func TestKV_ListSync_Success(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetKvOp().GetRequestId()
		client.ResolvePendingKVRequest(reqID, &KVResponse{
			Success:   true,
			Keys:      []string{"key1", "key2", "key3"},
			RequestId: reqID,
		})
	}()

	ctx := context.Background()
	resp, err := kv.ListSync(ctx, KVListOptions{
		KeyPrefix: "prefix_",
		Scope:     KVScopeGlobal,
		Timeout:   1 * time.Second,
	})

	if err != nil {
		t.Errorf("ListSync() error = %v", err)
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
	if len(resp.Keys) != 3 {
		t.Errorf("Keys length = %d, want 3", len(resp.Keys))
	}
}

func TestKV_DeleteSync_Success(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetKvOp().GetRequestId()
		client.ResolvePendingKVRequest(reqID, &KVResponse{Success: true, RequestId: reqID})
	}()

	ctx := context.Background()
	resp, err := kv.DeleteSync(ctx, KVDeleteOptions{
		Key:     "delete-key",
		Scope:   KVScopeGlobal,
		Timeout: 1 * time.Second,
	})

	if err != nil {
		t.Errorf("DeleteSync() error = %v", err)
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
}

// =============================================================================
// BaseClient Direct KV Methods (Python API Compatibility)
// =============================================================================

func TestBaseClient_KVGet(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.KVGet("test-key", KVScopeGlobal, "", "")
	if err != nil {
		t.Errorf("KVGet() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp.Op != pb.KVOperation_GET {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_GET)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestBaseClient_KVPut(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.KVPut("test-key", []byte("value"), KVScopeWorkspace, "", "ws", 3600)
	if err != nil {
		t.Errorf("KVPut() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp.Op != pb.KVOperation_PUT {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_PUT)
		}
		if kvOp.Ttl != 3600 {
			t.Errorf("TTL = %d, want 3600", kvOp.Ttl)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestBaseClient_KVList(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.KVList("prefix_", KVScopeUser, "user-1", "")
	if err != nil {
		t.Errorf("KVList() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp.Op != pb.KVOperation_LIST {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_LIST)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestBaseClient_KVDelete(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.KVDelete("test-key", KVScopeGlobal, "", "")
	if err != nil {
		t.Errorf("KVDelete() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp.Op != pb.KVOperation_DELETE {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_DELETE)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestBaseClient_KVGetSync(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetKvOp().GetRequestId()
		client.ResolvePendingKVRequest(reqID, &KVResponse{Success: true, Value: []byte("result"), RequestId: reqID})
	}()

	ctx := context.Background()
	resp, err := client.KVGetSync(ctx, "key", KVScopeGlobal, "", "", 1*time.Second)

	if err != nil {
		t.Errorf("KVGetSync() error = %v", err)
	}
	if string(resp.Value) != "result" {
		t.Errorf("Value = %q, want %q", resp.Value, "result")
	}
}

func TestBaseClient_KVPutSync(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetKvOp().GetRequestId()
		client.ResolvePendingKVRequest(reqID, &KVResponse{Success: true, RequestId: reqID})
	}()

	ctx := context.Background()
	resp, err := client.KVPutSync(ctx, "key", []byte("value"), KVScopeGlobal, "", "", 1*time.Hour, 1*time.Second)

	if err != nil {
		t.Errorf("KVPutSync() error = %v", err)
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
}

func TestBaseClient_KVListSync(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetKvOp().GetRequestId()
		client.ResolvePendingKVRequest(reqID, &KVResponse{Success: true, Keys: []string{"a", "b"}, RequestId: reqID})
	}()

	ctx := context.Background()
	resp, err := client.KVListSync(ctx, "prefix", KVScopeGlobal, "", "", 1*time.Second)

	if err != nil {
		t.Errorf("KVListSync() error = %v", err)
	}
	if len(resp.Keys) != 2 {
		t.Errorf("Keys length = %d, want 2", len(resp.Keys))
	}
}

func TestBaseClient_KVDeleteSync(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetKvOp().GetRequestId()
		client.ResolvePendingKVRequest(reqID, &KVResponse{Success: true, RequestId: reqID})
	}()

	ctx := context.Background()
	resp, err := client.KVDeleteSync(ctx, "key", KVScopeGlobal, "", "", 1*time.Second)

	if err != nil {
		t.Errorf("KVDeleteSync() error = %v", err)
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
}

// =============================================================================
// Drain Response Queue Tests
// =============================================================================

func TestKV_DrainResponseQueue(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Add some responses to the queue
	for i := 0; i < 3; i++ {
		client.KVResponseQueue() <- &KVResponse{Success: true}
	}

	kv := client.KV()
	kv.drainResponseQueue()

	// Verify queue is empty
	select {
	case <-client.KVResponseQueue():
		t.Error("Queue should be empty after draining")
	default:
		// Expected
	}
}

// =============================================================================
// Increment / Decrement Tests
// =============================================================================

func TestKV_Increment(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()
	err = kv.Increment("counter-key", KVScopeGlobal, "", "")
	if err != nil {
		t.Errorf("Increment() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp == nil {
			t.Fatal("Expected KVOperation in message")
		}
		if kvOp.Op != pb.KVOperation_INCREMENT {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_INCREMENT)
		}
		if kvOp.Key != "counter-key" {
			t.Errorf("Key = %q, want %q", kvOp.Key, "counter-key")
		}
		if kvOp.Scope != pb.KVOperation_GLOBAL {
			t.Errorf("Scope = %v, want %v", kvOp.Scope, pb.KVOperation_GLOBAL)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestKV_Decrement(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()
	err = kv.Decrement("counter-key", KVScopeWorkspace, "", "test-ws")
	if err != nil {
		t.Errorf("Decrement() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp == nil {
			t.Fatal("Expected KVOperation in message")
		}
		if kvOp.Op != pb.KVOperation_DECREMENT {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_DECREMENT)
		}
		if kvOp.Key != "counter-key" {
			t.Errorf("Key = %q, want %q", kvOp.Key, "counter-key")
		}
		if kvOp.Scope != pb.KVOperation_WORKSPACE {
			t.Errorf("Scope = %v, want %v", kvOp.Scope, pb.KVOperation_WORKSPACE)
		}
		if kvOp.Workspace != "test-ws" {
			t.Errorf("Workspace = %q, want %q", kvOp.Workspace, "test-ws")
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestKV_IncrementSync_Success(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetKvOp().GetRequestId()
		client.ResolvePendingKVRequest(reqID, &KVResponse{
			Success:      true,
			CounterValue: 5,
			RequestId:    reqID,
		})
	}()

	ctx := context.Background()
	resp, err := kv.IncrementSync(ctx, "counter", KVScopeGlobal, "", "", 1*time.Second)
	if err != nil {
		t.Errorf("IncrementSync() error = %v", err)
	}
	if resp == nil {
		t.Fatal("IncrementSync() response should not be nil")
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
	if resp.CounterValue != 5 {
		t.Errorf("CounterValue = %d, want 5", resp.CounterValue)
	}
}

func TestKV_DecrementSync_Success(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetKvOp().GetRequestId()
		client.ResolvePendingKVRequest(reqID, &KVResponse{
			Success:      true,
			CounterValue: 3,
			RequestId:    reqID,
		})
	}()

	ctx := context.Background()
	resp, err := kv.DecrementSync(ctx, "counter", KVScopeGlobal, "", "", 1*time.Second)
	if err != nil {
		t.Errorf("DecrementSync() error = %v", err)
	}
	if resp == nil {
		t.Fatal("DecrementSync() response should not be nil")
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
	if resp.CounterValue != 3 {
		t.Errorf("CounterValue = %d, want 3", resp.CounterValue)
	}
}

func TestKV_IncrementGlobal(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()
	err = kv.IncrementGlobal("my-counter")
	if err != nil {
		t.Errorf("IncrementGlobal() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp.Op != pb.KVOperation_INCREMENT {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_INCREMENT)
		}
		if kvOp.Key != "my-counter" {
			t.Errorf("Key = %q, want %q", kvOp.Key, "my-counter")
		}
		if kvOp.Scope != pb.KVOperation_GLOBAL {
			t.Errorf("Scope = %v, want %v", kvOp.Scope, pb.KVOperation_GLOBAL)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestKV_DecrementGlobal(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	kv := client.KV()
	err = kv.DecrementGlobal("my-counter")
	if err != nil {
		t.Errorf("DecrementGlobal() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp.Op != pb.KVOperation_DECREMENT {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_DECREMENT)
		}
		if kvOp.Key != "my-counter" {
			t.Errorf("Key = %q, want %q", kvOp.Key, "my-counter")
		}
		if kvOp.Scope != pb.KVOperation_GLOBAL {
			t.Errorf("Scope = %v, want %v", kvOp.Scope, pb.KVOperation_GLOBAL)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestBaseClient_KVIncrement(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.KV().Increment("ctr", KVScopeGlobal, "", "")
	if err != nil {
		t.Errorf("KV().Increment() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp.Op != pb.KVOperation_INCREMENT {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_INCREMENT)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestBaseClient_KVDecrement(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.KV().Decrement("ctr", KVScopeGlobal, "", "")
	if err != nil {
		t.Errorf("KV().Decrement() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		kvOp := msg.GetKvOp()
		if kvOp.Op != pb.KVOperation_DECREMENT {
			t.Errorf("Op = %v, want %v", kvOp.Op, pb.KVOperation_DECREMENT)
		}
	default:
		t.Error("Message should be in queue")
	}
}

// =============================================================================
// Error Cases
// =============================================================================

func TestKV_Get_NotRunning(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	// client.running is false by default

	kv := client.KV()
	err = kv.Get("test-key", KVScopeGlobal, "", "")

	if err == nil {
		t.Error("Get() should fail when client is not running")
	}
}

// =============================================================================
// Helper Functions
// =============================================================================

// isTimeoutError checks if an error is a TimeoutError using type assertion
func isTimeoutError(err error, target **TimeoutError) bool {
	if te, ok := err.(*TimeoutError); ok {
		*target = te
		return true
	}
	return false
}
