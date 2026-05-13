package aether

import (
	"context"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// =============================================================================
// Checkpoint Helper Tests
// =============================================================================

func TestCheckpoint_NewCheckpoint(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	cp := newCheckpoint(client)
	if cp == nil {
		t.Fatal("newCheckpoint() should not return nil")
	}
	if cp.client != client {
		t.Error("Checkpoint client reference should match")
	}
}

func TestBaseClient_Checkpoint(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	cp := client.Checkpoint()
	if cp == nil {
		t.Fatal("Checkpoint() should not return nil")
	}
}

// =============================================================================
// Async Checkpoint Operation Tests
// =============================================================================

func TestCheckpoint_Save(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()
	data := testCheckpointData()
	err = cp.Save(data, "my_checkpoint", 7200)
	if err != nil {
		t.Errorf("Save() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		cpOp := msg.GetCheckpointOp()
		if cpOp == nil {
			t.Fatal("Expected CheckpointOperation in message")
		}
		if cpOp.Op != pb.CheckpointOperation_SAVE {
			t.Errorf("Op = %v, want %v", cpOp.Op, pb.CheckpointOperation_SAVE)
		}
		if cpOp.Key != "my_checkpoint" {
			t.Errorf("Key = %q, want %q", cpOp.Key, "my_checkpoint")
		}
		if string(cpOp.Data) != string(data) {
			t.Errorf("Data = %q, want %q", cpOp.Data, data)
		}
		if cpOp.Ttl != 7200 {
			t.Errorf("TTL = %d, want 7200", cpOp.Ttl)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestCheckpoint_Load(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()
	err = cp.Load("my_checkpoint")
	if err != nil {
		t.Errorf("Load() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		cpOp := msg.GetCheckpointOp()
		if cpOp == nil {
			t.Fatal("Expected CheckpointOperation in message")
		}
		if cpOp.Op != pb.CheckpointOperation_LOAD {
			t.Errorf("Op = %v, want %v", cpOp.Op, pb.CheckpointOperation_LOAD)
		}
		if cpOp.Key != "my_checkpoint" {
			t.Errorf("Key = %q, want %q", cpOp.Key, "my_checkpoint")
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestCheckpoint_Delete(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()
	err = cp.Delete("my_checkpoint")
	if err != nil {
		t.Errorf("Delete() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		cpOp := msg.GetCheckpointOp()
		if cpOp == nil {
			t.Fatal("Expected CheckpointOperation in message")
		}
		if cpOp.Op != pb.CheckpointOperation_DELETE {
			t.Errorf("Op = %v, want %v", cpOp.Op, pb.CheckpointOperation_DELETE)
		}
		if cpOp.Key != "my_checkpoint" {
			t.Errorf("Key = %q, want %q", cpOp.Key, "my_checkpoint")
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestCheckpoint_List(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()
	err = cp.List()
	if err != nil {
		t.Errorf("List() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		cpOp := msg.GetCheckpointOp()
		if cpOp == nil {
			t.Fatal("Expected CheckpointOperation in message")
		}
		if cpOp.Op != pb.CheckpointOperation_LIST {
			t.Errorf("Op = %v, want %v", cpOp.Op, pb.CheckpointOperation_LIST)
		}
	default:
		t.Error("Message should be in queue")
	}
}

// =============================================================================
// Convenience Method Tests
// =============================================================================

func TestCheckpoint_SaveDefault(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()
	data := []byte("default checkpoint data")
	err = cp.SaveDefault(data)
	if err != nil {
		t.Errorf("SaveDefault() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		cpOp := msg.GetCheckpointOp()
		if cpOp.Op != pb.CheckpointOperation_SAVE {
			t.Errorf("Op = %v, want %v", cpOp.Op, pb.CheckpointOperation_SAVE)
		}
		if cpOp.Key != "" {
			t.Errorf("Key = %q, want empty string for default", cpOp.Key)
		}
		if cpOp.Ttl != -1 {
			t.Errorf("TTL = %d, want -1 (server default)", cpOp.Ttl)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestCheckpoint_LoadDefault(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()
	err = cp.LoadDefault()
	if err != nil {
		t.Errorf("LoadDefault() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		cpOp := msg.GetCheckpointOp()
		if cpOp.Op != pb.CheckpointOperation_LOAD {
			t.Errorf("Op = %v, want %v", cpOp.Op, pb.CheckpointOperation_LOAD)
		}
		if cpOp.Key != "" {
			t.Errorf("Key = %q, want empty string for default", cpOp.Key)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestCheckpoint_DeleteDefault(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()
	err = cp.DeleteDefault()
	if err != nil {
		t.Errorf("DeleteDefault() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		cpOp := msg.GetCheckpointOp()
		if cpOp.Op != pb.CheckpointOperation_DELETE {
			t.Errorf("Op = %v, want %v", cpOp.Op, pb.CheckpointOperation_DELETE)
		}
		if cpOp.Key != "" {
			t.Errorf("Key = %q, want empty string for default", cpOp.Key)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestCheckpoint_SaveWithTTL(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()
	data := []byte("ttl checkpoint data")
	err = cp.SaveWithTTL(data, "ttl-checkpoint", 2*time.Hour)
	if err != nil {
		t.Errorf("SaveWithTTL() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		cpOp := msg.GetCheckpointOp()
		if cpOp.Key != "ttl-checkpoint" {
			t.Errorf("Key = %q, want %q", cpOp.Key, "ttl-checkpoint")
		}
		if cpOp.Ttl != 7200 {
			t.Errorf("TTL = %d, want 7200 (2 hours)", cpOp.Ttl)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestCheckpoint_SaveWithTTL_ZeroDuration(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()
	data := []byte("zero ttl data")
	err = cp.SaveWithTTL(data, "zero-ttl", 0)
	if err != nil {
		t.Errorf("SaveWithTTL() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		cpOp := msg.GetCheckpointOp()
		if cpOp.Ttl != 0 {
			t.Errorf("TTL = %d, want 0 (no expiration)", cpOp.Ttl)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestCheckpoint_SavePermanent(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()
	data := []byte("permanent checkpoint data")
	err = cp.SavePermanent(data, "permanent-checkpoint")
	if err != nil {
		t.Errorf("SavePermanent() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		cpOp := msg.GetCheckpointOp()
		if cpOp.Key != "permanent-checkpoint" {
			t.Errorf("Key = %q, want %q", cpOp.Key, "permanent-checkpoint")
		}
		if cpOp.Ttl != 0 {
			t.Errorf("TTL = %d, want 0 (no expiration)", cpOp.Ttl)
		}
	default:
		t.Error("Message should be in queue")
	}
}

// =============================================================================
// Synchronous Checkpoint Operation Tests
// =============================================================================

func TestCheckpoint_SaveSync_Success(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetCheckpointOp().GetRequestId()
		client.ResolvePendingCheckpointRequest(reqID, &CheckpointResponse{
			Success:   true,
			SavedAt:   time.Now(),
			RequestId: reqID,
		})
	}()

	ctx := context.Background()
	resp, err := cp.SaveSync(ctx, CheckpointSaveOptions{
		Data:    []byte("sync save data"),
		Key:     "sync-checkpoint",
		TTL:     1 * time.Hour,
		Timeout: 1 * time.Second,
	})

	if err != nil {
		t.Errorf("SaveSync() error = %v", err)
	}
	if resp == nil {
		t.Fatal("SaveSync() response should not be nil")
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
}

func TestCheckpoint_SaveSync_TTLHandling(t *testing.T) {
	tests := []struct {
		name        string
		ttl         time.Duration
		expectedTTL int64
	}{
		{
			name:        "positive TTL",
			ttl:         2 * time.Hour,
			expectedTTL: 7200,
		},
		{
			name:        "zero TTL (no expiration)",
			ttl:         0,
			expectedTTL: 0,
		},
		{
			name:        "negative TTL (server default)",
			ttl:         -1,
			expectedTTL: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := BaseClientConfig{ServerAddr: TestServerAddr}
			client, err := NewBaseClient(cfg)
			if err != nil {
				t.Fatalf("NewBaseClient() error = %v", err)
			}
			client.running.Store(true)

			cp := client.Checkpoint()

			go func() {
				time.Sleep(10 * time.Millisecond)
				msg := <-client.RequestQueue()
				cpOp := msg.GetCheckpointOp()
				if cpOp.Ttl != tt.expectedTTL {
					t.Errorf("TTL = %d, want %d", cpOp.Ttl, tt.expectedTTL)
				}
				reqID := cpOp.GetRequestId()
				client.ResolvePendingCheckpointRequest(reqID, &CheckpointResponse{Success: true, RequestId: reqID})
			}()

			ctx := context.Background()
			_, err = cp.SaveSync(ctx, CheckpointSaveOptions{
				Data:    []byte("data"),
				Key:     "key",
				TTL:     tt.ttl,
				Timeout: 1 * time.Second,
			})

			if err != nil {
				t.Errorf("SaveSync() error = %v", err)
			}
		})
	}
}

func TestCheckpoint_SaveSync_Timeout(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()

	ctx := context.Background()
	_, err = cp.SaveSync(ctx, CheckpointSaveOptions{
		Data:    []byte("timeout data"),
		Key:     "timeout-checkpoint",
		Timeout: 50 * time.Millisecond,
	})

	if err == nil {
		t.Fatal("SaveSync() should timeout")
	}

	var timeoutErr *TimeoutError
	if !isTimeoutError(err, &timeoutErr) {
		t.Errorf("SaveSync() error type = %T, want *TimeoutError", err)
	}
}

func TestCheckpoint_SaveSync_ContextCanceled(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	_, err = cp.SaveSync(ctx, CheckpointSaveOptions{
		Data:    []byte("cancel data"),
		Timeout: 5 * time.Second,
	})

	if err == nil {
		t.Fatal("SaveSync() should return error on context cancel")
	}
}

func TestCheckpoint_SaveSync_DefaultTimeout(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetCheckpointOp().GetRequestId()
		client.ResolvePendingCheckpointRequest(reqID, &CheckpointResponse{Success: true, RequestId: reqID})
	}()

	ctx := context.Background()
	// Timeout = 0 should use DefaultCheckpointTimeout
	resp, err := cp.SaveSync(ctx, CheckpointSaveOptions{
		Data: []byte("default timeout data"),
	})

	if err != nil {
		t.Errorf("SaveSync() error = %v", err)
	}
	if resp == nil {
		t.Fatal("SaveSync() response should not be nil")
	}
}

func TestCheckpoint_LoadSync_Success(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()
	expectedData := testCheckpointData()

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetCheckpointOp().GetRequestId()
		client.ResolvePendingCheckpointRequest(reqID, &CheckpointResponse{
			Success:   true,
			Data:      expectedData,
			SavedAt:   time.Now(),
			RequestId: reqID,
		})
	}()

	ctx := context.Background()
	resp, err := cp.LoadSync(ctx, CheckpointLoadOptions{
		Key:     "load-checkpoint",
		Timeout: 1 * time.Second,
	})

	if err != nil {
		t.Errorf("LoadSync() error = %v", err)
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
	if string(resp.Data) != string(expectedData) {
		t.Errorf("Data = %q, want %q", resp.Data, expectedData)
	}
}

func TestCheckpoint_LoadSync_NotFound(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetCheckpointOp().GetRequestId()
		// Empty data indicates checkpoint doesn't exist, but still success
		client.ResolvePendingCheckpointRequest(reqID, &CheckpointResponse{
			Success:   true,
			Data:      nil,
			RequestId: reqID,
		})
	}()

	ctx := context.Background()
	resp, err := cp.LoadSync(ctx, CheckpointLoadOptions{
		Key:     "nonexistent-checkpoint",
		Timeout: 1 * time.Second,
	})

	if err != nil {
		t.Errorf("LoadSync() error = %v", err)
	}
	if !resp.Success {
		t.Error("Response should be successful even for nonexistent checkpoint")
	}
	if len(resp.Data) != 0 {
		t.Errorf("Data should be empty for nonexistent checkpoint")
	}
}

func TestCheckpoint_DeleteSync_Success(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetCheckpointOp().GetRequestId()
		client.ResolvePendingCheckpointRequest(reqID, &CheckpointResponse{Success: true, RequestId: reqID})
	}()

	ctx := context.Background()
	resp, err := cp.DeleteSync(ctx, CheckpointDeleteOptions{
		Key:     "delete-checkpoint",
		Timeout: 1 * time.Second,
	})

	if err != nil {
		t.Errorf("DeleteSync() error = %v", err)
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
}

func TestCheckpoint_ListSync_Success(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetCheckpointOp().GetRequestId()
		client.ResolvePendingCheckpointRequest(reqID, &CheckpointResponse{
			Success:   true,
			Keys:      []string{"checkpoint-1", "checkpoint-2", "default"},
			RequestId: reqID,
		})
	}()

	ctx := context.Background()
	resp, err := cp.ListSync(ctx, 1*time.Second)

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

func TestCheckpoint_ListSync_DefaultTimeout(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetCheckpointOp().GetRequestId()
		client.ResolvePendingCheckpointRequest(reqID, &CheckpointResponse{Success: true, RequestId: reqID})
	}()

	ctx := context.Background()
	// Timeout = 0 should use DefaultCheckpointTimeout
	resp, err := cp.ListSync(ctx, 0)

	if err != nil {
		t.Errorf("ListSync() error = %v", err)
	}
	if resp == nil {
		t.Fatal("ListSync() response should not be nil")
	}
}

// =============================================================================
// BaseClient Direct Checkpoint Methods (Python API Compatibility)
// =============================================================================

func TestBaseClient_CheckpointSave(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	data := testCheckpointData()
	err = client.CheckpointSave(data, "my_checkpoint", 7200)
	if err != nil {
		t.Errorf("CheckpointSave() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		cpOp := msg.GetCheckpointOp()
		if cpOp.Op != pb.CheckpointOperation_SAVE {
			t.Errorf("Op = %v, want %v", cpOp.Op, pb.CheckpointOperation_SAVE)
		}
		if cpOp.Key != "my_checkpoint" {
			t.Errorf("Key = %q, want %q", cpOp.Key, "my_checkpoint")
		}
		if cpOp.Ttl != 7200 {
			t.Errorf("TTL = %d, want 7200", cpOp.Ttl)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestBaseClient_CheckpointLoad(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.CheckpointLoad("my_checkpoint")
	if err != nil {
		t.Errorf("CheckpointLoad() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		cpOp := msg.GetCheckpointOp()
		if cpOp.Op != pb.CheckpointOperation_LOAD {
			t.Errorf("Op = %v, want %v", cpOp.Op, pb.CheckpointOperation_LOAD)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestBaseClient_CheckpointDelete(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.CheckpointDelete("my_checkpoint")
	if err != nil {
		t.Errorf("CheckpointDelete() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		cpOp := msg.GetCheckpointOp()
		if cpOp.Op != pb.CheckpointOperation_DELETE {
			t.Errorf("Op = %v, want %v", cpOp.Op, pb.CheckpointOperation_DELETE)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestBaseClient_CheckpointList(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	err = client.CheckpointList()
	if err != nil {
		t.Errorf("CheckpointList() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		cpOp := msg.GetCheckpointOp()
		if cpOp.Op != pb.CheckpointOperation_LIST {
			t.Errorf("Op = %v, want %v", cpOp.Op, pb.CheckpointOperation_LIST)
		}
	default:
		t.Error("Message should be in queue")
	}
}

func TestBaseClient_CheckpointSaveSync(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetCheckpointOp().GetRequestId()
		client.ResolvePendingCheckpointRequest(reqID, &CheckpointResponse{Success: true, RequestId: reqID})
	}()

	ctx := context.Background()
	resp, err := client.CheckpointSaveSync(ctx, []byte("data"), "key", 1*time.Hour, 1*time.Second)

	if err != nil {
		t.Errorf("CheckpointSaveSync() error = %v", err)
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
}

func TestBaseClient_CheckpointLoadSync(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	expectedData := []byte("loaded data")

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetCheckpointOp().GetRequestId()
		client.ResolvePendingCheckpointRequest(reqID, &CheckpointResponse{
			Success:   true,
			Data:      expectedData,
			RequestId: reqID,
		})
	}()

	ctx := context.Background()
	resp, err := client.CheckpointLoadSync(ctx, "key", 1*time.Second)

	if err != nil {
		t.Errorf("CheckpointLoadSync() error = %v", err)
	}
	if string(resp.Data) != string(expectedData) {
		t.Errorf("Data = %q, want %q", resp.Data, expectedData)
	}
}

func TestBaseClient_CheckpointDeleteSync(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetCheckpointOp().GetRequestId()
		client.ResolvePendingCheckpointRequest(reqID, &CheckpointResponse{Success: true, RequestId: reqID})
	}()

	ctx := context.Background()
	resp, err := client.CheckpointDeleteSync(ctx, "key", 1*time.Second)

	if err != nil {
		t.Errorf("CheckpointDeleteSync() error = %v", err)
	}
	if !resp.Success {
		t.Error("Response should be successful")
	}
}

func TestBaseClient_CheckpointListSync(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetCheckpointOp().GetRequestId()
		client.ResolvePendingCheckpointRequest(reqID, &CheckpointResponse{
			Success:   true,
			Keys:      []string{"a", "b"},
			RequestId: reqID,
		})
	}()

	ctx := context.Background()
	resp, err := client.CheckpointListSync(ctx, 1*time.Second)

	if err != nil {
		t.Errorf("CheckpointListSync() error = %v", err)
	}
	if len(resp.Keys) != 2 {
		t.Errorf("Keys length = %d, want 2", len(resp.Keys))
	}
}

// =============================================================================
// Drain Response Queue Tests
// =============================================================================

func TestCheckpoint_DrainResponseQueue(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	// Add some responses to the queue
	for i := 0; i < 3; i++ {
		client.CheckpointResponseQueue() <- &CheckpointResponse{Success: true}
	}

	cp := client.Checkpoint()
	cp.drainResponseQueue()

	// Verify queue is empty
	select {
	case <-client.CheckpointResponseQueue():
		t.Error("Queue should be empty after draining")
	default:
		// Expected
	}
}

// =============================================================================
// Error Cases
// =============================================================================

func TestCheckpoint_Save_NotRunning(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	// client.running is false by default

	cp := client.Checkpoint()
	err = cp.Save([]byte("data"), "key", -1)

	if err == nil {
		t.Error("Save() should fail when client is not running")
	}
}

func TestCheckpoint_Load_NotRunning(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	cp := client.Checkpoint()
	err = cp.Load("key")

	if err == nil {
		t.Error("Load() should fail when client is not running")
	}
}

func TestCheckpoint_Delete_NotRunning(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	cp := client.Checkpoint()
	err = cp.Delete("key")

	if err == nil {
		t.Error("Delete() should fail when client is not running")
	}
}

func TestCheckpoint_List_NotRunning(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}

	cp := client.Checkpoint()
	err = cp.List()

	if err == nil {
		t.Error("List() should fail when client is not running")
	}
}

// =============================================================================
// Response Handling Tests
// =============================================================================

func TestCheckpoint_ResponseWithError(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	cp := client.Checkpoint()

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		reqID := msg.GetCheckpointOp().GetRequestId()
		client.ResolvePendingCheckpointRequest(reqID, &CheckpointResponse{
			Success:   false,
			Error:     "checkpoint not found",
			RequestId: reqID,
		})
	}()

	ctx := context.Background()
	resp, err := cp.LoadSync(ctx, CheckpointLoadOptions{
		Key:     "nonexistent",
		Timeout: 1 * time.Second,
	})

	if err != nil {
		t.Errorf("LoadSync() error = %v", err)
	}
	if resp.Success {
		t.Error("Response should indicate failure")
	}
	if resp.Error != "checkpoint not found" {
		t.Errorf("Error = %q, want %q", resp.Error, "checkpoint not found")
	}
}

// =============================================================================
// Constants Tests
// =============================================================================

func TestCheckpoint_DefaultTimeout(t *testing.T) {
	if DefaultCheckpointTimeout != 5*time.Second {
		t.Errorf("DefaultCheckpointTimeout = %v, want 5s", DefaultCheckpointTimeout)
	}
}
