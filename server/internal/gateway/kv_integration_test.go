package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/internal/testutil"
	"github.com/scitrera/aether/pkg/models"
)

// TestKVOperationResponseFlow tests that KV operations receive proper responses
func TestKVOperationResponseFlow(t *testing.T) {
	// Use first Redis node from dev infrastructure
	redisAddrs := testutil.GetRedisAddrs()
	if len(redisAddrs) == 0 {
		t.Skip("No Redis addresses configured")
	}
	redisAddr := redisAddrs[0]

	// Create KV store
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()
	kvStore := kv.NewStoreFromClient(redisClient)

	// Test connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := kvStore.Ping(ctx); err != nil {
		t.Skipf("Redis not available at %s (dev infrastructure may not be running): %v", redisAddr, err)
	}

	// Create test agent identity
	agent := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "test-workspace",
		Implementation: "test-agent",
		Specifier:      "v1",
	}

	// Clean up before test
	t.Cleanup(func() {
		kvStore.DeleteByPattern(context.Background(), agent, kv.ScopeGlobal, "*", "", "")
		kvStore.DeleteByPattern(context.Background(), agent, kv.ScopeWorkspace, "*", "", "test-workspace")
	})

	// Create KV handler (nil audit logger for tests)
	handler := NewKVHandler(kvStore, nil, nil)

	t.Run("PUT operation sends success response", func(t *testing.T) {
		var responseReceived bool
		var responseSuccess bool

		sendResponse := func(msg *pb.DownstreamMessage) {
			responseReceived = true
			if kv := msg.GetKv(); kv != nil {
				responseSuccess = kv.Success
			}
		}

		op := &pb.KVOperation{
			Op:        pb.KVOperation_PUT,
			Scope:     pb.KVOperation_GLOBAL,
			Key:       "test_key",
			Value:     []byte("test_value"),
			Workspace: "",
			UserId:    "",
			Ttl:       0,
		}

		sessionID := uuid.New()
		err := handler.HandleKVOperation(context.Background(), agent, sessionID, nil, op, sendResponse)
		if err != nil {
			t.Fatalf("HandleKVOperation failed: %v", err)
		}

		if !responseReceived {
			t.Error("No response was sent")
		}
		if !responseSuccess {
			t.Error("Response did not indicate success")
		}
	})

	t.Run("GET operation sends value response", func(t *testing.T) {
		// First set a value
		kvStore.Set(context.Background(), agent, kv.ScopeGlobal, "get_test", "retrieved_value", "", "", 0)

		var responseReceived bool
		var responseValue string

		sendResponse := func(msg *pb.DownstreamMessage) {
			responseReceived = true
			if kv := msg.GetKv(); kv != nil {
				responseValue = string(kv.Value)
			}
		}

		op := &pb.KVOperation{
			Op:        pb.KVOperation_GET,
			Scope:     pb.KVOperation_GLOBAL,
			Key:       "get_test",
			Workspace: "",
			UserId:    "",
		}

		sessionID := uuid.New()
		err := handler.HandleKVOperation(context.Background(), agent, sessionID, nil, op, sendResponse)
		if err != nil {
			t.Fatalf("HandleKVOperation failed: %v", err)
		}

		if !responseReceived {
			t.Error("No response was sent")
		}
		if responseValue != "retrieved_value" {
			t.Errorf("Expected 'retrieved_value', got '%s'", responseValue)
		}
	})

	t.Run("LIST operation sends keys response", func(t *testing.T) {
		// Set some keys
		kvStore.Set(context.Background(), agent, kv.ScopeGlobal, "list_key1", "value1", "", "", 0)
		kvStore.Set(context.Background(), agent, kv.ScopeGlobal, "list_key2", "value2", "", "", 0)

		var responseReceived bool
		var responseKeys []string

		sendResponse := func(msg *pb.DownstreamMessage) {
			responseReceived = true
			if kv := msg.GetKv(); kv != nil {
				responseKeys = kv.Keys
			}
		}

		op := &pb.KVOperation{
			Op:        pb.KVOperation_LIST,
			Scope:     pb.KVOperation_GLOBAL,
			Workspace: "",
			UserId:    "",
		}

		sessionID := uuid.New()
		err := handler.HandleKVOperation(context.Background(), agent, sessionID, nil, op, sendResponse)
		if err != nil {
			t.Fatalf("HandleKVOperation failed: %v", err)
		}

		if !responseReceived {
			t.Error("No response was sent")
		}
		if len(responseKeys) < 2 {
			t.Errorf("Expected at least 2 keys, got %d", len(responseKeys))
		}
	})

	t.Run("DELETE operation sends success response", func(t *testing.T) {
		// Set a key first
		kvStore.Set(context.Background(), agent, kv.ScopeGlobal, "delete_test", "to_delete", "", "", 0)

		var responseReceived bool
		var responseSuccess bool

		sendResponse := func(msg *pb.DownstreamMessage) {
			responseReceived = true
			if kv := msg.GetKv(); kv != nil {
				responseSuccess = kv.Success
			}
		}

		op := &pb.KVOperation{
			Op:        pb.KVOperation_DELETE,
			Scope:     pb.KVOperation_GLOBAL,
			Key:       "delete_test",
			Workspace: "",
			UserId:    "",
		}

		sessionID := uuid.New()
		err := handler.HandleKVOperation(context.Background(), agent, sessionID, nil, op, sendResponse)
		if err != nil {
			t.Fatalf("HandleKVOperation failed: %v", err)
		}

		if !responseReceived {
			t.Error("No response was sent")
		}
		if !responseSuccess {
			t.Error("Response did not indicate success")
		}

		// Verify key is deleted
		exists, _ := kvStore.Exists(context.Background(), agent, kv.ScopeGlobal, "delete_test", "", "")
		if exists {
			t.Error("Key should have been deleted")
		}
	})

	t.Run("Permission denied for non-agent", func(t *testing.T) {
		user := models.Identity{
			Type: models.PrincipalUser,
			ID:   "alice",
		}

		sendResponse := func(msg *pb.DownstreamMessage) {
			t.Error("No response should be sent for permission denied")
		}

		op := &pb.KVOperation{
			Op:    pb.KVOperation_GET,
			Scope: pb.KVOperation_GLOBAL,
			Key:   "test",
		}

		sessionID := uuid.New()
		err := handler.HandleKVOperation(context.Background(), user, sessionID, nil, op, sendResponse)
		if err == nil {
			t.Error("Expected permission error for user")
		}
	})
}
