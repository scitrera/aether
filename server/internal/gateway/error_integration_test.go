package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/internal/testutil"
	pkgerrors "github.com/scitrera/aether/pkg/errors"
	"github.com/scitrera/aether/pkg/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestErrorIntegrationResponseFormatting tests that errors are properly converted to ErrorResponse messages
func TestErrorIntegrationResponseFormatting(t *testing.T) {
	t.Run("DuplicateIdentityError includes error code", func(t *testing.T) {
		err := &pkgerrors.DuplicateIdentityError{
			Identity:          "ag::test::worker::v1",
			ExistingSessionID: "session-123",
		}

		errResp := pkgerrors.ToErrorResponse(err)
		if errResp == nil {
			t.Fatal("Expected error response, got nil")
		}

		if errResp.Code != pkgerrors.ErrSessionDuplicate {
			t.Errorf("Expected code %s, got %s", pkgerrors.ErrSessionDuplicate, errResp.Code)
		}

		if errResp.Message == "" {
			t.Error("Expected non-empty message")
		}

		expectedMsg := "identity 'ag::test::worker::v1' is already connected (session: session-123)"
		if errResp.Message != expectedMsg {
			t.Errorf("Expected message '%s', got '%s'", expectedMsg, errResp.Message)
		}
	})

	t.Run("AgentNotFoundError includes error code", func(t *testing.T) {
		err := &pkgerrors.AgentNotFoundError{
			Implementation: "test-agent",
		}

		errResp := pkgerrors.ToErrorResponse(err)
		if errResp == nil {
			t.Fatal("Expected error response, got nil")
		}

		if errResp.Code != pkgerrors.ErrOrchAgentNotFound {
			t.Errorf("Expected code %s, got %s", pkgerrors.ErrOrchAgentNotFound, errResp.Code)
		}
	})

	t.Run("QuotaExceededError includes error code and is retryable", func(t *testing.T) {
		err := &pkgerrors.QuotaExceededError{
			Resource:  "connections",
			Workspace: "prod",
			Current:   100,
			Limit:     100,
		}

		errResp := pkgerrors.ToErrorResponse(err)
		if errResp == nil {
			t.Fatal("Expected error response, got nil")
		}

		if errResp.Code != pkgerrors.ErrQuotaExceeded {
			t.Errorf("Expected code %s, got %s", pkgerrors.ErrQuotaExceeded, errResp.Code)
		}

		if !errResp.Retryable {
			t.Error("QuotaExceededError should be retryable")
		}
	})
}

// TestErrorIntegrationKVOperations tests that KV operation errors are properly sent back to clients
func TestErrorIntegrationKVOperations(t *testing.T) {
	// Use first Redis node from dev infrastructure
	redisAddrs := testutil.GetRedisAddrs()
	if len(redisAddrs) == 0 {
		t.Skip("No Redis addresses configured")
	}
	redisAddr := redisAddrs[0]

	// Create KV store - we need a real one but don't need it to connect for permission tests
	// We'll use a dummy address since permission checks happen before store access
	kvStore := NewKVStoreForTesting(redisAddr)
	if kvStore == nil {
		t.Skip("Could not create KV store for testing")
	}
	// Note: Do not call kvStore.Close() - the Redis client is managed by the application

	// Test connectivity - skip if Redis not available
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

	// Create KV handler
	auditLogger := audit.NewAuditLogger(nil, "", nil) // Test mode with no DB, default config
	handler := NewKVHandler(kvStore, auditLogger, nil)

	t.Run("Permission denied for non-agent returns error", func(t *testing.T) {
		user := models.Identity{
			Type: models.PrincipalUser,
			ID:   "alice",
		}

		sendResponse := func(msg *pb.DownstreamMessage) {
			// Response function - not called when permission is denied
		}

		op := &pb.KVOperation{
			Op:    pb.KVOperation_PUT,
			Scope: pb.KVOperation_GLOBAL,
			Key:   "test_key",
			Value: []byte("test_value"),
		}

		sessionID := uuid.New()
		err := handler.HandleKVOperation(ctx, user, sessionID, nil, op, sendResponse)
		if err == nil {
			t.Fatal("Expected error for non-agent KV operation")
		}

		// The error should be a gRPC status error with PermissionDenied
		if st, ok := status.FromError(err); !ok || st.Code() != codes.PermissionDenied {
			t.Errorf("Expected PermissionDenied error, got: %v", err)
		}
	})

	t.Run("GET non-existent key returns success with empty value", func(t *testing.T) {
		var receivedResp *pb.DownstreamMessage
		sendResponse := func(msg *pb.DownstreamMessage) {
			receivedResp = msg
		}

		op := &pb.KVOperation{
			Op:        pb.KVOperation_GET,
			Scope:     pb.KVOperation_GLOBAL,
			Key:       "non_existent_key_12345",
			Workspace: "",
			UserId:    "",
		}

		sessionID := uuid.New()
		err := handler.HandleKVOperation(ctx, agent, sessionID, nil, op, sendResponse)
		if err != nil {
			t.Fatalf("unexpected error for non-existent key: %v", err)
		}

		// Key not found is a normal GET miss — returns success with nil value
		if receivedResp == nil {
			t.Fatal("expected a response for non-existent key")
		}
		kvResp := receivedResp.GetKv()
		if kvResp == nil {
			t.Fatal("expected KVResponse payload")
		}
		if !kvResp.Success {
			t.Errorf("expected success=true for non-existent key, got false")
		}
		if kvResp.Value != nil {
			t.Errorf("expected nil value for non-existent key, got %v", kvResp.Value)
		}
	})
}

// TestErrorIntegrationCategories verifies that all remaining error types are mapped to correct gRPC codes
func TestErrorIntegrationCategories(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		expectedCode string
		expectedGRPC codes.Code
	}{
		{
			name:         "DuplicateIdentityError",
			err:          &pkgerrors.DuplicateIdentityError{Identity: "ag::test::worker::v1"},
			expectedCode: pkgerrors.ErrSessionDuplicate,
			expectedGRPC: codes.AlreadyExists,
		},
		{
			name:         "AgentNotFoundError",
			err:          &pkgerrors.AgentNotFoundError{Implementation: "worker"},
			expectedCode: pkgerrors.ErrOrchAgentNotFound,
			expectedGRPC: codes.NotFound,
		},
		{
			name:         "OrchestratorNotFoundError",
			err:          &pkgerrors.OrchestratorNotFoundError{Profile: "k8s", Workspace: "prod"},
			expectedCode: pkgerrors.ErrOrchUnavailable,
			expectedGRPC: codes.NotFound,
		},
		{
			name:         "TaskNotFoundError",
			err:          &pkgerrors.TaskNotFoundError{TaskID: "task-123"},
			expectedCode: pkgerrors.ErrOrchTaskAssignment,
			expectedGRPC: codes.NotFound,
		},
		{
			name:         "QuotaExceededError",
			err:          &pkgerrors.QuotaExceededError{Resource: "connections", Workspace: "prod", Current: 100, Limit: 100},
			expectedCode: pkgerrors.ErrQuotaExceeded,
			expectedGRPC: codes.ResourceExhausted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test error code extraction
			errResp := pkgerrors.ToErrorResponse(tt.err)
			if errResp == nil {
				t.Fatal("Expected error response, got nil")
			}

			if errResp.Code != tt.expectedCode {
				t.Errorf("Expected code %s, got %s", tt.expectedCode, errResp.Code)
			}

			if errResp.Message == "" {
				t.Error("Expected non-empty message")
			}

			// Test gRPC status code mapping
			grpcErr := pkgerrors.ToGRPCStatus(tt.err)
			if grpcErr == nil {
				t.Fatal("Expected gRPC error, got nil")
			}

			st, ok := status.FromError(grpcErr)
			if !ok {
				t.Fatal("Expected gRPC status error")
			}

			if st.Code() != tt.expectedGRPC {
				t.Errorf("Expected gRPC code %v, got %v", tt.expectedGRPC, st.Code())
			}
		})
	}
}

// NewKVStoreForTesting creates a KV store for testing
func NewKVStoreForTesting(addr string) *kv.Store {
	if addr == "" {
		return nil
	}
	return kv.NewStoreFromClient(redis.NewClient(&redis.Options{Addr: addr}))
}
