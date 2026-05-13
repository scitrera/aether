package aether

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// =============================================================================
// AetherError Tests
// =============================================================================

// TestAetherError_Error tests the Error() method formatting
func TestAetherError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *AetherError
		expected string
	}{
		{
			name:     "message only",
			err:      &AetherError{Message: "something went wrong"},
			expected: "something went wrong",
		},
		{
			name:     "message with code",
			err:      &AetherError{Message: "authentication failed", Code: "UNAUTHENTICATED"},
			expected: "[UNAUTHENTICATED] authentication failed",
		},
		{
			name:     "message with details",
			err:      &AetherError{Message: "operation failed", Details: "timeout=30s"},
			expected: "operation failed (timeout=30s)",
		},
		{
			name:     "message with code and details",
			err:      &AetherError{Message: "operation timed out", Code: "DEADLINE_EXCEEDED", Details: "timeout=5.00s"},
			expected: "[DEADLINE_EXCEEDED] operation timed out (timeout=5.00s)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.err.Error()
			if result != tt.expected {
				t.Errorf("Error() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// TestAetherError_Unwrap tests error unwrapping
func TestAetherError_Unwrap(t *testing.T) {
	cause := fmt.Errorf("underlying error")
	err := &AetherError{
		Message: "wrapper error",
		cause:   cause,
	}

	unwrapped := err.Unwrap()
	if unwrapped != cause {
		t.Errorf("Unwrap() = %v, want %v", unwrapped, cause)
	}

	// Test errors.Is support
	if !errors.Is(err, cause) {
		t.Errorf("errors.Is should find the underlying cause")
	}
}

// TestAetherError_WithCause tests creating an error copy with a cause
func TestAetherError_WithCause(t *testing.T) {
	originalErr := &AetherError{
		Message: "original message",
		Code:    "TEST_CODE",
		Details: "some details",
	}

	cause := fmt.Errorf("the cause")
	withCause := originalErr.WithCause(cause)

	// Should be a new instance
	if withCause == originalErr {
		t.Error("WithCause should return a new instance")
	}

	// Should preserve all fields
	if withCause.Message != originalErr.Message {
		t.Errorf("Message = %q, want %q", withCause.Message, originalErr.Message)
	}
	if withCause.Code != originalErr.Code {
		t.Errorf("Code = %q, want %q", withCause.Code, originalErr.Code)
	}
	if withCause.Details != originalErr.Details {
		t.Errorf("Details = %q, want %q", withCause.Details, originalErr.Details)
	}

	// Should have the cause
	if withCause.Unwrap() != cause {
		t.Errorf("cause = %v, want %v", withCause.Unwrap(), cause)
	}
}

// =============================================================================
// Connection Error Tests
// =============================================================================

// TestNewConnectionError tests ConnectionError creation
func TestNewConnectionError(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    string
	}{
		{
			name:    "with message",
			message: "cannot reach gateway at localhost:50051",
			want:    "cannot reach gateway at localhost:50051",
		},
		{
			name:    "empty message uses default",
			message: "",
			want:    "failed to connect to Aether gateway",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewConnectionError(tt.message)
			if err.Message != tt.want {
				t.Errorf("Message = %q, want %q", err.Message, tt.want)
			}

			// Check it embeds AetherError
			if err.AetherError.Message != tt.want {
				t.Errorf("AetherError.Message = %q, want %q", err.AetherError.Message, tt.want)
			}
		})
	}
}

// TestNewConnectionClosedError tests ConnectionClosedError creation
func TestNewConnectionClosedError(t *testing.T) {
	reason := "server initiated disconnect"
	err := NewConnectionClosedError(reason)

	if err.Message != "connection closed unexpectedly" {
		t.Errorf("Message = %q, want %q", err.Message, "connection closed unexpectedly")
	}
	if err.Reason != reason {
		t.Errorf("Reason = %q, want %q", err.Reason, reason)
	}
}

// TestNewReconnectionError tests ReconnectionError creation
func TestNewReconnectionError(t *testing.T) {
	attempts := 5
	err := NewReconnectionError(attempts)

	if err.Message != "failed to reconnect after maximum retries" {
		t.Errorf("Message = %q, want %q", err.Message, "failed to reconnect after maximum retries")
	}
	if err.Attempts != attempts {
		t.Errorf("Attempts = %d, want %d", err.Attempts, attempts)
	}
	if err.Details != "attempts=5" {
		t.Errorf("Details = %q, want %q", err.Details, "attempts=5")
	}
}

// =============================================================================
// Authentication and Authorization Error Tests
// =============================================================================

// TestNewAuthenticationError tests AuthenticationError creation
func TestNewAuthenticationError(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    string
	}{
		{
			name:    "with message",
			message: "invalid API key",
			want:    "invalid API key",
		},
		{
			name:    "empty message uses default",
			message: "",
			want:    "authentication failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewAuthenticationError(tt.message)
			if err.Message != tt.want {
				t.Errorf("Message = %q, want %q", err.Message, tt.want)
			}
			if err.Code != "UNAUTHENTICATED" {
				t.Errorf("Code = %q, want %q", err.Code, "UNAUTHENTICATED")
			}
		})
	}
}

// TestNewPermissionDeniedError tests PermissionDeniedError creation
func TestNewPermissionDeniedError(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    string
	}{
		{
			name:    "with message",
			message: "cannot access workspace 'prod'",
			want:    "cannot access workspace 'prod'",
		},
		{
			name:    "empty message uses default",
			message: "",
			want:    "permission denied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewPermissionDeniedError(tt.message)
			if err.Message != tt.want {
				t.Errorf("Message = %q, want %q", err.Message, tt.want)
			}
			if err.Code != "PERMISSION_DENIED" {
				t.Errorf("Code = %q, want %q", err.Code, "PERMISSION_DENIED")
			}
		})
	}
}

// =============================================================================
// Identity Error Tests
// =============================================================================

// TestNewDuplicateIdentityError tests DuplicateIdentityError creation
func TestNewDuplicateIdentityError(t *testing.T) {
	identity := "ag.prod.my-agent.instance-1"
	err := NewDuplicateIdentityError(identity)

	if err.Message != "identity already connected" {
		t.Errorf("Message = %q, want %q", err.Message, "identity already connected")
	}
	if err.Code != "ALREADY_EXISTS" {
		t.Errorf("Code = %q, want %q", err.Code, "ALREADY_EXISTS")
	}
	if err.Identity != identity {
		t.Errorf("Identity = %q, want %q", err.Identity, identity)
	}
}

// =============================================================================
// Timeout Error Tests
// =============================================================================

// TestNewTimeoutError tests TimeoutError creation
func TestNewTimeoutError(t *testing.T) {
	tests := []struct {
		name           string
		operation      string
		timeoutSeconds float64
		wantDetails    string
	}{
		{
			name:           "with timeout value",
			operation:      "connect",
			timeoutSeconds: 5.0,
			wantDetails:    "timeout=5.00s",
		},
		{
			name:           "zero timeout",
			operation:      "send",
			timeoutSeconds: 0,
			wantDetails:    "",
		},
		{
			name:           "fractional timeout",
			operation:      "kv-get",
			timeoutSeconds: 0.5,
			wantDetails:    "timeout=0.50s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewTimeoutError(tt.operation, tt.timeoutSeconds)
			if err.Message != "operation timed out" {
				t.Errorf("Message = %q, want %q", err.Message, "operation timed out")
			}
			if err.Code != "DEADLINE_EXCEEDED" {
				t.Errorf("Code = %q, want %q", err.Code, "DEADLINE_EXCEEDED")
			}
			if err.Details != tt.wantDetails {
				t.Errorf("Details = %q, want %q", err.Details, tt.wantDetails)
			}
			if err.Operation != tt.operation {
				t.Errorf("Operation = %q, want %q", err.Operation, tt.operation)
			}
			if err.TimeoutSeconds != tt.timeoutSeconds {
				t.Errorf("TimeoutSeconds = %v, want %v", err.TimeoutSeconds, tt.timeoutSeconds)
			}
		})
	}
}

// =============================================================================
// Request/Response Error Tests
// =============================================================================

// TestNewInvalidArgumentError tests InvalidArgumentError creation
func TestNewInvalidArgumentError(t *testing.T) {
	tests := []struct {
		name     string
		message  string
		argument string
		wantMsg  string
	}{
		{
			name:     "with message",
			message:  "workspace name cannot be empty",
			argument: "workspace",
			wantMsg:  "workspace name cannot be empty",
		},
		{
			name:     "empty message uses default",
			message:  "",
			argument: "timeout",
			wantMsg:  "invalid argument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewInvalidArgumentError(tt.message, tt.argument)
			if err.Message != tt.wantMsg {
				t.Errorf("Message = %q, want %q", err.Message, tt.wantMsg)
			}
			if err.Code != "INVALID_ARGUMENT" {
				t.Errorf("Code = %q, want %q", err.Code, "INVALID_ARGUMENT")
			}
			if err.Argument != tt.argument {
				t.Errorf("Argument = %q, want %q", err.Argument, tt.argument)
			}
		})
	}
}

// TestNewNotFoundError tests NotFoundError creation
func TestNewNotFoundError(t *testing.T) {
	tests := []struct {
		name     string
		resource string
		wantMsg  string
	}{
		{
			name:     "with resource",
			resource: "agent 'my-agent'",
			wantMsg:  "agent 'my-agent' not found",
		},
		{
			name:     "empty resource",
			resource: "",
			wantMsg:  "resource not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewNotFoundError(tt.resource)
			if err.Message != tt.wantMsg {
				t.Errorf("Message = %q, want %q", err.Message, tt.wantMsg)
			}
			if err.Code != "NOT_FOUND" {
				t.Errorf("Code = %q, want %q", err.Code, "NOT_FOUND")
			}
			if err.Resource != tt.resource {
				t.Errorf("Resource = %q, want %q", err.Resource, tt.resource)
			}
		})
	}
}

// TestNewUnimplementedError tests UnimplementedError creation
func TestNewUnimplementedError(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		wantMsg   string
	}{
		{
			name:      "with operation",
			operation: "SwitchWorkspace",
			wantMsg:   "operation 'SwitchWorkspace' not implemented",
		},
		{
			name:      "empty operation",
			operation: "",
			wantMsg:   "operation not implemented",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewUnimplementedError(tt.operation)
			if err.Message != tt.wantMsg {
				t.Errorf("Message = %q, want %q", err.Message, tt.wantMsg)
			}
			if err.Code != "UNIMPLEMENTED" {
				t.Errorf("Code = %q, want %q", err.Code, "UNIMPLEMENTED")
			}
			if err.Operation != tt.operation {
				t.Errorf("Operation = %q, want %q", err.Operation, tt.operation)
			}
		})
	}
}

// =============================================================================
// Message and Protocol Error Tests
// =============================================================================

// TestNewMessageError tests MessageError creation
func TestNewMessageError(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    string
	}{
		{
			name:    "with message",
			message: "invalid message format",
			want:    "invalid message format",
		},
		{
			name:    "empty message uses default",
			message: "",
			want:    "message error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewMessageError(tt.message)
			if err.Message != tt.want {
				t.Errorf("Message = %q, want %q", err.Message, tt.want)
			}
		})
	}
}

// TestNewKVOperationError tests KVOperationError creation
func TestNewKVOperationError(t *testing.T) {
	cause := fmt.Errorf("redis timeout")
	err := NewKVOperationError("get", "user.settings", cause)

	expectedMsg := "KV get operation failed for key 'user.settings'"
	if err.Message != expectedMsg {
		t.Errorf("Message = %q, want %q", err.Message, expectedMsg)
	}
	if err.Operation != "get" {
		t.Errorf("Operation = %q, want %q", err.Operation, "get")
	}
	if err.Key != "user.settings" {
		t.Errorf("Key = %q, want %q", err.Key, "user.settings")
	}
	if err.Unwrap() != cause {
		t.Errorf("Unwrap() = %v, want %v", err.Unwrap(), cause)
	}
}

// TestNewKVOperationError_EmptyFields tests KVOperationError with empty fields
func TestNewKVOperationError_EmptyFields(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		key       string
		wantMsg   string
	}{
		{
			name:      "empty operation",
			operation: "",
			key:       "test.key",
			wantMsg:   "KV operation failed for key 'test.key'",
		},
		{
			name:      "empty key",
			operation: "put",
			key:       "",
			wantMsg:   "KV put operation failed",
		},
		{
			name:      "both empty",
			operation: "",
			key:       "",
			wantMsg:   "KV operation failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewKVOperationError(tt.operation, tt.key, nil)
			if err.Message != tt.wantMsg {
				t.Errorf("Message = %q, want %q", err.Message, tt.wantMsg)
			}
		})
	}
}

// TestNewCheckpointError tests CheckpointError creation
func TestNewCheckpointError(t *testing.T) {
	cause := fmt.Errorf("serialization failed")
	err := NewCheckpointError("save", "state.json", cause)

	expectedMsg := "checkpoint save operation failed for key 'state.json'"
	if err.Message != expectedMsg {
		t.Errorf("Message = %q, want %q", err.Message, expectedMsg)
	}
	if err.Operation != "save" {
		t.Errorf("Operation = %q, want %q", err.Operation, "save")
	}
	if err.Key != "state.json" {
		t.Errorf("Key = %q, want %q", err.Key, "state.json")
	}
	if err.Unwrap() != cause {
		t.Errorf("Unwrap() = %v, want %v", err.Unwrap(), cause)
	}
}

// =============================================================================
// gRPC Error Mapping Tests
// =============================================================================

// TestFromGRPCError_NilError tests that nil returns nil
func TestFromGRPCError_NilError(t *testing.T) {
	result := FromGRPCError(nil)
	if result != nil {
		t.Errorf("FromGRPCError(nil) = %v, want nil", result)
	}
}

// TestFromGRPCError_NonGRPCError tests non-gRPC errors are wrapped
func TestFromGRPCError_NonGRPCError(t *testing.T) {
	originalErr := fmt.Errorf("some random error")
	result := FromGRPCError(originalErr)

	var aetherErr *AetherError
	if !errors.As(result, &aetherErr) {
		t.Fatal("FromGRPCError should return an AetherError")
	}

	if aetherErr.Message != originalErr.Error() {
		t.Errorf("Message = %q, want %q", aetherErr.Message, originalErr.Error())
	}
	if !errors.Is(result, originalErr) {
		t.Error("wrapped error should contain original as cause")
	}
}

// TestFromGRPCError_StatusCodes tests gRPC status code mapping
func TestFromGRPCError_StatusCodes(t *testing.T) {
	tests := []struct {
		name     string
		code     codes.Code
		message  string
		errType  interface{}
		wantCode string
	}{
		{
			name:     "Unauthenticated",
			code:     codes.Unauthenticated,
			message:  "invalid credentials",
			errType:  &AuthenticationError{},
			wantCode: "Unauthenticated",
		},
		{
			name:     "PermissionDenied",
			code:     codes.PermissionDenied,
			message:  "access denied",
			errType:  &PermissionDeniedError{},
			wantCode: "PermissionDenied",
		},
		{
			name:     "AlreadyExists",
			code:     codes.AlreadyExists,
			message:  "identity in use",
			errType:  &DuplicateIdentityError{},
			wantCode: "AlreadyExists",
		},
		{
			name:     "InvalidArgument",
			code:     codes.InvalidArgument,
			message:  "bad parameter",
			errType:  &InvalidArgumentError{},
			wantCode: "InvalidArgument",
		},
		{
			name:     "NotFound",
			code:     codes.NotFound,
			message:  "resource missing",
			errType:  &NotFoundError{},
			wantCode: "NotFound",
		},
		{
			name:     "Unimplemented",
			code:     codes.Unimplemented,
			message:  "not supported",
			errType:  &UnimplementedError{},
			wantCode: "Unimplemented",
		},
		{
			name:     "DeadlineExceeded",
			code:     codes.DeadlineExceeded,
			message:  "timeout",
			errType:  &TimeoutError{},
			wantCode: "DeadlineExceeded",
		},
		{
			name:     "Unavailable",
			code:     codes.Unavailable,
			message:  "service unavailable",
			errType:  &ConnectionError{},
			wantCode: "Unavailable",
		},
		{
			name:     "Canceled",
			code:     codes.Canceled,
			message:  "request canceled",
			errType:  &ConnectionClosedError{},
			wantCode: "Canceled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grpcErr := status.Error(tt.code, tt.message)
			result := FromGRPCError(grpcErr)

			// Check the error type
			switch tt.errType.(type) {
			case *AuthenticationError:
				var err *AuthenticationError
				if !errors.As(result, &err) {
					t.Fatalf("expected *AuthenticationError, got %T", result)
				}
				if err.Code != tt.wantCode {
					t.Errorf("Code = %q, want %q", err.Code, tt.wantCode)
				}
			case *PermissionDeniedError:
				var err *PermissionDeniedError
				if !errors.As(result, &err) {
					t.Fatalf("expected *PermissionDeniedError, got %T", result)
				}
			case *DuplicateIdentityError:
				var err *DuplicateIdentityError
				if !errors.As(result, &err) {
					t.Fatalf("expected *DuplicateIdentityError, got %T", result)
				}
			case *InvalidArgumentError:
				var err *InvalidArgumentError
				if !errors.As(result, &err) {
					t.Fatalf("expected *InvalidArgumentError, got %T", result)
				}
			case *NotFoundError:
				var err *NotFoundError
				if !errors.As(result, &err) {
					t.Fatalf("expected *NotFoundError, got %T", result)
				}
			case *UnimplementedError:
				var err *UnimplementedError
				if !errors.As(result, &err) {
					t.Fatalf("expected *UnimplementedError, got %T", result)
				}
			case *TimeoutError:
				var err *TimeoutError
				if !errors.As(result, &err) {
					t.Fatalf("expected *TimeoutError, got %T", result)
				}
			case *ConnectionError:
				var err *ConnectionError
				if !errors.As(result, &err) {
					t.Fatalf("expected *ConnectionError, got %T", result)
				}
			case *ConnectionClosedError:
				var err *ConnectionClosedError
				if !errors.As(result, &err) {
					t.Fatalf("expected *ConnectionClosedError, got %T", result)
				}
			}

			// Check that the original gRPC error is preserved as cause
			if !errors.Is(result, grpcErr) {
				t.Error("FromGRPCError should preserve original error as cause")
			}
		})
	}
}

// TestFromGRPCError_UnknownCode tests unknown gRPC codes default to AetherError
func TestFromGRPCError_UnknownCode(t *testing.T) {
	grpcErr := status.Error(codes.DataLoss, "data corruption")
	result := FromGRPCError(grpcErr)

	var aetherErr *AetherError
	if !errors.As(result, &aetherErr) {
		t.Fatal("FromGRPCError should return an AetherError for unknown codes")
	}

	if aetherErr.Code != "DataLoss" {
		t.Errorf("Code = %q, want %q", aetherErr.Code, "DataLoss")
	}
	if aetherErr.Message != "data corruption" {
		t.Errorf("Message = %q, want %q", aetherErr.Message, "data corruption")
	}
}

// =============================================================================
// Error Classification Tests
// =============================================================================

// TestIsRecoverable tests the IsRecoverable classification function
func TestIsRecoverable(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		recoverable bool
	}{
		// nil is recoverable (no error)
		{
			name:        "nil error",
			err:         nil,
			recoverable: true,
		},

		// Non-recoverable errors
		{
			name:        "AuthenticationError",
			err:         NewAuthenticationError("bad credentials"),
			recoverable: false,
		},
		{
			name:        "PermissionDeniedError",
			err:         NewPermissionDeniedError("access denied"),
			recoverable: false,
		},
		{
			name:        "DuplicateIdentityError",
			err:         NewDuplicateIdentityError("ag.test.agent"),
			recoverable: false,
		},
		{
			name:        "InvalidArgumentError",
			err:         NewInvalidArgumentError("bad input", "param"),
			recoverable: false,
		},
		{
			name:        "NotFoundError",
			err:         NewNotFoundError("resource"),
			recoverable: false,
		},
		{
			name:        "UnimplementedError",
			err:         NewUnimplementedError("op"),
			recoverable: false,
		},

		// Recoverable errors
		{
			name:        "ConnectionError",
			err:         NewConnectionError("network down"),
			recoverable: true,
		},
		{
			name:        "ConnectionClosedError",
			err:         NewConnectionClosedError("server disconnect"),
			recoverable: true,
		},
		{
			name:        "TimeoutError",
			err:         NewTimeoutError("connect", 5.0),
			recoverable: true,
		},
		{
			name:        "ReconnectionError",
			err:         NewReconnectionError(3),
			recoverable: true,
		},
		{
			name:        "MessageError",
			err:         NewMessageError("parse error"),
			recoverable: true,
		},

		// gRPC status errors
		{
			name:        "gRPC Unauthenticated",
			err:         status.Error(codes.Unauthenticated, "bad creds"),
			recoverable: false,
		},
		{
			name:        "gRPC PermissionDenied",
			err:         status.Error(codes.PermissionDenied, "denied"),
			recoverable: false,
		},
		{
			name:        "gRPC Unavailable",
			err:         status.Error(codes.Unavailable, "server down"),
			recoverable: true,
		},
		{
			name:        "gRPC DeadlineExceeded",
			err:         status.Error(codes.DeadlineExceeded, "timeout"),
			recoverable: true,
		},
		{
			name:        "gRPC Internal",
			err:         status.Error(codes.Internal, "internal error"),
			recoverable: true,
		},

		// Generic errors are recoverable by default
		{
			name:        "generic error",
			err:         fmt.Errorf("some error"),
			recoverable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRecoverable(tt.err)
			if result != tt.recoverable {
				t.Errorf("IsRecoverable() = %v, want %v", result, tt.recoverable)
			}
		})
	}
}

// TestIsRecoverable_WrappedError tests that wrapped errors are correctly detected
func TestIsRecoverable_WrappedError(t *testing.T) {
	// Wrap a non-recoverable error
	innerErr := NewAuthenticationError("bad token")
	wrappedErr := fmt.Errorf("context: %w", innerErr)

	if IsRecoverable(wrappedErr) {
		t.Error("IsRecoverable should detect wrapped non-recoverable error")
	}
}

// TestIsConnectionError tests the IsConnectionError classification function
func TestIsConnectionError(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		isConnError bool
	}{
		{
			name:        "nil error",
			err:         nil,
			isConnError: false,
		},
		{
			name:        "ConnectionError",
			err:         NewConnectionError("failed"),
			isConnError: true,
		},
		{
			name:        "ConnectionClosedError",
			err:         NewConnectionClosedError("closed"),
			isConnError: true,
		},
		{
			name:        "ReconnectionError",
			err:         NewReconnectionError(5),
			isConnError: true,
		},
		{
			name:        "gRPC Unavailable",
			err:         status.Error(codes.Unavailable, "unavailable"),
			isConnError: true,
		},
		{
			name:        "gRPC Canceled",
			err:         status.Error(codes.Canceled, "canceled"),
			isConnError: true,
		},
		{
			name:        "AuthenticationError",
			err:         NewAuthenticationError("bad creds"),
			isConnError: false,
		},
		{
			name:        "TimeoutError",
			err:         NewTimeoutError("op", 5),
			isConnError: false,
		},
		{
			name:        "generic error",
			err:         fmt.Errorf("some error"),
			isConnError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsConnectionError(tt.err)
			if result != tt.isConnError {
				t.Errorf("IsConnectionError() = %v, want %v", result, tt.isConnError)
			}
		})
	}
}

// TestIsTimeoutError tests the IsTimeoutError classification function
func TestIsTimeoutError(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		isTimeoutErr bool
	}{
		{
			name:         "nil error",
			err:          nil,
			isTimeoutErr: false,
		},
		{
			name:         "TimeoutError",
			err:          NewTimeoutError("connect", 30),
			isTimeoutErr: true,
		},
		{
			name:         "gRPC DeadlineExceeded",
			err:          status.Error(codes.DeadlineExceeded, "deadline exceeded"),
			isTimeoutErr: true,
		},
		{
			name:         "ConnectionError",
			err:          NewConnectionError("failed"),
			isTimeoutErr: false,
		},
		{
			name:         "gRPC Unavailable",
			err:          status.Error(codes.Unavailable, "unavailable"),
			isTimeoutErr: false,
		},
		{
			name:         "generic error",
			err:          fmt.Errorf("some error"),
			isTimeoutErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsTimeoutError(tt.err)
			if result != tt.isTimeoutErr {
				t.Errorf("IsTimeoutError() = %v, want %v", result, tt.isTimeoutErr)
			}
		})
	}
}

// =============================================================================
// Error Interface Compliance Tests
// =============================================================================

// TestErrorTypes_ImplementError tests that all error types implement the error interface
func TestErrorTypes_ImplementError(t *testing.T) {
	// This is a compile-time test via type assertions
	var _ error = (*AetherError)(nil)
	var _ error = (*ConnectionError)(nil)
	var _ error = (*ConnectionClosedError)(nil)
	var _ error = (*ReconnectionError)(nil)
	var _ error = (*AuthenticationError)(nil)
	var _ error = (*PermissionDeniedError)(nil)
	var _ error = (*DuplicateIdentityError)(nil)
	var _ error = (*TimeoutError)(nil)
	var _ error = (*InvalidArgumentError)(nil)
	var _ error = (*NotFoundError)(nil)
	var _ error = (*UnimplementedError)(nil)
	var _ error = (*MessageError)(nil)
	var _ error = (*KVOperationError)(nil)
	var _ error = (*CheckpointError)(nil)
}

// TestErrorTypes_Unwrapper tests that error types with causes properly unwrap
func TestErrorTypes_Unwrapper(t *testing.T) {
	cause := fmt.Errorf("root cause")

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "AetherError",
			err:  &AetherError{Message: "test", cause: cause},
		},
		{
			name: "ConnectionError",
			err:  &ConnectionError{AetherError: AetherError{Message: "test", cause: cause}},
		},
		{
			name: "KVOperationError",
			err:  NewKVOperationError("get", "key", cause),
		},
		{
			name: "CheckpointError",
			err:  NewCheckpointError("save", "key", cause),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !errors.Is(tt.err, cause) {
				t.Errorf("errors.Is should find the underlying cause")
			}
		})
	}
}

// TestErrorsAs_SpecificTypes tests that errors can be matched by their specific types
func TestErrorsAs_SpecificTypes(t *testing.T) {
	// Test that each error type can be extracted with errors.As
	t.Run("ConnectionError", func(t *testing.T) {
		err := NewConnectionError("test")
		var target *ConnectionError
		if !errors.As(err, &target) {
			t.Error("errors.As should match *ConnectionError")
		}
	})

	t.Run("ConnectionClosedError", func(t *testing.T) {
		err := NewConnectionClosedError("test")
		var target *ConnectionClosedError
		if !errors.As(err, &target) {
			t.Error("errors.As should match *ConnectionClosedError")
		}
	})

	t.Run("ReconnectionError", func(t *testing.T) {
		err := NewReconnectionError(3)
		var target *ReconnectionError
		if !errors.As(err, &target) {
			t.Error("errors.As should match *ReconnectionError")
		}
	})

	t.Run("AuthenticationError", func(t *testing.T) {
		err := NewAuthenticationError("test")
		var target *AuthenticationError
		if !errors.As(err, &target) {
			t.Error("errors.As should match *AuthenticationError")
		}
	})

	t.Run("PermissionDeniedError", func(t *testing.T) {
		err := NewPermissionDeniedError("test")
		var target *PermissionDeniedError
		if !errors.As(err, &target) {
			t.Error("errors.As should match *PermissionDeniedError")
		}
	})

	t.Run("DuplicateIdentityError", func(t *testing.T) {
		err := NewDuplicateIdentityError("test")
		var target *DuplicateIdentityError
		if !errors.As(err, &target) {
			t.Error("errors.As should match *DuplicateIdentityError")
		}
	})

	t.Run("TimeoutError", func(t *testing.T) {
		err := NewTimeoutError("test", 5)
		var target *TimeoutError
		if !errors.As(err, &target) {
			t.Error("errors.As should match *TimeoutError")
		}
	})

	t.Run("InvalidArgumentError", func(t *testing.T) {
		err := NewInvalidArgumentError("test", "arg")
		var target *InvalidArgumentError
		if !errors.As(err, &target) {
			t.Error("errors.As should match *InvalidArgumentError")
		}
	})

	t.Run("NotFoundError", func(t *testing.T) {
		err := NewNotFoundError("test")
		var target *NotFoundError
		if !errors.As(err, &target) {
			t.Error("errors.As should match *NotFoundError")
		}
	})

	t.Run("UnimplementedError", func(t *testing.T) {
		err := NewUnimplementedError("test")
		var target *UnimplementedError
		if !errors.As(err, &target) {
			t.Error("errors.As should match *UnimplementedError")
		}
	})

	t.Run("MessageError", func(t *testing.T) {
		err := NewMessageError("test")
		var target *MessageError
		if !errors.As(err, &target) {
			t.Error("errors.As should match *MessageError")
		}
	})

	t.Run("KVOperationError", func(t *testing.T) {
		err := NewKVOperationError("get", "key", nil)
		var target *KVOperationError
		if !errors.As(err, &target) {
			t.Error("errors.As should match *KVOperationError")
		}
	})

	t.Run("CheckpointError", func(t *testing.T) {
		err := NewCheckpointError("save", "key", nil)
		var target *CheckpointError
		if !errors.As(err, &target) {
			t.Error("errors.As should match *CheckpointError")
		}
	})
}
