package errors

import (
	"errors"
	"fmt"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestToGRPCStatus_NilError tests that nil errors return nil
func TestToGRPCStatus_NilError(t *testing.T) {
	result := ToGRPCStatus(nil)
	if result != nil {
		t.Errorf("ToGRPCStatus(nil) = %v, want nil", result)
	}
}

// TestToGRPCStatus_AlreadyGRPCStatus tests that existing gRPC status errors are returned as-is
func TestToGRPCStatus_AlreadyGRPCStatus(t *testing.T) {
	originalErr := status.Error(codes.Canceled, "already a status error")
	result := ToGRPCStatus(originalErr)

	if result != originalErr {
		t.Errorf("ToGRPCStatus should return existing gRPC status error unchanged")
	}
}

// TestToGRPCStatus_SessionErrors tests session error mapping
func TestToGRPCStatus_SessionErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{
			name: "DuplicateIdentityError",
			err:  &DuplicateIdentityError{Identity: "ag::test::impl::spec", ExistingSessionID: "sess-456"},
			code: codes.AlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToGRPCStatus(tt.err)
			st, ok := status.FromError(result)
			if !ok {
				t.Fatalf("ToGRPCStatus did not return a gRPC status error")
			}
			if st.Code() != tt.code {
				t.Errorf("ToGRPCStatus code = %v, want %v", st.Code(), tt.code)
			}
		})
	}
}

// TestToGRPCStatus_OrchestrationErrors tests orchestration error mapping
func TestToGRPCStatus_OrchestrationErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{
			name: "AgentNotFoundError",
			err:  &AgentNotFoundError{Implementation: "my-agent"},
			code: codes.NotFound,
		},
		{
			name: "OrchestratorNotFoundError",
			err:  &OrchestratorNotFoundError{Profile: "k8s", Workspace: "prod"},
			code: codes.NotFound,
		},
		{
			name: "TaskNotFoundError",
			err:  &TaskNotFoundError{TaskID: "task-123"},
			code: codes.NotFound,
		},
		{
			name: "InvalidAssignmentModeError",
			err:  &InvalidAssignmentModeError{Mode: "unknown"},
			code: codes.InvalidArgument,
		},
		{
			name: "TargetAgentRequiredError",
			err:  &TargetAgentRequiredError{},
			code: codes.InvalidArgument,
		},
		{
			name: "ProfileRequiredError",
			err:  &ProfileRequiredError{},
			code: codes.InvalidArgument,
		},
		{
			name: "DuplicateRegistrationError",
			err:  &DuplicateRegistrationError{Implementation: "duplicate-agent"},
			code: codes.AlreadyExists,
		},
		{
			name: "InitializationError",
			err:  &InitializationError{Component: "TaskStore", Err: fmt.Errorf("init failed")},
			code: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToGRPCStatus(tt.err)
			st, ok := status.FromError(result)
			if !ok {
				t.Fatalf("ToGRPCStatus did not return a gRPC status error")
			}
			if st.Code() != tt.code {
				t.Errorf("ToGRPCStatus code = %v, want %v", st.Code(), tt.code)
			}
		})
	}
}

// TestToGRPCStatus_QuotaErrors tests quota error mapping to ResourceExhausted
func TestToGRPCStatus_QuotaErrors(t *testing.T) {
	err := &QuotaExceededError{
		Resource:  "connections",
		Workspace: "prod",
		Current:   100,
		Limit:     100,
	}
	result := ToGRPCStatus(err)
	st, ok := status.FromError(result)
	if !ok {
		t.Fatalf("ToGRPCStatus did not return a gRPC status error")
	}
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("ToGRPCStatus code = %v, want %v", st.Code(), codes.ResourceExhausted)
	}
}

// TestToGRPCStatus_UnknownError tests unknown error mapping to Internal
func TestToGRPCStatus_UnknownError(t *testing.T) {
	unknownErr := fmt.Errorf("some random error")
	result := ToGRPCStatus(unknownErr)
	st, ok := status.FromError(result)
	if !ok {
		t.Fatalf("ToGRPCStatus did not return a gRPC status error")
	}
	if st.Code() != codes.Internal {
		t.Errorf("ToGRPCStatus code = %v, want %v for unknown error", st.Code(), codes.Internal)
	}
	if st.Message() != unknownErr.Error() {
		t.Errorf("ToGRPCStatus message = %q, want %q", st.Message(), unknownErr.Error())
	}
}

// TestToGRPCStatus_WrappedError tests that wrapped errors are correctly detected
func TestToGRPCStatus_WrappedError(t *testing.T) {
	innerErr := &AgentNotFoundError{Implementation: "test-agent"}
	wrappedErr := fmt.Errorf("context: %w", innerErr)

	result := ToGRPCStatus(wrappedErr)
	st, ok := status.FromError(result)
	if !ok {
		t.Fatalf("ToGRPCStatus did not return a gRPC status error")
	}
	if st.Code() != codes.NotFound {
		t.Errorf("ToGRPCStatus should detect wrapped AgentNotFoundError, got code %v, want %v", st.Code(), codes.NotFound)
	}
}

// TestToErrorResponse_NilError tests that nil errors return nil
func TestToErrorResponse_NilError(t *testing.T) {
	result := ToErrorResponse(nil)
	if result != nil {
		t.Errorf("ToErrorResponse(nil) = %v, want nil", result)
	}
}

// TestToErrorResponse_GenericError tests error response for generic errors
func TestToErrorResponse_GenericError(t *testing.T) {
	err := fmt.Errorf("generic error message")
	result := ToErrorResponse(err)

	if result == nil {
		t.Fatalf("ToErrorResponse returned nil for non-nil error")
	}
	if result.Code != "ERR_UNKNOWN" {
		t.Errorf("ToErrorResponse code = %q, want %q", result.Code, "ERR_UNKNOWN")
	}
	if result.Message != err.Error() {
		t.Errorf("ToErrorResponse message = %q, want %q", result.Message, err.Error())
	}
}

// TestToErrorResponse_SpecificErrors tests error response for specific error types
func TestToErrorResponse_SpecificErrors(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		expectedCode string
	}{
		{
			name:         "DuplicateIdentityError",
			err:          &DuplicateIdentityError{Identity: "ag::test::impl::spec"},
			expectedCode: ErrSessionDuplicate,
		},
		{
			name:         "AgentNotFoundError",
			err:          &AgentNotFoundError{Implementation: "test-agent"},
			expectedCode: ErrOrchAgentNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToErrorResponse(tt.err)
			if result == nil {
				t.Fatalf("ToErrorResponse returned nil")
			}
			if result.Code != tt.expectedCode {
				t.Errorf("ToErrorResponse code = %q, want %q", result.Code, tt.expectedCode)
			}
			if result.Message != tt.err.Error() {
				t.Errorf("ToErrorResponse message = %q, want %q", result.Message, tt.err.Error())
			}
		})
	}
}

// TestExtractErrorCode tests error code extraction for all error types
func TestExtractErrorCode(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		expectedCode string
	}{
		// Session errors
		{"DuplicateIdentityError", &DuplicateIdentityError{Identity: "ag::test"}, ErrSessionDuplicate},

		// Orchestration errors
		{"AgentNotFoundError", &AgentNotFoundError{Implementation: "agent"}, ErrOrchAgentNotFound},
		{"OrchestratorNotFoundError", &OrchestratorNotFoundError{Profile: "k8s", Workspace: "prod"}, ErrOrchUnavailable},
		{"TaskNotFoundError", &TaskNotFoundError{TaskID: "task-1"}, ErrOrchTaskAssignment},
		{"InvalidAssignmentModeError", &InvalidAssignmentModeError{Mode: "bad"}, ErrOrchTaskAssignment},
		{"TargetAgentRequiredError", &TargetAgentRequiredError{}, ErrOrchTaskAssignment},
		{"ProfileRequiredError", &ProfileRequiredError{}, ErrOrchTaskAssignment},
		{"DuplicateRegistrationError", &DuplicateRegistrationError{Implementation: "agent"}, ErrOrchDuplicateRegistration},
		{"InitializationError", &InitializationError{Component: "Store", Err: fmt.Errorf("err")}, ErrOrchUnavailable},

		// Quota errors
		{"QuotaExceededError", &QuotaExceededError{Resource: "connections", Workspace: "prod"}, ErrQuotaExceeded},

		// Unknown error
		{"UnknownError", fmt.Errorf("random error"), "ERR_UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code := extractErrorCode(tt.err)
			if code != tt.expectedCode {
				t.Errorf("extractErrorCode() = %q, want %q", code, tt.expectedCode)
			}
		})
	}
}

// TestExtractErrorCode_WrappedError tests that wrapped errors are correctly detected
func TestExtractErrorCode_WrappedError(t *testing.T) {
	innerErr := &AgentNotFoundError{Implementation: "test-agent"}
	wrappedErr := fmt.Errorf("context: %w", innerErr)

	code := extractErrorCode(wrappedErr)
	if code != ErrOrchAgentNotFound {
		t.Errorf("extractErrorCode should detect wrapped error, got %q, want %q", code, ErrOrchAgentNotFound)
	}
}

// TestWrapError_NilError tests that wrapping nil returns nil
func TestWrapError_NilError(t *testing.T) {
	result := WrapError(nil, "some context")
	if result != nil {
		t.Errorf("WrapError(nil, ...) = %v, want nil", result)
	}
}

// TestWrapError_PreservesErrorType tests that WrapError preserves error type
func TestWrapError_PreservesErrorType(t *testing.T) {
	originalErr := &AgentNotFoundError{Implementation: "test-agent"}
	wrappedErr := WrapError(originalErr, "failed to find agent")

	// Should still be able to extract original error type
	var agentErr *AgentNotFoundError
	if !errors.As(wrappedErr, &agentErr) {
		t.Fatalf("WrapError should preserve error type for errors.As")
	}

	if agentErr.Implementation != "test-agent" {
		t.Errorf("Wrapped error implementation = %q, want %q", agentErr.Implementation, "test-agent")
	}

	// Should also work with errors.Is
	if !errors.Is(wrappedErr, originalErr) {
		t.Errorf("WrapError should preserve error identity for errors.Is")
	}

	// Check that context is prepended
	expectedMsg := "failed to find agent: agent implementation 'test-agent' not found in registry"
	if wrappedErr.Error() != expectedMsg {
		t.Errorf("WrapError message = %q, want %q", wrappedErr.Error(), expectedMsg)
	}
}

// TestSendErrorResponse tests the SendErrorResponse helper
func TestSendErrorResponse(t *testing.T) {
	mockStream := &mockGRPCStream{
		sent: make([]*pb.DownstreamMessage, 0),
	}

	err := &AgentNotFoundError{Implementation: "test-agent"}
	sendErr := SendErrorResponse(mockStream, err)

	if sendErr != nil {
		t.Fatalf("SendErrorResponse returned error: %v", sendErr)
	}

	if len(mockStream.sent) != 1 {
		t.Fatalf("Expected 1 message sent, got %d", len(mockStream.sent))
	}

	msg := mockStream.sent[0]
	errResp, ok := msg.Payload.(*pb.DownstreamMessage_Error)
	if !ok {
		t.Fatalf("Sent message is not an ErrorResponse")
	}

	if errResp.Error.Code != ErrOrchAgentNotFound {
		t.Errorf("Error code = %q, want %q", errResp.Error.Code, ErrOrchAgentNotFound)
	}
	if errResp.Error.Message != err.Error() {
		t.Errorf("Error message = %q, want %q", errResp.Error.Message, err.Error())
	}
}

// TestSendErrorResponse_NilError tests that nil errors are handled
func TestSendErrorResponse_NilError(t *testing.T) {
	mockStream := &mockGRPCStream{
		sent: make([]*pb.DownstreamMessage, 0),
	}

	err := SendErrorResponse(mockStream, nil)
	if err != nil {
		t.Errorf("SendErrorResponse(nil) should return nil, got %v", err)
	}

	if len(mockStream.sent) != 0 {
		t.Errorf("SendErrorResponse(nil) should not send any messages, got %d", len(mockStream.sent))
	}
}

// TestSendErrorResponse_StreamError tests that stream send errors are propagated
func TestSendErrorResponse_StreamError(t *testing.T) {
	streamErr := fmt.Errorf("stream write failed")
	mockStream := &mockGRPCStream{
		sendErr: streamErr,
		sent:    make([]*pb.DownstreamMessage, 0),
	}

	err := SendErrorResponse(mockStream, &AgentNotFoundError{Implementation: "test"})
	if err != streamErr {
		t.Errorf("SendErrorResponse should return stream error, got %v, want %v", err, streamErr)
	}
}

// mockGRPCStream is a mock implementation of the stream interface for testing
type mockGRPCStream struct {
	sent    []*pb.DownstreamMessage
	sendErr error
}

func (m *mockGRPCStream) Send(msg *pb.DownstreamMessage) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sent = append(m.sent, msg)
	return nil
}
