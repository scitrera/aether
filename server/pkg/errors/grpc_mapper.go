package errors

import (
	"errors"
	"fmt"

	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mappings is the authoritative table of all known Aether error types.
// Order matters for errors that wrap others: more specific types should come first.
var mappings = []struct {
	grpc      codes.Code
	errCode   string
	retryable bool
	check     func(err error) bool
}{
	// Session errors
	{codes.AlreadyExists, ErrSessionDuplicate, false, func(err error) bool { var t *DuplicateIdentityError; return errors.As(err, &t) }},

	// Orchestration errors
	{codes.NotFound, ErrOrchAgentNotFound, false, func(err error) bool { var t *AgentNotFoundError; return errors.As(err, &t) }},
	{codes.NotFound, ErrOrchUnavailable, false, func(err error) bool { var t *OrchestratorNotFoundError; return errors.As(err, &t) }},
	{codes.NotFound, ErrOrchTaskAssignment, false, func(err error) bool { var t *TaskNotFoundError; return errors.As(err, &t) }},
	{codes.InvalidArgument, ErrOrchTaskAssignment, false, func(err error) bool { var t *InvalidAssignmentModeError; return errors.As(err, &t) }},
	{codes.InvalidArgument, ErrOrchTaskAssignment, false, func(err error) bool { var t *TargetAgentRequiredError; return errors.As(err, &t) }},
	{codes.InvalidArgument, ErrOrchTaskAssignment, false, func(err error) bool { var t *ProfileRequiredError; return errors.As(err, &t) }},
	{codes.AlreadyExists, ErrOrchDuplicateRegistration, false, func(err error) bool { var t *DuplicateRegistrationError; return errors.As(err, &t) }},
	{codes.Internal, ErrOrchUnavailable, false, func(err error) bool { var t *InitializationError; return errors.As(err, &t) }},

	// Quota errors → ResourceExhausted (retryable: wait and retry)
	{codes.ResourceExhausted, ErrQuotaExceeded, true, func(err error) bool { var t *QuotaExceededError; return errors.As(err, &t) }},
}

// ToGRPCStatus converts an error to a gRPC status error with appropriate status code.
// It maps error categories to gRPC status codes and preserves error messages.
//
// Mapping:
// - Session errors -> AlreadyExists (duplicate identity)
// - Orchestration errors -> NotFound (agent/orchestrator/task not found), InvalidArgument (bad assignment params), Internal (init failures)
// - Unknown errors -> Internal
func ToGRPCStatus(err error) error {
	if err == nil {
		return nil
	}

	// Pass through errors that are already native gRPC status errors.
	// status.FromError returns (status, true) for ANY error (non-gRPC errors get codes.Unknown),
	// so we must use a type assertion to check for the gRPC status interface instead.
	type grpcStatusError interface {
		GRPCStatus() *status.Status
	}
	var se grpcStatusError
	if errors.As(err, &se) {
		if se.GRPCStatus().Code() != codes.Unknown {
			return err
		}
	}

	for _, m := range mappings {
		if m.check(err) {
			return status.Error(m.grpc, err.Error())
		}
	}

	return status.Error(codes.Internal, err.Error())
}

// isRetryable returns whether the error is transient and the client should retry.
func isRetryable(err error) bool {
	for _, m := range mappings {
		if m.check(err) {
			return m.retryable
		}
	}
	return false
}

// ToErrorResponse converts an error to a protobuf ErrorResponse message.
// For other errors, returns a generic error response.
func ToErrorResponse(err error) *pb.ErrorResponse {
	if err == nil {
		return nil
	}

	code := extractErrorCode(err)
	message := err.Error()

	return &pb.ErrorResponse{
		Code:      code,
		Message:   message,
		Retryable: isRetryable(err),
	}
}

// extractErrorCode returns the Aether error code string for a known error type,
// or "ERR_UNKNOWN" for unrecognized errors.
func extractErrorCode(err error) string {
	for _, m := range mappings {
		if m.check(err) {
			return m.errCode
		}
	}
	return "ERR_UNKNOWN"
}

// SendErrorResponse is a helper function that sends an error as an ErrorResponse
// through the gRPC stream. It converts the error to a protobuf ErrorResponse and
// sends it downstream.
func SendErrorResponse(stream interface {
	Send(*pb.DownstreamMessage) error
}, err error) error {
	if err == nil {
		return nil
	}

	errResp := ToErrorResponse(err)
	return stream.Send(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Error{
			Error: errResp,
		},
	})
}

// WrapError wraps an existing error with context while preserving error type information.
// This is useful for adding context to errors without losing the ability to use errors.As/Is.
func WrapError(err error, context string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", context, err)
}
