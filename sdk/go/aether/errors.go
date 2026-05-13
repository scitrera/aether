// Package aether error types for the Go SDK.
//
// This file provides a structured set of error types that map to common error
// scenarios in Aether client operations, including gRPC status codes.

package aether

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// =============================================================================
// Base Error
// =============================================================================

// AetherError is the base error type for all Aether SDK errors.
//
// All Aether-specific errors embed this type, making it easy to catch all
// SDK-related errors with a single errors.As call.
type AetherError struct {
	// Message is the human-readable error description.
	Message string

	// Code is an optional error code (e.g., gRPC status code name).
	Code string

	// Details contains optional additional error information.
	Details string

	// cause is the underlying error, if any.
	cause error
}

// Error implements the error interface.
func (e *AetherError) Error() string {
	parts := e.Message
	if e.Code != "" {
		parts = fmt.Sprintf("[%s] %s", e.Code, parts)
	}
	if e.Details != "" {
		parts = fmt.Sprintf("%s (%s)", parts, e.Details)
	}
	return parts
}

// Unwrap returns the underlying error for errors.Is/As support.
func (e *AetherError) Unwrap() error {
	return e.cause
}

// WithCause returns a copy of the error with the given cause.
func (e *AetherError) WithCause(cause error) *AetherError {
	return &AetherError{
		Message: e.Message,
		Code:    e.Code,
		Details: e.Details,
		cause:   cause,
	}
}

// =============================================================================
// Connection Errors
// =============================================================================

// ConnectionError indicates a connection to the Aether gateway failed.
//
// This includes initial connection failures, network errors, and
// disconnection events that cannot be automatically recovered.
type ConnectionError struct {
	AetherError
}

// NewConnectionError creates a new ConnectionError with the given message.
func NewConnectionError(message string) *ConnectionError {
	if message == "" {
		message = "failed to connect to Aether gateway"
	}
	return &ConnectionError{
		AetherError: AetherError{Message: message},
	}
}

// ConnectionClosedError indicates the connection was unexpectedly closed.
//
// This can happen due to network issues, server restarts, or
// force disconnect signals from the server.
type ConnectionClosedError struct {
	AetherError
	// Reason provides additional context about why the connection closed.
	Reason string
}

// NewConnectionClosedError creates a new ConnectionClosedError.
func NewConnectionClosedError(reason string) *ConnectionClosedError {
	return &ConnectionClosedError{
		AetherError: AetherError{Message: "connection closed unexpectedly"},
		Reason:      reason,
	}
}

// ReconnectionError indicates automatic reconnection failed after exhausting all retries.
type ReconnectionError struct {
	AetherError
	// Attempts is the number of reconnection attempts made.
	Attempts int
}

// NewReconnectionError creates a new ReconnectionError.
func NewReconnectionError(attempts int) *ReconnectionError {
	return &ReconnectionError{
		AetherError: AetherError{
			Message: "failed to reconnect after maximum retries",
			Details: fmt.Sprintf("attempts=%d", attempts),
		},
		Attempts: attempts,
	}
}

// =============================================================================
// Authentication and Authorization Errors
// =============================================================================

// AuthenticationError indicates authentication failed.
//
// This maps to gRPC UNAUTHENTICATED status code. Authentication errors
// are non-recoverable and will not trigger automatic reconnection.
type AuthenticationError struct {
	AetherError
}

// NewAuthenticationError creates a new AuthenticationError.
func NewAuthenticationError(message string) *AuthenticationError {
	if message == "" {
		message = "authentication failed"
	}
	return &AuthenticationError{
		AetherError: AetherError{
			Message: message,
			Code:    "UNAUTHENTICATED",
		},
	}
}

// PermissionDeniedError indicates the client lacks permission to perform an operation.
//
// This maps to gRPC PERMISSION_DENIED status code. Permission errors
// are non-recoverable and will not trigger automatic reconnection.
type PermissionDeniedError struct {
	AetherError
}

// NewPermissionDeniedError creates a new PermissionDeniedError.
func NewPermissionDeniedError(message string) *PermissionDeniedError {
	if message == "" {
		message = "permission denied"
	}
	return &PermissionDeniedError{
		AetherError: AetherError{
			Message: message,
			Code:    "PERMISSION_DENIED",
		},
	}
}

// =============================================================================
// Identity Errors
// =============================================================================

// DuplicateIdentityError indicates an identity is already in use.
//
// In Aether, each agent or unique task identity can only have one active
// connection at a time (Connection = Lock paradigm). This error indicates
// another client is already connected with the same identity.
//
// This maps to gRPC ALREADY_EXISTS status code.
type DuplicateIdentityError struct {
	AetherError
	// Identity is the conflicting identity string.
	Identity string
}

// NewDuplicateIdentityError creates a new DuplicateIdentityError.
func NewDuplicateIdentityError(identity string) *DuplicateIdentityError {
	return &DuplicateIdentityError{
		AetherError: AetherError{
			Message: "identity already connected",
			Code:    "ALREADY_EXISTS",
		},
		Identity: identity,
	}
}

// =============================================================================
// Timeout Errors
// =============================================================================

// TimeoutError indicates an operation timed out.
//
// This can occur during connection attempts, message sends, or
// synchronous KV/checkpoint operations.
//
// This maps to gRPC DEADLINE_EXCEEDED status code.
type TimeoutError struct {
	AetherError
	// Operation identifies which operation timed out.
	Operation string
	// TimeoutSeconds is the timeout duration that was exceeded.
	TimeoutSeconds float64
}

// NewTimeoutError creates a new TimeoutError.
func NewTimeoutError(operation string, timeoutSeconds float64) *TimeoutError {
	details := ""
	if timeoutSeconds > 0 {
		details = fmt.Sprintf("timeout=%.2fs", timeoutSeconds)
	}
	return &TimeoutError{
		AetherError: AetherError{
			Message: "operation timed out",
			Code:    "DEADLINE_EXCEEDED",
			Details: details,
		},
		Operation:      operation,
		TimeoutSeconds: timeoutSeconds,
	}
}

// =============================================================================
// Request/Response Errors
// =============================================================================

// InvalidArgumentError indicates an invalid argument was provided to an operation.
//
// This maps to gRPC INVALID_ARGUMENT status code.
type InvalidArgumentError struct {
	AetherError
	// Argument is the name of the invalid argument.
	Argument string
}

// NewInvalidArgumentError creates a new InvalidArgumentError.
func NewInvalidArgumentError(message string, argument string) *InvalidArgumentError {
	if message == "" {
		message = "invalid argument"
	}
	return &InvalidArgumentError{
		AetherError: AetherError{
			Message: message,
			Code:    "INVALID_ARGUMENT",
		},
		Argument: argument,
	}
}

// NotFoundError indicates a requested resource was not found.
//
// This maps to gRPC NOT_FOUND status code.
type NotFoundError struct {
	AetherError
	// Resource identifies the resource that was not found.
	Resource string
}

// NewNotFoundError creates a new NotFoundError.
func NewNotFoundError(resource string) *NotFoundError {
	message := "resource not found"
	if resource != "" {
		message = fmt.Sprintf("%s not found", resource)
	}
	return &NotFoundError{
		AetherError: AetherError{
			Message: message,
			Code:    "NOT_FOUND",
		},
		Resource: resource,
	}
}

// UnimplementedError indicates an operation is not implemented by the server.
//
// This maps to gRPC UNIMPLEMENTED status code.
type UnimplementedError struct {
	AetherError
	// Operation is the unimplemented operation name.
	Operation string
}

// NewUnimplementedError creates a new UnimplementedError.
func NewUnimplementedError(operation string) *UnimplementedError {
	message := "operation not implemented"
	if operation != "" {
		message = fmt.Sprintf("operation '%s' not implemented", operation)
	}
	return &UnimplementedError{
		AetherError: AetherError{
			Message: message,
			Code:    "UNIMPLEMENTED",
		},
		Operation: operation,
	}
}

// =============================================================================
// Message and Protocol Errors
// =============================================================================

// MessageError indicates an error with message handling.
//
// This includes serialization errors, invalid message formats,
// or protocol violations.
type MessageError struct {
	AetherError
}

// NewMessageError creates a new MessageError.
func NewMessageError(message string) *MessageError {
	if message == "" {
		message = "message error"
	}
	return &MessageError{
		AetherError: AetherError{Message: message},
	}
}

// KVOperationError indicates a KV store operation failed.
type KVOperationError struct {
	AetherError
	// Operation is the KV operation that failed (get, put, delete, etc.).
	Operation string
	// Key is the key involved in the failed operation.
	Key string
}

// NewKVOperationError creates a new KVOperationError.
func NewKVOperationError(operation, key string, cause error) *KVOperationError {
	message := "KV operation failed"
	if operation != "" {
		message = fmt.Sprintf("KV %s operation failed", operation)
	}
	if key != "" {
		message = fmt.Sprintf("%s for key '%s'", message, key)
	}
	return &KVOperationError{
		AetherError: AetherError{
			Message: message,
			cause:   cause,
		},
		Operation: operation,
		Key:       key,
	}
}

// CheckpointError indicates a checkpoint operation failed.
type CheckpointError struct {
	AetherError
	// Operation is the checkpoint operation that failed.
	Operation string
	// Key is the checkpoint key involved.
	Key string
}

// NewCheckpointError creates a new CheckpointError.
func NewCheckpointError(operation, key string, cause error) *CheckpointError {
	message := "checkpoint operation failed"
	if operation != "" {
		message = fmt.Sprintf("checkpoint %s operation failed", operation)
	}
	if key != "" {
		message = fmt.Sprintf("%s for key '%s'", message, key)
	}
	return &CheckpointError{
		AetherError: AetherError{
			Message: message,
			cause:   cause,
		},
		Operation: operation,
		Key:       key,
	}
}

// =============================================================================
// gRPC Error Mapping
// =============================================================================

// FromGRPCError converts a gRPC error to an appropriate Aether error type.
//
// This function maps gRPC status codes to specific error types,
// preserving the original error details.
func FromGRPCError(err error) error {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if !ok {
		// Not a gRPC status error, wrap it as a generic error
		return &AetherError{
			Message: err.Error(),
			cause:   err,
		}
	}

	code := st.Code()
	message := st.Message()
	codeName := code.String()

	switch code {
	case codes.Unauthenticated:
		return &AuthenticationError{
			AetherError: AetherError{
				Message: message,
				Code:    codeName,
				Details: message,
				cause:   err,
			},
		}

	case codes.PermissionDenied:
		return &PermissionDeniedError{
			AetherError: AetherError{
				Message: message,
				Code:    codeName,
				Details: message,
				cause:   err,
			},
		}

	case codes.AlreadyExists:
		return &DuplicateIdentityError{
			AetherError: AetherError{
				Message: message,
				Code:    codeName,
				Details: message,
				cause:   err,
			},
		}

	case codes.InvalidArgument:
		return &InvalidArgumentError{
			AetherError: AetherError{
				Message: message,
				Code:    codeName,
				Details: message,
				cause:   err,
			},
		}

	case codes.NotFound:
		return &NotFoundError{
			AetherError: AetherError{
				Message: message,
				Code:    codeName,
				Details: message,
				cause:   err,
			},
		}

	case codes.Unimplemented:
		return &UnimplementedError{
			AetherError: AetherError{
				Message: message,
				Code:    codeName,
				Details: message,
				cause:   err,
			},
		}

	case codes.DeadlineExceeded:
		return &TimeoutError{
			AetherError: AetherError{
				Message: message,
				Code:    codeName,
				Details: message,
				cause:   err,
			},
		}

	case codes.Unavailable:
		return &ConnectionError{
			AetherError: AetherError{
				Message: message,
				Code:    codeName,
				Details: message,
				cause:   err,
			},
		}

	case codes.Canceled:
		return &ConnectionClosedError{
			AetherError: AetherError{
				Message: message,
				Code:    codeName,
				cause:   err,
			},
			Reason: message,
		}

	default:
		// Default to base AetherError for unknown codes
		return &AetherError{
			Message: message,
			Code:    codeName,
			Details: message,
			cause:   err,
		}
	}
}

// =============================================================================
// Error Classification
// =============================================================================

// IsRecoverable checks if an error is recoverable (should trigger reconnection).
//
// Non-recoverable errors include authentication failures, permission denials,
// and other terminal error conditions.
func IsRecoverable(err error) bool {
	if err == nil {
		return true
	}

	// Check for non-recoverable Aether error types
	var authErr *AuthenticationError
	if errors.As(err, &authErr) {
		return false
	}

	var permErr *PermissionDeniedError
	if errors.As(err, &permErr) {
		return false
	}

	var dupErr *DuplicateIdentityError
	if errors.As(err, &dupErr) {
		return false
	}

	var argErr *InvalidArgumentError
	if errors.As(err, &argErr) {
		return false
	}

	var notFoundErr *NotFoundError
	if errors.As(err, &notFoundErr) {
		return false
	}

	var unimplErr *UnimplementedError
	if errors.As(err, &unimplErr) {
		return false
	}

	// Check gRPC status codes directly
	if st, ok := status.FromError(err); ok {
		nonRecoverableCodes := map[codes.Code]bool{
			codes.PermissionDenied: true,
			codes.Unauthenticated:  true,
			codes.AlreadyExists:    true,
			codes.InvalidArgument:  true,
			codes.NotFound:         true,
			codes.Unimplemented:    true,
		}
		return !nonRecoverableCodes[st.Code()]
	}

	// Default to recoverable for unknown errors
	return true
}

// IsConnectionError checks if an error is a connection-related error.
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}

	var connErr *ConnectionError
	if errors.As(err, &connErr) {
		return true
	}

	var closedErr *ConnectionClosedError
	if errors.As(err, &closedErr) {
		return true
	}

	var reconnErr *ReconnectionError
	if errors.As(err, &reconnErr) {
		return true
	}

	// Check gRPC status code
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.Unavailable || st.Code() == codes.Canceled
	}

	return false
}

// IsTimeoutError checks if an error is a timeout-related error.
func IsTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	var timeoutErr *TimeoutError
	if errors.As(err, &timeoutErr) {
		return true
	}

	// Check gRPC status code
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.DeadlineExceeded
	}

	return false
}
