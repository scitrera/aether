"""
Custom exception hierarchy for the Aether client.

This module provides a structured set of exceptions that map to common error
scenarios in Aether client operations, including gRPC status codes.
"""
from typing import Optional

import grpc


# =============================================================================
# Base Exception
# =============================================================================

class AetherError(Exception):
    """
    Base exception for all Aether client errors.

    All Aether-specific exceptions inherit from this class, making it easy
    to catch all client-related errors with a single except clause.

    Attributes:
        message: Human-readable error description.
        code: Optional error code (e.g., gRPC status code name).
        details: Optional additional error details.
    """

    def __init__(
        self,
        message: str,
        code: Optional[str] = None,
        details: Optional[str] = None
    ) -> None:
        self.message = message
        self.code = code
        self.details = details
        super().__init__(message)

    def __str__(self) -> str:
        parts = [self.message]
        if self.code:
            parts.insert(0, f"[{self.code}]")
        if self.details:
            parts.append(f"({self.details})")
        return " ".join(parts)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"message={self.message!r}, "
            f"code={self.code!r}, "
            f"details={self.details!r})"
        )


# =============================================================================
# Connection Errors
# =============================================================================

class ConnectionError(AetherError):
    """
    Raised when a connection to the Aether gateway fails.

    This includes initial connection failures, network errors, and
    disconnection events that cannot be automatically recovered.

    Note: This shadows the built-in ConnectionError, which is intentional
    for a cleaner API. Users can access the built-in via builtins.ConnectionError
    if needed.
    """

    def __init__(
        self,
        message: str = "Failed to connect to Aether gateway",
        code: Optional[str] = None,
        details: Optional[str] = None
    ) -> None:
        super().__init__(message, code, details)


class ConnectionClosedError(ConnectionError):
    """
    Raised when the connection is unexpectedly closed.

    This can happen due to network issues, server restarts, or
    force disconnect signals from the server.
    """

    def __init__(
        self,
        message: str = "Connection closed unexpectedly",
        reason: Optional[str] = None,
        code: Optional[str] = None
    ) -> None:
        super().__init__(message, code, reason)
        self.reason = reason


class ReconnectionError(ConnectionError):
    """
    Raised when automatic reconnection fails after exhausting all retries.
    """

    def __init__(
        self,
        message: str = "Failed to reconnect after maximum retries",
        attempts: int = 0,
        code: Optional[str] = None
    ) -> None:
        super().__init__(message, code, f"attempts={attempts}")
        self.attempts = attempts


# =============================================================================
# Authentication and Authorization Errors
# =============================================================================

class AuthenticationError(AetherError):
    """
    Raised when authentication fails.

    This maps to gRPC UNAUTHENTICATED status code. Authentication errors
    are non-recoverable and will not trigger automatic reconnection.
    """

    def __init__(
        self,
        message: str = "Authentication failed",
        code: Optional[str] = "UNAUTHENTICATED",
        details: Optional[str] = None
    ) -> None:
        super().__init__(message, code, details)


class PermissionDeniedError(AetherError):
    """
    Raised when the client lacks permission to perform an operation.

    This maps to gRPC PERMISSION_DENIED status code. Permission errors
    are non-recoverable and will not trigger automatic reconnection.
    """

    def __init__(
        self,
        message: str = "Permission denied",
        code: Optional[str] = "PERMISSION_DENIED",
        details: Optional[str] = None
    ) -> None:
        super().__init__(message, code, details)


class TaskRevokedError(AetherError):
    """
    Raised when reconnection is rejected because the per-task token was revoked.

    Signals that the worker's associated task has transitioned to a terminal
    state (failed/cancelled) on the server — typically because the disconnect
    grace window elapsed before the worker reconnected. Workers receiving this
    should treat their work as permanently dead and clean up; they will not be
    able to resume.
    """

    def __init__(
        self,
        message: str = "Task token was revoked; the associated task is in a terminal state",
        code: Optional[str] = "TASK_REVOKED",
        details: Optional[str] = None
    ) -> None:
        super().__init__(message, code, details)


# =============================================================================
# Identity Errors
# =============================================================================

class DuplicateIdentityError(AetherError):
    """
    Raised when attempting to connect with an identity that is already in use.

    In Aether, each agent or unique task identity can only have one active
    connection at a time (Connection = Lock paradigm). This error indicates
    another client is already connected with the same identity.

    This maps to gRPC ALREADY_EXISTS status code.
    """

    def __init__(
        self,
        message: str = "Identity already connected",
        identity: Optional[str] = None,
        code: Optional[str] = "ALREADY_EXISTS",
        details: Optional[str] = None
    ) -> None:
        super().__init__(message, code, details)
        self.identity = identity


# =============================================================================
# Timeout Errors
# =============================================================================

class TimeoutError(AetherError):
    """
    Raised when an operation times out.

    This can occur during connection attempts, message sends, or
    synchronous KV/checkpoint operations.

    Note: This shadows the built-in TimeoutError, which is intentional
    for a cleaner API. Users can access the built-in via builtins.TimeoutError
    if needed.
    """

    def __init__(
        self,
        message: str = "Operation timed out",
        operation: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
        code: Optional[str] = "DEADLINE_EXCEEDED",
        details: Optional[str] = None
    ) -> None:
        if timeout_seconds is not None:
            details = f"timeout={timeout_seconds}s" + (f", {details}" if details else "")
        super().__init__(message, code, details)
        self.operation = operation
        self.timeout_seconds = timeout_seconds


# =============================================================================
# Request/Response Errors
# =============================================================================

class InvalidArgumentError(AetherError):
    """
    Raised when an invalid argument is provided to an operation.

    This maps to gRPC INVALID_ARGUMENT status code.
    """

    def __init__(
        self,
        message: str = "Invalid argument",
        argument: Optional[str] = None,
        code: Optional[str] = "INVALID_ARGUMENT",
        details: Optional[str] = None
    ) -> None:
        super().__init__(message, code, details)
        self.argument = argument


class NotFoundError(AetherError):
    """
    Raised when a requested resource is not found.

    This maps to gRPC NOT_FOUND status code.
    """

    def __init__(
        self,
        message: str = "Resource not found",
        resource: Optional[str] = None,
        code: Optional[str] = "NOT_FOUND",
        details: Optional[str] = None
    ) -> None:
        super().__init__(message, code, details)
        self.resource = resource


class NotImplementedError(AetherError):
    """
    Raised when an operation is not implemented by the server.

    This maps to gRPC UNIMPLEMENTED status code.

    Note: This shadows the built-in NotImplementedError. Users can
    access the built-in via builtins.NotImplementedError if needed.
    """

    def __init__(
        self,
        message: str = "Operation not implemented",
        operation: Optional[str] = None,
        code: Optional[str] = "UNIMPLEMENTED",
        details: Optional[str] = None
    ) -> None:
        super().__init__(message, code, details)
        self.operation = operation


# =============================================================================
# Message and Protocol Errors
# =============================================================================

class MessageError(AetherError):
    """
    Raised when there is an error with message handling.

    This includes serialization errors, invalid message formats,
    or protocol violations.
    """

    def __init__(
        self,
        message: str = "Message error",
        code: Optional[str] = None,
        details: Optional[str] = None
    ) -> None:
        super().__init__(message, code, details)


class KVOperationError(AetherError):
    """
    Raised when a KV store operation fails.
    """

    def __init__(
        self,
        message: str = "KV operation failed",
        operation: Optional[str] = None,
        key: Optional[str] = None,
        code: Optional[str] = None,
        details: Optional[str] = None
    ) -> None:
        super().__init__(message, code, details)
        self.operation = operation
        self.key = key


class CheckpointError(AetherError):
    """
    Raised when a checkpoint operation fails.
    """

    def __init__(
        self,
        message: str = "Checkpoint operation failed",
        operation: Optional[str] = None,
        key: Optional[str] = None,
        code: Optional[str] = None,
        details: Optional[str] = None
    ) -> None:
        super().__init__(message, code, details)
        self.operation = operation
        self.key = key


# =============================================================================
# Utility Functions
# =============================================================================

def from_grpc_error(error: grpc.RpcError) -> AetherError:
    """
    Convert a gRPC error to an appropriate AetherError subclass.

    This function maps gRPC status codes to specific exception types,
    preserving the original error details.

    Args:
        error: A gRPC RpcError instance.

    Returns:
        An appropriate AetherError subclass instance.
    """
    code = error.code() if hasattr(error, 'code') else None
    details = error.details() if hasattr(error, 'details') else str(error)
    code_name = code.name if code else "UNKNOWN"

    # Map gRPC status codes to exception types
    if code == grpc.StatusCode.UNAUTHENTICATED:
        return AuthenticationError(
            message=details or "Authentication failed",
            code=code_name,
            details=details
        )
    elif code == grpc.StatusCode.PERMISSION_DENIED:
        return PermissionDeniedError(
            message=details or "Permission denied",
            code=code_name,
            details=details
        )
    elif code == grpc.StatusCode.ALREADY_EXISTS:
        return DuplicateIdentityError(
            message=details or "Identity already connected",
            code=code_name,
            details=details
        )
    elif code == grpc.StatusCode.INVALID_ARGUMENT:
        return InvalidArgumentError(
            message=details or "Invalid argument",
            code=code_name,
            details=details
        )
    elif code == grpc.StatusCode.NOT_FOUND:
        return NotFoundError(
            message=details or "Resource not found",
            code=code_name,
            details=details
        )
    elif code == grpc.StatusCode.UNIMPLEMENTED:
        return NotImplementedError(
            message=details or "Operation not implemented",
            code=code_name,
            details=details
        )
    elif code == grpc.StatusCode.DEADLINE_EXCEEDED:
        return TimeoutError(
            message=details or "Operation timed out",
            code=code_name,
            details=details
        )
    elif code == grpc.StatusCode.UNAVAILABLE:
        return ConnectionError(
            message=details or "Service unavailable",
            code=code_name,
            details=details
        )
    elif code == grpc.StatusCode.CANCELLED:
        return ConnectionClosedError(
            message=details or "Operation cancelled",
            code=code_name
        )
    else:
        # Default to base AetherError for unknown codes
        return AetherError(
            message=details or "Unknown error",
            code=code_name,
            details=details
        )


def is_recoverable_error(error: Exception) -> bool:
    """
    Check if an error is recoverable (should trigger reconnection).

    Non-recoverable errors include authentication failures, permission
    denials, and other terminal error conditions.

    Args:
        error: The exception to check.

    Returns:
        True if the error is recoverable, False otherwise.
    """
    # Non-recoverable Aether errors
    non_recoverable_types = (
        AuthenticationError,
        PermissionDeniedError,
        DuplicateIdentityError,
        InvalidArgumentError,
        NotFoundError,
        NotImplementedError,
    )

    if isinstance(error, non_recoverable_types):
        return False

    # Check gRPC errors
    if isinstance(error, grpc.RpcError):
        if hasattr(error, 'code'):
            non_recoverable_codes = {
                grpc.StatusCode.PERMISSION_DENIED,
                grpc.StatusCode.UNAUTHENTICATED,
                grpc.StatusCode.ALREADY_EXISTS,
                grpc.StatusCode.INVALID_ARGUMENT,
                grpc.StatusCode.NOT_FOUND,
                grpc.StatusCode.UNIMPLEMENTED,
            }
            return error.code() not in non_recoverable_codes

    # Default to recoverable for unknown errors
    return True


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Base
    "AetherError",
    # Connection
    "ConnectionError",
    "ConnectionClosedError",
    "ReconnectionError",
    # Auth
    "AuthenticationError",
    "PermissionDeniedError",
    # Identity
    "DuplicateIdentityError",
    # Timeout
    "TimeoutError",
    # Request/Response
    "InvalidArgumentError",
    "NotFoundError",
    "NotImplementedError",
    # Message/Protocol
    "MessageError",
    "KVOperationError",
    "CheckpointError",
    # Utilities
    "from_grpc_error",
    "is_recoverable_error",
]
