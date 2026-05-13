/**
 * Error hierarchy for the Aether TypeScript SDK.
 *
 * This module provides a structured set of error classes that map to common
 * error scenarios in Aether client operations, mirroring the error hierarchies
 * in the Go SDK (errors.go) and Python SDK (exceptions.py).
 *
 * All errors extend the base AetherError class, making it easy to catch all
 * SDK-related errors with a single catch clause.
 *
 * @module errors
 */

// =============================================================================
// Base Error
// =============================================================================

/**
 * Base error class for all Aether SDK errors.
 *
 * All Aether-specific errors extend this class.
 */
export class AetherError extends Error {
  /** Optional error code (e.g., gRPC status code name). */
  readonly code: string;
  /** Optional additional error details. */
  readonly details: string;
  /** The underlying cause, if any. */
  readonly cause?: Error;

  constructor(message: string, code = "", details = "", cause?: Error) {
    const parts: string[] = [];
    if (code) parts.push(`[${code}]`);
    parts.push(message);
    if (details) parts.push(`(${details})`);
    super(parts.join(" "));

    this.name = "AetherError";
    this.code = code;
    this.details = details;
    this.cause = cause;
  }
}

// =============================================================================
// Connection Errors
// =============================================================================

/**
 * Indicates a connection to the Aether gateway failed.
 *
 * This includes initial connection failures, network errors, and
 * disconnection events that cannot be automatically recovered.
 */
export class ConnectionError extends AetherError {
  constructor(message = "Failed to connect to Aether gateway", code = "", details = "", cause?: Error) {
    super(message, code, details, cause);
    this.name = "ConnectionError";
  }
}

/**
 * Indicates the connection was unexpectedly closed.
 *
 * This can happen due to network issues, server restarts, or
 * force disconnect signals from the server.
 */
export class ConnectionClosedError extends AetherError {
  /** Reason for the disconnection. */
  readonly reason: string;

  constructor(reason = "", code = "", cause?: Error) {
    super("Connection closed unexpectedly", code, reason, cause);
    this.name = "ConnectionClosedError";
    this.reason = reason;
  }
}

/**
 * Indicates automatic reconnection failed after exhausting all retries.
 */
export class ReconnectionError extends AetherError {
  /** Number of reconnection attempts made. */
  readonly attempts: number;

  constructor(attempts: number, cause?: Error) {
    super("Failed to reconnect after maximum retries", "", `attempts=${attempts}`, cause);
    this.name = "ReconnectionError";
    this.attempts = attempts;
  }
}

// =============================================================================
// Authentication and Authorization Errors
// =============================================================================

/**
 * Indicates authentication failed.
 *
 * Maps to gRPC UNAUTHENTICATED status code. Authentication errors
 * are non-recoverable and will not trigger automatic reconnection.
 */
export class AuthenticationError extends AetherError {
  constructor(message = "Authentication failed", details = "", cause?: Error) {
    super(message, "UNAUTHENTICATED", details, cause);
    this.name = "AuthenticationError";
  }
}

/**
 * Indicates the client lacks permission to perform an operation.
 *
 * Maps to gRPC PERMISSION_DENIED status code. Permission errors
 * are non-recoverable and will not trigger automatic reconnection.
 */
export class PermissionDeniedError extends AetherError {
  constructor(message = "Permission denied", details = "", cause?: Error) {
    super(message, "PERMISSION_DENIED", details, cause);
    this.name = "PermissionDeniedError";
  }
}

// =============================================================================
// Identity Errors
// =============================================================================

/**
 * Indicates an identity is already in use.
 *
 * In Aether, each agent or unique task identity can only have one active
 * connection at a time (Connection = Lock paradigm). This error indicates
 * another client is already connected with the same identity.
 *
 * Maps to gRPC ALREADY_EXISTS status code.
 */
export class DuplicateIdentityError extends AetherError {
  /** The conflicting identity string. */
  readonly identity: string;

  constructor(identity = "", details = "", cause?: Error) {
    super("Identity already connected", "ALREADY_EXISTS", details, cause);
    this.name = "DuplicateIdentityError";
    this.identity = identity;
  }
}

// =============================================================================
// Timeout Errors
// =============================================================================

/**
 * Indicates an operation timed out.
 *
 * This can occur during connection attempts, message sends, or
 * synchronous KV/checkpoint operations.
 *
 * Maps to gRPC DEADLINE_EXCEEDED status code.
 */
export class TimeoutError extends AetherError {
  /** The operation that timed out. */
  readonly operation: string;
  /** The timeout duration that was exceeded, in seconds. */
  readonly timeoutSeconds: number;

  constructor(operation = "", timeoutSeconds = 0, cause?: Error) {
    const details = timeoutSeconds > 0 ? `timeout=${timeoutSeconds.toFixed(2)}s` : "";
    super("Operation timed out", "DEADLINE_EXCEEDED", details, cause);
    this.name = "TimeoutError";
    this.operation = operation;
    this.timeoutSeconds = timeoutSeconds;
  }
}

// =============================================================================
// Request/Response Errors
// =============================================================================

/**
 * Indicates an invalid argument was provided to an operation.
 *
 * Maps to gRPC INVALID_ARGUMENT status code.
 */
export class InvalidArgumentError extends AetherError {
  /** The name of the invalid argument. */
  readonly argument: string;

  constructor(message = "Invalid argument", argument = "", cause?: Error) {
    super(message, "INVALID_ARGUMENT", "", cause);
    this.name = "InvalidArgumentError";
    this.argument = argument;
  }
}

/**
 * Indicates a requested resource was not found.
 *
 * Maps to gRPC NOT_FOUND status code.
 */
export class NotFoundError extends AetherError {
  /** The resource that was not found. */
  readonly resource: string;

  constructor(resource = "", cause?: Error) {
    const message = resource ? `${resource} not found` : "Resource not found";
    super(message, "NOT_FOUND", "", cause);
    this.name = "NotFoundError";
    this.resource = resource;
  }
}

/**
 * Indicates an operation is not implemented by the server.
 *
 * Maps to gRPC UNIMPLEMENTED status code.
 */
export class UnimplementedError extends AetherError {
  /** The unimplemented operation name. */
  readonly operation: string;

  constructor(operation = "", cause?: Error) {
    const message = operation ? `Operation '${operation}' not implemented` : "Operation not implemented";
    super(message, "UNIMPLEMENTED", "", cause);
    this.name = "UnimplementedError";
    this.operation = operation;
  }
}

// =============================================================================
// Message and Protocol Errors
// =============================================================================

/**
 * Indicates an error with message handling.
 *
 * This includes serialization errors, invalid message formats,
 * or protocol violations.
 */
export class MessageError extends AetherError {
  constructor(message = "Message error", cause?: Error) {
    super(message, "", "", cause);
    this.name = "MessageError";
  }
}

/**
 * Indicates a KV store operation failed.
 */
export class KVOperationError extends AetherError {
  /** The KV operation that failed. */
  readonly operation: string;
  /** The key involved in the failed operation. */
  readonly key: string;

  constructor(operation = "", key = "", cause?: Error) {
    let message = "KV operation failed";
    if (operation) message = `KV ${operation} operation failed`;
    if (key) message = `${message} for key '${key}'`;
    super(message, "", "", cause);
    this.name = "KVOperationError";
    this.operation = operation;
    this.key = key;
  }
}

/**
 * Indicates a checkpoint operation failed.
 */
export class CheckpointError extends AetherError {
  /** The checkpoint operation that failed. */
  readonly operation: string;
  /** The checkpoint key involved. */
  readonly key: string;

  constructor(operation = "", key = "", cause?: Error) {
    let message = "Checkpoint operation failed";
    if (operation) message = `Checkpoint ${operation} operation failed`;
    if (key) message = `${message} for key '${key}'`;
    super(message, "", "", cause);
    this.name = "CheckpointError";
    this.operation = operation;
    this.key = key;
  }
}

// =============================================================================
// Error Classification
// =============================================================================

/**
 * Checks if an error is recoverable (should trigger reconnection).
 *
 * Non-recoverable errors include authentication failures, permission denials,
 * and other terminal error conditions.
 *
 * @param error - The error to check
 * @returns true if the error is recoverable, false otherwise
 */
export function isRecoverable(error: Error): boolean {
  if (
    error instanceof AuthenticationError ||
    error instanceof PermissionDeniedError ||
    error instanceof DuplicateIdentityError ||
    error instanceof InvalidArgumentError ||
    error instanceof NotFoundError ||
    error instanceof UnimplementedError
  ) {
    return false;
  }
  return true;
}

/**
 * Checks if an error is a connection-related error.
 *
 * @param error - The error to check
 * @returns true if the error is connection-related
 */
export function isConnectionError(error: Error): boolean {
  return (
    error instanceof ConnectionError ||
    error instanceof ConnectionClosedError ||
    error instanceof ReconnectionError
  );
}

/**
 * Checks if an error is a timeout-related error.
 *
 * @param error - The error to check
 * @returns true if the error is timeout-related
 */
export function isTimeoutError(error: Error): boolean {
  return error instanceof TimeoutError;
}
