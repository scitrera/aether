// Package aether foreign audit-event submission ops for the Go SDK.
//
// This file exposes SubmitAuditEvent, the synchronous wrapper around the
// gateway's SubmitAuditEventRequest / SubmitAuditEventResponse round-trip.
// The gateway always stamps the authenticated identity over any
// client-supplied actor fields, validates event_type against a whitelist
// (gateway-truth categories like connection/auth/admin/acl are rejected),
// enforces a per-principal rate limit, and sanitizes metadata for
// credential-shaped keys regardless of the configured audit verbosity.
//
// Correlation pattern mirrors AuthorityGrantOps: a fresh client_request_id
// is minted per call, registered in pendingAuditSubmitRequests, and resolved
// by handleSubmitAuditEventResponse on the dispatch loop.

package aether

import (
	"context"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// DefaultAuditSubmitTimeout is the default timeout for synchronous foreign
// audit-event submissions.
const DefaultAuditSubmitTimeout = 10 * time.Second

// SubmitAuditResponse is the SDK-facing response for a SubmitAuditEvent call.
// Fields mirror the proto SubmitAuditEventResponse: success indicates the
// event was accepted into the async audit queue (drop-on-overflow still
// applies — acceptance is NOT a persistence guarantee). On rejection,
// ErrorCode/ErrorMessage carry the gateway's reason.
type SubmitAuditResponse struct {
	// Success is true if the gateway accepted the event into the audit queue.
	Success bool

	// ErrorCode is populated on failure (e.g. "ERR_AUDIT_TYPE_FORBIDDEN",
	// "ERR_AUDIT_RATE_LIMITED", "ERR_PERMISSION_DENIED"). Empty on success.
	ErrorCode string

	// ErrorMessage is the human-readable reason. Empty on success.
	ErrorMessage string

	// ClientRequestID is the correlation token echoed back from the request.
	ClientRequestID string
}

// SubmitAuditEventOpts configures a single SubmitAuditEvent call. EventType
// is required ("message" | "kv" | "task" | "custom" — gateway whitelist
// excludes gateway-truth categories such as connection/auth/admin/acl).
type SubmitAuditEventOpts struct {
	// EventType is required: one of "message", "kv", "task", "custom".
	EventType string

	// Operation is free-form within EventType (e.g. "send", "get", "complete").
	Operation string

	// ResourceType is an optional resource classifier.
	ResourceType string

	// ResourceID is an optional resource identifier.
	ResourceID string

	// Workspace optionally overrides the caller's authenticated workspace.
	// A non-empty Workspace different from the caller's home workspace
	// requires the capability/audit_submit permission on that workspace.
	Workspace string

	// Success indicates whether the audited operation itself succeeded.
	Success bool

	// ErrorMessage carries the audited operation's error when Success=false.
	ErrorMessage string

	// Metadata is sanitized for credential-shaped keys server-side.
	Metadata map[string]string

	// Timeout caps the wait for the correlated response. Zero uses
	// DefaultAuditSubmitTimeout.
	Timeout time.Duration
}

// SubmitAuditEvent publishes a single audit event into the gateway audit
// pipeline and waits for the gateway's acceptance/rejection response.
//
// Mirrors the AuthorityGrantOps.SendOpSync correlation pattern.
func (c *BaseClient) SubmitAuditEvent(ctx context.Context, opts SubmitAuditEventOpts) (*SubmitAuditResponse, error) {
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultAuditSubmitTimeout
	}

	clientRequestID := c.NextRequestID()
	ch := c.RegisterPendingAuditSubmitRequest(clientRequestID)
	defer c.pendingAuditSubmitRequests.Delete(clientRequestID)

	req := &pb.SubmitAuditEventRequest{
		EventType:       opts.EventType,
		Operation:       opts.Operation,
		ResourceType:    opts.ResourceType,
		ResourceId:      opts.ResourceID,
		Workspace:       opts.Workspace,
		Success:         opts.Success,
		ErrorMessage:    opts.ErrorMessage,
		Metadata:        opts.Metadata,
		ClientRequestId: clientRequestID,
	}

	if err := c.Send(&pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_SubmitAuditEvent{SubmitAuditEvent: req},
	}); err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, NewTimeoutError("context canceled", timeout.Seconds())
	case <-timer.C:
		return nil, NewTimeoutError("submit audit event timed out", timeout.Seconds())
	case resp := <-ch:
		return &SubmitAuditResponse{
			Success:         resp.GetSuccess(),
			ErrorCode:       resp.GetErrorCode(),
			ErrorMessage:    resp.GetErrorMessage(),
			ClientRequestID: resp.GetClientRequestId(),
		}, nil
	}
}
