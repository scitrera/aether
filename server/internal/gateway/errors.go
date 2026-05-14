package gateway

import (
	pb "github.com/scitrera/aether/api/proto"
)

// sendClientError sends a structured error response to the client.
// Optional errorOpt functions can set Retryable, RetryAfterMs, or RequestId fields.
// Use withRequestID when the error is in direct response to a specific RPC so the
// client can correlate the error to its originating request future; omit for
// connection-scoped or un-correlated errors.
func sendClientError(client *ClientSession, code, message string, opts ...errorOpt) {
	resp := &pb.ErrorResponse{Code: code, Message: message}
	for _, opt := range opts {
		opt(resp)
	}
	client.SafeSend(&pb.DownstreamMessage{ //nolint:errcheck
		Payload: &pb.DownstreamMessage_Error{Error: resp},
	})
}

// errorOpt is a functional option for configuring an ErrorResponse.
type errorOpt func(*pb.ErrorResponse)

// withRetryable sets the Retryable field on the error response.
func withRetryable(retryable bool) errorOpt {
	return func(r *pb.ErrorResponse) {
		r.Retryable = retryable
	}
}

// withRetryAfter sets the RetryAfterMs field on the error response.
func withRetryAfter(ms int64) errorOpt {
	return func(r *pb.ErrorResponse) {
		r.RetryAfterMs = ms
	}
}

// withRequestID sets the RequestId field on the error response so the
// client can correlate the error to its originating request future.
// Use whenever the error is in direct response to a specific RPC; omit
// for connection-scoped or un-correlated errors.
func withRequestID(id string) errorOpt {
	return func(r *pb.ErrorResponse) {
		r.RequestId = id
	}
}
