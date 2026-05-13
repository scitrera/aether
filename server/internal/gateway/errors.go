package gateway

import (
	pb "github.com/scitrera/aether/api/proto"
)

// sendClientError sends a structured error response to the client.
// Optional errorOpt functions can set Retryable, RetryAfterMs, or RequestId fields.
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
