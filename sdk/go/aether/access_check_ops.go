package aether

import (
	"context"
	"fmt"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// DefaultAccessCheckTimeout bounds synchronous runtime authorization checks.
const DefaultAccessCheckTimeout = 10 * time.Second

// CheckAccess asks the gateway to evaluate one exact logical resource. A deny
// is a successful response with receipt.Allowed=false, not an SDK error.
func (c *BaseClient) CheckAccess(ctx context.Context, access *pb.ResourceAccessRequest, authorization *pb.AuthorizationContext) (*pb.AccessDecisionReceipt, error) {
	requestID := c.NextRequestID()
	ch := c.pendingAccessCheckRequests.Register(requestID)
	defer c.pendingAccessCheckRequests.Delete(requestID)

	if err := c.Send(&pb.UpstreamMessage{Payload: &pb.UpstreamMessage_AccessCheck{
		AccessCheck: &pb.AccessCheckOperation{
			RequestId:     requestID,
			Access:        access,
			Authorization: authorization,
		},
	}}); err != nil {
		return nil, err
	}

	timer := time.NewTimer(DefaultAccessCheckTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, NewTimeoutError("access check timed out", DefaultAccessCheckTimeout.Seconds())
	case response := <-ch:
		if !response.GetSuccess() {
			return nil, fmt.Errorf("access check failed: %s", response.GetError())
		}
		return response.GetDecision(), nil
	}
}

// BatchCheckAccess evaluates 1-100 requests under one authorization context.
// Results preserve input order and cardinality. The gateway rejects an invalid
// batch as a whole before evaluating any item.
func (c *BaseClient) BatchCheckAccess(ctx context.Context, access []*pb.ResourceAccessRequest, authorization *pb.AuthorizationContext) ([]*pb.AccessDecisionReceipt, error) {
	requestID := c.NextRequestID()
	ch := c.pendingBatchAccessCheckRequests.Register(requestID)
	defer c.pendingBatchAccessCheckRequests.Delete(requestID)

	if err := c.Send(&pb.UpstreamMessage{Payload: &pb.UpstreamMessage_BatchAccessCheck{
		BatchAccessCheck: &pb.BatchAccessCheckOperation{
			RequestId:     requestID,
			Access:        access,
			Authorization: authorization,
		},
	}}); err != nil {
		return nil, err
	}

	timer := time.NewTimer(DefaultAccessCheckTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, NewTimeoutError("batch access check timed out", DefaultAccessCheckTimeout.Seconds())
	case response := <-ch:
		if !response.GetSuccess() {
			return nil, fmt.Errorf("batch access check failed: %s", response.GetError())
		}
		return response.GetDecisions(), nil
	}
}
