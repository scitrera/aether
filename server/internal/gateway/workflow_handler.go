package gateway

import (
	"context"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
)

// pendingWorkflowRequest tracks an in-flight WorkflowOperation waiting for a response.
type pendingWorkflowRequest struct {
	client    *ClientSession
	createdAt time.Time
}

// handleWorkflowOp forwards a WorkflowOperation from a client to the connected workflow engine.
// If no workflow engine is connected, an error response is sent back to the requesting client.
func (s *GatewayServer) handleWorkflowOp(ctx context.Context, client *ClientSession, op *pb.WorkflowOperation) {
	wfClient := s.findWorkflowEngineClient()
	if wfClient == nil {
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_WorkflowResponse{
				WorkflowResponse: &pb.WorkflowResponse{
					Success:   false,
					Error:     "workflow engine not connected",
					RequestId: op.RequestId,
				},
			},
		})
		return
	}

	// Ensure a request_id exists for correlation
	requestID := op.RequestId
	if requestID == "" {
		requestID = uuid.New().String()
		op.RequestId = requestID
	}

	// Store the pending request so the response can be routed back
	s.pendingWorkflowRequests.Store(requestID, &pendingWorkflowRequest{
		client:    client,
		createdAt: time.Now(),
	})

	// Forward the operation downstream to the workflow engine
	if err := wfClient.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_WorkflowOp{
			WorkflowOp: op,
		},
	}); err != nil {
		logging.Logger.Error().Err(err).Str("request_id", requestID).Msg("failed to forward workflow op to workflow engine")
		s.pendingWorkflowRequests.Delete(requestID)
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_WorkflowResponse{
				WorkflowResponse: &pb.WorkflowResponse{
					Success:   false,
					Error:     "failed to forward workflow operation to workflow engine",
					RequestId: requestID,
				},
			},
		})
	}
}

// handleWorkflowResponse routes a WorkflowResponse from the workflow engine back to
// the original requesting client.
func (s *GatewayServer) handleWorkflowResponse(ctx context.Context, client *ClientSession, resp *pb.WorkflowResponse) {
	val, ok := s.pendingWorkflowRequests.LoadAndDelete(resp.RequestId)
	if !ok {
		logging.Logger.Warn().Str("request_id", resp.RequestId).Msg("received workflow response for unknown request_id (orphaned response)")
		return
	}

	origReq := val.(*pendingWorkflowRequest)
	if err := origReq.client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_WorkflowResponse{
			WorkflowResponse: resp,
		},
	}); err != nil {
		logging.Logger.Error().Err(err).Str("request_id", resp.RequestId).Msg("failed to relay workflow response to original client")
	}
}

// findWorkflowEngineClient returns the first connected WorkflowEngine ClientSession, or nil
// if no workflow engine is connected. Performs an O(n) scan over activeStreams; workflow
// engines are expected to be very few (typically one per deployment).
func (s *GatewayServer) findWorkflowEngineClient() *ClientSession {
	var found *ClientSession
	s.activeStreams.Range(func(_, value interface{}) bool {
		client, ok := value.(*ClientSession)
		if !ok {
			return true
		}
		client.identityMu.RLock()
		identType := client.Identity.Type
		client.identityMu.RUnlock()
		if identType == models.PrincipalWorkflowEngine {
			found = client
			return false // stop iteration
		}
		return true
	})
	return found
}

// startWorkflowRequestSweeper periodically cleans up timed-out pending workflow requests.
// Requests older than 30 seconds are considered timed out and receive an error response.
// This goroutine runs until ctx is cancelled.
func (s *GatewayServer) startWorkflowRequestSweeper(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.sweepTimedOutWorkflowRequests()
			}
		}
	}()
}

// sweepTimedOutWorkflowRequests removes pending workflow requests older than 30 seconds
// and sends timeout error responses to the waiting clients.
func (s *GatewayServer) sweepTimedOutWorkflowRequests() {
	cutoff := time.Now().Add(-30 * time.Second)
	s.pendingWorkflowRequests.Range(func(key, value interface{}) bool {
		pending, ok := value.(*pendingWorkflowRequest)
		if !ok {
			s.pendingWorkflowRequests.Delete(key)
			return true
		}
		if pending.createdAt.Before(cutoff) {
			requestID, _ := key.(string)
			if _, deleted := s.pendingWorkflowRequests.LoadAndDelete(key); deleted {
				logging.Logger.Warn().Str("request_id", requestID).Msg("workflow request timed out")
				_ = pending.client.SafeSend(&pb.DownstreamMessage{
					Payload: &pb.DownstreamMessage_WorkflowResponse{
						WorkflowResponse: &pb.WorkflowResponse{
							Success:   false,
							Error:     "workflow operation timed out",
							RequestId: requestID,
						},
					},
				})
			}
		}
		return true
	})
}

// cleanupPendingWorkflowRequests removes all pending requests associated with a disconnected
// client and sends error responses. Called on client disconnect.
func (s *GatewayServer) cleanupPendingWorkflowRequests(client *ClientSession) {
	s.pendingWorkflowRequests.Range(func(key, value interface{}) bool {
		pending, ok := value.(*pendingWorkflowRequest)
		if !ok {
			s.pendingWorkflowRequests.Delete(key)
			return true
		}
		if pending.client == client {
			requestID, _ := key.(string)
			if _, deleted := s.pendingWorkflowRequests.LoadAndDelete(key); deleted {
				logging.Logger.Debug().Str("request_id", requestID).Str("identity", client.Identity.String()).Msg("cleaning up pending workflow request on client disconnect")
			}
		}
		return true
	})
}
