package gateway

import (
	"context"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/logging"
)

// handleWorkspaceOp processes a WorkspaceOperation from a connected client.
func (s *GatewayServer) handleWorkspaceOp(ctx context.Context, client *ClientSession, op *pb.WorkspaceOperation) {
	if s.adminProvider == nil {
		sendWorkspaceError(client, "admin provider not configured")
		return
	}

	switch op.Op {
	case pb.WorkspaceOperation_LIST:
		workspaces, err := s.adminProvider.GetWorkspaces(ctx)
		if err != nil {
			logging.Logger.Error().Err(err).Msg("handleWorkspaceOp: list workspaces failed")
			sendWorkspaceError(client, err.Error())
			return
		}
		protoWorkspaces := make([]*pb.WorkspaceInfo, 0, len(workspaces))
		for _, w := range workspaces {
			protoWorkspaces = append(protoWorkspaces, adminWorkspaceToProto(w))
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Workspace{
				Workspace: &pb.WorkspaceResponse{
					Success:    true,
					Workspaces: protoWorkspaces,
					TotalCount: int32(len(protoWorkspaces)),
				},
			},
		})

	case pb.WorkspaceOperation_GET:
		workspace, err := s.adminProvider.GetWorkspaceByID(ctx, op.WorkspaceId)
		if err != nil {
			logging.Logger.Error().Err(err).Str("workspace_id", op.WorkspaceId).Msg("handleWorkspaceOp: get workspace failed")
			sendWorkspaceError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Workspace{
				Workspace: &pb.WorkspaceResponse{
					Success:   true,
					Workspace: adminWorkspaceToProto(workspace),
				},
			},
		})

	case pb.WorkspaceOperation_CREATE:
		if op.Workspace == nil {
			sendWorkspaceError(client, "workspace data required for CREATE")
			return
		}
		adminWs := protoWorkspaceToAdmin(op.Workspace)
		if err := s.adminProvider.CreateWorkspace(ctx, adminWs); err != nil {
			logging.Logger.Error().Err(err).Str("workspace_id", op.Workspace.WorkspaceId).Msg("handleWorkspaceOp: create workspace failed")
			sendWorkspaceError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Workspace{
				Workspace: &pb.WorkspaceResponse{
					Success: true,
					Message: "workspace created successfully",
				},
			},
		})

	case pb.WorkspaceOperation_UPDATE:
		if op.Workspace == nil {
			sendWorkspaceError(client, "workspace data required for UPDATE")
			return
		}
		adminWs := protoWorkspaceToAdmin(op.Workspace)
		if err := s.adminProvider.UpdateWorkspace(ctx, op.WorkspaceId, adminWs); err != nil {
			logging.Logger.Error().Err(err).Str("workspace_id", op.WorkspaceId).Msg("handleWorkspaceOp: update workspace failed")
			sendWorkspaceError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Workspace{
				Workspace: &pb.WorkspaceResponse{
					Success: true,
					Message: "workspace updated successfully",
				},
			},
		})

	case pb.WorkspaceOperation_DELETE:
		if err := s.adminProvider.DeleteWorkspace(ctx, op.WorkspaceId); err != nil {
			logging.Logger.Error().Err(err).Str("workspace_id", op.WorkspaceId).Msg("handleWorkspaceOp: delete workspace failed")
			sendWorkspaceError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Workspace{
				Workspace: &pb.WorkspaceResponse{
					Success: true,
					Message: "workspace deleted successfully",
				},
			},
		})

	case pb.WorkspaceOperation_GET_MESSAGE_FLOW:
		flow, err := s.adminProvider.GetMessageFlow(ctx, op.WorkspaceId)
		if err != nil {
			logging.Logger.Error().Err(err).Str("workspace_id", op.WorkspaceId).Msg("handleWorkspaceOp: get message flow failed")
			sendWorkspaceError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Workspace{
				Workspace: &pb.WorkspaceResponse{
					Success:     true,
					MessageFlow: adminMessageFlowToProto(flow),
				},
			},
		})

	default:
		sendWorkspaceError(client, "unknown workspace operation")
	}
}

func sendWorkspaceError(client *ClientSession, msg string) {
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Workspace{
			Workspace: &pb.WorkspaceResponse{
				Success: false,
				Error:   msg,
			},
		},
	})
}

func adminWorkspaceToProto(w *admin.WorkspaceInfo) *pb.WorkspaceInfo {
	meta := make(map[string]string)
	for k, v := range w.Metadata {
		if s, ok := v.(string); ok {
			meta[k] = s
		}
	}
	return &pb.WorkspaceInfo{
		WorkspaceId:   w.WorkspaceID,
		DisplayName:   w.DisplayName,
		Description:   w.Description,
		TenantId:      w.TenantID,
		CreatedAt:     w.CreatedAt.Unix(),
		UpdatedAt:     w.UpdatedAt.Unix(),
		Metadata:      meta,
		ActiveAgents:  int32(w.ActiveAgents),
		ActiveTasks:   int32(w.ActiveTasks),
		ActiveUsers:   int32(w.ActiveUsers),
		TotalMessages: w.TotalMessages,
	}
}

func protoWorkspaceToAdmin(w *pb.WorkspaceInfo) *admin.WorkspaceInfo {
	meta := make(map[string]interface{})
	for k, v := range w.Metadata {
		meta[k] = v
	}
	return &admin.WorkspaceInfo{
		WorkspaceID: w.WorkspaceId,
		DisplayName: w.DisplayName,
		Description: w.Description,
		TenantID:    w.TenantId,
		CreatedAt:   time.Unix(w.CreatedAt, 0),
		UpdatedAt:   time.Unix(w.UpdatedAt, 0),
		Metadata:    meta,
	}
}

// principalTypeStringToProto converts an admin string principal type to pb.PrincipalType.
func principalTypeStringToProto(t string) pb.PrincipalType {
	switch t {
	case "agent":
		return pb.PrincipalType_PRINCIPAL_AGENT
	case "task":
		return pb.PrincipalType_PRINCIPAL_TASK
	case "user":
		return pb.PrincipalType_PRINCIPAL_USER
	case "orchestrator":
		return pb.PrincipalType_PRINCIPAL_ORCHESTRATOR
	case "workflow_engine":
		return pb.PrincipalType_PRINCIPAL_WORKFLOW_ENGINE
	case "metrics_bridge":
		return pb.PrincipalType_PRINCIPAL_METRICS_BRIDGE
	case "bridge":
		return pb.PrincipalType_PRINCIPAL_BRIDGE
	case "service":
		return pb.PrincipalType_PRINCIPAL_SERVICE
	default:
		return pb.PrincipalType_PRINCIPAL_TYPE_UNSPECIFIED
	}
}

func adminMessageFlowToProto(f *admin.MessageFlowInfo) *pb.MessageFlowInfo {
	if f == nil {
		return nil
	}
	nodes := make([]*pb.FlowNode, 0, len(f.Nodes))
	for _, n := range f.Nodes {
		nodes = append(nodes, &pb.FlowNode{
			Id:             n.ID,
			Label:          n.Label,
			Type:           principalTypeStringToProto(n.Type),
			Status:         n.Status,
			Implementation: n.Implementation,
			Specifier:      n.Specifier,
			Topic:          n.Topic,
		})
	}
	edges := make([]*pb.FlowEdge, 0, len(f.Edges))
	for _, e := range f.Edges {
		edges = append(edges, &pb.FlowEdge{
			From:  e.From,
			To:    e.To,
			Label: e.Label,
			Count: e.Count,
		})
	}
	return &pb.MessageFlowInfo{
		WorkspaceId: f.WorkspaceID,
		Nodes:       nodes,
		Edges:       edges,
		UpdatedAt:   f.UpdatedAt.Unix(),
	}
}
