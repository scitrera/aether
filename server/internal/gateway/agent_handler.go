package gateway

import (
	"context"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/logging"
)

// handleAgentOp processes an AgentOperation from a connected client.
func (s *GatewayServer) handleAgentOp(ctx context.Context, client *ClientSession, op *pb.AgentOperation) {
	if s.adminProvider == nil {
		sendAgentError(client, "admin provider not configured")
		return
	}

	switch op.Op {
	case pb.AgentOperation_LIST:
		agents, err := s.adminProvider.GetAgentRegistrations(ctx)
		if err != nil {
			logging.Logger.Error().Err(err).Msg("handleAgentOp: list agents failed")
			sendAgentError(client, err.Error())
			return
		}
		protoAgents := make([]*pb.AgentRegistrationInfo, 0, len(agents))
		for _, a := range agents {
			protoAgents = append(protoAgents, adminAgentToProto(a))
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Agent{
				Agent: &pb.AgentResponse{
					Success:    true,
					Agents:     protoAgents,
					TotalCount: int32(len(protoAgents)),
				},
			},
		})

	case pb.AgentOperation_GET:
		agent, err := s.adminProvider.GetAgentByImplementation(ctx, op.Implementation)
		if err != nil {
			logging.Logger.Error().Err(err).Str("implementation", op.Implementation).Msg("handleAgentOp: get agent failed")
			sendAgentError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Agent{
				Agent: &pb.AgentResponse{
					Success: true,
					Agent:   adminAgentToProto(agent),
				},
			},
		})

	case pb.AgentOperation_REGISTER:
		if op.Agent == nil {
			sendAgentError(client, "agent data required for REGISTER")
			return
		}
		adminAgent := protoAgentToAdmin(op.Agent)
		if err := s.adminProvider.RegisterAgent(ctx, adminAgent); err != nil {
			logging.Logger.Error().Err(err).Str("implementation", op.Agent.Implementation).Msg("handleAgentOp: register agent failed")
			sendAgentError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Agent{
				Agent: &pb.AgentResponse{
					Success: true,
					Message: "agent registered successfully",
				},
			},
		})

	case pb.AgentOperation_UPDATE:
		if op.Agent == nil {
			sendAgentError(client, "agent data required for UPDATE")
			return
		}
		adminAgent := protoAgentToAdmin(op.Agent)
		if err := s.adminProvider.UpdateAgent(ctx, op.Implementation, adminAgent); err != nil {
			logging.Logger.Error().Err(err).Str("implementation", op.Implementation).Msg("handleAgentOp: update agent failed")
			sendAgentError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Agent{
				Agent: &pb.AgentResponse{
					Success: true,
					Message: "agent updated successfully",
				},
			},
		})

	case pb.AgentOperation_DELETE:
		if err := s.adminProvider.DeleteAgent(ctx, op.Implementation); err != nil {
			logging.Logger.Error().Err(err).Str("implementation", op.Implementation).Msg("handleAgentOp: delete agent failed")
			sendAgentError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Agent{
				Agent: &pb.AgentResponse{
					Success: true,
					Message: "agent deleted successfully",
				},
			},
		})

	case pb.AgentOperation_LAUNCH:
		launchReq := &admin.LaunchAgentRequest{
			Implementation: op.Implementation,
		}
		if op.LaunchParams != nil {
			launchReq.Specifier = op.LaunchParams.Specifier
			launchReq.Workspace = op.LaunchParams.Workspace
		}
		resp, err := s.adminProvider.LaunchAgent(ctx, launchReq)
		if err != nil {
			logging.Logger.Error().Err(err).Str("implementation", op.Implementation).Msg("handleAgentOp: launch agent failed")
			sendAgentError(client, err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Agent{
				Agent: &pb.AgentResponse{
					Success: true,
					Message: resp.Message,
					LaunchResult: &pb.AgentLaunchResult{
						TaskId:  resp.TaskID,
						Message: resp.Message,
					},
				},
			},
		})

	case pb.AgentOperation_LIST_ORCHESTRATORS:
		profiles, err := s.adminProvider.GetOrchestratorProfiles(ctx)
		if err != nil {
			logging.Logger.Error().Err(err).Msg("handleAgentOp: list orchestrators failed")
			sendAgentError(client, err.Error())
			return
		}
		protoOrches := make([]*pb.OrchestratorInfo, 0, len(profiles))
		for _, p := range profiles {
			protoOrches = append(protoOrches, &pb.OrchestratorInfo{
				OrchestratorId: p.OrchestratorID,
				Profiles:       p.Profiles,
				ConnectedAt:    p.ConnectedAt.Unix(),
			})
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Agent{
				Agent: &pb.AgentResponse{
					Success:       true,
					Orchestrators: protoOrches,
				},
			},
		})

	default:
		sendAgentError(client, "unknown agent operation")
	}
}

func sendAgentError(client *ClientSession, msg string) {
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Agent{
			Agent: &pb.AgentResponse{
				Success: false,
				Error:   msg,
			},
		},
	})
}

func adminAgentToProto(a *admin.AgentRegistrationInfo) *pb.AgentRegistrationInfo {
	params := make(map[string]string)
	for k, v := range a.LaunchParams {
		if s, ok := v.(string); ok {
			params[k] = s
		}
	}
	return &pb.AgentRegistrationInfo{
		Implementation:      a.Implementation,
		OrchestratorProfile: a.OrchestratorProfile,
		Description:         a.Description,
		LaunchParams:        params,
		RegisteredAt:        a.RegisteredAt.Unix(),
		UpdatedAt:           a.UpdatedAt.Unix(),
	}
}

func protoAgentToAdmin(a *pb.AgentRegistrationInfo) *admin.AgentRegistrationInfo {
	params := make(map[string]interface{})
	for k, v := range a.LaunchParams {
		params[k] = v
	}
	return &admin.AgentRegistrationInfo{
		Implementation:      a.Implementation,
		OrchestratorProfile: a.OrchestratorProfile,
		Description:         a.Description,
		LaunchParams:        params,
		RegisteredAt:        time.Unix(a.RegisteredAt, 0),
		UpdatedAt:           time.Unix(a.UpdatedAt, 0),
	}
}
