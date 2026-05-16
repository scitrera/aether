package gateway

import (
	"context"
	"errors"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/logging"
	aethererrors "github.com/scitrera/aether/pkg/errors"
)

// ErrCodePrefixConflict is the error_code returned to clients when
// REGISTER/UPDATE rejects an agent because its resource_type_prefix is
// already claimed by another active registration. See
// aethererrors.ResourceTypePrefixConflictError for the typed error that
// triggers this code.
const ErrCodePrefixConflict = "ERR_PREFIX_CONFLICT"

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
			sendAgentError(client, formatAgentError(err))
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
			sendAgentError(client, formatAgentError(err))
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

// formatAgentError shapes the error string returned to AgentResponse callers
// for REGISTER/UPDATE failures. Phase 5 Stage B prepends a typed error-code
// prefix ("ERR_PREFIX_CONFLICT: ...") for the resource_type_prefix uniqueness
// conflict so SDK callers can parse the code without a dedicated proto field.
// All other errors fall through to Error().
func formatAgentError(err error) string {
	if err == nil {
		return ""
	}
	var conflict *aethererrors.ResourceTypePrefixConflictError
	if errors.As(err, &conflict) {
		return ErrCodePrefixConflict + ": " + conflict.Error()
	}
	return err.Error()
}

func adminAgentToProto(a *admin.AgentRegistrationInfo) *pb.AgentRegistrationInfo {
	params := make(map[string]string)
	for k, v := range a.LaunchParams {
		if s, ok := v.(string); ok {
			params[k] = s
		}
	}
	out := &pb.AgentRegistrationInfo{
		Implementation:      a.Implementation,
		OrchestratorProfile: a.OrchestratorProfile,
		Description:         a.Description,
		LaunchParams:        params,
		RegisteredAt:        a.RegisteredAt.Unix(),
		UpdatedAt:           a.UpdatedAt.Unix(),
		Capabilities:        a.Capabilities,
		Extensions:          a.Extensions,
	}
	if len(a.ResourceSchema) > 0 {
		out.ResourceSchema = make([]*pb.AgentResourceSchemaEntry, len(a.ResourceSchema))
		for i, e := range a.ResourceSchema {
			out.ResourceSchema[i] = &pb.AgentResourceSchemaEntry{
				ResourceTypePrefix: e.ResourceTypePrefix,
				PermissionVerbs:    e.PermissionVerbs,
				ResourceIdSchema:   e.ResourceIDSchema,
			}
		}
	}
	return out
}

func protoAgentToAdmin(a *pb.AgentRegistrationInfo) *admin.AgentRegistrationInfo {
	params := make(map[string]interface{})
	for k, v := range a.LaunchParams {
		params[k] = v
	}
	out := &admin.AgentRegistrationInfo{
		Implementation:      a.Implementation,
		OrchestratorProfile: a.OrchestratorProfile,
		Description:         a.Description,
		LaunchParams:        params,
		RegisteredAt:        time.Unix(a.RegisteredAt, 0),
		UpdatedAt:           time.Unix(a.UpdatedAt, 0),
		Capabilities:        a.Capabilities,
		Extensions:          a.Extensions,
	}
	if len(a.ResourceSchema) > 0 {
		out.ResourceSchema = make([]admin.AgentResourceSchemaEntry, len(a.ResourceSchema))
		for i, e := range a.ResourceSchema {
			if e == nil {
				continue
			}
			out.ResourceSchema[i] = admin.AgentResourceSchemaEntry{
				ResourceTypePrefix: e.ResourceTypePrefix,
				PermissionVerbs:    e.PermissionVerbs,
				ResourceIDSchema:   e.ResourceIdSchema,
			}
		}
	}
	return out
}
