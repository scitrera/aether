package gateway

import (
	"context"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/logging"
)

// handleTokenOp processes a TokenOperation from a connected client.
func (s *GatewayServer) handleTokenOp(ctx context.Context, client *ClientSession, op *pb.TokenOperation) {
	if s.adminProvider == nil {
		sendTokenError(client, op.GetRequestId(), "admin provider not configured")
		return
	}

	switch op.Op {
	case pb.TokenOperation_LIST:
		limit := int(op.GetFilter().GetLimit())
		offset := int(op.GetFilter().GetOffset())
		includeRevoked := op.GetFilter().GetIncludeRevoked()
		if limit <= 0 {
			limit = 100
		}

		tokens, err := s.adminProvider.ListTokens(ctx, limit, offset, includeRevoked)
		if err != nil {
			logging.Logger.Error().Err(err).Msg("handleTokenOp: list tokens failed")
			sendTokenError(client, op.GetRequestId(), err.Error())
			return
		}

		protoTokens := make([]*pb.TokenInfo, 0, len(tokens))
		for _, t := range tokens {
			protoTokens = append(protoTokens, adminTokenToProto(t))
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Token{
				Token: &pb.TokenResponse{
					Success:    true,
					Tokens:     protoTokens,
					TotalCount: int32(len(protoTokens)),
					RequestId:  op.GetRequestId(),
				},
			},
		})

	case pb.TokenOperation_GET:
		token, err := s.adminProvider.GetToken(ctx, op.TokenId)
		if err != nil {
			logging.Logger.Error().Err(err).Str("token_id", op.TokenId).Msg("handleTokenOp: get token failed")
			sendTokenError(client, op.GetRequestId(), err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Token{
				Token: &pb.TokenResponse{
					Success:   true,
					Token:     adminTokenToProto(token),
					RequestId: op.GetRequestId(),
				},
			},
		})

	case pb.TokenOperation_CREATE:
		req := op.GetCreateRequest()
		if req == nil {
			sendTokenError(client, op.GetRequestId(), "create_request is required")
			return
		}
		if req.Name == "" {
			sendTokenError(client, op.GetRequestId(), "name is required")
			return
		}
		if req.PrincipalType == "" {
			sendTokenError(client, op.GetRequestId(), "principal_type is required")
			return
		}

		result, err := s.adminProvider.CreateToken(ctx, &admin.CreateTokenRequest{
			Name:              req.Name,
			PrincipalType:     req.PrincipalType,
			WorkspacePatterns: req.WorkspacePatterns,
			Scopes:            req.Scopes,
			ExpiresInHours:    int(req.ExpiresInHours),
			CreatedBy:         req.CreatedBy,
		})
		if err != nil {
			logging.Logger.Error().Err(err).Msg("handleTokenOp: create token failed")
			sendTokenError(client, op.GetRequestId(), err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Token{
				Token: &pb.TokenResponse{
					Success:        true,
					Message:        "token created",
					PlaintextToken: result.PlaintextToken,
					CreatedToken:   adminTokenToProto(result.Token),
					RequestId:      op.GetRequestId(),
				},
			},
		})

	case pb.TokenOperation_DELETE:
		if err := s.adminProvider.DeleteToken(ctx, op.TokenId); err != nil {
			logging.Logger.Error().Err(err).Str("token_id", op.TokenId).Msg("handleTokenOp: delete token failed")
			sendTokenError(client, op.GetRequestId(), err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Token{
				Token: &pb.TokenResponse{
					Success:   true,
					Message:   "deleted",
					RequestId: op.GetRequestId(),
				},
			},
		})

	case pb.TokenOperation_REVOKE:
		if err := s.adminProvider.RevokeToken(ctx, op.TokenId); err != nil {
			logging.Logger.Error().Err(err).Str("token_id", op.TokenId).Msg("handleTokenOp: revoke token failed")
			sendTokenError(client, op.GetRequestId(), err.Error())
			return
		}
		_ = client.SafeSend(&pb.DownstreamMessage{
			Payload: &pb.DownstreamMessage_Token{
				Token: &pb.TokenResponse{
					Success:   true,
					Message:   "revoked",
					RequestId: op.GetRequestId(),
				},
			},
		})

	default:
		sendTokenError(client, op.GetRequestId(), "unknown token operation")
	}
}

// sendTokenError sends a failed TokenResponse to the client.
func sendTokenError(client *ClientSession, requestID, errMsg string) {
	_ = client.SafeSend(&pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_Token{
			Token: &pb.TokenResponse{
				Success:   false,
				Error:     errMsg,
				RequestId: requestID,
			},
		},
	})
}

// adminTokenToProto converts an admin.TokenInfo to the proto TokenInfo message.
func adminTokenToProto(t *admin.TokenInfo) *pb.TokenInfo {
	info := &pb.TokenInfo{
		Id:                t.ID,
		Name:              t.Name,
		PrincipalType:     t.PrincipalType,
		WorkspacePatterns: t.WorkspacePatterns,
		Scopes:            t.Scopes,
		CreatedBy:         t.CreatedBy,
		Revoked:           t.Revoked,
		CreatedAt:         t.CreatedAt.Unix(),
		UpdatedAt:         t.UpdatedAt.Unix(),
	}
	if t.ExpiresAt != nil {
		info.ExpiresAt = t.ExpiresAt.Unix()
	}
	if t.LastUsedAt != nil {
		info.LastUsedAt = t.LastUsedAt.Unix()
	}
	if t.RevokedAt != nil {
		info.RevokedAt = t.RevokedAt.Unix()
	}
	return info
}
