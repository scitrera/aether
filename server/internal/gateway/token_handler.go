package gateway

import (
	"context"
	"strings"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/internal/acl"
	"github.com/scitrera/aether/server/internal/admin"
	"github.com/scitrera/aether/server/internal/logging"
	"github.com/scitrera/aether/server/pkg/models"
)

// handleTokenOp processes a TokenOperation from a connected client.
//
// caller is the connection's authenticated identity. The umbrella
// admin/tokens (or admin/*) gate is already enforced by isAllowedAdminOp at
// the connect.go dispatch site for ALL token operations. For CREATE of a
// principal_type=user token, a SECOND, stricter gate applies here: see the
// CREATE branch.
func (s *GatewayServer) handleTokenOp(ctx context.Context, client *ClientSession, caller models.Identity, op *pb.TokenOperation) {
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

		// SECURITY: minting a principal_type=user token issues a credential
		// that authenticates AS a real user (see authenticateCredentials'
		// claim↔key binding). The umbrella admin/tokens gate that admitted us
		// here is sufficient for agent/service/task tokens, but user tokens
		// require a SECOND, narrower gate so that holding admin/tokens does not
		// implicitly grant the ability to impersonate arbitrary users. The
		// caller must hold capability/mint_user_tokens OR the global admin
		// umbrella admin/*. We also stop trusting req.CreatedBy blindly: for a
		// user token, created_by IS the impersonated user id, so an unset value
		// is rejected (it would otherwise silently default to "admin" — an
		// invalid user identity) and the value is recorded server-side via the
		// validated request below.
		createdBy := req.CreatedBy
		// created_by enforcement scope: user-only (intentional).
		//
		// For principal_type=user, created_by IS the impersonated user id and an
		// empty value is dangerous (it would authenticate as a principal with no ID,
		// effectively bypassing identity binding). We enforce non-empty here.
		//
		// For agent/service/task tokens, created_by has historically been an optional
		// audit annotation ("who minted this") rather than an authenticating field.
		// Enforcing non-empty would break all pre-existing non-user mint flows. The
		// connect-path binding in authenticateCredentials already handles the
		// empty-keyID case safely: identity.ID = keyID is always adopted from the key
		// (even when empty), so the claim can never supply an ID the key doesn't
		// authenticate. The empty-keyID case for service/agent keys is therefore
		// defense-in-depth at the connect path, not a mint-time gap.
		if models.PrincipalType(req.PrincipalType) == models.PrincipalUser {
			holdsCapability := s.callerHoldsCapability(ctx, client, caller, acl.PermissionMintUserTokens, "mint_user_tokens")
			holdsGlobalAdmin := s.isAllowedAdminOpQuiet(client, caller, "*")
			if !holdsCapability && !holdsGlobalAdmin {
				logging.Logger.Warn().
					Str("caller", caller.String()).
					Str("requested_user", createdBy).
					Msg("handleTokenOp: user-token mint denied (missing capability/mint_user_tokens and admin/*)")
				sendTokenError(client, op.GetRequestId(),
					"minting user tokens requires capability/mint_user_tokens or global admin (admin/*)")
				return
			}
			if strings.TrimSpace(createdBy) == "" {
				sendTokenError(client, op.GetRequestId(),
					"created_by (the target user id) is required for user tokens")
				return
			}
			logging.Logger.Info().
				Str("caller", caller.String()).
				Str("target_user", createdBy).
				Bool("via_capability", holdsCapability).
				Bool("via_global_admin", holdsGlobalAdmin).
				Msg("handleTokenOp: user-token mint authorized")
		}

		result, err := s.adminProvider.CreateToken(ctx, &admin.CreateTokenRequest{
			Name:              req.Name,
			PrincipalType:     req.PrincipalType,
			WorkspacePatterns: req.WorkspacePatterns,
			Scopes:            req.Scopes,
			ExpiresInHours:    int(req.ExpiresInHours),
			CreatedBy:         createdBy,
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
