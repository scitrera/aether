package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/auth"
)

// =============================================================================
// API Token Management
// =============================================================================

func (p *GatewayStateProvider) ListTokens(ctx context.Context, limit, offset int, includeRevoked bool) ([]*admin.TokenInfo, error) {
	store := p.tokenStore()
	if store == nil {
		return nil, fmt.Errorf("token management not available (no token store configured)")
	}
	tokens, err := store.ListTokens(ctx, limit, offset, includeRevoked)
	if err != nil {
		return nil, err
	}
	result := make([]*admin.TokenInfo, 0, len(tokens))
	for _, t := range tokens {
		result = append(result, authTokenToAdmin(t))
	}
	return result, nil
}

func (p *GatewayStateProvider) GetToken(ctx context.Context, tokenID string) (*admin.TokenInfo, error) {
	store := p.tokenStore()
	if store == nil {
		return nil, fmt.Errorf("token management not available (no token store configured)")
	}
	t, err := store.GetToken(ctx, tokenID)
	if err != nil {
		return nil, err
	}
	return authTokenToAdmin(t), nil
}

func (p *GatewayStateProvider) CreateToken(ctx context.Context, req *admin.CreateTokenRequest) (*admin.CreateTokenResult, error) {
	store := p.tokenStore()
	if store == nil {
		return nil, fmt.Errorf("token management not available (no token store configured)")
	}

	workspacePatterns := req.WorkspacePatterns
	if len(workspacePatterns) == 0 {
		workspacePatterns = []string{"*"}
	}
	scopes := req.Scopes
	if len(scopes) == 0 {
		scopes = []string{"connect"}
	}
	createdBy := req.CreatedBy
	if createdBy == "" {
		createdBy = "admin"
	}

	var expiresAt *time.Time
	if req.ExpiresInHours > 0 {
		t := time.Now().Add(time.Duration(req.ExpiresInHours) * time.Hour)
		expiresAt = &t
	}

	result, err := store.CreateToken(ctx, req.Name, req.PrincipalType, workspacePatterns, scopes, createdBy, expiresAt)
	if err != nil {
		return nil, err
	}

	return &admin.CreateTokenResult{
		PlaintextToken: result.Token,
		Token:          authTokenToAdmin(result.APIToken),
	}, nil
}

func (p *GatewayStateProvider) DeleteToken(ctx context.Context, tokenID string) error {
	store := p.tokenStore()
	if store == nil {
		return fmt.Errorf("token management not available (no token store configured)")
	}
	return store.DeleteToken(ctx, tokenID)
}

func (p *GatewayStateProvider) RevokeToken(ctx context.Context, tokenID string) error {
	store := p.tokenStore()
	if store == nil {
		return fmt.Errorf("token management not available (no token store configured)")
	}
	return store.RevokeToken(ctx, tokenID)
}

// tokenStore returns the configured APITokenStore, or nil if none was provided.
func (p *GatewayStateProvider) tokenStore() auth.APITokenStore {
	return p.apiTokenStore
}

// authTokenToAdmin converts an auth.APIToken to an admin.TokenInfo.
func authTokenToAdmin(t *auth.APIToken) *admin.TokenInfo {
	return &admin.TokenInfo{
		ID:                t.ID,
		Name:              t.Name,
		PrincipalType:     t.PrincipalType,
		WorkspacePatterns: t.WorkspacePatterns,
		Scopes:            t.Scopes,
		CreatedBy:         t.CreatedBy,
		ExpiresAt:         t.ExpiresAt,
		LastUsedAt:        t.LastUsedAt,
		Revoked:           t.Revoked,
		RevokedAt:         t.RevokedAt,
		CreatedAt:         t.CreatedAt,
		UpdatedAt:         t.UpdatedAt,
	}
}
