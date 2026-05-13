package auth

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/scitrera/aether/internal/logging"
	"sync"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"

	"github.com/scitrera/aether/pkg/models"
)

// OAuthProviderConfig configures a single OAuth/OIDC provider for JWT validation
type OAuthProviderConfig struct {
	Name             string
	Issuer           string
	JWKSURL          string
	Audience         string
	ClaimsMapping    ClaimsMapping
	DefaultPrincipal string
	DefaultWorkspace string
}

// ClaimsMapping defines how JWT claims map to Identity fields
type ClaimsMapping struct {
	PrincipalType string // JWT claim key for principal type (e.g., "principal_type")
	Workspace     string // JWT claim key for workspace (e.g., "workspace")
	Identity      string // JWT claim key for identity string (e.g., "sub")
}

// OAuthAuthenticator validates OAuth/JWT bearer tokens against configured providers
type OAuthAuthenticator struct {
	providers       []OAuthProviderConfig
	verifySignature bool

	// JWKS keyfuncs per provider (lazy-initialized on first use)
	mu       sync.Mutex
	keyfuncs map[string]keyfunc.Keyfunc    // provider name → keyfunc
	cancels  map[string]context.CancelFunc // provider name → cancel func for JWKS refresh
}

// NewOAuthAuthenticator creates a new OAuth authenticator with the given provider configurations.
// When verifySignature is nil or points to true, JWT signatures are verified against the provider's
// JWKS endpoint (safe default). Pass a pointer to false only for development/testing — a loud
// WARNING will be logged at construction time.
func NewOAuthAuthenticator(providers []OAuthProviderConfig, verifySignature *bool) *OAuthAuthenticator {
	verify := true
	if verifySignature != nil && !*verifySignature {
		if os.Getenv("AETHER_DEV_MODE") != "true" {
			logging.Logger.Error().Msg("JWT signature verification cannot be disabled outside dev mode; set AETHER_DEV_MODE=true to override")
			// Force verification on
		} else {
			verify = false
			logging.Logger.Warn().Msg("JWT signature verification DISABLED (AETHER_DEV_MODE=true) — NOT FOR PRODUCTION")
		}
	}
	return &OAuthAuthenticator{
		providers:       providers,
		verifySignature: verify,
		keyfuncs:        make(map[string]keyfunc.Keyfunc),
		cancels:         make(map[string]context.CancelFunc),
	}
}

// Name returns the authenticator name
func (a *OAuthAuthenticator) Name() string {
	return "oauth"
}

// Authenticate validates a JWT bearer token from the credentials map.
// Returns (nil, nil) if no bearer token credential is present.
// Returns (nil, error) if a token is present but invalid.
// Returns (result, nil) on successful authentication.
func (a *OAuthAuthenticator) Authenticate(ctx context.Context, credentials map[string]string) (*AuthResult, error) {
	tokenStr := credentials["bearer_token"]
	if tokenStr == "" {
		tokenStr = credentials["authorization"]
	}
	if tokenStr == "" {
		return nil, nil
	}

	// Strip "Bearer " prefix if present
	if strings.HasPrefix(tokenStr, "Bearer ") {
		tokenStr = strings.TrimPrefix(tokenStr, "Bearer ")
	} else if strings.HasPrefix(tokenStr, "bearer ") {
		tokenStr = strings.TrimPrefix(tokenStr, "bearer ")
	}

	if tokenStr == "" {
		return nil, fmt.Errorf("empty bearer token")
	}

	// Try each provider in order
	for _, provider := range a.providers {
		result, err := a.validateWithProvider(ctx, tokenStr, provider)
		if err != nil {
			continue
		}
		if result != nil {
			return result, nil
		}
	}

	return nil, fmt.Errorf("no OAuth provider accepted the token")
}

// validateWithProvider attempts to validate a JWT against a single provider configuration.
func (a *OAuthAuthenticator) validateWithProvider(ctx context.Context, tokenStr string, provider OAuthProviderConfig) (*AuthResult, error) {
	claims, err := a.parseToken(ctx, tokenStr, provider)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JWT for provider %s: %w", provider.Name, err)
	}

	// Validate issuer if configured
	if provider.Issuer != "" {
		iss, _ := claims["iss"].(string)
		if iss != provider.Issuer {
			return nil, fmt.Errorf("issuer mismatch: expected %s, got %s", provider.Issuer, iss)
		}
	}

	// Validate audience if configured
	if provider.Audience != "" {
		if !audienceMatches(claims, provider.Audience) {
			return nil, fmt.Errorf("audience mismatch for provider %s", provider.Name)
		}
	}

	// Map claims to identity
	identity, err := a.mapClaimsToIdentity(claims, provider)
	if err != nil {
		return nil, fmt.Errorf("failed to map claims to identity for provider %s: %w", provider.Name, err)
	}

	return &AuthResult{
		Authenticated: true,
		Identity:      identity,
		Method:        "oauth",
		Metadata: map[string]interface{}{
			"provider": provider.Name,
			"issuer":   claims["iss"],
			"subject":  claims["sub"],
			"claims":   claims,
		},
	}, nil
}

// parseToken parses and optionally verifies a JWT depending on the verifySignature setting.
func (a *OAuthAuthenticator) parseToken(ctx context.Context, tokenStr string, provider OAuthProviderConfig) (jwt.MapClaims, error) {
	if !a.verifySignature {
		logging.Logger.Warn().Msg("SECURITY WARNING: JWT signature verification is DISABLED — do not use in production")
		return a.parseUnverified(tokenStr)
	}
	return a.parseVerified(ctx, tokenStr, provider)
}

// parseVerified parses a JWT with full JWKS signature verification.
func (a *OAuthAuthenticator) parseVerified(ctx context.Context, tokenStr string, provider OAuthProviderConfig) (jwt.MapClaims, error) {
	kf, err := a.getKeyfunc(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("failed to get JWKS keyfunc: %w", err)
	}

	// Build parser options
	opts := []jwt.ParserOption{
		jwt.WithValidMethods([]string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512"}),
	}
	if provider.Issuer != "" {
		opts = append(opts, jwt.WithIssuer(provider.Issuer))
	}
	if provider.Audience != "" {
		opts = append(opts, jwt.WithAudience(provider.Audience))
	}

	token, err := jwt.Parse(tokenStr, kf.KeyfuncCtx(ctx), opts...)
	if err != nil {
		return nil, fmt.Errorf("JWT validation failed: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("unexpected claims type")
	}

	return claims, nil
}

// parseUnverified parses JWT claims without signature verification (development only).
func (a *OAuthAuthenticator) parseUnverified(tokenStr string) (jwt.MapClaims, error) {
	parser := jwt.NewParser()
	token, _, err := parser.ParseUnverified(tokenStr, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("failed to parse JWT: %w", err)
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("unexpected claims type")
	}
	return claims, nil
}

// getKeyfunc returns or lazily creates a JWKS keyfunc for the given provider.
//
// The supplied caller context (unused below — see notes) is accepted for API
// symmetry with other Authenticator methods, but the JWKS poll context must
// outlive the caller so cached entries keep refreshing across requests. We
// therefore build a fresh background context for the long-lived keyfunc and
// track its cancel in a.cancels so Close() can stop it cleanly.
//
//nolint:revive // ctx accepted for callsite symmetry with peers (SA4009 expected and unrelated)
func (a *OAuthAuthenticator) getKeyfunc(_ context.Context, provider OAuthProviderConfig) (keyfunc.Keyfunc, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if kf, ok := a.keyfuncs[provider.Name]; ok {
		return kf, nil
	}

	if provider.JWKSURL == "" {
		return nil, fmt.Errorf("no JWKS URL configured for provider %s", provider.Name)
	}

	ctx, cancel := context.WithCancel(context.Background())
	kf, err := keyfunc.NewDefaultCtx(ctx, []string{provider.JWKSURL})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create JWKS keyfunc for provider %s: %w", provider.Name, err)
	}

	a.keyfuncs[provider.Name] = kf
	a.cancels[provider.Name] = cancel
	return kf, nil
}

// mapClaimsToIdentity maps JWT claims to a models.Identity based on the provider's ClaimsMapping
func (a *OAuthAuthenticator) mapClaimsToIdentity(claims jwt.MapClaims, provider OAuthProviderConfig) (models.Identity, error) {
	identity := models.Identity{}

	// Determine principal type from claims or default
	principalTypeStr := provider.DefaultPrincipal
	if provider.ClaimsMapping.PrincipalType != "" {
		if v, ok := claims[provider.ClaimsMapping.PrincipalType].(string); ok && v != "" {
			principalTypeStr = v
		}
	}
	if principalTypeStr == "" {
		principalTypeStr = string(models.PrincipalUser)
	}
	pt, err := parsePrincipalType(principalTypeStr)
	if err != nil {
		return identity, fmt.Errorf("invalid principal type %q: %w", principalTypeStr, err)
	}
	// Reject system-level principal types that must not be claimed via OAuth
	switch pt {
	case models.PrincipalWorkflowEngine, models.PrincipalMetricsBridge, models.PrincipalOrchestrator, models.PrincipalBridge, models.PrincipalService:
		return identity, fmt.Errorf("system principal type %q cannot be claimed via OAuth", principalTypeStr)
	}
	identity.Type = pt

	// Set workspace from claims or default
	if provider.ClaimsMapping.Workspace != "" {
		if v, ok := claims[provider.ClaimsMapping.Workspace].(string); ok && v != "" {
			identity.Workspace = v
		}
	}
	if identity.Workspace == "" {
		identity.Workspace = provider.DefaultWorkspace
	}

	// Set identity ID from claims
	if provider.ClaimsMapping.Identity != "" {
		if v, ok := claims[provider.ClaimsMapping.Identity].(string); ok && v != "" {
			identity.ID = v
		}
	}
	if identity.ID == "" {
		if sub, ok := claims["sub"].(string); ok {
			identity.ID = sub
		}
	}

	return identity, nil
}

// Close cleans up background JWKS refresh goroutines for all providers.
func (a *OAuthAuthenticator) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()

	for name, cancel := range a.cancels {
		cancel()
		delete(a.cancels, name)
		delete(a.keyfuncs, name)
	}
}

// audienceMatches checks if the expected audience is present in the JWT claims
func audienceMatches(claims jwt.MapClaims, expected string) bool {
	aud, ok := claims["aud"]
	if !ok {
		return false
	}
	switch v := aud.(type) {
	case string:
		return v == expected
	case []interface{}:
		for _, a := range v {
			if s, ok := a.(string); ok && s == expected {
				return true
			}
		}
	}
	return false
}
