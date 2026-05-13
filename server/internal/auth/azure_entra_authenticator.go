package auth

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
)

const (
	entraJWKSURL      = "https://login.microsoftonline.com/common/discovery/v2.0/keys"
	entraIssuerPrefix = "https://login.microsoftonline.com/"
	entraIssuerSuffix = "/v2.0"
)

// AzureEntraAuthenticator validates Microsoft Entra (Azure AD) JWT bearer tokens.
// It supports both single-tenant and multi-tenant Entra app registrations.
type AzureEntraAuthenticator struct {
	tenantID        string
	clientID        string
	allowedTenants  []string // nil/empty = accept any tenant (multi-tenant open)
	verifySignature bool

	// JWKS keyfunc (lazy-initialized on first use)
	mu     sync.Mutex
	kf     keyfunc.Keyfunc
	cancel context.CancelFunc
}

// NewAzureEntraAuthenticator creates a new Azure Entra authenticator.
//
// tenantID is the configured tenant ID for single-tenant validation, or "common"
// for multi-tenant. clientID is the Entra application's client ID (used as audience).
// allowedTenants is an optional whitelist of tenant IDs for multi-tenant mode;
// empty means accept any tenant. verifySignature follows the same dev-mode override
// pattern as OAuthAuthenticator — pass nil or a pointer to true for production.
func NewAzureEntraAuthenticator(tenantID, clientID string, allowedTenants []string, verifySignature *bool) *AzureEntraAuthenticator {
	verify := true
	if verifySignature != nil && !*verifySignature {
		if os.Getenv("AETHER_DEV_MODE") != "true" {
			logging.Logger.Error().Msg("Azure Entra JWT signature verification cannot be disabled outside dev mode; set AETHER_DEV_MODE=true to override")
			// Force verification on
		} else {
			verify = false
			logging.Logger.Warn().Msg("Azure Entra JWT signature verification DISABLED (AETHER_DEV_MODE=true) — NOT FOR PRODUCTION")
		}
	}
	return &AzureEntraAuthenticator{
		tenantID:        tenantID,
		clientID:        clientID,
		allowedTenants:  allowedTenants,
		verifySignature: verify,
	}
}

// Name returns the authenticator name.
func (a *AzureEntraAuthenticator) Name() string {
	return "azure_entra"
}

// Authenticate validates an Azure Entra JWT bearer token from the credentials map.
// Returns (nil, nil) if no bearer token is present or if the issuer is not a
// Microsoft login URL (allowing other authenticators to be tried).
// Returns (nil, error) if a Microsoft-issued token is present but invalid.
// Returns (result, nil) on successful authentication.
func (a *AzureEntraAuthenticator) Authenticate(ctx context.Context, credentials map[string]string) (*AuthResult, error) {
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

	// Quick pre-check: parse without verification to read the issuer.
	// If the issuer doesn't look like a Microsoft login URL, skip this authenticator
	// so the composite can try others.
	parser := jwt.NewParser()
	unverifiedToken, _, err := parser.ParseUnverified(tokenStr, jwt.MapClaims{})
	if err != nil {
		// Malformed token — not ours to claim
		return nil, nil
	}
	unverifiedClaims, ok := unverifiedToken.Claims.(jwt.MapClaims)
	if !ok {
		return nil, nil
	}
	iss, _ := unverifiedClaims["iss"].(string)
	if !strings.HasPrefix(iss, entraIssuerPrefix) || !strings.HasSuffix(iss, entraIssuerSuffix) {
		// Not a Microsoft Entra token — let other authenticators try
		return nil, nil
	}

	return a.validate(ctx, tokenStr)
}

// validate performs full JWT validation for a token identified as Entra-issued.
func (a *AzureEntraAuthenticator) validate(ctx context.Context, tokenStr string) (*AuthResult, error) {
	var claims jwt.MapClaims

	if !a.verifySignature {
		logging.Logger.Warn().Msg("SECURITY WARNING: Azure Entra JWT signature verification is DISABLED — do not use in production")
		parser := jwt.NewParser()
		token, _, err := parser.ParseUnverified(tokenStr, jwt.MapClaims{})
		if err != nil {
			return nil, fmt.Errorf("failed to parse Azure Entra JWT: %w", err)
		}
		var ok bool
		claims, ok = token.Claims.(jwt.MapClaims)
		if !ok {
			return nil, fmt.Errorf("unexpected claims type in Azure Entra JWT")
		}
	} else {
		kf, err := a.getKeyfunc(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get Azure Entra JWKS keyfunc: %w", err)
		}

		token, err := jwt.Parse(
			tokenStr,
			kf.KeyfuncCtx(ctx),
			jwt.WithValidMethods([]string{"RS256"}),
			jwt.WithExpirationRequired(),
			jwt.WithAudience(a.clientID),
		)
		if err != nil {
			return nil, fmt.Errorf("azure entra JWT validation failed: %w", err)
		}

		var ok bool
		claims, ok = token.Claims.(jwt.MapClaims)
		if !ok {
			return nil, fmt.Errorf("unexpected claims type in Azure Entra JWT")
		}
	}

	// Validate issuer pattern and extract the tid embedded in the issuer URL.
	iss := extractClaim(claims, "iss")
	if !strings.HasPrefix(iss, entraIssuerPrefix) || !strings.HasSuffix(iss, entraIssuerSuffix) {
		return nil, fmt.Errorf("azure entra JWT issuer %q does not match expected pattern", iss)
	}

	// Extract the tenant ID from the issuer URL:
	// https://login.microsoftonline.com/{tid}/v2.0
	issTID := strings.TrimPrefix(iss, entraIssuerPrefix)
	issTID = strings.TrimSuffix(issTID, entraIssuerSuffix)

	tid := extractClaim(claims, "tid")

	if a.tenantID != "" && a.tenantID != "common" {
		// Single-tenant mode: issuer must match our configured tenant exactly.
		expectedIss := entraIssuerPrefix + a.tenantID + entraIssuerSuffix
		if iss != expectedIss {
			return nil, fmt.Errorf("azure entra JWT issuer %q does not match expected single-tenant issuer %q", iss, expectedIss)
		}
	} else {
		// Multi-tenant mode: the tid in the issuer URL must match the tid claim.
		if tid != "" && issTID != tid {
			return nil, fmt.Errorf("azure entra JWT issuer tenant %q does not match tid claim %q", issTID, tid)
		}
		// If AllowedTenants is non-empty, the tid must be in the whitelist.
		if len(a.allowedTenants) > 0 {
			effectiveTID := tid
			if effectiveTID == "" {
				effectiveTID = issTID
			}
			if !tenantAllowed(effectiveTID, a.allowedTenants) {
				return nil, fmt.Errorf("azure entra JWT tenant %q is not in the allowed tenants list", effectiveTID)
			}
		}
	}

	// Require the oid claim — it is the stable Azure object ID for the user.
	oid := extractClaim(claims, "oid")
	if oid == "" {
		return nil, fmt.Errorf("azure entra JWT is missing required oid claim")
	}

	// Build metadata from Azure-specific claims.
	metadata := map[string]interface{}{
		"oid": oid,
		"tid": tid,
	}
	for _, key := range []string{"email", "upn", "preferred_username", "name"} {
		if v := extractClaim(claims, key); v != "" {
			metadata[key] = v
		}
	}

	return &AuthResult{
		Authenticated: true,
		Identity: models.Identity{
			Type:      models.PrincipalUser,
			ID:        oid,
			Workspace: "", // downstream services map oid → Scitrera user/workspace
		},
		Method:   "azure_entra",
		Metadata: metadata,
	}, nil
}

// getKeyfunc returns or lazily creates the JWKS keyfunc for the Entra JWKS endpoint.
func (a *AzureEntraAuthenticator) getKeyfunc(ctx context.Context) (keyfunc.Keyfunc, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.kf != nil {
		return a.kf, nil
	}

	refreshCtx, cancel := context.WithCancel(context.Background())
	kf, err := keyfunc.NewDefaultCtx(refreshCtx, []string{entraJWKSURL})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create Azure Entra JWKS keyfunc: %w", err)
	}

	a.kf = kf
	a.cancel = cancel
	return kf, nil
}

// Close cancels the background JWKS refresh goroutine.
func (a *AzureEntraAuthenticator) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.cancel != nil {
		a.cancel()
		a.cancel = nil
		a.kf = nil
	}
}

// extractClaim safely extracts a string value from JWT MapClaims.
// Returns an empty string if the key is absent or the value is not a string.
func extractClaim(claims jwt.MapClaims, key string) string {
	v, ok := claims[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

// tenantAllowed returns true if the given tid is present in the allowed list.
func tenantAllowed(tid string, allowed []string) bool {
	for _, a := range allowed {
		if a == tid {
			return true
		}
	}
	return false
}
