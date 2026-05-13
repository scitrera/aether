package login

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// ProviderConfig describes one configured OIDC identity provider.
//
// Discovery is performed once at construction; the resulting Provider holds
// the cached JWKS keyfunc and endpoint URLs for the auth-code exchange.
type ProviderConfig struct {
	// Name is the URL-path-safe provider identifier (e.g. "azure", "google").
	// Login routes are mounted at /auth/login/<name> and /auth/callback/<name>.
	Name string
	// IssuerURL is the OIDC discovery URL base. For Azure AD multi-tenant use
	// "https://login.microsoftonline.com/organizations/v2.0"; for Google use
	// "https://accounts.google.com"; for a custom OIDC IdP use its issuer.
	IssuerURL string
	// ClientID and ClientSecret come from the provider's app registration.
	ClientID     string
	ClientSecret string
	// RedirectURL is the absolute URL the IdP will redirect to after login.
	// Must match the provider's registered redirect URI exactly.
	RedirectURL string
	// Scopes are appended to the implicit ["openid"] set. Common defaults are
	// "email", "profile". Pass nil to use ["email", "profile"].
	Scopes []string
	// AllowedTenantIDs is an optional whitelist of Azure tids enforced AFTER
	// ID token verification (defence in depth on top of any AuthRules).
	// Empty means accept any tid.
	AllowedTenantIDs []string
}

// Provider couples a ProviderConfig with the live oauth2 + oidc verifier
// objects required to drive a login flow.
type Provider struct {
	Config   ProviderConfig
	OAuth    *oauth2.Config
	Verifier *oidc.IDTokenVerifier
	provider *oidc.Provider
}

// NewProvider performs OIDC discovery for cfg and returns a usable Provider.
// Returns an error if discovery fails (typo'd issuer, network down at boot).
func NewProvider(ctx context.Context, cfg ProviderConfig) (*Provider, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("provider name is required")
	}
	if cfg.IssuerURL == "" {
		return nil, fmt.Errorf("provider %q: issuer URL is required", cfg.Name)
	}
	if cfg.ClientID == "" {
		return nil, fmt.Errorf("provider %q: client id is required", cfg.Name)
	}
	if cfg.RedirectURL == "" {
		return nil, fmt.Errorf("provider %q: redirect URL is required", cfg.Name)
	}

	oidcProv, err := oidc.NewProvider(ctx, cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("provider %q: discovery failed: %w", cfg.Name, err)
	}

	scopes := append([]string{oidc.ScopeOpenID}, defaultScopes(cfg.Scopes)...)

	o2 := &oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		Endpoint:     oidcProv.Endpoint(),
		RedirectURL:  cfg.RedirectURL,
		Scopes:       scopes,
	}

	verifier := oidcProv.Verifier(&oidc.Config{ClientID: cfg.ClientID})

	return &Provider{
		Config:   cfg,
		OAuth:    o2,
		Verifier: verifier,
		provider: oidcProv,
	}, nil
}

func defaultScopes(scopes []string) []string {
	if len(scopes) == 0 {
		return []string{"email", "profile"}
	}
	return scopes
}

// VerifyCallback exchanges the auth code for tokens and verifies the
// returned id_token. On success it returns the verified claims map and the
// canonical user id (sub). The caller is expected to enforce additional
// per-provider invariants (Azure tid whitelist, hd domain check, etc.) via
// the IdentityResolver layer.
func (p *Provider) VerifyCallback(ctx context.Context, code string) (string, map[string]any, error) {
	tok, err := p.OAuth.Exchange(ctx, code)
	if err != nil {
		return "", nil, fmt.Errorf("token exchange: %w", err)
	}
	rawIDToken, ok := tok.Extra("id_token").(string)
	if !ok || rawIDToken == "" {
		return "", nil, fmt.Errorf("provider response missing id_token")
	}
	idTok, err := p.Verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return "", nil, fmt.Errorf("id_token verify: %w", err)
	}

	var claims map[string]any
	if err := idTok.Claims(&claims); err != nil {
		return "", nil, fmt.Errorf("id_token claims: %w", err)
	}

	// Azure-specific: enforce AllowedTenantIDs (defence in depth — also checked
	// by AzureEntraAuthenticator when bearer-token auth is used).
	if len(p.Config.AllowedTenantIDs) > 0 {
		tid := stringClaim(claims, "tid")
		if tid == "" {
			return "", nil, fmt.Errorf("token missing tid claim; provider %q requires tenant whitelist", p.Config.Name)
		}
		if !containsString(p.Config.AllowedTenantIDs, tid) {
			return "", nil, fmt.Errorf("tenant %q is not in the allowed list", tid)
		}
	}

	// idTok.Subject is also returned for convenience; many resolvers prefer
	// "email" but Azure tokens use "oid" — leaving choice to the resolver.
	return idTok.Subject, claims, nil
}

func stringClaim(claims map[string]any, key string) string {
	if v, ok := claims[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// Registry is a name → Provider map populated at startup from environment
// variables. Lookups are O(1) per request.
type Registry struct {
	providers map[string]*Provider
}

// NewRegistry returns an empty Registry; use Register to add providers.
func NewRegistry() *Registry { return &Registry{providers: map[string]*Provider{}} }

// Register adds p to the registry. Re-registering an existing name overwrites
// the previous provider — callers should treat names as unique.
func (r *Registry) Register(p *Provider) {
	if p == nil {
		return
	}
	r.providers[p.Config.Name] = p
}

// Lookup returns the provider with the given name, or nil if absent.
func (r *Registry) Lookup(name string) *Provider {
	if r == nil {
		return nil
	}
	return r.providers[name]
}

// Names returns the registered provider names in insertion order is not
// guaranteed; callers should sort if a stable list is needed.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.providers))
	for n := range r.providers {
		names = append(names, n)
	}
	return names
}

// String returns a debug-friendly listing of the registered providers.
func (r *Registry) String() string {
	if r == nil || len(r.providers) == 0 {
		return "login.Registry{}"
	}
	return "login.Registry[" + strings.Join(r.Names(), ",") + "]"
}

// claimsAsJSON returns a deterministic JSON encoding of claims for logging.
// Logging-only helper; never used to produce header values.
func claimsAsJSON(claims map[string]any) string {
	b, err := json.Marshal(claims)
	if err != nil {
		return "{}"
	}
	return string(b)
}
