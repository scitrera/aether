// Package authproxy exposes the public surface used by external auth-proxy
// builds (e.g. the Scitrera multi-tenant variant) to plug their own identity
// resolution logic into the OSS auth-proxy middleware.
//
// IdentityResolver runs after credential validation (the composite
// Authenticator) and after workspace ACL evaluation. It maps a verified
// principal + raw claims into a ResolvedIdentity that drives header injection.
//
// External binaries import this package; they MUST NOT depend on
// github.com/scitrera/aether/internal/...
package authproxy

import (
	"context"
	"net/http"

	"github.com/scitrera/aether/pkg/models"
)

// TenantInfo carries optional per-tenant metadata that callers may surface to
// downstream services (e.g. the frontend's tenant picker).
type TenantInfo struct {
	ID               string
	Name             string
	Logo             string
	DefaultWorkspace string
}

// Rejection signals that a request must be denied at the resolver stage. The
// middleware translates a non-nil Reject into an HTTP error response.
type Rejection struct {
	// Status is the HTTP status code to return (typically 401 or 403).
	Status int
	// Code is a short machine-readable token logged with the rejection.
	Code string
	// Message is the human-readable reason. It is logged but NOT sent verbatim
	// to the client — clients see a generic "access denied" body.
	Message string
}

// ResolverInput is the data the middleware hands to a resolver. Claims is the
// raw provider-specific claims map (oid/tid/email/hd/...); Method names the
// authenticator that produced the verified Identity.
type ResolverInput struct {
	// Identity is the verified principal returned by the composite authenticator.
	Identity models.Identity
	// Method is the authenticator name (e.g. "azure_entra", "oauth", "api_key",
	// "task_token", "session"). Resolvers use this to skip claim-based checks
	// for non-user methods.
	Method string
	// Claims carries authenticator-specific metadata. For OAuth/Entra this is
	// the verified JWT claim set; for sessions it is the persisted session
	// blob; for api_key/task_token it is typically empty.
	Claims map[string]any
	// Workspace is the requested workspace (empty when none was provided).
	Workspace string
	// Request is the inbound HTTP request — useful for client IP, UA, or
	// reading additional cookies. Resolvers MUST NOT consume the body.
	Request *http.Request
}

// ResolvedIdentity is what an IdentityResolver returns. The middleware mints
// the canonical X-Auth-* headers (via pkg/identityheaders) using
// DefaultTenantID + UserID + PrincipalType, and additionally writes any
// ExtraHeaders the resolver supplies (e.g. Scitrera's X-Scitrera-*).
type ResolvedIdentity struct {
	// UserID is the canonical user identifier downstream services should use.
	// For a Scitrera multi-tenant deployment this is typically the email; for
	// a single-tenant OSS deployment it is whatever the JWT "sub" / Entra
	// "oid" supplied by the authenticator.
	UserID string
	// DisplayName is an optional human-readable label.
	DisplayName string
	// PrincipalType mirrors models.PrincipalType but is a plain string for
	// header emission ("User", "Service", "Agent", ...).
	PrincipalType string
	// DefaultTenantID is the tenant emitted as X-Auth-Tenant-ID.
	DefaultTenantID string
	// TenantIDs is the full set of tenants the principal can act in (single-
	// element for OSS-default; multi-element for Scitrera multi-tenant). If
	// non-empty it is emitted as X-Scitrera-Tenants by resolvers that opt in
	// via ExtraHeaders.
	TenantIDs []string
	// Tenants carries richer per-tenant metadata. Optional; resolvers that do
	// not need it leave it nil.
	Tenants []TenantInfo
	// ExtraHeaders are written verbatim onto the proxied/verify request after
	// the canonical X-Auth-* set. Use this for resolver-specific names like
	// X-Scitrera-User, X-Scitrera-Default-Tenant, X-Scitrera-Tenants.
	ExtraHeaders map[string]string
	// Reject, when non-nil, instructs the middleware to deny the request.
	// All other fields are ignored.
	Reject *Rejection
}

// IdentityResolver maps an authenticator-verified principal + raw claims into
// a ResolvedIdentity. Implementations MUST be safe for concurrent use.
type IdentityResolver interface {
	// Name returns the resolver name for logs (e.g. "single_tenant",
	// "scitrera_mt").
	Name() string
	// Resolve returns the resolved identity, or a non-nil error for transient
	// failures (DB unreachable, etc.). Permanent denials should be surfaced
	// via ResolvedIdentity.Reject so the middleware can produce a clean HTTP
	// error response without 500-level noise.
	Resolve(ctx context.Context, in ResolverInput) (*ResolvedIdentity, error)
}

// NoOpResolver is the legacy/default resolver. It echoes the verified identity
// back with the configured static tenant id and does no rule evaluation. The
// OSS auth-proxy uses NewSingleTenantResolver (with optional rules) by
// default; NoOpResolver is exposed so embedders can fall back to legacy
// behavior with one line.
type NoOpResolver struct {
	// TenantID is emitted as DefaultTenantID and as the sole TenantIDs entry.
	TenantID string
}

// Name implements IdentityResolver.
func (NoOpResolver) Name() string { return "noop" }

// Resolve implements IdentityResolver.
func (n NoOpResolver) Resolve(_ context.Context, in ResolverInput) (*ResolvedIdentity, error) {
	tenants := []string(nil)
	if n.TenantID != "" {
		tenants = []string{n.TenantID}
	}
	return &ResolvedIdentity{
		UserID:          in.Identity.ID,
		PrincipalType:   string(in.Identity.Type),
		DefaultTenantID: n.TenantID,
		TenantIDs:       tenants,
	}, nil
}
