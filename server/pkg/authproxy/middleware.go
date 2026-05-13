package authproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/auth"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/identityheaders"
	"github.com/scitrera/aether/pkg/models"
)

// Trusted header constants injected after successful authentication. These
// are re-exported from pkg/identityheaders so that the auth-proxy and any
// other component minting these headers share a single source of truth for
// the wire format. Downstream services MUST only trust X-Auth-* headers
// observed from the proxy.
const (
	HeaderTenantID        = identityheaders.HeaderTenantID
	HeaderUserID          = identityheaders.HeaderUserID
	HeaderPrincipalType   = identityheaders.HeaderPrincipalType
	HeaderWorkspaceAccess = identityheaders.HeaderWorkspaceAccess
	HeaderScopes          = identityheaders.HeaderScopes
	HeaderAPIKeyID        = identityheaders.HeaderAPIKeyID
	HeaderWorkspaceID     = identityheaders.HeaderWorkspaceID

	HeaderActorType       = identityheaders.HeaderActorType
	HeaderActorID         = identityheaders.HeaderActorID
	HeaderAuthorityMode   = identityheaders.HeaderAuthorityMode
	HeaderGrantID         = identityheaders.HeaderGrantID
	HeaderSubjectType     = identityheaders.HeaderSubjectType
	HeaderSubjectID       = identityheaders.HeaderSubjectID
	HeaderRootSubjectType = identityheaders.HeaderRootSubjectType
	HeaderRootSubjectID   = identityheaders.HeaderRootSubjectID
	HeaderAudienceType    = identityheaders.HeaderAudienceType
	HeaderAudienceID      = identityheaders.HeaderAudienceID
	HeaderMaxAccessLevel  = identityheaders.HeaderMaxAccessLevel
	HeaderWorkspaceScope  = identityheaders.HeaderWorkspaceScope

	HeaderXAetherCallerTopic   = identityheaders.HeaderXAetherCallerTopic
	HeaderXAetherCallerSubject = identityheaders.HeaderXAetherCallerSubject
)

// Client-origin OBO hint headers (read on inbound, stripped before forwarding).
// The proxy treats these as a request — never as trusted claims. Subject /
// scope / ceiling authoritative values come from the grant store after
// resolution, NOT from the client.
const (
	HeaderAetherGrantID       = "X-Aether-Grant-ID"
	HeaderAetherAuthorityMode = "X-Aether-Authority-Mode"
	HeaderAetherSubjectType   = "X-Aether-Subject-Type"
	HeaderAetherSubjectID     = "X-Aether-Subject-ID"
)

// Authority mode values.
const (
	AuthorityModeDirect     = identityheaders.AuthorityModeDirect
	AuthorityModeOnBehalfOf = identityheaders.AuthorityModeOnBehalfOf
)

// defaultWorkspace is used when no workspace can be determined from the request.
const defaultWorkspace = "_default"

// maxBodyReadSize limits how much of the request body we read when
// extracting workspace_id, preventing memory exhaustion from large payloads.
const maxBodyReadSize = 1 << 20 // 1 MB

// ACLEvaluator abstracts the ACL evaluation needed by the auth middleware.
type ACLEvaluator interface {
	EvaluateAccess(ctx context.Context, principal models.Identity, resourceType, resourceID string, requiredLevel int) (*acl.ACLDecision, error)
}

// AuthorityResolver validates a request-time on-behalf-of authority context
// against the authenticated actor and live audience. When nil on the
// middleware, OBO requests are rejected.
type AuthorityResolver interface {
	ResolveAuthority(ctx context.Context, actor models.Identity, req acl.RequestAuthorityContext, audience acl.GrantAudienceContext) (*acl.ResolvedAuthority, error)
}

// AuthMiddleware encapsulates credential validation, ACL evaluation, and
// post-auth identity resolution.
type AuthMiddleware struct {
	authenticator     *auth.CompositeAuthenticator
	evaluator         ACLEvaluator
	resolver          AuthorityResolver
	identityResolver  IdentityResolver
	tenantID          string
	sessionCookieName string // optional; when set, value is fed to auth.CredKeySession
}

// SetSessionCookieName configures which cookie the middleware reads to
// produce the session_token credential. Empty disables session lookup
// (default). Typically called once at startup after the login module is
// wired in.
func (m *AuthMiddleware) SetSessionCookieName(name string) {
	m.sessionCookieName = name
}

// NewAuthMiddleware creates a new AuthMiddleware with the given authenticator,
// ACL evaluator, and tenant identifier. OBO authority resolution is disabled
// and a pass-through identity resolver is used; use NewAuthMiddlewareWithResolver
// or NewAuthMiddlewareFull for richer behaviour.
func NewAuthMiddleware(authenticator *auth.CompositeAuthenticator, evaluator ACLEvaluator, tenantID string) *AuthMiddleware {
	return &AuthMiddleware{
		authenticator:    authenticator,
		evaluator:        evaluator,
		tenantID:         tenantID,
		identityResolver: NoOpResolver{TenantID: tenantID},
	}
}

// NewAuthMiddlewareWithResolver creates a new AuthMiddleware that can validate
// on-behalf-of authority grants supplied via X-Aether-* headers. Identity
// resolution falls back to a pass-through resolver bound to tenantID.
func NewAuthMiddlewareWithResolver(authenticator *auth.CompositeAuthenticator, evaluator ACLEvaluator, resolver AuthorityResolver, tenantID string) *AuthMiddleware {
	return &AuthMiddleware{
		authenticator:    authenticator,
		evaluator:        evaluator,
		resolver:         resolver,
		tenantID:         tenantID,
		identityResolver: NoOpResolver{TenantID: tenantID},
	}
}

// NewAuthMiddlewareFull is the full constructor: it accepts both the OBO
// authority resolver and a post-auth IdentityResolver. Pass nil for either
// to fall back to defaults (no OBO support / pass-through identity).
func NewAuthMiddlewareFull(
	authenticator *auth.CompositeAuthenticator,
	evaluator ACLEvaluator,
	authorityResolver AuthorityResolver,
	identityResolver IdentityResolver,
	tenantID string,
) *AuthMiddleware {
	if identityResolver == nil {
		identityResolver = NoOpResolver{TenantID: tenantID}
	}
	return &AuthMiddleware{
		authenticator:    authenticator,
		evaluator:        evaluator,
		resolver:         authorityResolver,
		identityResolver: identityResolver,
		tenantID:         tenantID,
	}
}

// AuthenticatedAuthority is the resolved on-behalf-of authority envelope for a
// single request. Populated when the request carries a valid authority grant.
// Aliased from pkg/identityheaders so the auth-proxy and other minting
// components share a single type.
type AuthenticatedAuthority = identityheaders.AuthenticatedAuthority

// AuthenticatedRequest contains the results of a successful authentication
// and authorization check, ready for header injection.
type AuthenticatedRequest struct {
	UserID          string
	PrincipalType   string
	WorkspaceAccess int
	Scopes          string
	APIKeyID        string

	// Authority is populated when the request carries a valid on-behalf-of
	// grant. When nil, the request is in direct mode.
	Authority *AuthenticatedAuthority

	// Resolved carries the post-auth IdentityResolver output. Always set on
	// successful Authenticate calls. UserID/PrincipalType above are pre-
	// resolution (raw from the authenticator); Resolved.UserID and
	// Resolved.DefaultTenantID are what header injection prefers.
	Resolved *ResolvedIdentity
}

// Authenticate extracts credentials from the request, validates them against
// the composite authenticator, evaluates workspace ACL, and returns the
// authenticated request data. On failure it writes the appropriate HTTP error
// response and returns a non-nil error.
func (m *AuthMiddleware) Authenticate(w http.ResponseWriter, r *http.Request) (*AuthenticatedRequest, error) {
	// Build request context for debug logging
	clientIP := r.RemoteAddr
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		clientIP = strings.Split(fwd, ",")[0]
	}
	reqLog := logging.Logger.With().
		Str("method", r.Method).
		Str("path", r.URL.Path).
		Str("client_ip", clientIP).
		Logger()

	// Extract credentials from the Authorization header
	credentials := extractCredentials(r)

	// If a session cookie name is configured AND no Authorization-derived
	// credential is present, fall back to the session cookie. Authorization
	// always wins (machine principals carrying API keys/JWTs should not be
	// shadowed by a stale browser cookie on the same connection).
	if m.sessionCookieName != "" && len(credentials) == 0 {
		if c, err := r.Cookie(m.sessionCookieName); err == nil && c.Value != "" {
			credentials[auth.CredKeySession] = c.Value
		}
	}

	if len(credentials) == 0 {
		reqLog.Debug().Msg("auth: no credentials provided")
		writeJSONError(w, http.StatusUnauthorized, "missing credentials", "Authorization header or session cookie required")
		return nil, fmt.Errorf("no credentials provided")
	}

	// Log credential type and token prefix for debugging
	if apiKey, ok := credentials[auth.CredKeyAPIKey]; ok {
		prefix := apiKey
		if len(prefix) > 8 {
			prefix = prefix[:8] + "..."
		}
		reqLog.Debug().Str("auth_type", "api_key").Str("token_prefix", prefix).Msg("auth: attempting API key authentication")
	} else if _, ok := credentials["bearer_token"]; ok {
		reqLog.Debug().Str("auth_type", "oauth_jwt").Msg("auth: attempting OAuth/JWT authentication")
	}

	// Validate credentials
	result, err := m.authenticator.Authenticate(r.Context(), credentials)
	if err != nil {
		reqLog.Warn().Err(err).Msg("auth: authentication failed")
		writeJSONError(w, http.StatusUnauthorized, "authentication failed", "invalid credentials")
		return nil, fmt.Errorf("authentication failed: %w", err)
	}
	if result == nil {
		reqLog.Warn().Msg("auth: no authenticator matched the provided credentials")
		writeJSONError(w, http.StatusUnauthorized, "authentication failed", "no authenticator matched the provided credentials")
		return nil, fmt.Errorf("no authenticator matched")
	}

	// Determine workspace
	workspace := extractWorkspace(r)

	reqLog.Debug().
		Str("user_id", result.Identity.ID).
		Str("principal_type", string(result.Identity.Type)).
		Str("workspace", workspace).
		Msg("auth: credentials validated, evaluating ACL")

	// Enforce API token workspace patterns before ACL evaluation
	if result.Metadata != nil {
		if patterns, ok := result.Metadata["workspace_patterns"].([]string); ok && len(patterns) > 0 {
			if !matchesWorkspacePattern(patterns, workspace) {
				reqLog.Warn().
					Str("user", result.Identity.ID).
					Str("workspace", workspace).
					Strs("patterns", patterns).
					Msg("auth: workspace pattern denied")
				writeJSONError(w, http.StatusForbidden, "access denied", "token not authorized for this workspace")
				return nil, fmt.Errorf("workspace %q not matched by token patterns", workspace)
			}
		}
	}

	// Evaluate ACL
	decision, err := m.evaluator.EvaluateAccess(
		r.Context(),
		result.Identity,
		acl.ResourceTypeWorkspace,
		workspace,
		acl.AccessRead,
	)
	if err != nil {
		reqLog.Error().Err(err).Str("user", result.Identity.ID).Str("workspace", workspace).Msg("auth: ACL evaluation error")
		writeJSONError(w, http.StatusInternalServerError, "authorization error", "failed to evaluate access")
		return nil, fmt.Errorf("ACL evaluation failed: %w", err)
	}
	if decision == nil {
		// No explicit rule matched and no fallback — deny by default
		decision = &acl.ACLDecision{
			Allowed:              false,
			EffectiveAccessLevel: acl.AccessNone,
			Decision:             acl.DecisionDeny,
			Reason:               "no matching ACL rule",
		}
	}
	if decision.Denied() {
		reqLog.Warn().
			Str("user", result.Identity.ID).
			Str("workspace", workspace).
			Str("reason", decision.Reason).
			Int("access_level", decision.EffectiveAccessLevel).
			Msg("auth: ACL denied")
		writeJSONError(w, http.StatusForbidden, "access denied", "insufficient permissions for workspace")
		return nil, fmt.Errorf("access denied: %s", decision.Reason)
	}

	reqLog.Debug().
		Str("user_id", result.Identity.ID).
		Str("workspace", workspace).
		Int("access_level", decision.EffectiveAccessLevel).
		Msg("auth: access granted")

	// Build authenticated request
	authed := &AuthenticatedRequest{
		UserID:          result.Identity.ID,
		PrincipalType:   string(result.Identity.Type),
		WorkspaceAccess: decision.EffectiveAccessLevel,
	}

	// Extract metadata from auth result
	if result.Metadata != nil {
		if scopes, ok := result.Metadata["scopes"].([]string); ok {
			authed.Scopes = strings.Join(scopes, ",")
		}
		if tokenID, ok := result.Metadata["token_id"].(string); ok {
			authed.APIKeyID = tokenID
		}
	}

	// OBO (on-behalf-of) resolution. When the client supplied an
	// X-Aether-Grant-ID, validate it against the grant store. Failures map
	// to 401/403 here so MemoryLayer and other downstreams never see an
	// unvalidated grant.
	if grantID := r.Header.Get(HeaderAetherGrantID); grantID != "" {
		if err := m.resolveAuthority(w, r, authed, result.Identity, workspace, grantID); err != nil {
			return nil, err
		}
	}

	// Post-auth identity resolution. Maps the verified principal + raw
	// claims (typically authenticator metadata) into a ResolvedIdentity that
	// drives header injection. The OSS default is a pass-through resolver
	// bound to the configured static tenant id; the Scitrera platform
	// binary swaps in a multi-tenant resolver via NewAuthMiddlewareFull.
	resolverIn := ResolverInput{
		Identity:  result.Identity,
		Method:    result.Method,
		Claims:    map[string]any(result.Metadata),
		Workspace: workspace,
		Request:   r,
	}
	resolved, err := m.identityResolver.Resolve(r.Context(), resolverIn)
	if err != nil {
		reqLog.Error().Err(err).
			Str("resolver", m.identityResolver.Name()).
			Str("user", result.Identity.ID).
			Msg("auth: identity resolution failed")
		writeJSONError(w, http.StatusInternalServerError, "identity resolution error", "failed to resolve identity")
		return nil, fmt.Errorf("identity resolver %q failed: %w", m.identityResolver.Name(), err)
	}
	if resolved == nil {
		reqLog.Error().Str("resolver", m.identityResolver.Name()).Msg("auth: identity resolver returned nil")
		writeJSONError(w, http.StatusInternalServerError, "identity resolution error", "resolver returned no identity")
		return nil, fmt.Errorf("identity resolver %q returned nil", m.identityResolver.Name())
	}
	if resolved.Reject != nil {
		status := resolved.Reject.Status
		if status == 0 {
			status = http.StatusForbidden
		}
		reqLog.Warn().
			Str("resolver", m.identityResolver.Name()).
			Str("user", result.Identity.ID).
			Str("code", resolved.Reject.Code).
			Str("reason", resolved.Reject.Message).
			Int("status", status).
			Msg("auth: identity resolver rejected request")
		writeJSONError(w, status, "access denied", "identity rejected by resolver")
		return nil, fmt.Errorf("identity resolver %q rejected: %s", m.identityResolver.Name(), resolved.Reject.Code)
	}
	authed.Resolved = resolved

	// Per-grant proxy_path resource scope enforcement. Auth-proxy does not
	// expose a multi-backend dispatch model, so the matcher receives a fixed
	// synthetic backend identifier (identityheaders.AuthProxyDefaultBackend);
	// grants intended for the auth-proxy direct path should target that name
	// or use a "*" backend glob. Direct-mode requests (no Authority) skip
	// this check, preserving the legacy blanket-allow behaviour.
	if authed.Authority != nil {
		patterns := authed.Authority.ResourceScope[identityheaders.ResourceTypeProxyPath]
		if !identityheaders.MatchProxyPath(patterns, identityheaders.AuthProxyDefaultBackend, r.Method, r.URL.Path) {
			reqLog.Warn().
				Str("user", result.Identity.ID).
				Str("grant_id", authed.Authority.GrantID).
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Strs("patterns", patterns).
				Msg("auth: proxy_path scope denied")
			writeJSONError(w, http.StatusForbidden, "access denied", "proxy_path_scope_denied: request path not in grant scope")
			return nil, fmt.Errorf("proxy_path_scope_denied: %s %s not in grant scope", r.Method, r.URL.Path)
		}
	}

	return authed, nil
}

// resolveAuthority validates an OBO grant carried on the inbound request and
// populates authed.Authority on success. On failure it writes the appropriate
// HTTP error response and returns a non-nil error.
func (m *AuthMiddleware) resolveAuthority(
	w http.ResponseWriter,
	r *http.Request,
	authed *AuthenticatedRequest,
	actor models.Identity,
	workspace string,
	grantID string,
) error {
	if m.resolver == nil {
		writeJSONError(w, http.StatusNotImplemented, "on_behalf_of not supported", "this auth-proxy was not configured with an authority resolver")
		return fmt.Errorf("obo: no resolver configured")
	}

	subjectTypeStr := r.Header.Get(HeaderAetherSubjectType)
	subjectIDStr := r.Header.Get(HeaderAetherSubjectID)
	if subjectTypeStr == "" || subjectIDStr == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid authority context", "X-Aether-Subject-Type and X-Aether-Subject-ID are required when X-Aether-Grant-ID is set")
		return fmt.Errorf("obo: missing subject headers")
	}

	subject, err := buildSubjectIdentity(subjectTypeStr, subjectIDStr)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid authority subject", err.Error())
		return fmt.Errorf("obo: invalid subject: %w", err)
	}

	resolved, err := m.resolver.ResolveAuthority(
		r.Context(),
		actor,
		acl.RequestAuthorityContext{
			Mode:    AuthorityModeOnBehalfOf,
			Subject: subject,
			GrantID: grantID,
		},
		acl.GrantAudienceContext{Actor: actor},
	)
	if err != nil {
		status := mapAuthorityErrorToStatus(err)
		logging.Logger.Warn().
			Err(err).
			Str("grant_id", grantID).
			Str("actor", actor.CanonicalPrincipalID()).
			Str("subject", subject.CanonicalPrincipalID()).
			Msg("auth: OBO authority resolution failed")
		writeJSONError(w, status, "authority validation failed", err.Error())
		return fmt.Errorf("obo: resolve failed: %w", err)
	}

	// Enforce grant workspace scope. An empty WorkspaceScope means "any";
	// otherwise the requested workspace must match an entry (exact or glob).
	if !workspaceInGrantScope(workspace, resolved.Grant.WorkspaceScope) {
		logging.Logger.Warn().
			Str("grant_id", grantID).
			Str("workspace", workspace).
			Strs("allowed", resolved.Grant.WorkspaceScope).
			Msg("auth: OBO workspace out of grant scope")
		writeJSONError(w, http.StatusForbidden, "workspace not in grant scope", fmt.Sprintf("grant %s does not permit workspace %q", grantID, workspace))
		return fmt.Errorf("obo: workspace %q not in grant scope", workspace)
	}

	authed.Authority = identityheaders.AuthorityFromResolved(resolved)

	logging.Logger.Debug().
		Str("grant_id", grantID).
		Str("actor", authed.Authority.ActorID).
		Str("subject", authed.Authority.SubjectID).
		Int("max_access_level", authed.Authority.MaxAccessLevel).
		Msg("auth: OBO authority resolved")

	return nil
}

// mapAuthorityErrorToStatus selects an HTTP status code for authority
// resolution failures.
func mapAuthorityErrorToStatus(err error) int {
	switch {
	case errors.Is(err, acl.ErrInvalidAuthorityContext):
		return http.StatusBadRequest
	case errors.Is(err, acl.ErrAuthorityGrantNotFound),
		errors.Is(err, acl.ErrAuthorityGrantExpired),
		errors.Is(err, acl.ErrAuthorityGrantRevoked):
		return http.StatusUnauthorized
	case errors.Is(err, acl.ErrAuthorityGrantDelegateMismatch),
		errors.Is(err, acl.ErrAuthorityGrantSubjectMismatch),
		errors.Is(err, acl.ErrAuthorityGrantAudienceMismatch):
		return http.StatusForbidden
	default:
		return http.StatusForbidden
	}
}

// buildSubjectIdentity reconstructs a models.Identity from the caller-supplied
// subject type/id headers. For User principals the id is treated as a raw user
// id; for structured principals it is parsed as a canonical identity string
// (e.g. "sv::foo::bar"). The resulting Identity's PrincipalRef() must match the
// grant's persisted subject for ResolveAuthority to succeed.
func buildSubjectIdentity(principalType, principalID string) (models.Identity, error) {
	if principalID == "" {
		return models.Identity{}, fmt.Errorf("subject id is empty")
	}
	pt, err := parsePrincipalType(principalType)
	if err != nil {
		return models.Identity{}, err
	}
	switch pt {
	case models.PrincipalUser:
		return models.Identity{Type: models.PrincipalUser, ID: principalID}, nil
	case models.PrincipalAgent, models.PrincipalTask, models.PrincipalService, models.PrincipalBridge:
		// Structured principals are serialised as canonical identity strings.
		parsed, perr := models.ParseIdentity(principalID)
		if perr != nil {
			return models.Identity{}, fmt.Errorf("invalid %s identity %q: %w", principalType, principalID, perr)
		}
		return parsed, nil
	default:
		return models.Identity{}, fmt.Errorf("unsupported subject principal type: %s", principalType)
	}
}

// parsePrincipalType maps the caller's subject-type string to a models
// constant. Accepts lowercase (as stored in grants) or TitleCase (as used
// in Aether SDK/proto).
func parsePrincipalType(s string) (models.PrincipalType, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "user":
		return models.PrincipalUser, nil
	case "agent":
		return models.PrincipalAgent, nil
	case "task":
		return models.PrincipalTask, nil
	case "service":
		return models.PrincipalService, nil
	case "bridge":
		return models.PrincipalBridge, nil
	default:
		return "", fmt.Errorf("unknown principal type: %q", s)
	}
}

// workspaceInGrantScope reports whether the requested workspace is allowed by
// the grant's workspace scope. Empty scope means "any workspace"; otherwise
// entries are matched exactly or via filepath.Match glob semantics, with "*"
// as a shorthand for any workspace.
func workspaceInGrantScope(workspace string, scope []string) bool {
	if len(scope) == 0 {
		return true
	}
	for _, allowed := range scope {
		if allowed == "*" || allowed == workspace {
			return true
		}
		if matched, err := filepath.Match(allowed, workspace); err == nil && matched {
			return true
		}
	}
	return false
}

// stripInboundIdentityHeaders removes all X-Auth-* and X-Aether-* headers
// from an inbound request to prevent clients from spoofing identity headers.
// X-Aether-* headers are read by Authenticate() before this is called (via
// InjectHeaders), so stripping them here prevents forwarding raw client hints
// to the backend.
func stripInboundIdentityHeaders(r *http.Request) {
	identityheaders.StripInbound(r.Header)
}

// authedToIdentity adapts the auth-proxy's AuthenticatedRequest into the
// shape consumed by pkg/identityheaders.Mint.
//
// In the auth-proxy direct path the "caller" is the authenticated principal
// itself (no proxy hop yet). CallerTopic is derived from the principal type
// and user ID. CallerSubject is the OBO grant subject's ID in on_behalf_of
// mode (empty for direct mode), mirroring the sidecar's post-resolution stamp.
//
// When a ResolvedIdentity is present, its UserID and PrincipalType take
// precedence over the raw authenticator-derived values. This lets resolvers
// canonicalise identities (e.g. map an Azure oid to an email).
func authedToIdentity(authed *AuthenticatedRequest) identityheaders.Identity {
	userID := authed.UserID
	principalType := authed.PrincipalType
	if authed.Resolved != nil {
		if authed.Resolved.UserID != "" {
			userID = authed.Resolved.UserID
		}
		if authed.Resolved.PrincipalType != "" {
			principalType = authed.Resolved.PrincipalType
		}
	}
	ident := identityheaders.Identity{
		UserID:          userID,
		PrincipalType:   principalType,
		WorkspaceAccess: authed.WorkspaceAccess,
		Scopes:          authed.Scopes,
		APIKeyID:        authed.APIKeyID,
		Authority:       authed.Authority,
		// In auth-proxy the caller IS the authenticated principal.
		CallerTopic: userID,
	}
	if authed.Authority != nil && authed.Authority.SubjectID != "" {
		ident.CallerSubject = authed.Authority.SubjectID
	}
	return ident
}

// effectiveTenantID returns the resolver-supplied tenant id when present,
// otherwise falls back to the middleware's static configured tenant.
func (m *AuthMiddleware) effectiveTenantID(authed *AuthenticatedRequest) string {
	if authed != nil && authed.Resolved != nil && authed.Resolved.DefaultTenantID != "" {
		return authed.Resolved.DefaultTenantID
	}
	return m.tenantID
}

// applyExtraHeaders writes resolver-supplied headers (e.g. X-Scitrera-User)
// onto h. Reserved X-Auth-* / X-Aether-* names are silently dropped to
// preserve the integrity of the canonical trusted header set.
func applyExtraHeaders(h http.Header, extra map[string]string) {
	for k, v := range extra {
		if isReservedHeader(k) {
			continue
		}
		h.Set(k, v)
	}
}

// isReservedHeader reports whether k collides with the canonical
// trusted-header namespace minted by pkg/identityheaders.
func isReservedHeader(k string) bool {
	lower := strings.ToLower(k)
	return strings.HasPrefix(lower, "x-auth-") || strings.HasPrefix(lower, "x-aether-")
}

// InjectHeaders strips any existing X-Auth-* and X-Aether-* headers, sets the
// trusted X-Auth-* headers on the request, and removes the Authorization
// header before forwarding to the backend.
//
// OBO mode: when authed.Authority is non-nil, HeaderUserID and
// HeaderPrincipalType are set to the grant subject (for backward-compat with
// downstreams that read those headers as "effective user"). Actor identity and
// authority-mode headers are always emitted so downstreams can distinguish
// direct from on-behalf-of requests.
//
// Extra headers from the IdentityResolver (e.g. X-Scitrera-User) are written
// after the canonical X-Auth-* set; they cannot collide with the reserved
// X-Auth-* / X-Aether-* namespace.
func (m *AuthMiddleware) InjectHeaders(r *http.Request, authed *AuthenticatedRequest) {
	stripInboundIdentityHeaders(r)
	identityheaders.MintInto(r.Header, m.effectiveTenantID(authed), authedToIdentity(authed))
	if authed != nil && authed.Resolved != nil {
		applyExtraHeaders(r.Header, authed.Resolved.ExtraHeaders)
	}
	// Strip the original Authorization header before forwarding
	r.Header.Del("Authorization")
}

// SetResponseHeaders writes the trusted X-Auth-* headers onto an HTTP
// response. Used in verify mode to pass identity data back to nginx/envoy.
// Mirrors InjectHeaders OBO semantics: subject overrides UserID/PrincipalType
// in OBO mode; actor and authority-mode headers are always emitted.
//
// Extra headers from the IdentityResolver are also emitted so verify-mode
// gateways (nginx auth_request, Envoy ext_authz) can forward them.
func (m *AuthMiddleware) SetResponseHeaders(w http.ResponseWriter, authed *AuthenticatedRequest) {
	identityheaders.MintInto(w.Header(), m.effectiveTenantID(authed), authedToIdentity(authed))
	if authed != nil && authed.Resolved != nil {
		applyExtraHeaders(w.Header(), authed.Resolved.ExtraHeaders)
	}
}

// extractCredentials builds the credentials map expected by the
// composite authenticator from the HTTP Authorization header.
// It routes the token to the appropriate authenticator based on format:
// JWTs (3 dot-separated segments) go to OAuth, others go to API key auth.
func extractCredentials(r *http.Request) map[string]string {
	credentials := make(map[string]string)

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return credentials
	}

	var token string
	if strings.HasPrefix(authHeader, "Bearer ") {
		token = authHeader[len("Bearer "):]
	} else if strings.HasPrefix(authHeader, "bearer ") {
		token = authHeader[len("bearer "):]
	}

	if token != "" {
		if strings.Count(token, ".") == 2 {
			// JWT format (header.payload.signature) — route to OAuth
			credentials["bearer_token"] = token
		} else {
			// Opaque token — route to API key authenticator
			credentials[auth.CredKeyAPIKey] = token
		}
	}

	return credentials
}

// extractWorkspace determines the workspace identifier from the request,
// checking in order: X-Workspace-ID header, workspace_id query param,
// JSON body workspace_id field. Falls back to defaultWorkspace.
func extractWorkspace(r *http.Request) string {
	// 1. Check header
	if ws := r.Header.Get(HeaderWorkspaceID); ws != "" {
		return ws
	}

	// 2. Check query parameter
	if ws := r.URL.Query().Get("workspace_id"); ws != "" {
		return ws
	}

	// 3. Check JSON body for POST requests
	if r.Method == http.MethodPost && r.Body != nil {
		ws := extractWorkspaceFromBody(r)
		if ws != "" {
			return ws
		}
	}

	return defaultWorkspace
}

// extractWorkspaceFromBody reads up to maxBodyReadSize bytes of the JSON
// request body looking for a workspace_id field, then resets the body so
// it can be read again by downstream handlers.
func extractWorkspaceFromBody(r *http.Request) string {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyReadSize))
	if err != nil {
		return ""
	}
	// Replace the body so it can be read again downstream
	r.Body = io.NopCloser(bytes.NewReader(body))

	// Only attempt parse if the body looks like JSON
	if len(body) == 0 || body[0] != '{' {
		return ""
	}

	var parsed struct {
		WorkspaceID string `json:"workspace_id"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return ""
	}

	return parsed.WorkspaceID
}

// matchesWorkspacePattern checks if a workspace matches any of the given
// glob patterns using filepath.Match semantics.
func matchesWorkspacePattern(patterns []string, workspace string) bool {
	for _, pattern := range patterns {
		matched, err := filepath.Match(pattern, workspace)
		if err != nil {
			continue
		}
		if matched {
			return true
		}
	}
	return false
}

// writeJSONError writes a JSON-formatted error response with the correct
// Content-Type header.
func writeJSONError(w http.ResponseWriter, status int, errMsg, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"error":%q,"detail":%q}`, errMsg, detail)
}

// contextKey is a private type for context value keys in this package.
type contextKey int

const authedRequestKey contextKey = iota

// ContextWithAuth stores an AuthenticatedRequest in the context.
func ContextWithAuth(ctx context.Context, authed *AuthenticatedRequest) context.Context {
	return context.WithValue(ctx, authedRequestKey, authed)
}

// AuthFromContext retrieves an AuthenticatedRequest from the context, if present.
func AuthFromContext(ctx context.Context) (*AuthenticatedRequest, bool) {
	authed, ok := ctx.Value(authedRequestKey).(*AuthenticatedRequest)
	return authed, ok
}
