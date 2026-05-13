// Package identityheaders mints the canonical X-Auth-* trusted header set
// stamped on outbound requests after authoritative identity is established.
//
// The auth-proxy (and any future sidecar that fronts an HTTP backend) shares
// this package so the wire format is identical across components.
package identityheaders

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/pkg/models"
)

// Trusted header constants set on the proxied request after authoritative
// identity is established. Downstream services MUST only trust X-Auth-*
// headers observed from a known proxy.
const (
	HeaderTenantID        = "X-Auth-Tenant-ID"
	HeaderUserID          = "X-Auth-User-ID"
	HeaderPrincipalType   = "X-Auth-Principal-Type"
	HeaderWorkspaceAccess = "X-Auth-Workspace-Access"
	HeaderScopes          = "X-Auth-Scopes"
	HeaderAPIKeyID        = "X-Auth-API-Key-ID"
	HeaderWorkspaceID     = "X-Workspace-ID"

	// OBO (on-behalf-of) trusted headers. Actor / authority-mode are always
	// set so downstreams can distinguish direct calls from on_behalf_of;
	// grant-scoped headers are populated only in on_behalf_of mode.
	HeaderActorType       = "X-Auth-Actor-Type"
	HeaderActorID         = "X-Auth-Actor-ID"
	HeaderAuthorityMode   = "X-Auth-Authority-Mode"
	HeaderGrantID         = "X-Auth-Grant-ID"
	HeaderSubjectType     = "X-Auth-Subject-Type"
	HeaderSubjectID       = "X-Auth-Subject-ID"
	HeaderRootSubjectType = "X-Auth-Root-Subject-Type"
	HeaderRootSubjectID   = "X-Auth-Root-Subject-ID"
	HeaderAudienceType    = "X-Auth-Audience-Type"
	HeaderAudienceID      = "X-Auth-Audience-ID"
	HeaderMaxAccessLevel  = "X-Auth-Max-Access-Level"
	HeaderWorkspaceScope  = "X-Auth-Workspace-Scope"

	// Caller identity headers — server-stamped by the gateway before the
	// ProxyHttpRequest envelope is forwarded to the terminator sidecar.
	// These are always stripped on inbound (StripInbound) to prevent spoofing.
	// CallerTopic is the sender's principal topic (e.g. "ag.ws.impl.spec").
	// CallerSubject is the OBO grant subject's topic when authority_mode=on_behalf_of;
	// empty in direct mode.
	HeaderXAetherCallerTopic   = "X-Aether-Caller-Topic"
	HeaderXAetherCallerSubject = "X-Aether-Caller-Subject"
)

// Authority mode values.
const (
	AuthorityModeDirect     = "direct"
	AuthorityModeOnBehalfOf = "on_behalf_of"
)

// AuthenticatedAuthority is the resolved on-behalf-of authority envelope for
// a single request. Populated when the request carries a valid authority
// grant. ActorType/ActorID identify the authenticated principal making the
// call; Subject* identifies the principal whose authority is being borrowed.
type AuthenticatedAuthority struct {
	ActorType       string
	ActorID         string
	GrantID         string
	SubjectType     string
	SubjectID       string
	RootSubjectType string
	RootSubjectID   string
	AudienceType    string
	AudienceID      string
	MaxAccessLevel  int
	WorkspaceScope  []string
	// ResourceScope mirrors the grant's resource_scope map. Per-resource
	// matchers (e.g. proxy_path, tunnel_target) read patterns out of this
	// map; an absent or "*" entry means blanket allow for that resource type.
	ResourceScope map[string][]string
}

// Identity holds the minimal data needed to mint headers in direct mode.
// In OBO mode, the Authority overrides the per-user backward-compat fields
// (UserID, PrincipalType) with the grant's subject.
type Identity struct {
	UserID          string
	PrincipalType   string
	WorkspaceAccess int
	Scopes          string
	APIKeyID        string

	// Authority is populated when the request carries a valid on-behalf-of
	// grant. When nil, the request is in direct mode.
	Authority *AuthenticatedAuthority

	// CallerTopic is the server-stamped topic of the principal that originated
	// the proxy request (e.g. "ag.ws.impl.spec"). Set by the terminator sidecar
	// from the gateway-stamped x-aether-actor-topic envelope header.
	// Empty in non-proxy contexts.
	CallerTopic string

	// CallerSubject is the OBO grant subject's canonical topic string when
	// authority_mode=on_behalf_of. Empty in direct mode and non-proxy contexts.
	CallerSubject string
}

// Mint produces the canonical X-Auth-* header set for the given tenant and
// authenticated identity. In OBO mode the backward-compat headers (UserID,
// PrincipalType) carry the grant subject; actor headers carry the
// authenticated principal.
func Mint(_ context.Context, tenantID string, ident Identity) http.Header {
	h := http.Header{}
	MintInto(h, tenantID, ident)
	return h
}

// MintInto writes the canonical X-Auth-* header set onto h. Existing entries
// for these header names are overwritten.
func MintInto(h http.Header, tenantID string, ident Identity) {
	h.Set(HeaderTenantID, tenantID)
	h.Set(HeaderWorkspaceAccess, strconv.Itoa(ident.WorkspaceAccess))

	if ident.Scopes != "" {
		h.Set(HeaderScopes, ident.Scopes)
	}
	if ident.APIKeyID != "" {
		h.Set(HeaderAPIKeyID, ident.APIKeyID)
	}

	// Caller headers — stamped when the proxy sidecar or auth-proxy supplies
	// the originating principal topic. These are always stripped on inbound,
	// so only trusted minting paths can set them.
	if ident.CallerTopic != "" {
		h.Set(HeaderXAetherCallerTopic, ident.CallerTopic)
	}
	if ident.CallerSubject != "" {
		h.Set(HeaderXAetherCallerSubject, ident.CallerSubject)
	}

	if ident.Authority == nil {
		// Direct mode: actor == the authenticated principal.
		h.Set(HeaderUserID, ident.UserID)
		h.Set(HeaderPrincipalType, ident.PrincipalType)
		h.Set(HeaderActorType, ident.PrincipalType)
		h.Set(HeaderActorID, ident.UserID)
		h.Set(HeaderAuthorityMode, AuthorityModeDirect)
		return
	}

	// OBO mode: backward-compat headers carry the subject; actor headers
	// carry the authenticated service/agent identity.
	a := ident.Authority
	h.Set(HeaderUserID, a.SubjectID)
	h.Set(HeaderPrincipalType, a.SubjectType)
	h.Set(HeaderActorType, a.ActorType)
	h.Set(HeaderActorID, a.ActorID)
	h.Set(HeaderAuthorityMode, AuthorityModeOnBehalfOf)

	h.Set(HeaderGrantID, a.GrantID)
	h.Set(HeaderSubjectType, a.SubjectType)
	h.Set(HeaderSubjectID, a.SubjectID)
	if a.RootSubjectType != "" {
		h.Set(HeaderRootSubjectType, a.RootSubjectType)
	}
	if a.RootSubjectID != "" {
		h.Set(HeaderRootSubjectID, a.RootSubjectID)
	}
	h.Set(HeaderAudienceType, a.AudienceType)
	h.Set(HeaderAudienceID, a.AudienceID)
	h.Set(HeaderMaxAccessLevel, strconv.Itoa(a.MaxAccessLevel))
	if len(a.WorkspaceScope) > 0 {
		h.Set(HeaderWorkspaceScope, strings.Join(a.WorkspaceScope, ","))
	}
}

// AuthorityResolver validates a request-time on-behalf-of authority context
// against the authenticated actor and live audience.
type AuthorityResolver interface {
	ResolveAuthority(ctx context.Context, actor models.Identity, req acl.RequestAuthorityContext, audience acl.GrantAudienceContext) (*acl.ResolvedAuthority, error)
}

// AuthorizationContext is the message-borne OBO surface decoded from
// `aetherpb.AuthorizationContext`. Callers translate the proto into this
// shape so this package does not depend on the api/proto module directly.
type AuthorizationContext struct {
	Mode    string
	Subject models.Identity
	GrantID string
}

// IsZero reports whether the context carries no OBO data (i.e., direct mode).
func (a AuthorizationContext) IsZero() bool {
	return a.Mode == "" && a.GrantID == "" && a.Subject.ID == "" && a.Subject.Type == ""
}

// ResolveAndMint resolves an OBO grant carried inside a SendMessage-style
// AuthorizationContext, then produces the canonical X-Auth-* header set.
//
// When authCtx is nil or in direct mode, headers are minted from the actor
// identity alone and the returned authority is nil. When authCtx requests
// on_behalf_of mode, the resolver validates the grant against the actor and
// audience; on success, the resolved authority is folded into the headers.
//
// The actor and audience parameters supply the live request context the
// resolver needs (session id, associated task id, etc.). The returned
// AuthenticatedAuthority is populated only on a successful OBO resolution.
func ResolveAndMint(
	ctx context.Context,
	resolver AuthorityResolver,
	tenantID string,
	actor Identity,
	actorIdentity models.Identity,
	authCtx *AuthorizationContext,
	audience acl.GrantAudienceContext,
) (http.Header, *AuthenticatedAuthority, error) {
	if authCtx == nil || authCtx.IsZero() || authCtx.Mode == "" || authCtx.Mode == AuthorityModeDirect {
		// Direct mode: no resolver call.
		if authCtx != nil && (!authCtx.Subject.PrincipalRef().IsZero() || authCtx.GrantID != "") {
			return nil, nil, fmt.Errorf("direct authorization context must not include subject or grant")
		}
		return Mint(ctx, tenantID, actor), nil, nil
	}

	if authCtx.Mode != AuthorityModeOnBehalfOf {
		return nil, nil, fmt.Errorf("unsupported authority mode %q", authCtx.Mode)
	}

	if resolver == nil {
		return nil, nil, fmt.Errorf("on_behalf_of resolution requires an authority resolver")
	}
	if authCtx.GrantID == "" {
		return nil, nil, fmt.Errorf("on_behalf_of authorization requires grant_id")
	}
	if authCtx.Subject.PrincipalRef().IsZero() {
		return nil, nil, fmt.Errorf("on_behalf_of authorization requires subject")
	}

	resolved, err := resolver.ResolveAuthority(ctx, actorIdentity, acl.RequestAuthorityContext{
		Mode:    authCtx.Mode,
		Subject: authCtx.Subject,
		GrantID: authCtx.GrantID,
	}, audience)
	if err != nil {
		return nil, nil, err
	}
	if resolved == nil || resolved.Grant == nil {
		return nil, nil, fmt.Errorf("authority resolver returned no grant")
	}

	authority := AuthorityFromResolved(resolved)
	identWithAuth := actor
	identWithAuth.Authority = authority
	return Mint(ctx, tenantID, identWithAuth), authority, nil
}

// AuthorityFromResolved adapts a resolved acl authority into the package's
// AuthenticatedAuthority shape used for header minting.
func AuthorityFromResolved(resolved *acl.ResolvedAuthority) *AuthenticatedAuthority {
	if resolved == nil || resolved.Grant == nil {
		return nil
	}
	g := resolved.Grant
	return &AuthenticatedAuthority{
		ActorType:       string(resolved.Actor.Type),
		ActorID:         resolved.Actor.CanonicalPrincipalID(),
		GrantID:         g.GrantID,
		SubjectType:     g.SubjectType,
		SubjectID:       g.SubjectID,
		RootSubjectType: g.RootSubjectType,
		RootSubjectID:   g.RootSubjectID,
		AudienceType:    g.AudienceType,
		AudienceID:      g.AudienceID,
		MaxAccessLevel:  g.MaxAccessLevel,
		WorkspaceScope:  g.WorkspaceScope,
		ResourceScope:   g.ResourceScope,
	}
}

// StripInbound removes all X-Auth-* and X-Aether-* headers from h to prevent
// clients from spoofing identity headers. X-Aether-* hint headers, when
// honoured, must be read by the caller before this is invoked.
func StripInbound(h http.Header) {
	for key := range h {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "x-auth-") || strings.HasPrefix(lower, "x-aether-") {
			h.Del(key)
		}
	}
}
