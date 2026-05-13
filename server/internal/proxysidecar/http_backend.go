package proxysidecar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/pkg/identityheaders"
	"github.com/scitrera/aether/pkg/models"
)

// proxyResponseError signals that the proxy could not deliver the request to
// the backend. The terminator translates these into ProxyError frames.
type proxyResponseError struct {
	Kind    pb.ProxyError_Kind
	Message string
}

func (e *proxyResponseError) Error() string {
	return fmt.Sprintf("proxy %s: %s", e.Kind.String(), e.Message)
}

func newProxyError(kind pb.ProxyError_Kind, format string, args ...any) *proxyResponseError {
	return &proxyResponseError{Kind: kind, Message: fmt.Sprintf(format, args...)}
}

// httpBackend dispatches a single HTTP backend rule.
type httpBackend struct {
	cfg      BackendConfig
	tenantID string
	resolver identityheaders.AuthorityResolver
	client   *http.Client
	// streamingClient is used for stream_response_indefinitely requests; it
	// has no per-request Timeout (which would otherwise cap the whole
	// response). Idle/max-bytes are enforced inline as bytes flow.
	streamingClient *http.Client
}

func newHTTPBackend(cfg BackendConfig, tenantID string, resolver identityheaders.AuthorityResolver) *httpBackend {
	return &httpBackend{
		cfg:      cfg,
		tenantID: tenantID,
		resolver: resolver,
		client: &http.Client{
			Timeout: time.Duration(cfg.IdleTimeoutMs) * time.Millisecond,
		},
		streamingClient: &http.Client{},
	}
}

// matches reports whether this backend should handle the given method/path.
func (b *httpBackend) matches(method, reqPath string) bool {
	if !methodAllowed(b.cfg.AllowMethods, method) {
		return false
	}
	if !pathAllowed(b.cfg.AllowPaths, reqPath) {
		return false
	}
	return true
}

// dispatch forwards a ProxyHttpRequest envelope to the backend and returns
// the assembled response. Body assembly (including chunked input) is the
// responsibility of the caller; this entry receives the fully-assembled body.
func (b *httpBackend) dispatch(ctx context.Context, req *pb.ProxyHttpRequest, body []byte) (*http.Response, *proxyResponseError) {
	httpReq, perr := b.buildBackendRequest(ctx, req, body)
	if perr != nil {
		return nil, perr
	}

	if req.GetTimeoutMs() > 0 {
		// Per-request timeout overrides the client default; honour the
		// shorter of the request and the backend idle.
		// http.Client.Timeout is per-request, so leave the default in
		// place and rely on ctx for the per-request deadline.
		dl := time.Now().Add(time.Duration(req.GetTimeoutMs()) * time.Millisecond)
		dlCtx, cancel := context.WithDeadline(httpReq.Context(), dl)
		// We cannot defer cancel here without leaking; the streaming caller
		// owns response lifetime. For the bounded path we cancel after the
		// response body is consumed; for now wire the context and let the
		// caller close the body which releases the request.
		_ = cancel
		httpReq = httpReq.WithContext(dlCtx)
	}

	resp, err := b.client.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, newProxyError(pb.ProxyError_TIMEOUT, "backend %q timed out", b.cfg.Name)
		}
		return nil, newProxyError(pb.ProxyError_DIAL_FAILED, "backend %q: %v", b.cfg.Name, err)
	}
	return resp, nil
}

// dispatchStreaming forwards a ProxyHttpRequest envelope and returns the
// backend response without buffering the body. Used when
// stream_response_indefinitely=true: timeout_ms governs time-to-first-byte
// only (i.e., the dial + headers wait), and idle / max-bytes enforcement
// happens as the caller iterates the body.
//
// The returned cancel func MUST be invoked once the caller is done draining
// the body to release the per-request context.
//
// Implementation note: a Go http.Request's Body is cancelled when the
// associated context is cancelled. We therefore implement the time-to-first-
// byte deadline by spawning a watchdog that cancels the long-lived stream
// context if Do() does not return in time, instead of layering a deadline
// context onto the request (which would propagate to the body and cut the
// stream off after TTFB).
func (b *httpBackend) dispatchStreaming(ctx context.Context, req *pb.ProxyHttpRequest, body []byte) (*http.Response, context.CancelFunc, *proxyResponseError) {
	httpReq, perr := b.buildBackendRequest(ctx, req, body)
	if perr != nil {
		return nil, nil, perr
	}

	streamCtx, cancelStream := context.WithCancel(httpReq.Context())
	httpReq = httpReq.WithContext(streamCtx)

	// Time-to-first-byte watchdog: cancels the request context if Do() is
	// still in flight when the deadline expires. We use a ttfb-completed
	// flag so the watchdog stops chasing once Do() returns.
	var (
		ttfbDone    = make(chan struct{})
		ttfbExpired bool
	)
	if req.GetTimeoutMs() > 0 {
		go func() {
			select {
			case <-time.After(time.Duration(req.GetTimeoutMs()) * time.Millisecond):
				ttfbExpired = true
				cancelStream()
			case <-ttfbDone:
			}
		}()
	}

	resp, err := b.streamingClient.Do(httpReq)
	close(ttfbDone)
	if err != nil {
		cancelStream()
		if ttfbExpired {
			return nil, nil, newProxyError(pb.ProxyError_TIMEOUT,
				"backend %q time-to-first-byte timeout", b.cfg.Name)
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, nil, newProxyError(pb.ProxyError_TIMEOUT,
				"backend %q time-to-first-byte cancelled", b.cfg.Name)
		}
		return nil, nil, newProxyError(pb.ProxyError_DIAL_FAILED, "backend %q: %v", b.cfg.Name, err)
	}
	return resp, cancelStream, nil
}

// buildBackendRequest constructs the outgoing http.Request and applies the
// header mode + per-grant ACL gates. Returned proxyResponseError is the
// caller-facing translation; nil means the request is ready to send.
func (b *httpBackend) buildBackendRequest(ctx context.Context, req *pb.ProxyHttpRequest, body []byte) (*http.Request, *proxyResponseError) {
	if int64(len(body)) > b.cfg.MaxBodyBytes {
		return nil, newProxyError(pb.ProxyError_PAYLOAD_TOO_LARGE,
			"request body %d bytes exceeds backend limit %d", len(body), b.cfg.MaxBodyBytes)
	}

	if !b.matches(req.GetMethod(), req.GetPath()) {
		return nil, newProxyError(pb.ProxyError_ACL_DENIED,
			"method %s path %s not permitted by backend %q", req.GetMethod(), req.GetPath(), b.cfg.Name)
	}

	url := strings.TrimRight(b.cfg.URL, "/") + req.GetPath()
	httpReq, err := http.NewRequestWithContext(ctx, req.GetMethod(), url, bytes.NewReader(body))
	if err != nil {
		return nil, newProxyError(pb.ProxyError_DECODE_FAILED, "build backend request: %v", err)
	}

	authority, err := b.applyHeaders(ctx, req, httpReq)
	if err != nil {
		var pe *proxyResponseError
		if errors.As(err, &pe) {
			return nil, pe
		}
		return nil, newProxyError(pb.ProxyError_ACL_DENIED, "header minting failed: %v", err)
	}

	// Per-grant proxy_path resource scope: enforced AFTER backend selection so
	// the matcher sees the canonical backend name. An absent or "*" scope on
	// the grant preserves the legacy blanket-allow behaviour. See
	// pkg/identityheaders/proxypath.go for the pattern grammar.
	if authority != nil {
		patterns := authority.ResourceScope[identityheaders.ResourceTypeProxyPath]
		if !identityheaders.MatchProxyPath(patterns, b.cfg.Name, req.GetMethod(), req.GetPath()) {
			return nil, newProxyError(pb.ProxyError_ACL_DENIED,
				"proxy_path_scope_denied: backend %q method %s path %s not in grant scope",
				b.cfg.Name, req.GetMethod(), req.GetPath())
		}
	}
	return httpReq, nil
}

// applyHeaders implements the header_mode logic. For strict and both modes it
// uses pkg/identityheaders so the wire format matches auth-proxy's
// InjectHeaders byte-for-byte. Returns the resolved on-behalf-of authority
// when one was present (so callers can enforce per-grant resource scopes), or
// nil for direct-mode / passthrough requests.
func (b *httpBackend) applyHeaders(ctx context.Context, req *pb.ProxyHttpRequest, httpReq *http.Request) (*identityheaders.AuthenticatedAuthority, error) {
	// Copy caller headers onto the outgoing request first; mode-specific
	// logic below decides what to keep, strip, or overlay.
	for k, v := range req.GetHeaders() {
		// Strip hop-by-hop headers and the Authorization header — the
		// terminator never forwards client credentials.
		if isHopByHop(k) || strings.EqualFold(k, "Authorization") {
			continue
		}
		httpReq.Header.Set(k, v)
	}

	switch b.cfg.HeaderMode {
	case HeaderModePassthrough:
		// Caller headers are already applied; mint nothing.
		return nil, nil

	case HeaderModeStrict:
		identityheaders.StripInbound(httpReq.Header)
		return b.mintInto(ctx, req, httpReq)

	case HeaderModeBoth:
		// Keep caller headers; mint and overlay (mint wins on conflict).
		return b.mintInto(ctx, req, httpReq)

	default:
		return nil, fmt.Errorf("unsupported header mode %q", b.cfg.HeaderMode)
	}
}

// mintInto resolves the request's authorization context (if any), mints the
// canonical X-Auth-* header set onto httpReq, and returns the resolved
// authority for downstream scope enforcement.
func (b *httpBackend) mintInto(ctx context.Context, req *pb.ProxyHttpRequest, httpReq *http.Request) (*identityheaders.AuthenticatedAuthority, error) {
	authCtx, err := translateAuthorizationContext(req.GetAuthorization())
	if err != nil {
		return nil, newProxyError(pb.ProxyError_ACL_DENIED, "invalid authorization context: %v", err)
	}

	actorIdentity, actor := buildActorFromHeaders(req.GetHeaders())
	if actor.UserID == "" {
		// No actor data on the envelope means the gateway considers this a
		// trusted internal call; mint with whatever identity hints we have.
		actor.UserID = string(actorIdentity.Type)
		actor.PrincipalType = string(actorIdentity.Type)
	}

	// The gateway stamps x-aether-actor-topic on every ProxyHttpRequest
	// envelope before forwarding; use it as the trustworthy caller topic.
	actor.CallerTopic = req.GetHeaders()["x-aether-actor-topic"]

	header, authority, err := identityheaders.ResolveAndMint(
		ctx,
		b.resolver,
		b.tenantID,
		actor,
		actorIdentity,
		authCtx,
		buildAudience(actorIdentity),
	)
	if err != nil {
		return nil, newProxyError(pb.ProxyError_ACL_DENIED, "mint identity headers: %v", err)
	}

	// In OBO mode the subject is only known after grant resolution; stamp it
	// as X-Aether-Caller-Subject now that we have it.
	if authority != nil && authority.SubjectID != "" {
		header.Set(identityheaders.HeaderXAetherCallerSubject, authority.SubjectID)
	}

	for k, vs := range header {
		httpReq.Header[k] = append([]string(nil), vs...)
	}

	if ws := req.GetAppWorkspace(); ws != "" {
		httpReq.Header.Set(identityheaders.HeaderWorkspaceID, ws)
	}
	return authority, nil
}

// buildActorFromHeaders extracts the actor identity from caller-supplied
// X-Auth-* headers (for header_mode=both) or returns a zero value for direct
// callers. The proto envelope itself carries no actor identity in v1; the
// gateway is expected to forward the validated identity via the caller
// headers map. When absent (e.g. tests), returns zero values.
func buildActorFromHeaders(hdrs map[string]string) (models.Identity, identityheaders.Identity) {
	get := func(k string) string {
		if hdrs == nil {
			return ""
		}
		return hdrs[k]
	}
	actor := identityheaders.Identity{
		UserID:        get(identityheaders.HeaderUserID),
		PrincipalType: get(identityheaders.HeaderPrincipalType),
		Scopes:        get(identityheaders.HeaderScopes),
		APIKeyID:      get(identityheaders.HeaderAPIKeyID),
	}
	id := models.Identity{
		Type: models.PrincipalType(actor.PrincipalType),
		ID:   actor.UserID,
	}
	return id, actor
}

// translateAuthorizationContext converts the proto AuthorizationContext into
// the identityheaders shape so pkg/identityheaders does not depend on
// api/proto.
func translateAuthorizationContext(authz *pb.AuthorizationContext) (*identityheaders.AuthorizationContext, error) {
	if authz == nil {
		return nil, nil
	}
	mode := strings.TrimSpace(authz.GetAuthorityMode())
	switch mode {
	case "", identityheaders.AuthorityModeDirect:
		if authz.Subject != nil || authz.GetGrantId() != "" {
			return nil, fmt.Errorf("direct authorization context must not include subject or grant")
		}
		return &identityheaders.AuthorizationContext{Mode: identityheaders.AuthorityModeDirect}, nil
	case identityheaders.AuthorityModeOnBehalfOf:
		if authz.Subject == nil {
			return nil, fmt.Errorf("on_behalf_of authorization requires subject")
		}
		subject, err := protoSubjectToIdentity(authz.Subject)
		if err != nil {
			return nil, err
		}
		return &identityheaders.AuthorizationContext{
			Mode:    identityheaders.AuthorityModeOnBehalfOf,
			Subject: subject,
			GrantID: authz.GetGrantId(),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported authority mode %q", mode)
	}
}

func protoSubjectToIdentity(ref *pb.PrincipalRef) (models.Identity, error) {
	if ref == nil {
		return models.Identity{}, fmt.Errorf("principal ref is required")
	}
	if strings.TrimSpace(ref.GetPrincipalId()) == "" {
		return models.Identity{}, fmt.Errorf("principal_id is required")
	}
	pt, err := parsePrincipalTypeString(ref.GetPrincipalType())
	if err != nil {
		return models.Identity{}, err
	}
	identity := models.Identity{Type: pt, ID: ref.GetPrincipalId()}
	switch pt {
	case models.PrincipalAgent, models.PrincipalTask, models.PrincipalBridge, models.PrincipalService:
		if parsed, perr := models.ParseIdentity(ref.GetPrincipalId()); perr == nil && parsed.Type == pt {
			return parsed, nil
		}
	}
	return identity, nil
}

func parsePrincipalTypeString(value string) (models.PrincipalType, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "user":
		return models.PrincipalUser, nil
	case "agent":
		return models.PrincipalAgent, nil
	case "task", "unique_task", "non_unique_task":
		return models.PrincipalTask, nil
	case "service":
		return models.PrincipalService, nil
	case "bridge":
		return models.PrincipalBridge, nil
	case "workflow_engine", "workflowengine":
		return models.PrincipalWorkflowEngine, nil
	case "metrics_bridge", "metricsbridge":
		return models.PrincipalMetricsBridge, nil
	case "orchestrator":
		return models.PrincipalOrchestrator, nil
	default:
		return "", fmt.Errorf("unknown principal type %q", value)
	}
}

// buildAudience returns a minimal audience context used when ResolveAndMint
// is called from the sidecar. The sidecar does not have session/task
// liveness data; downstream resolvers (where wired) treat empty fields as
// "no live binding".
func buildAudience(actor models.Identity) acl.GrantAudienceContext {
	return acl.GrantAudienceContext{Actor: actor}
}

// methodAllowed reports whether method is in the allowlist.
func methodAllowed(allowed []string, method string) bool {
	method = strings.ToUpper(method)
	for _, m := range allowed {
		if m == "*" || strings.EqualFold(m, method) {
			return true
		}
	}
	return false
}

// pathAllowed reports whether reqPath is permitted by the allowlist using
// path.Match (glob) semantics. "/*" matches any path; entries without a glob
// wildcard fall back to prefix matching.
func pathAllowed(patterns []string, reqPath string) bool {
	for _, p := range patterns {
		if p == "*" || p == "/*" {
			return true
		}
		// Try glob match first.
		if matched, err := path.Match(p, reqPath); err == nil && matched {
			return true
		}
		// Fall back to prefix match for entries ending in "/*".
		if strings.HasSuffix(p, "/*") {
			prefix := strings.TrimSuffix(p, "/*")
			if strings.HasPrefix(reqPath, prefix+"/") || reqPath == prefix {
				return true
			}
		} else if p == reqPath {
			return true
		}
	}
	return false
}

// isHopByHop reports whether a header should be stripped before forwarding.
func isHopByHop(name string) bool {
	switch strings.ToLower(name) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
		"te", "trailer", "transfer-encoding", "upgrade":
		return true
	}
	return false
}
