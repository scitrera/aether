package authproxy

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/scitrera/aether/internal/logging"
)

// SingleTenantResolver is the OSS-default IdentityResolver.
//
// It emits the configured static tenant id as both DefaultTenantID and the
// sole TenantIDs entry, and runs declarative AuthRules against the verified
// claim set for user-presented credentials. API keys and task tokens are
// passed through without rule evaluation: they are pre-issued by the gateway
// and already represent a trusted machine principal.
type SingleTenantResolver struct {
	tenantID            string
	rules               []AuthRuleSpec
	allowedEmailDomains []string
}

// AuthRuleSpec couples an AuthRule with an optional authenticator-method
// scope. An empty Method (or "*") applies the rule to every user-presented
// method (oauth, azure_entra, session); a specific method (e.g. "azure_entra")
// applies it only when that authenticator produced the verified claim set.
//
// The Label is used only in logs; it has no effect on evaluation.
type AuthRuleSpec struct {
	Method string
	Rule   AuthRule
	Label  string
}

// NewSingleTenantResolver builds a resolver from explicit configuration.
// Pass nil rules / nil domains for plain pass-through behavior bound to the
// configured tenant id.
func NewSingleTenantResolver(tenantID string, rules []AuthRuleSpec, allowedEmailDomains []string) *SingleTenantResolver {
	domains := make([]string, 0, len(allowedEmailDomains))
	for _, d := range allowedEmailDomains {
		d = strings.ToLower(strings.TrimSpace(d))
		if d != "" {
			domains = append(domains, d)
		}
	}
	return &SingleTenantResolver{
		tenantID:            tenantID,
		rules:               rules,
		allowedEmailDomains: domains,
	}
}

// LoadSingleTenantResolverFromEnv constructs a SingleTenantResolver from
// environment variables.
//
// Recognised vars:
//
//   - AUTH_PROXY_AUTH_RULE_* (any suffix) — one rule per var. Value format:
//     "[method=]claim:val1[,val2,...][,REQUIRED]". Examples:
//
//     AUTH_PROXY_AUTH_RULE_AZURE_TID=azure_entra=tid:beeaef61-...,REQUIRED
//     AUTH_PROXY_AUTH_RULE_GOOGLE_HD=oauth=hd:scitrera.com
//     AUTH_PROXY_AUTH_RULE_VERIFIED=email_verified:true,REQUIRED
//
//     A leading "method=" pins the rule to that authenticator method; without
//     it, the rule applies to every user-presented method.
//
//   - AUTH_PROXY_ALLOWED_EMAIL_DOMAINS — comma-separated list of email
//     domains. When set, user-presented credentials whose email-bearing claim
//     does not end in one of the listed domains are rejected.
func LoadSingleTenantResolverFromEnv(tenantID string) (*SingleTenantResolver, error) {
	var specs []AuthRuleSpec
	for _, kv := range os.Environ() {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		key, val := kv[:eq], kv[eq+1:]
		if !strings.HasPrefix(key, "AUTH_PROXY_AUTH_RULE_") {
			continue
		}
		spec, err := parseAuthRuleSpec(val)
		if err != nil {
			return nil, fmt.Errorf("env %s: %w", key, err)
		}
		spec.Label = key
		specs = append(specs, spec)
	}

	var domains []string
	if v := os.Getenv("AUTH_PROXY_ALLOWED_EMAIL_DOMAINS"); v != "" {
		domains = strings.Split(v, ",")
	}

	return NewSingleTenantResolver(tenantID, specs, domains), nil
}

// parseAuthRuleSpec parses a method-scoped rule spec.
//
// Format: "[method=]claim:val1[,val2,...][,REQUIRED]"
//
// If the spec contains "=" before the first ":" the substring before "=" is
// the authenticator-method scope ("azure_entra", "oauth", "session", or "*"
// for any user method); otherwise no scope is set and the rule applies to
// every user-presented method.
//
// Note: this is intentionally simpler than ParseRule — we do
// NOT support a leading "name=" label here. Use the env var's own name as
// the rule label.
func parseAuthRuleSpec(spec string) (AuthRuleSpec, error) {
	method := ""
	body := spec

	colon := strings.IndexByte(spec, ':')
	eq := strings.IndexByte(spec, '=')
	if eq >= 0 && (colon < 0 || eq < colon) {
		method = strings.TrimSpace(spec[:eq])
		body = spec[eq+1:]
	}

	rule, err := ParseRule(body)
	if err != nil {
		return AuthRuleSpec{}, err
	}
	return AuthRuleSpec{Method: method, Rule: rule}, nil
}

// Name implements IdentityResolver.
func (r *SingleTenantResolver) Name() string { return "single_tenant" }

// Resolve implements IdentityResolver.
//
// The resolver always echoes the verified principal back with the configured
// tenant id. Rule failures and domain-gate failures are surfaced as
// ResolvedIdentity.Reject (HTTP 403) so the middleware can produce a clean
// error response without 500-level noise.
func (r *SingleTenantResolver) Resolve(_ context.Context, in ResolverInput) (*ResolvedIdentity, error) {
	base := &ResolvedIdentity{
		UserID:          in.Identity.ID,
		PrincipalType:   string(in.Identity.Type),
		DefaultTenantID: r.tenantID,
	}
	if r.tenantID != "" {
		base.TenantIDs = []string{r.tenantID}
	}

	// Pass-through for machine principals: api keys and task tokens are
	// pre-issued by the gateway and carry no user-style claims.
	if !isUserMethod(in.Method) {
		return base, nil
	}

	if rej := r.checkEmailDomain(in.Claims); rej != nil {
		base.Reject = rej
		return base, nil
	}

	if rej := r.checkRules(in.Method, in.Claims); rej != nil {
		base.Reject = rej
		return base, nil
	}

	return base, nil
}

func (r *SingleTenantResolver) checkEmailDomain(claims map[string]any) *Rejection {
	if len(r.allowedEmailDomains) == 0 {
		return nil
	}
	email := firstStringClaim(claims, "email", "upn", "preferred_username")
	if email == "" {
		return &Rejection{
			Status:  http.StatusForbidden,
			Code:    "missing_email",
			Message: "domain gate active but no email claim available",
		}
	}
	if !domainAllowed(email, r.allowedEmailDomains) {
		return &Rejection{
			Status:  http.StatusForbidden,
			Code:    "domain_not_allowed",
			Message: fmt.Sprintf("email domain not allowed for: %s", email),
		}
	}
	return nil
}

func (r *SingleTenantResolver) checkRules(method string, claims map[string]any) *Rejection {
	for _, spec := range r.rules {
		if spec.Method != "" && spec.Method != "*" && spec.Method != method {
			continue
		}
		ok, reason := EvaluateAuthRules(claims, []AuthRule{spec.Rule})
		if !ok {
			logging.Logger.Warn().
				Str("rule", spec.Label).
				Str("method", method).
				Str("reason", reason).
				Msg("single_tenant: auth rule failed")
			return &Rejection{
				Status:  http.StatusForbidden,
				Code:    "auth_rule_failed",
				Message: reason,
			}
		}
	}
	return nil
}

// isUserMethod returns true for authenticator methods that produce
// human-driven claim sets where AuthRules are meaningful.
func isUserMethod(method string) bool {
	switch method {
	case "oauth", "azure_entra", "session":
		return true
	default:
		return false
	}
}

// firstStringClaim returns the first non-empty string claim found among keys.
func firstStringClaim(claims map[string]any, keys ...string) string {
	for _, k := range keys {
		v, ok := claims[k]
		if !ok {
			continue
		}
		s, ok := v.(string)
		if ok && s != "" {
			return s
		}
	}
	return ""
}

// domainAllowed reports whether email's domain (lowercased) is in allowed.
func domainAllowed(email string, allowed []string) bool {
	at := strings.LastIndexByte(email, '@')
	if at < 0 {
		return false
	}
	domain := strings.ToLower(email[at+1:])
	for _, a := range allowed {
		if a == domain {
			return true
		}
	}
	return false
}
