package identityheaders

import (
	"path"
	"strings"
)

// ResourceTypeProxyPath is the AuthorityGrant.ResourceScope key under which
// HTTP proxy path patterns are stored. When a grant sets this scope, callers
// must intersect the inbound request (backend + method + path) with the
// configured patterns; an absent key or a "*" entry means blanket allow.
const ResourceTypeProxyPath = "proxy_path"

// AuthProxyDefaultBackend is the synthetic backend identifier used by the
// auth-proxy when invoking the proxy_path matcher. The auth-proxy does not
// expose the multi-backend dispatch model the proxy sidecar uses; instead it
// fronts a single configured downstream. Grants that intend to apply across
// the auth-proxy direct-HTTP path should target this name (or "*"). Picking a
// stable string here keeps grant patterns portable between sidecar and
// auth-proxy deployments.
const AuthProxyDefaultBackend = "_default"

// MatchProxyPath reports whether the given backend / method / request-path
// triple is admitted by the supplied proxy_path patterns.
//
// Pattern grammar: `<backend_glob>::<method_glob> <path_glob>`. The backend
// segment is matched against backendName via path.Match (so "*", "api-*", or
// an exact name all work). The remainder is matched against
// `<METHOD> <reqPath>` (method upper-cased) via path.Match. A literal "*"
// pattern is shorthand for "match anything".
//
// Behaviour:
//   - len(patterns) == 0 → allow (no proxy_path scope set on the grant).
//   - any pattern equals "*" → allow.
//   - any pattern matches backend+method+path → allow.
//   - otherwise → deny.
//
// Used by both the proxy sidecar HTTP backend dispatcher and the auth-proxy
// HTTP path enforcement so the wire-level grant semantics stay identical
// across the two components.
func MatchProxyPath(patterns []string, backendName, method, reqPath string) bool {
	if len(patterns) == 0 {
		return true
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	target := method + " " + reqPath
	for _, raw := range patterns {
		p := strings.TrimSpace(raw)
		if p == "" {
			continue
		}
		if p == "*" {
			return true
		}
		idx := strings.Index(p, "::")
		if idx < 0 {
			// Malformed entry — skip rather than match by accident.
			continue
		}
		backendPat := p[:idx]
		rest := p[idx+2:]
		if !globMatch(backendPat, backendName) {
			continue
		}
		if globMatch(rest, target) {
			return true
		}
	}
	return false
}

// globMatch wraps path.Match with a literal-equality fallback so callers do
// not have to special-case patterns without metacharacters. A "*" pattern
// matches anything (including the empty string).
func globMatch(pattern, value string) bool {
	if pattern == "*" {
		return true
	}
	if pattern == value {
		return true
	}
	matched, err := path.Match(pattern, value)
	if err == nil && matched {
		return true
	}
	return false
}
