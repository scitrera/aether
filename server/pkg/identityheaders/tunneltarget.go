package identityheaders

import "strings"

// ResourceTypeTunnelTarget is the AuthorityGrant.ResourceScope key under
// which tunnel target patterns are stored. When a grant sets this scope,
// callers must intersect the inbound TunnelOpen (backend + protocol +
// remote_hint) with the configured patterns; an absent key or a "*" entry
// means blanket allow.
const ResourceTypeTunnelTarget = "tunnel_target"

// Tunnel protocol identifiers used in tunnel_target patterns. Always
// lower-case so grant authors do not need to memorise mixed-case spellings.
const (
	TunnelProtocolTCP = "tcp"
	TunnelProtocolUDP = "udp"
	TunnelProtocolWS  = "ws"
)

// MatchTunnelTarget reports whether the given backend / protocol /
// remote_hint triple is admitted by the supplied tunnel_target patterns.
//
// Pattern grammar: `<backend_glob>::<protocol_glob> <remote_hint_glob>`. The
// backend segment is matched against backendName via path.Match (so "*",
// "db-*", or an exact name all work). The remainder is matched against
// `<protocol> <remote_hint>` (protocol lower-cased) via path.Match, so the
// remote hint may itself be a glob like "prod-*:5432". A literal "*"
// pattern is shorthand for "match anything".
//
// Behaviour:
//   - len(patterns) == 0 → allow (no tunnel_target scope set on the grant).
//   - any pattern equals "*" → allow.
//   - any pattern matches backend+protocol+remote_hint → allow.
//   - otherwise → deny.
//
// Invoked by the proxy sidecar TCP / WebSocket / UDP tunnel open paths so
// per-grant tunnel ACLs stay consistent across protocols.
func MatchTunnelTarget(patterns []string, backendName, protocol, remoteHint string) bool {
	if len(patterns) == 0 {
		return true
	}
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	target := protocol + " " + remoteHint
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
