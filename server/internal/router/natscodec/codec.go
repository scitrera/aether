// Package natscodec translates aether topic strings (:: separator) to NATS
// subjects (. separator) and back, and provides charset-aware per-token
// escape helpers for the three NATS namespaces (subject, KV key, consumer
// name) that each enforce a different allowed character set.
//
// Namespace allowed sets (besides the _XX_ escape itself):
//
//	Subject       : [A-Za-z0-9-]                — escapes . * > whitespace : @ + & ?
//	KV key        : [A-Za-z0-9-=/]              — escapes . (reserved as separator) and the rest
//	Consumer name : [A-Za-z0-9-]                — strictest; also escapes . = /
//
// All variants share the same _XX_ escape format, so a single Unescape can
// reverse any of them. The "_" character is always escaped to "_5F_" regardless
// of which variant is used so the encoding stays bijective.
package natscodec

import (
	"strings"
)

// ToNATSSubject converts an aether topic (tokens separated by "::") to a NATS
// subject (tokens separated by "."). Each token is escaped via EscapeForSubject
// so that NATS-unsafe characters cannot appear literally in the output.
func ToNATSSubject(aetherTopic string) string {
	tokens := strings.Split(aetherTopic, "::")
	for i, t := range tokens {
		tokens[i] = EscapeForSubject(t)
	}
	return strings.Join(tokens, ".")
}

// FromNATSSubject converts a NATS subject back to an aether topic. It is the
// exact inverse of ToNATSSubject.
func FromNATSSubject(natsSubject string) string {
	tokens := strings.Split(natsSubject, ".")
	for i, t := range tokens {
		tokens[i] = Unescape(t)
	}
	return strings.Join(tokens, "::")
}

// ---------------------------------------------------------------------------
// Per-namespace pass-through predicates.
//
// Each predicate returns true when the byte may pass through unescaped.
// The _ character is reserved as the escape sentinel and is ALWAYS escaped;
// these predicates therefore must not allow it through. (escapeBytes enforces
// this anyway as a defense-in-depth check.)
// ---------------------------------------------------------------------------

func isAlphaNum(c byte) bool {
	return (c >= '0' && c <= '9') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= 'a' && c <= 'z')
}

// allowSubject: alphanumeric + '-'. Everything else (including '.', '*', '>',
// whitespace, ':', '@', '+', '&', '?', non-ASCII) is escaped.
func allowSubject(c byte) bool {
	return isAlphaNum(c) || c == '-'
}

// allowKVKey: alphanumeric + '-' + '=' + '/'. NATS KV permits '.' inside keys,
// but it is reserved by callers as the intra-key separator so we escape it
// here.
func allowKVKey(c byte) bool {
	return isAlphaNum(c) || c == '-' || c == '=' || c == '/'
}

// allowConsumerName: alphanumeric + '-' only. NATS rejects '.', '=', '/' in
// durable consumer names.
func allowConsumerName(c byte) bool {
	return isAlphaNum(c) || c == '-'
}

// ---------------------------------------------------------------------------
// Public escape API. Each variant first consults its dedicated LRU cache to
// short-circuit the hot path when the same identity strings recur.
// ---------------------------------------------------------------------------

// EscapeForSubject escapes s for use as a NATS subject token.
// Passes through: alphanumeric, '-'. Escapes everything else (including '.',
// '*', '>', whitespace, ':', '@', '+', '&', '?', etc.) using the _XX_ scheme.
func EscapeForSubject(s string) string {
	if v, ok := subjectCache.get(s); ok {
		return v
	}
	out := escapeBytes(s, allowSubject)
	subjectCache.add(s, out)
	return out
}

// EscapeForKVKey escapes s for use as a NATS JetStream KV key segment.
// Passes through: alphanumeric, '-', '=', '/'. Note '.' is escaped here even
// though NATS accepts it inside KV keys — '.' is reserved by callers as the
// intra-key separator joined externally.
func EscapeForKVKey(s string) string {
	if v, ok := kvKeyCache.get(s); ok {
		return v
	}
	out := escapeBytes(s, allowKVKey)
	kvKeyCache.add(s, out)
	return out
}

// EscapeForConsumerName escapes s for use as a NATS JetStream durable consumer
// name. Passes through: alphanumeric, '-'. (Strictest variant — '.', '=', '/'
// are all escaped here.)
func EscapeForConsumerName(s string) string {
	if v, ok := consumerNameCache.get(s); ok {
		return v
	}
	out := escapeBytes(s, allowConsumerName)
	consumerNameCache.add(s, out)
	return out
}

// Unescape reverses any of the three Escape* functions. Format is identical
// across all three; only the pass-through set differs. Not cached: this is
// called on data flowing from JetStream which we do not control, and caching
// it would offer less benefit while inflating memory.
func Unescape(s string) string {
	if !strings.Contains(s, "_") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		if s[i] == '_' && i+3 < len(s) {
			j := i + 1
			if isHex(s[j]) && isHex(s[j+1]) && s[j+2] == '_' {
				val := (hexVal(s[j]) << 4) | hexVal(s[j+1])
				b.WriteByte(val)
				i += 4
				continue
			}
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

// escapeToken is preserved as a thin alias for EscapeForSubject for backward
// compatibility with older intra-package callers.
func escapeToken(s string) string { return EscapeForSubject(s) }

// unescapeToken is preserved as a thin alias for Unescape for backward
// compatibility with older intra-package callers.
func unescapeToken(s string) string { return Unescape(s) }

// escapeBytes is the single source of truth for per-namespace escaping.
// allow(c) returns true for bytes that pass through literally; everything
// else is encoded as _XX_ (two upper-case hex digits). The _ byte is
// hard-wired to "_5F_" so that the encoding stays bijective regardless of
// what allow() returns.
func escapeBytes(s string, allow func(byte) bool) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '_' {
			b.WriteString("_5F_")
			continue
		}
		if allow(c) {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('_')
		b.WriteByte(hexNibble(c >> 4))
		b.WriteByte(hexNibble(c & 0x0F))
		b.WriteByte('_')
	}
	return b.String()
}

func hexNibble(v byte) byte {
	if v < 10 {
		return '0' + v
	}
	return 'A' + v - 10
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f')
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	default:
		return c - 'a' + 10
	}
}
