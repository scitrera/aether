// Package natscodec translates aether topic strings (:: separator) to NATS subjects (. separator) and back.
package natscodec

import (
	"strings"
)

// ToNATSSubject converts an aether topic (tokens separated by "::") to a NATS subject (tokens separated by ".").
// Each token is escaped so that NATS-unsafe characters cannot appear literally in the output.
func ToNATSSubject(aetherTopic string) string {
	tokens := strings.Split(aetherTopic, "::")
	for i, t := range tokens {
		tokens[i] = escapeToken(t)
	}
	return strings.Join(tokens, ".")
}

// FromNATSSubject converts a NATS subject back to an aether topic. It is the exact inverse of ToNATSSubject.
func FromNATSSubject(natsSubject string) string {
	tokens := strings.Split(natsSubject, ".")
	for i, t := range tokens {
		tokens[i] = unescapeToken(t)
	}
	return strings.Join(tokens, "::")
}

// escapeToken escapes a single aether token segment so it is safe as a NATS subject token.
// Escape order matters: _ must be escaped first to preserve bijectivity.
func escapeToken(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '_':
			b.WriteString("_5F_")
		case c == '.':
			b.WriteString("_2E_")
		case c == '*':
			b.WriteString("_2A_")
		case c == '>':
			b.WriteString("_3E_")
		case c == ' ':
			b.WriteString("_20_")
		case c == '\t':
			b.WriteString("_09_")
		case c == '\n':
			b.WriteString("_0A_")
		case c == '\r':
			b.WriteString("_0D_")
		case c == ':':
			b.WriteString("_3A_")
		case c < 0x20 || c > 0x7E:
			// other control characters and non-ASCII bytes
			b.WriteByte('_')
			b.WriteByte(hexNibble(c >> 4))
			b.WriteByte(hexNibble(c & 0x0F))
			b.WriteByte('_')
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// unescapeToken reverses escapeToken for a single NATS subject token.
func unescapeToken(s string) string {
	if !strings.Contains(s, "_") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		if s[i] == '_' && i+3 < len(s) {
			// look for _XX_ pattern (2 hex digits)
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
