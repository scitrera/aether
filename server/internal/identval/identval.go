// Package identval provides identifier charset validation for aether principals.
//
// Aether identifiers (workspace names, agent implementations, specifiers, user
// IDs, window IDs, etc.) must be safe for use as NATS subject tokens after the
// natscodec package translates them.  Certain characters cause ambiguity either
// in NATS subjects or in aether's own "::" segment separator, so they are
// rejected at every ingestion boundary:
//
//   - '*' and '>' are NATS wildcard characters.
//   - Whitespace and control characters (< 0x20 or == 0x7F) are never safe in
//     subject tokens and indicate malformed input.
//   - The substring "::" is aether's own segment separator; having it inside a
//     token makes the resulting topic string ambiguous.
//
// The '.' character is explicitly allowed: reverse-DNS implementation names
// such as "com.example.chat-agent" are a documented convention, and the
// natscodec package escapes '.' when translating to NATS subjects.
//
// Validation can be disabled globally by setting the environment variable
// AETHER_STRICT_IDENTIFIER_CHARSET=false.  This opt-out exists for operators
// migrating from deployments that pre-date this policy.
package identval

import (
	"fmt"
	"os"
	"strings"
	"sync"
)

// MaxTokenLen is the maximum allowed byte-length of a single identifier token.
const MaxTokenLen = 128

// strictOnce and strictVal cache the parsed value of AETHER_STRICT_IDENTIFIER_CHARSET.
// They are read once on first use and never change for the lifetime of the process,
// matching the pattern used for other env-driven feature flags in the gateway.
var (
	strictOnce sync.Once
	strictVal  bool
)

// IsStrictMode returns true when identifier charset validation is active.
// The effective value is read from AETHER_STRICT_IDENTIFIER_CHARSET; the
// default when the variable is absent or empty is true (strict on).
// Setting the variable to "false", "0", or "no" disables validation.
func IsStrictMode() bool {
	strictOnce.Do(func() {
		raw := strings.TrimSpace(os.Getenv("AETHER_STRICT_IDENTIFIER_CHARSET"))
		if raw == "" {
			// Absent or empty → strict by default.
			strictVal = true
			return
		}
		// Treat "false", "0", "no" (case-insensitive) as opt-out.
		lower := strings.ToLower(raw)
		strictVal = lower != "false" && lower != "0" && lower != "no"
	})
	return strictVal
}

// ValidateToken validates a single aether identifier token.
//
// Forbidden characters: '*', '>', any ASCII whitespace, any control character
// (byte < 0x20 or byte == 0x7F).  The substring "::" is also rejected.
// Maximum length: MaxTokenLen bytes.
//
// The '.' character is allowed — reverse-DNS names like "com.example.bot" are
// a documented convention.
//
// The kind argument is used only to produce descriptive error messages
// (e.g., "workspace", "impl", "specifier").
//
// When IsStrictMode() returns false, ValidateToken always returns nil.
func ValidateToken(name, kind string) error {
	if !IsStrictMode() {
		return nil
	}
	return validateInternal(name, kind)
}

// ValidateImpl is a convenience alias for ValidateToken when the token is an
// implementation name.  It is semantically identical; the separate entry point
// exists so call sites can document intent and the function name appears in
// error messages.
func ValidateImpl(impl string) error {
	return ValidateToken(impl, "impl")
}

// validateInternal performs the actual validation unconditionally.
func validateInternal(name, kind string) error {
	if len(name) == 0 {
		return fmt.Errorf("invalid %s: must not be empty", kind)
	}
	if len(name) > MaxTokenLen {
		return fmt.Errorf("invalid %s: length %d exceeds maximum %d", kind, len(name), MaxTokenLen)
	}
	if strings.Contains(name, "::") {
		return fmt.Errorf("invalid %s %q: contains reserved separator \"::\"", kind, name)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c == '*':
			return fmt.Errorf("invalid %s %q: contains forbidden character '*' (NATS wildcard)", kind, name)
		case c == '>':
			return fmt.Errorf("invalid %s %q: contains forbidden character '>' (NATS wildcard)", kind, name)
		case c == ' ':
			return fmt.Errorf("invalid %s %q: contains whitespace", kind, name)
		case c == 0x7F:
			return fmt.Errorf("invalid %s %q: contains DEL control character (0x7F)", kind, name)
		case c < 0x20:
			return fmt.Errorf("invalid %s %q: contains control character 0x%02X", kind, name, c)
		}
	}
	return nil
}
