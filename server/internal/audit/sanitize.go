package audit

import "strings"

// credentialKeyPatterns lists case-insensitive substrings that identify
// credential-shaped metadata keys. When a key matches any pattern the
// corresponding value is replaced with "[REDACTED]" before the event is
// persisted, preventing accidental exposure of secrets in the audit log.
var credentialKeyPatterns = []string{
	"password",
	"passwd",
	"api_key",
	"apikey",
	"secret",
	"token",
	"bearer",
	"authorization",
	"credential",
	"private_key",
	"privatekey",
}

// isCredentialKey reports whether key matches any credential pattern
// (case-insensitive substring match).
func isCredentialKey(key string) bool {
	lower := strings.ToLower(key)
	for _, pattern := range credentialKeyPatterns {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}

// SanitizeMetadata returns a deep copy of m with values for credential-shaped
// keys replaced by "[REDACTED]". The original map is never mutated.
//
// Recursion handles nested map[string]interface{} values. Slices are walked
// element-by-element; slice elements that are themselves maps are sanitized.
// All other value types (strings, numbers, booleans, nil) are copied as-is
// unless the containing key matched a credential pattern.
func SanitizeMetadata(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		if isCredentialKey(k) {
			out[k] = "[REDACTED]"
			continue
		}
		out[k] = sanitizeValue(v)
	}
	return out
}

// sanitizeValue recursively sanitizes a single value. Maps and slices are
// deep-copied; all other types are returned unchanged.
func sanitizeValue(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		return SanitizeMetadata(val)
	case []interface{}:
		out := make([]interface{}, len(val))
		for i, elem := range val {
			out[i] = sanitizeValue(elem)
		}
		return out
	default:
		return v
	}
}
