package identityheaders

import "testing"

func TestMatchProxyPath_NoPatterns_AllowsAll(t *testing.T) {
	if !MatchProxyPath(nil, "api-v1", "GET", "/v1/users") {
		t.Error("nil patterns must allow")
	}
	if !MatchProxyPath([]string{}, "api-v1", "POST", "/anything") {
		t.Error("empty patterns must allow")
	}
}

func TestMatchProxyPath_StarAllowsAll(t *testing.T) {
	if !MatchProxyPath([]string{"*"}, "any-backend", "DELETE", "/any/path") {
		t.Error("star pattern must allow any request")
	}
}

func TestMatchProxyPath_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		backend  string
		method   string
		path     string
		want     bool
	}{
		// Exact backend + glob path
		{"exact backend, glob path: hit", []string{"api-v1::GET /v1/*"}, "api-v1", "GET", "/v1/users", true},
		{"exact backend, glob path: wrong backend", []string{"api-v1::GET /v1/*"}, "api-v2", "GET", "/v1/users", false},
		{"exact backend, glob path: wrong method", []string{"api-v1::GET /v1/*"}, "api-v1", "POST", "/v1/users", false},
		{"exact backend, glob path: wrong path", []string{"api-v1::GET /v1/*"}, "api-v1", "GET", "/v2/users", false},

		// Backend wildcard
		{"wildcard backend, glob path: hit", []string{"*::GET /health"}, "anything", "GET", "/health", true},
		{"wildcard backend, glob path: method mismatch", []string{"*::GET /health"}, "anything", "POST", "/health", false},

		// Backend glob (prefix)
		{"glob backend prefix: hit", []string{"api-*::GET /v1/users"}, "api-v3", "GET", "/v1/users", true},
		{"glob backend prefix: miss", []string{"api-*::GET /v1/users"}, "internal", "GET", "/v1/users", false},

		// Method wildcard
		{"method wildcard: hit GET", []string{"api-v1::* /v1/users"}, "api-v1", "GET", "/v1/users", true},
		{"method wildcard: hit POST", []string{"api-v1::* /v1/users"}, "api-v1", "POST", "/v1/users", true},

		// Path wildcard
		{"path total wildcard: hit", []string{"api-v1::GET *"}, "api-v1", "GET", "/anything/here", false}, // path.Match "*" doesn't span "/"
		{"path with /*: hit", []string{"api-v1::GET /v1/*"}, "api-v1", "GET", "/v1/x", true},
		{"path with /*: miss for nested", []string{"api-v1::GET /v1/*"}, "api-v1", "GET", "/v1/x/y", false},

		// Auth-proxy default backend convention
		{"auth-proxy default backend hit", []string{"_default::POST /memory/*"}, AuthProxyDefaultBackend, "POST", "/memory/store", true},
		{"auth-proxy default backend wrong path", []string{"_default::POST /memory/*"}, AuthProxyDefaultBackend, "POST", "/billing", false},

		// Multiple patterns - any match wins
		{"multiple patterns: second matches", []string{"api-v1::POST /v1/*", "api-v1::GET /v1/*"}, "api-v1", "GET", "/v1/users", true},
		{"multiple patterns: none matches", []string{"api-v1::POST /v1/*", "api-v2::GET /v1/*"}, "api-v1", "GET", "/v1/users", false},

		// Method case-insensitive
		{"lowercase method on request, uppercase pattern", []string{"api-v1::GET /v1/users"}, "api-v1", "get", "/v1/users", true},

		// Malformed pattern (no "::") is skipped
		{"malformed pattern skipped", []string{"no-separator-here"}, "any", "GET", "/x", false},
		{"malformed + valid: valid wins", []string{"no-separator-here", "*::GET /x"}, "any", "GET", "/x", true},

		// Empty entries are skipped
		{"empty pattern skipped", []string{"", "api-v1::GET /v1/x"}, "api-v1", "GET", "/v1/x", true},

		// "*" shorthand wins anywhere in the list
		{"star inside list: allows all", []string{"api-v1::GET /v1/x", "*"}, "anything", "DELETE", "/whatever", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchProxyPath(tt.patterns, tt.backend, tt.method, tt.path)
			if got != tt.want {
				t.Errorf("MatchProxyPath(%v, %q, %q, %q) = %v, want %v",
					tt.patterns, tt.backend, tt.method, tt.path, got, tt.want)
			}
		})
	}
}
