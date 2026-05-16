package identval

import (
	"strings"
	"sync"
	"testing"
)

// resetStrict allows tests to reset the strict-mode singleton so env-var
// toggling tests work correctly within a single process.
func resetStrict() {
	strictOnce = sync.Once{}
}

func TestValidateToken_Positive(t *testing.T) {
	t.Setenv("AETHER_STRICT_IDENTIFIER_CHARSET", "true")
	resetStrict()

	cases := []string{
		"a",
		"workspace",
		"my-workspace",
		"my_workspace",
		strings.Repeat("a", 64),
		strings.Repeat("a", 128),
		"com.example.chat-agent", // dots are allowed
		"abc123",
		"ABC-XYZ_123",
		"hello.world",
	}
	for _, tc := range cases {
		if err := ValidateToken(tc, "workspace"); err != nil {
			t.Errorf("ValidateToken(%q) returned unexpected error: %v", tc, err)
		}
	}
}

func TestValidateToken_Negative(t *testing.T) {
	t.Setenv("AETHER_STRICT_IDENTIFIER_CHARSET", "true")
	resetStrict()

	cases := []struct {
		name string
		want string // substring expected in error message
	}{
		{"contains*star", "'*'"},
		{"contains>gt", "'>'"},
		{"has space", "whitespace"},
		{"has\ttab", "control"},
		{"has\nnewline", "control"},
		{string([]byte{0x01}), "control"},
		{"foo::bar", "::"},
		{strings.Repeat("a", 129), "exceeds maximum"},
		{"", "must not be empty"},
	}
	for _, tc := range cases {
		err := ValidateToken(tc.name, "workspace")
		if err == nil {
			t.Errorf("ValidateToken(%q) expected error, got nil", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("ValidateToken(%q) error = %q, want substring %q", tc.name, err.Error(), tc.want)
		}
	}
}

func TestValidateToken_DELControlChar(t *testing.T) {
	t.Setenv("AETHER_STRICT_IDENTIFIER_CHARSET", "true")
	resetStrict()

	name := string([]byte{0x7F})
	err := ValidateToken(name, "specifier")
	if err == nil {
		t.Error("expected error for DEL (0x7F), got nil")
	}
}

func TestValidateImpl_ReverseDNS(t *testing.T) {
	t.Setenv("AETHER_STRICT_IDENTIFIER_CHARSET", "true")
	resetStrict()

	if err := ValidateImpl("com.example.chat-agent"); err != nil {
		t.Errorf("ValidateImpl(reverse-DNS) unexpected error: %v", err)
	}
}

func TestValidateImpl_WithDot(t *testing.T) {
	t.Setenv("AETHER_STRICT_IDENTIFIER_CHARSET", "true")
	resetStrict()

	if err := ValidateImpl("scitrera_ai_runtime.cowork.aether_bridge"); err != nil {
		t.Errorf("ValidateImpl(dotted) unexpected error: %v", err)
	}
}

func TestValidateImpl_ForbiddenChars(t *testing.T) {
	t.Setenv("AETHER_STRICT_IDENTIFIER_CHARSET", "true")
	resetStrict()

	if err := ValidateImpl("com.example.*"); err == nil {
		t.Error("expected error for impl containing '*', got nil")
	}
}

func TestIsStrictMode_Default(t *testing.T) {
	t.Setenv("AETHER_STRICT_IDENTIFIER_CHARSET", "")
	resetStrict()

	if !IsStrictMode() {
		t.Error("expected strict mode on by default (empty env var)")
	}
}

func TestIsStrictMode_FalseDisablesValidation(t *testing.T) {
	t.Setenv("AETHER_STRICT_IDENTIFIER_CHARSET", "false")
	resetStrict()

	// Even a clearly bad token should pass when strict mode is off.
	if err := ValidateToken("bad::token*here", "workspace"); err != nil {
		t.Errorf("expected nil error when strict mode off, got: %v", err)
	}
}

func TestIsStrictMode_ZeroDisablesValidation(t *testing.T) {
	t.Setenv("AETHER_STRICT_IDENTIFIER_CHARSET", "0")
	resetStrict()

	if IsStrictMode() {
		t.Error("expected strict mode off when env=0")
	}
}

func TestIsStrictMode_NoDisablesValidation(t *testing.T) {
	t.Setenv("AETHER_STRICT_IDENTIFIER_CHARSET", "no")
	resetStrict()

	if IsStrictMode() {
		t.Error("expected strict mode off when env=no")
	}
}

func TestValidateToken_Lengths(t *testing.T) {
	t.Setenv("AETHER_STRICT_IDENTIFIER_CHARSET", "true")
	resetStrict()

	if err := ValidateToken("a", "workspace"); err != nil {
		t.Errorf("length 1 failed: %v", err)
	}
	if err := ValidateToken(strings.Repeat("b", 64), "workspace"); err != nil {
		t.Errorf("length 64 failed: %v", err)
	}
	if err := ValidateToken(strings.Repeat("c", 128), "workspace"); err != nil {
		t.Errorf("length 128 failed: %v", err)
	}
	if err := ValidateToken(strings.Repeat("d", 129), "workspace"); err == nil {
		t.Error("length 129 expected error, got nil")
	}
}
