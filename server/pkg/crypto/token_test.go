package crypto

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

// testHMACKey is a default key used in tests that don't care about key material.
var testHMACKey = []byte("test-default-hmac-key-32-bytes!!")

// resetHMACKey resets to the default test key between tests.
func resetHMACKey() {
	InitTokenHMAC(testHMACKey)
}

// clearHMACKey clears the HMAC key to test the panic path.
func clearHMACKey() {
	hmacKeyMu.Lock()
	defer hmacKeyMu.Unlock()
	hmacKey = nil
}

func TestGenerateToken(t *testing.T) {
	resetHMACKey()

	token, hash, err := GenerateToken(32)
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}

	if token == "" {
		t.Error("expected non-empty token")
	}
	if hash == "" {
		t.Error("expected non-empty hash")
	}

	// Token should be valid base64url
	decoded, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		t.Fatalf("token is not valid base64url: %v", err)
	}
	if len(decoded) != 32 {
		t.Errorf("decoded token length = %d, want 32", len(decoded))
	}

	// Hash should be 64 hex chars (HMAC-SHA256 produces 32 bytes)
	if len(hash) != 64 {
		t.Errorf("hash length = %d, want 64", len(hash))
	}

	// Hash should match HashToken
	got, err := HashToken(token)
	if err != nil {
		t.Fatalf("HashToken() unexpected error: %v", err)
	}
	if got != hash {
		t.Errorf("HashToken(token) = %q, want %q", got, hash)
	}
}

func TestGenerateToken_Uniqueness(t *testing.T) {
	resetHMACKey()

	token1, _, err := GenerateToken(32)
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}

	token2, _, err := GenerateToken(32)
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}

	if token1 == token2 {
		t.Error("two generated tokens should not be identical")
	}
}

func TestGenerateToken_DifferentLengths(t *testing.T) {
	resetHMACKey()

	for _, length := range []int{16, 32, 64} {
		token, _, err := GenerateToken(length)
		if err != nil {
			t.Fatalf("GenerateToken(%d) error = %v", length, err)
		}

		decoded, err := base64.URLEncoding.DecodeString(token)
		if err != nil {
			t.Fatalf("token is not valid base64url: %v", err)
		}
		if len(decoded) != length {
			t.Errorf("GenerateToken(%d): decoded length = %d", length, len(decoded))
		}
	}
}

func TestHashToken(t *testing.T) {
	resetHMACKey()

	hash, err := HashToken("test-token")
	if err != nil {
		t.Fatalf("HashToken() unexpected error: %v", err)
	}

	// HMAC-SHA256 produces 64 hex chars
	if len(hash) != 64 {
		t.Errorf("hash length = %d, want 64", len(hash))
	}

	// Should only contain hex chars
	for _, c := range hash {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Errorf("hash contains non-hex character: %c", c)
			break
		}
	}
}

func TestHashToken_Deterministic(t *testing.T) {
	resetHMACKey()

	hash1, err := HashToken("same-token")
	if err != nil {
		t.Fatalf("HashToken() unexpected error: %v", err)
	}
	hash2, err := HashToken("same-token")
	if err != nil {
		t.Fatalf("HashToken() unexpected error: %v", err)
	}

	if hash1 != hash2 {
		t.Errorf("HashToken should be deterministic: %q != %q", hash1, hash2)
	}
}

func TestHashToken_DifferentInputs(t *testing.T) {
	resetHMACKey()

	hash1, err := HashToken("token-a")
	if err != nil {
		t.Fatalf("HashToken() unexpected error: %v", err)
	}
	hash2, err := HashToken("token-b")
	if err != nil {
		t.Fatalf("HashToken() unexpected error: %v", err)
	}

	if hash1 == hash2 {
		t.Error("different inputs should produce different hashes")
	}
}

func TestHashToken_ErrorsWithoutHMACKey(t *testing.T) {
	clearHMACKey()
	defer resetHMACKey()

	_, err := HashToken("test-token")
	if err == nil {
		t.Error("HashToken should return an error when HMAC key is not initialized")
	}
	if !errors.Is(err, ErrHMACNotInitialized) {
		t.Errorf("HashToken error = %v, want ErrHMACNotInitialized", err)
	}
}

func TestHashToken_HMACMode(t *testing.T) {
	defer resetHMACKey()

	key := []byte("test-hmac-key-at-least-32-bytes-long")
	InitTokenHMAC(key)

	hash, err := HashToken("test-token")
	if err != nil {
		t.Fatalf("HashToken() unexpected error: %v", err)
	}

	// HMAC-SHA256 also produces 64 hex chars
	if len(hash) != 64 {
		t.Errorf("HMAC hash length = %d, want 64", len(hash))
	}

	// Should only contain hex chars
	for _, c := range hash {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Errorf("HMAC hash contains non-hex character: %c", c)
			break
		}
	}
}

func TestHashToken_HMACMode_Deterministic(t *testing.T) {
	defer resetHMACKey()

	key := []byte("test-hmac-key-at-least-32-bytes-long")
	InitTokenHMAC(key)

	hash1, err := HashToken("same-token")
	if err != nil {
		t.Fatalf("HashToken() unexpected error: %v", err)
	}
	hash2, err := HashToken("same-token")
	if err != nil {
		t.Fatalf("HashToken() unexpected error: %v", err)
	}

	if hash1 != hash2 {
		t.Errorf("HMAC HashToken should be deterministic: %q != %q", hash1, hash2)
	}
}

func TestHashToken_DifferentKeysProduceDifferentHashes(t *testing.T) {
	defer resetHMACKey()

	InitTokenHMAC([]byte("key-one-at-least-32-bytes-long!!"))
	hash1, err := HashToken("test-token")
	if err != nil {
		t.Fatalf("HashToken() unexpected error: %v", err)
	}

	InitTokenHMAC([]byte("key-two-at-least-32-bytes-long!!"))
	hash2, err := HashToken("test-token")
	if err != nil {
		t.Fatalf("HashToken() unexpected error: %v", err)
	}

	if hash1 == hash2 {
		t.Error("different HMAC keys should produce different hashes for the same token")
	}
}

func TestGenerateToken_HMACRoundtrip(t *testing.T) {
	defer resetHMACKey()

	key := []byte("roundtrip-hmac-key-32-bytes-long")
	InitTokenHMAC(key)

	token, hash, err := GenerateToken(32)
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}

	// HashToken should reproduce the same hash
	got, err := HashToken(token)
	if err != nil {
		t.Fatalf("HashToken() unexpected error: %v", err)
	}
	if got != hash {
		t.Errorf("HMAC roundtrip: HashToken(token) = %q, want %q", got, hash)
	}
}

func TestInitTokenHMAC_KeyIsCopied(t *testing.T) {
	defer resetHMACKey()

	key := []byte("mutable-key-32-bytes-long-here!!")
	InitTokenHMAC(key)
	hash1, err := HashToken("test-token")
	if err != nil {
		t.Fatalf("HashToken() unexpected error: %v", err)
	}

	// Mutate the original key slice
	key[0] = 'X'

	hash2, err := HashToken("test-token")
	if err != nil {
		t.Fatalf("HashToken() unexpected error: %v", err)
	}

	// Hash should be unchanged because InitTokenHMAC copied the key
	if hash1 != hash2 {
		t.Error("InitTokenHMAC should copy the key; mutating original slice should not affect hashing")
	}
}
