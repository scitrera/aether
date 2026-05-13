package crypto

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

// hmacKey is the server-side secret used for HMAC-SHA256 token hashing.
// Set via InitTokenHMAC. Must be initialized before any HashToken calls.
var hmacKey []byte
var hmacKeyMu sync.RWMutex

// ErrHMACNotInitialized is returned by HashToken when no HMAC key has been
// configured via InitTokenHMAC. Callers should treat this as a fatal
// misconfiguration at the call site rather than retrying.
var ErrHMACNotInitialized = errors.New("crypto: HMAC key not initialized — call InitTokenHMAC before any token operations")

// InitTokenHMAC sets the HMAC key used for token hashing.
// Must be called before any HashToken calls in production.
// Key should be at least 32 bytes.
func InitTokenHMAC(key []byte) {
	hmacKeyMu.Lock()
	defer hmacKeyMu.Unlock()
	hmacKey = make([]byte, len(key))
	copy(hmacKey, key)
}

// IsHMACInitialized returns true if the HMAC key has been initialized via InitTokenHMAC.
func IsHMACInitialized() bool {
	hmacKeyMu.RLock()
	defer hmacKeyMu.RUnlock()
	return len(hmacKey) > 0
}

// HashToken returns the HMAC-SHA256 hash of the token.
// Returns ErrHMACNotInitialized if the HMAC key has not been initialized via
// InitTokenHMAC. In test environments where HMAC is not needed, call
// InitTokenHMAC with a dummy key before using this function.
func HashToken(token string) (string, error) {
	hmacKeyMu.RLock()
	key := hmacKey
	hmacKeyMu.RUnlock()

	if len(key) == 0 {
		return "", ErrHMACNotInitialized
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(token))
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// GenerateToken creates a cryptographically secure random token string
// and returns both the token and its HMAC-SHA256 hash. Returns an error
// if randomness fails or if the HMAC key has not been initialized.
func GenerateToken(length int) (token string, hash string, err error) {
	tokenBytes := make([]byte, length)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", "", fmt.Errorf("failed to generate random token: %w", err)
	}
	token = base64.URLEncoding.EncodeToString(tokenBytes)
	hash, err = HashToken(token)
	if err != nil {
		return "", "", err
	}
	return token, hash, nil
}
