package login

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// SignedJWTSessionStore is an alternative SessionStore that places a signed
// JWT directly in the cookie — no server-side state lookup. Loses immediate
// revocation (sessions live until exp) but eases multi-replica deployments
// because no Redis is required for session validation.
//
// The cookie payload IS the JWT; the "id" parameter to Get/Delete is
// therefore the JWT itself. New() returns the JWT.
type SignedJWTSessionStore struct {
	signingKey []byte
	issuer     string
}

// NewSignedJWTSessionStore returns a JWT-cookie session store. signingKey
// must be at least 32 bytes (HS256 spec). issuer is stamped into the JWT
// "iss" claim and verified on Get.
func NewSignedJWTSessionStore(signingKey []byte, issuer string) (*SignedJWTSessionStore, error) {
	if len(signingKey) < 32 {
		return nil, fmt.Errorf("signing key must be at least 32 bytes (got %d)", len(signingKey))
	}
	if issuer == "" {
		issuer = "aether-auth-proxy"
	}
	return &SignedJWTSessionStore{signingKey: signingKey, issuer: issuer}, nil
}

// Name implements SessionStore.
func (s *SignedJWTSessionStore) Name() string { return "signed_jwt" }

// jwtSessionClaims is the JWT payload shape. The full SessionData is embedded
// as a JSON-encoded claim ("ses") rather than spread across many top-level
// claims so unmarshalling round-trips cleanly.
type jwtSessionClaims struct {
	Session json.RawMessage `json:"ses"`
	jwt.RegisteredClaims
}

// New implements SessionStore. Returns the signed JWT as the "id".
func (s *SignedJWTSessionStore) New(_ context.Context, data *SessionData) (string, error) {
	if data == nil {
		return "", errors.New("nil session data")
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("marshal session: %w", err)
	}
	now := time.Now()
	exp := data.ExpiresAt
	if exp.IsZero() {
		exp = now.Add(24 * time.Hour)
	}
	claims := jwtSessionClaims{
		Session: payload,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.issuer,
			Subject:   data.UserID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString(s.signingKey)
	if err != nil {
		return "", fmt.Errorf("sign jwt session: %w", err)
	}
	return signed, nil
}

// Get implements SessionStore. Returns (nil, nil) for any verification
// failure (invalid signature, expired, malformed) so misuse maps to "no
// session" rather than 500-level noise.
func (s *SignedJWTSessionStore) Get(_ context.Context, id string) (*SessionData, error) {
	if id == "" {
		return nil, nil
	}
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"HS256"}), jwt.WithIssuer(s.issuer), jwt.WithExpirationRequired())
	tok, err := parser.ParseWithClaims(id, &jwtSessionClaims{}, func(t *jwt.Token) (any, error) {
		return s.signingKey, nil
	})
	if err != nil || !tok.Valid {
		return nil, nil
	}
	claims, ok := tok.Claims.(*jwtSessionClaims)
	if !ok || len(claims.Session) == 0 {
		return nil, nil
	}
	var data SessionData
	if err := json.Unmarshal(claims.Session, &data); err != nil {
		return nil, nil
	}
	if data.IsExpired() {
		return nil, nil
	}
	return &data, nil
}

// Delete is a no-op for stateless JWT cookies. Callers that need
// hard-revocation should use RedisOpaqueSessionStore.
func (s *SignedJWTSessionStore) Delete(_ context.Context, _ string) error {
	return nil
}
