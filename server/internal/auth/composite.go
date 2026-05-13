package auth

import (
	"context"
	"fmt"

	"github.com/scitrera/aether/internal/logging"
)

// CompositeAuthenticator tries multiple authenticators in order
type CompositeAuthenticator struct {
	authenticators []Authenticator
}

// NewCompositeAuthenticator creates a composite that tries each authenticator in order
func NewCompositeAuthenticator(authenticators ...Authenticator) *CompositeAuthenticator {
	return &CompositeAuthenticator{authenticators: authenticators}
}

// Add appends an authenticator to the composite chain. Order matters: the
// first authenticator that returns a non-nil result wins. Add is intended
// for one-time setup at startup; it is not safe for concurrent use.
func (c *CompositeAuthenticator) Add(a Authenticator) {
	if a == nil {
		return
	}
	c.authenticators = append(c.authenticators, a)
}

// Authenticate tries each authenticator in order.
// Returns the first successful result.
// Returns error only if credentials were provided but all relevant authenticators rejected them.
// Returns (nil, nil) if no authenticators matched the credentials at all.
func (c *CompositeAuthenticator) Authenticate(ctx context.Context, credentials map[string]string) (*AuthResult, error) {
	var lastErr error
	for _, auth := range c.authenticators {
		result, err := auth.Authenticate(ctx, credentials)
		if err != nil {
			logging.Logger.Debug().Err(err).Str("authenticator", auth.Name()).Msg("authenticator failed")
			lastErr = err
			continue
		}
		if result != nil {
			return result, nil
		}
		// result == nil && err == nil means this authenticator doesn't apply
	}
	if lastErr != nil {
		return nil, fmt.Errorf("authentication failed: %w", lastErr)
	}
	return nil, nil // No authenticator matched
}
