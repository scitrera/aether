package login

import (
	"net/http"
	"time"
)

// CookieConfig governs the session cookie and the OAuth-state cookie
// emitted/read by the login handlers.
type CookieConfig struct {
	// Name is the session cookie name (e.g. "aether_session",
	// "scitrera_session"). Default: "aether_session".
	Name string
	// Domain optionally pins the cookie to a domain (e.g. "scitrera.ai" so
	// the cookie is shared across *.scitrera.ai). Empty leaves the cookie
	// scoped to the host that issued it.
	Domain string
	// Path defaults to "/".
	Path string
	// Secure should be true in any HTTPS deployment (default true). The
	// auth-proxy logs a warning when Secure is forced off in dev.
	Secure bool
	// SameSite defaults to http.SameSiteLaxMode (suitable for top-level
	// OIDC redirects); use SameSiteNoneMode for cross-site embeds.
	SameSite http.SameSite
	// MaxAge controls the cookie lifetime mirroring the session ExpiresAt.
	// Defaults to 24h.
	MaxAge time.Duration
}

// withDefaults returns c with any zero-value fields populated.
func (c CookieConfig) withDefaults() CookieConfig {
	if c.Name == "" {
		c.Name = "aether_session"
	}
	if c.Path == "" {
		c.Path = "/"
	}
	if c.SameSite == 0 {
		c.SameSite = http.SameSiteLaxMode
	}
	if c.MaxAge == 0 {
		c.MaxAge = 24 * time.Hour
	}
	return c
}

// SetSession writes the session cookie onto w with the configured
// attributes. value is the opaque session id (or signed JWT for the
// stateless store).
func SetSession(w http.ResponseWriter, cfg CookieConfig, value string) {
	cfg = cfg.withDefaults()
	http.SetCookie(w, &http.Cookie{
		Name:     cfg.Name,
		Value:    value,
		Path:     cfg.Path,
		Domain:   cfg.Domain,
		MaxAge:   int(cfg.MaxAge.Seconds()),
		Secure:   cfg.Secure,
		HttpOnly: true,
		SameSite: cfg.SameSite,
	})
}

// ClearSession sets an expired cookie that overrides any active session
// cookie on the client.
func ClearSession(w http.ResponseWriter, cfg CookieConfig) {
	cfg = cfg.withDefaults()
	http.SetCookie(w, &http.Cookie{
		Name:     cfg.Name,
		Value:    "",
		Path:     cfg.Path,
		Domain:   cfg.Domain,
		MaxAge:   -1,
		Secure:   cfg.Secure,
		HttpOnly: true,
		SameSite: cfg.SameSite,
	})
}

// ReadSession returns the session cookie value, or "" if absent.
func ReadSession(r *http.Request, cfg CookieConfig) string {
	cfg = cfg.withDefaults()
	c, err := r.Cookie(cfg.Name)
	if err != nil || c == nil {
		return ""
	}
	return c.Value
}

// stateCookieConfig derives the OAuth-state cookie config from the session
// cookie config. Same Secure/Domain/SameSite, but a short MaxAge (5min) and
// a separate name to avoid collisions.
func stateCookieConfig(base CookieConfig, name string) CookieConfig {
	base = base.withDefaults()
	base.Name = name
	base.MaxAge = 5 * time.Minute
	return base
}
