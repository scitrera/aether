package login

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/scitrera/aether/internal/logging"
)

// Handlers exposes the login flow's HTTP endpoints. Wire it into the server
// router via Handlers.Mount(mux).
type Handlers struct {
	registry *Registry
	store    SessionStore
	cookies  CookieConfig
	// stateCookieName is the short-lived cookie used to bind the OAuth
	// state parameter to the browser session. Default: "aether_oauth_state".
	stateCookieName string
	// onSessionCreated is an optional hook invoked after a successful
	// session creation (e.g. for audit logging).
	onSessionCreated func(*http.Request, *SessionData)
}

// Options configures the login handlers.
type Options struct {
	Registry         *Registry
	Store            SessionStore
	Cookies          CookieConfig
	StateCookieName  string
	OnSessionCreated func(*http.Request, *SessionData)
}

// NewHandlers returns a configured Handlers. registry and store are required.
func NewHandlers(opts Options) (*Handlers, error) {
	if opts.Registry == nil || len(opts.Registry.providers) == 0 {
		return nil, fmt.Errorf("login.NewHandlers: registry must contain at least one provider")
	}
	if opts.Store == nil {
		return nil, fmt.Errorf("login.NewHandlers: session store is required")
	}
	if opts.StateCookieName == "" {
		opts.StateCookieName = "aether_oauth_state"
	}
	return &Handlers{
		registry:         opts.Registry,
		store:            opts.Store,
		cookies:          opts.Cookies.withDefaults(),
		stateCookieName:  opts.StateCookieName,
		onSessionCreated: opts.OnSessionCreated,
	}, nil
}

// Mount registers the login routes on mux:
//
//	GET  /auth/login/{provider}     — redirect browser to provider's authorize endpoint
//	GET  /auth/callback/{provider}  — verify code, set session cookie, redirect
//	POST /auth/logout                — clear session cookie + delete server-side
//	GET  /auth/me                    — JSON dump of current session (debug)
//	GET  /auth/checkz                — JSON: {auth: "valid", session: {...}}
func (h *Handlers) Mount(mux *http.ServeMux) {
	mux.HandleFunc("/auth/login/", h.handleLogin)
	mux.HandleFunc("/auth/callback/", h.handleCallback)
	mux.HandleFunc("/auth/logout", h.handleLogout)
	mux.HandleFunc("/auth/me", h.handleMe)
	mux.HandleFunc("/auth/checkz", h.handleCheckz)
}

// handleLogin redirects to the provider's authorize endpoint with a freshly
// minted state nonce stored in a short-lived cookie. The "next" query param,
// if present and same-origin, is stashed in the state cookie so the
// callback can route the browser back where it came from.
func (h *Handlers) handleLogin(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/auth/login/")
	if name == "" {
		http.Error(w, `{"error":"missing provider"}`, http.StatusBadRequest)
		return
	}
	prov := h.registry.Lookup(name)
	if prov == nil {
		http.Error(w, fmt.Sprintf(`{"error":"unknown provider %q"}`, name), http.StatusNotFound)
		return
	}

	state, err := newOpaqueID(24)
	if err != nil {
		logging.Logger.Error().Err(err).Msg("login: failed to mint state nonce")
		http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
		return
	}
	stateCfg := stateCookieConfig(h.cookies, h.stateCookieName)
	statePayload := encodeStatePayload(state, r.URL.Query().Get("next"))
	http.SetCookie(w, &http.Cookie{
		Name:     stateCfg.Name,
		Value:    statePayload,
		Path:     stateCfg.Path,
		Domain:   stateCfg.Domain,
		MaxAge:   int(stateCfg.MaxAge.Seconds()),
		Secure:   stateCfg.Secure,
		HttpOnly: true,
		SameSite: stateCfg.SameSite,
	})

	url := prov.OAuth.AuthCodeURL(state)
	http.Redirect(w, r, url, http.StatusFound)
}

// handleCallback validates the state, exchanges the code, verifies the
// id_token, persists a new session, and redirects to the post-login
// destination ("next" param at login time, or "/").
func (h *Handlers) handleCallback(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/auth/callback/")
	if name == "" {
		http.Error(w, `{"error":"missing provider"}`, http.StatusBadRequest)
		return
	}
	prov := h.registry.Lookup(name)
	if prov == nil {
		http.Error(w, fmt.Sprintf(`{"error":"unknown provider %q"}`, name), http.StatusNotFound)
		return
	}

	stateCfg := stateCookieConfig(h.cookies, h.stateCookieName)
	stateCookie, err := r.Cookie(stateCfg.Name)
	if err != nil || stateCookie.Value == "" {
		http.Error(w, `{"error":"missing state cookie"}`, http.StatusBadRequest)
		return
	}
	expectedState, next := decodeStatePayload(stateCookie.Value)
	if r.URL.Query().Get("state") != expectedState || expectedState == "" {
		http.Error(w, `{"error":"state mismatch"}`, http.StatusForbidden)
		return
	}
	// One-shot: clear the state cookie regardless of outcome.
	http.SetCookie(w, &http.Cookie{
		Name:     stateCfg.Name,
		Value:    "",
		Path:     stateCfg.Path,
		Domain:   stateCfg.Domain,
		MaxAge:   -1,
		Secure:   stateCfg.Secure,
		HttpOnly: true,
		SameSite: stateCfg.SameSite,
	})

	if errParam := r.URL.Query().Get("error"); errParam != "" {
		desc := r.URL.Query().Get("error_description")
		logging.Logger.Warn().Str("provider", name).Str("error", errParam).Str("description", desc).Msg("login: provider returned error")
		http.Error(w, fmt.Sprintf(`{"error":"provider error: %s"}`, errParam), http.StatusUnauthorized)
		return
	}

	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, `{"error":"missing code"}`, http.StatusBadRequest)
		return
	}

	subject, claims, err := prov.VerifyCallback(r.Context(), code)
	if err != nil {
		logging.Logger.Warn().Err(err).Str("provider", name).Msg("login: callback verification failed")
		http.Error(w, `{"error":"verification failed"}`, http.StatusUnauthorized)
		return
	}

	now := time.Now()
	data := &SessionData{
		UserID:    canonicalUserID(claims, subject),
		Email:     stringClaim(claims, "email"),
		Name:      stringClaim(claims, "name"),
		Provider:  name,
		Claims:    claims,
		IssuedAt:  now,
		ExpiresAt: now.Add(h.cookies.MaxAge),
	}
	id, err := h.store.New(r.Context(), data)
	if err != nil {
		logging.Logger.Error().Err(err).Str("provider", name).Msg("login: failed to persist session")
		http.Error(w, `{"error":"session persistence failed"}`, http.StatusInternalServerError)
		return
	}
	SetSession(w, h.cookies, id)

	if h.onSessionCreated != nil {
		h.onSessionCreated(r, data)
	}

	dest := next
	if dest == "" || !strings.HasPrefix(dest, "/") {
		dest = "/"
	}
	http.Redirect(w, r, dest, http.StatusFound)
}

// canonicalUserID returns email when present and looks like an email, else the
// id_token sub. Most multi-tenant resolvers index on email, so this is the
// pragmatic default.
func canonicalUserID(claims map[string]any, sub string) string {
	if e := stringClaim(claims, "email"); e != "" && strings.Contains(e, "@") {
		return e
	}
	if upn := stringClaim(claims, "upn"); upn != "" && strings.Contains(upn, "@") {
		return upn
	}
	if pu := stringClaim(claims, "preferred_username"); pu != "" && strings.Contains(pu, "@") {
		return pu
	}
	return sub
}

// handleLogout removes the session record (best-effort) and clears the cookie.
func (h *Handlers) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	id := ReadSession(r, h.cookies)
	if id != "" {
		_ = h.store.Delete(r.Context(), id)
	}
	ClearSession(w, h.cookies)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// handleMe returns the current session as JSON. Useful for debugging and as
// a frontend "current user" endpoint. Returns 401 when no session.
func (h *Handlers) handleMe(w http.ResponseWriter, r *http.Request) {
	data, err := h.lookupSession(r)
	if err != nil {
		http.Error(w, `{"error":"session lookup failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if data == nil {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"auth":"none"}`))
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"auth":     "valid",
		"user_id":  data.UserID,
		"email":    data.Email,
		"name":     data.Name,
		"provider": data.Provider,
		"expires":  data.ExpiresAt,
	})
}

// handleCheckz mirrors scitrera_forward_auth's /checkz JSON response shape so
// existing frontends keep working unchanged: {auth: "valid"} on success,
// non-200 with {error: "..."} otherwise. Identity headers are NOT set here;
// nginx forward-auth path uses /auth/verify for that purpose.
func (h *Handlers) handleCheckz(w http.ResponseWriter, r *http.Request) {
	data, err := h.lookupSession(r)
	if err != nil {
		http.Error(w, `{"error":"session lookup failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if data == nil {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"no session"}`))
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"auth":     "valid",
		"user_id":  data.UserID,
		"email":    data.Email,
		"provider": data.Provider,
	})
}

// lookupSession returns the SessionData for the request, or (nil, nil) when
// no cookie is present or the session has expired/been revoked.
func (h *Handlers) lookupSession(r *http.Request) (*SessionData, error) {
	id := ReadSession(r, h.cookies)
	if id == "" {
		return nil, nil
	}
	return h.store.Get(r.Context(), id)
}

// encodeStatePayload combines the OAuth state nonce with an optional "next"
// destination. The result is "<base64(state)>|<base64(next)>" — using base64
// in each half guarantees neither contains '|' so the splitter is unambiguous,
// and round-trips cleanly even when next is empty.
func encodeStatePayload(state, next string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(state)) + "|" + base64.RawURLEncoding.EncodeToString([]byte(next))
}

// decodeStatePayload reverses encodeStatePayload. Returns ("", "") on any
// malformed input.
func decodeStatePayload(payload string) (state, next string) {
	i := strings.IndexByte(payload, '|')
	if i < 0 {
		return "", ""
	}
	stateBytes, err := base64.RawURLEncoding.DecodeString(payload[:i])
	if err != nil {
		return "", ""
	}
	nextBytes, err := base64.RawURLEncoding.DecodeString(payload[i+1:])
	if err != nil {
		return "", ""
	}
	return string(stateBytes), string(nextBytes)
}

// _ silences unused-import linters: claimsAsJSON is held in reserve for
// trace-level logging on the OIDC verification path.
var _ = claimsAsJSON
