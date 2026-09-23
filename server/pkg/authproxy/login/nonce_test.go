package login

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

type nonceVerifier struct{ nonce string }

func (v nonceVerifier) Verify(context.Context, string) (*oidc.IDToken, error) {
	return &oidc.IDToken{Nonce: v.nonce}, nil
}

func TestBrowserLoginNonceBoundToProviderAndState(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&Provider{Config: ProviderConfig{Name: "custom"}, OAuth: &oauth2.Config{ClientID: "client", Endpoint: oauth2.Endpoint{AuthURL: "https://issuer.example/authorize"}, RedirectURL: "https://app.example/callback"}, Verifier: nonceVerifier{}})
	store, _ := NewSignedJWTSessionStore([]byte(strings.Repeat("synthetic", 8)), "test")
	handlers, err := NewHandlers(Options{Registry: reg, Store: store, Cookies: CookieConfig{Name: "test", Secure: true}})
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handlers.handleLogin(rr, httptest.NewRequest("GET", "https://app.example/auth/login/custom", nil))
	location, err := url.Parse(rr.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	state, nonce := location.Query().Get("state"), location.Query().Get("nonce")
	if state == "" || nonce == "" || nonce != loginNonce("custom", state) {
		t.Fatal("nonce not included or bound to state")
	}
	if nonce == loginNonce("other", state) || nonce == loginNonce("custom", state+"other") {
		t.Fatal("nonce not domain-separated")
	}
	cookie := rr.Result().Cookies()[0]
	stored, _ := decodeStatePayload(cookie.Value)
	if stored != state {
		t.Fatal("state cookie not retained")
	}
}

func TestCallbackRejectsMissingAndWrongNonceBeforeSession(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"access_token": "synthetic", "token_type": "Bearer", "id_token": "synthetic-id"})
	}))
	defer server.Close()
	for _, nonce := range []string{"", "wrong"} {
		t.Run(nonce, func(t *testing.T) {
			reg := NewRegistry()
			reg.Register(&Provider{Config: ProviderConfig{Name: "custom"}, OAuth: &oauth2.Config{ClientID: "client", Endpoint: oauth2.Endpoint{TokenURL: server.URL}}, Verifier: nonceVerifier{nonce: nonce}})
			store, _ := NewSignedJWTSessionStore([]byte(strings.Repeat("synthetic", 8)), "test")
			created := false
			handlers, _ := NewHandlers(Options{Registry: reg, Store: store, Cookies: CookieConfig{Name: "session", Secure: false}, OnSessionCreated: func(*http.Request, *SessionData) { created = true }})
			request := httptest.NewRequest("GET", "http://app.example/auth/callback/custom?state=expected-state&code=synthetic", nil)
			request.AddCookie(&http.Cookie{Name: handlers.stateCookieName, Value: encodeStatePayload("expected-state", "")})
			rr := httptest.NewRecorder()
			handlers.handleCallback(rr, request)
			if rr.Code != http.StatusUnauthorized || created {
				t.Fatalf("nonce mismatch created session: %d", rr.Code)
			}
			for _, c := range rr.Result().Cookies() {
				if c.Name == "session" && c.Value != "" {
					t.Fatal("session cookie emitted on failure")
				}
			}
		})
	}
}
