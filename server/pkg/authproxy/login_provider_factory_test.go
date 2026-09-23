package authproxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/scitrera/aether/server/pkg/authproxy/login"
	"golang.org/x/oauth2"
)

func TestLoginProviderFactory(t *testing.T) {
	cfg := &LoginConfig{Providers: []login.ProviderConfig{{Name: "custom", IssuerURL: "https://issuer.example", ClientID: "synthetic-client", RedirectURL: "https://app.example/callback"}}}
	called := false
	factory := func(ctx context.Context, pc login.ProviderConfig) (*login.Provider, error) {
		called = true
		if pc.Name != "custom" || pc.ClientID != "synthetic-client" {
			t.Fatal("configuration not passed to factory")
		}
		return &login.Provider{Config: pc, OAuth: &oauth2.Config{ClientID: pc.ClientID}, Verifier: oidc.NewVerifier(pc.IssuerURL, &oidc.StaticKeySet{}, &oidc.Config{ClientID: pc.ClientID})}, nil
	}
	opts := &runOptions{}
	WithLoginProviderFactory(factory)(opts)
	cfg.ProviderFactory = opts.loginProviderFactory
	reg, err := cfg.BuildRegistry(context.Background())
	if err != nil || !called || reg.Lookup("custom") == nil {
		t.Fatalf("factory not used: %v", err)
	}
}

func TestLoginProviderFactoryFailsClosed(t *testing.T) {
	sentinel := errors.New("synthetic discovery failure")
	cases := []struct {
		name     string
		provider *login.Provider
		err      error
	}{
		{name: "nil result"},
		{name: "discovery error", err: sentinel},
		{name: "missing OAuth", provider: &login.Provider{Config: login.ProviderConfig{Name: "custom"}}},
		{name: "missing verifier", provider: &login.Provider{Config: login.ProviderConfig{Name: "custom"}, OAuth: &oauth2.Config{}}},
		{name: "wrong identity", provider: &login.Provider{Config: login.ProviderConfig{Name: "wrong"}, OAuth: &oauth2.Config{}, Verifier: oidc.NewVerifier("https://issuer.example", &oidc.StaticKeySet{}, &oidc.Config{ClientID: "client"})}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &LoginConfig{Providers: []login.ProviderConfig{{Name: "custom"}}, ProviderFactory: func(context.Context, login.ProviderConfig) (*login.Provider, error) { return tc.provider, tc.err }}
			if _, err := cfg.BuildRegistry(context.Background()); err == nil {
				t.Fatal("invalid factory result accepted")
			} else if tc.err != nil && !errors.Is(err, sentinel) {
				t.Fatalf("factory error lost: %v", err)
			}
		})
	}
}

func TestDefaultLoginProviderKeepsStrictDiscovery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"issuer": "https://different.example", "authorization_endpoint": "https://different.example/auth", "token_endpoint": "https://different.example/token", "jwks_uri": "https://different.example/keys"})
	}))
	defer server.Close()
	cfg := &LoginConfig{Providers: []login.ProviderConfig{{Name: "standard", IssuerURL: server.URL, ClientID: "client", RedirectURL: "https://app.example/callback"}}}
	if _, err := cfg.BuildRegistry(context.Background()); err == nil {
		t.Fatal("default provider accepted issuer mismatch")
	}
}
