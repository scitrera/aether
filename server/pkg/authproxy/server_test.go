package authproxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleHealthz(t *testing.T) {
	cfg := &Config{Mode: ModeVerify, ListenAddr: ":0"}
	s := &Server{cfg: cfg}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	s.handleHealthz(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected application/json, got %s", ct)
	}
	if body := w.Body.String(); body != `{"status":"ok"}` {
		t.Errorf("unexpected body: %s", body)
	}
}

func TestNewServer_VerifyMode(t *testing.T) {
	cfg := &Config{Mode: ModeVerify, ListenAddr: ":0"}
	m := &AuthMiddleware{tenantID: "test"}

	srv, err := NewServer(cfg, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.proxy != nil {
		t.Error("verify mode should not have a reverse proxy")
	}
}

func TestNewServer_ProxyMode(t *testing.T) {
	cfg := &Config{Mode: ModeProxy, ListenAddr: ":0", BackendURL: "http://localhost:9999"}
	m := &AuthMiddleware{tenantID: "test"}

	srv, err := NewServer(cfg, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if srv.proxy == nil {
		t.Error("proxy mode should have a reverse proxy")
	}
}

func TestNewServer_InvalidBackendURL(t *testing.T) {
	cfg := &Config{Mode: ModeProxy, ListenAddr: ":0", BackendURL: "://invalid"}
	m := &AuthMiddleware{tenantID: "test"}

	_, err := NewServer(cfg, m)
	if err == nil {
		t.Fatal("expected error for invalid backend URL")
	}
}

func TestVerifyMode_NotFoundOnRoot(t *testing.T) {
	cfg := &Config{Mode: ModeVerify, ListenAddr: ":0"}
	m := &AuthMiddleware{tenantID: "test"}

	srv, err := NewServer(cfg, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/some-path", nil)

	srv.httpServer.Handler.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for non-verify path in verify mode, got %d", w.Code)
	}
}

func TestHealthzRouting(t *testing.T) {
	cfg := &Config{Mode: ModeProxy, ListenAddr: ":0", BackendURL: "http://localhost:9999"}
	m := &AuthMiddleware{tenantID: "test"}

	srv, err := NewServer(cfg, m)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	srv.httpServer.Handler.ServeHTTP(w, r)

	// /healthz should return 200 without auth
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for /healthz, got %d", w.Code)
	}
}
