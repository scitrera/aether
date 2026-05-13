package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Token handler tests exercise the StateProvider-backed token management.
// The mock returns empty/error responses to verify routing and error handling.

func TestListTokens_Empty(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tokens", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if int(resp["count"].(float64)) != 0 {
		t.Errorf("expected 0 tokens, got %v", resp["count"])
	}
}

func TestCreateToken_Error(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"name":"my-token","principal_type":"agent"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 (mock returns error), got %d", rec.Code)
	}
}

func TestCreateToken_InvalidJSON(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestCreateToken_MissingName(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"principal_type":"agent"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 (missing name), got %d", rec.Code)
	}
}

func TestCreateToken_MissingPrincipalType(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"name":"my-token"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 (missing principal_type), got %d", rec.Code)
	}
}

func TestGetToken_NotFound(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tokens/token-123", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestDeleteToken_Error(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tokens/token-123", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 (mock returns error), got %d", rec.Code)
	}
}

func TestRevokeToken_Error(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens/token-123/revoke", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 (mock returns error), got %d", rec.Code)
	}
}
