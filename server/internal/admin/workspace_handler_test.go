package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Workspace handler tests exercise the StateProvider-backed workspace CRUD
// endpoints (list, get, create, update, delete, message-flow). The shared
// mockStateProvider in server_test.go supplies stub responses.

func TestListWorkspaces_Empty(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if count, _ := resp["count"].(float64); count != 0 {
		t.Errorf("expected count=0, got %v", resp["count"])
	}
}

func TestListWorkspaces_Multiple(t *testing.T) {
	mock := &mockStateProvider{
		workspaces: []*WorkspaceInfo{
			{WorkspaceID: "ws-1", DisplayName: "Workspace One"},
			{WorkspaceID: "ws-2", DisplayName: "Workspace Two"},
		},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if count, _ := resp["count"].(float64); count != 2 {
		t.Errorf("expected count=2, got %v", resp["count"])
	}
}

func TestListWorkspaces_BackendError(t *testing.T) {
	mock := &mockStateProvider{workspacesErr: fmt.Errorf("db down")}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestGetWorkspace_Found(t *testing.T) {
	mock := &mockStateProvider{
		workspaceByID: &WorkspaceInfo{WorkspaceID: "prod", DisplayName: "Production"},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces/prod", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var got WorkspaceInfo
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if got.WorkspaceID != "prod" {
		t.Errorf("expected workspace_id=prod, got %s", got.WorkspaceID)
	}
}

func TestGetWorkspace_NotFound(t *testing.T) {
	mock := &mockStateProvider{workspaceByIDErr: fmt.Errorf("workspace not found")}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces/nonexistent", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestCreateWorkspace_OK(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"workspace_id":"newws","display_name":"New Workspace","description":"d","tenant_id":"t1"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", rec.Code)
	}
}

func TestCreateWorkspace_InvalidJSON(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestCreateWorkspace_MissingID_Validation(t *testing.T) {
	// Validation failure: workspace_id is required. (server_test.go already
	// covers the same case under the older name; this test re-asserts it
	// alongside the rest of the workspace-handler coverage.)
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"display_name":"No ID"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestCreateWorkspace_BackendError(t *testing.T) {
	mock := &mockStateProvider{createWorkspaceErr: fmt.Errorf("conflict")}
	_, router := newTestServer(t, mock)

	body := `{"workspace_id":"newws","display_name":"x"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestUpdateWorkspace_OK(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"display_name":"Renamed","description":"new desc"}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/workspaces/prod", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestUpdateWorkspace_BackendError(t *testing.T) {
	mock := &mockStateProvider{updateWorkspaceErr: fmt.Errorf("update failed")}
	_, router := newTestServer(t, mock)

	body := `{"display_name":"Renamed"}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/workspaces/prod", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestUpdateWorkspace_InvalidJSON(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodPut, "/api/v1/workspaces/prod", strings.NewReader("garbage"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestDeleteWorkspace_OK(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/workspaces/old", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestDeleteWorkspace_BackendError(t *testing.T) {
	mock := &mockStateProvider{deleteWorkspaceErr: fmt.Errorf("cannot delete")}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/workspaces/old", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestGetMessageFlow_OK(t *testing.T) {
	mock := &mockStateProvider{
		messageFlow: &MessageFlowInfo{},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces/prod/message-flow", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestGetMessageFlow_BackendError(t *testing.T) {
	mock := &mockStateProvider{messageFlowErr: fmt.Errorf("nope")}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/workspaces/prod/message-flow", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

// =============================================================================
// Auth (401) for workspace endpoints when APIKey is configured.
// =============================================================================

func TestWorkspaceEndpoints_Unauthorized(t *testing.T) {
	cfg := ServerConfig{
		APIKey:         "secret-key",
		InsecureNoAuth: true,
		RateLimit:      1000,
		RateLimitBurst: 1000,
	}
	srv := NewServer(cfg, &mockStateProvider{})
	router := buildTestRouter(srv, true)
	t.Cleanup(func() { srv.rateLimiter.Close() })

	endpoints := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/api/v1/workspaces", ""},
		{http.MethodGet, "/api/v1/workspaces/prod", ""},
		{http.MethodPost, "/api/v1/workspaces", `{"workspace_id":"x"}`},
		{http.MethodPut, "/api/v1/workspaces/prod", `{}`},
		{http.MethodDelete, "/api/v1/workspaces/prod", ""},
	}

	for _, ep := range endpoints {
		var bodyReader = strings.NewReader(ep.body)
		req := httptest.NewRequest(ep.method, ep.path, bodyReader)
		if ep.body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s: expected 401, got %d", ep.method, ep.path, rec.Code)
		}
	}
}

// Sanity: ensure context.Context type stays imported for any future test that
// needs a non-Background context.
var _ = context.Background
