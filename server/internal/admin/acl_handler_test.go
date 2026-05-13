package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestListACLRules(t *testing.T) {
	now := time.Now()
	mock := &mockStateProvider{
		aclRules: []*ACLRuleInfo{
			{
				RuleID:        "rule-1",
				PrincipalType: "agent",
				PrincipalID:   "ag::default::myagent::main",
				ResourceType:  "workspace",
				ResourceID:    "default",
				AccessLevel:   2,
				GrantedBy:     "admin",
				GrantedAt:     now,
			},
		},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/acl/rules", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if count, _ := resp["count"].(float64); count != 1 {
		t.Errorf("expected count=1, got %v", resp["count"])
	}
}

func TestListACLRules_Error(t *testing.T) {
	mock := &mockStateProvider{aclRulesErr: fmt.Errorf("db error")}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/acl/rules", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestGrantACLAccess_ValidBody(t *testing.T) {
	now := time.Now()
	mock := &mockStateProvider{
		grantACLRule: &ACLRuleInfo{
			RuleID:        "rule-new",
			PrincipalType: "agent",
			PrincipalID:   "ag::default::foo::bar",
			ResourceType:  "workspace",
			ResourceID:    "default",
			AccessLevel:   1,
			GrantedBy:     "admin",
			GrantedAt:     now,
		},
	}
	_, router := newTestServer(t, mock)

	body := `{
		"principal_type": "agent",
		"principal_id": "ag::default::foo::bar",
		"resource_type": "workspace",
		"resource_id": "default",
		"access_level": 1,
		"granted_by": "admin"
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/acl/rules", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", rec.Code)
	}
}

func TestGrantACLAccess_InvalidJSON(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/acl/rules", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestGrantACLAccess_MissingPrincipalType(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{
		"principal_id": "ag::default::foo::bar",
		"resource_type": "workspace",
		"resource_id": "default",
		"granted_by": "admin"
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/acl/rules", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestGrantACLAccess_MissingGrantedBy(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{
		"principal_type": "agent",
		"principal_id": "ag::default::foo::bar",
		"resource_type": "workspace",
		"resource_id": "default"
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/acl/rules", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestRevokeACLAccess(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/acl/rules/rule-1?principal_type=agent&principal_id=ag.default.foo.bar&resource_type=workspace&resource_id=default", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestRevokeACLAccess_MissingParams(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	// Missing principal_id and resource params
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/acl/rules/rule-1?principal_type=agent", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestGetACLFallbackPolicy(t *testing.T) {
	now := time.Now()
	mock := &mockStateProvider{
		fallbackPolicy: &ACLFallbackPolicyInfo{
			PolicyID:            "pol-1",
			RuleCategory:        "messaging",
			FallbackAccessLevel: 0,
			UpdatedBy:           "admin",
			UpdatedAt:           now,
		},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/acl/fallback-policy?rule_category=messaging", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var policy ACLFallbackPolicyInfo
	if err := json.NewDecoder(rec.Body).Decode(&policy); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if policy.RuleCategory != "messaging" {
		t.Errorf("expected rule_category=messaging, got %s", policy.RuleCategory)
	}
}

func TestGetACLFallbackPolicy_MissingCategory(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/acl/fallback-policy", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestGetACLFallbackPolicy_NotFound(t *testing.T) {
	mock := &mockStateProvider{
		fallbackPolicyErr: fmt.Errorf("policy not found"),
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/acl/fallback-policy?rule_category=messaging", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestListACLAuthorityGrants(t *testing.T) {
	now := time.Now()
	mock := &mockStateProvider{
		authorityGrants: []*ACLAuthorityGrantInfo{
			{
				GrantID:        "grant-1",
				RootGrantID:    "grant-1",
				Subject:        &PrincipalRef{PrincipalType: "user", PrincipalID: "alice"},
				Delegate:       &PrincipalRef{PrincipalType: "service", PrincipalID: "sv::frontend::api-1"},
				IssuedBy:       &PrincipalRef{PrincipalType: "service", PrincipalID: "sv::gateway::admin"},
				RootSubject:    &PrincipalRef{PrincipalType: "user", PrincipalID: "alice"},
				MaxAccessLevel: 20,
				AudienceType:   "session",
				AudienceID:     "session-1",
				ExpiresAt:      now.Add(time.Hour),
				RenewableUntil: now.Add(2 * time.Hour),
				CreatedAt:      now,
			},
		},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/acl/authority-grants", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if count, _ := resp["count"].(float64); count != 1 {
		t.Errorf("expected count=1, got %v", resp["count"])
	}
}

func TestCreateACLAuthorityGrant(t *testing.T) {
	now := time.Now()
	mock := &mockStateProvider{
		createAuthorityGrant: &ACLAuthorityGrantInfo{
			GrantID:        "grant-new",
			RootGrantID:    "grant-new",
			Subject:        &PrincipalRef{PrincipalType: "user", PrincipalID: "alice"},
			Delegate:       &PrincipalRef{PrincipalType: "service", PrincipalID: "sv::frontend::api-1"},
			IssuedBy:       &PrincipalRef{PrincipalType: "service", PrincipalID: "sv::gateway::admin"},
			RootSubject:    &PrincipalRef{PrincipalType: "user", PrincipalID: "alice"},
			MaxAccessLevel: 20,
			AudienceType:   "session",
			AudienceID:     "session-1",
			ExpiresAt:      now.Add(time.Hour),
			RenewableUntil: now.Add(2 * time.Hour),
			CreatedAt:      now,
		},
	}
	_, router := newTestServer(t, mock)

	body := fmt.Sprintf(`{
		"subject": {"principal_type": "user", "principal_id": "alice"},
		"delegate": {"principal_type": "service", "principal_id": "sv::frontend::api-1"},
		"issued_by": {"principal_type": "service", "principal_id": "sv::gateway::admin"},
		"may_delegate": true,
		"remaining_hops": 2,
		"max_access_level": 20,
		"audience_type": "session",
		"audience_id": "session-1",
		"expires_at": %q,
		"renewable_until": %q
	}`, now.Add(time.Hour).Format(time.RFC3339), now.Add(2*time.Hour).Format(time.RFC3339))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/acl/authority-grants", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", rec.Code)
	}
}

func TestRenewACLAuthorityGrant(t *testing.T) {
	now := time.Now()
	mock := &mockStateProvider{
		renewAuthorityGrant: &ACLAuthorityGrantInfo{
			GrantID:        "grant-1",
			RootGrantID:    "grant-1",
			Subject:        &PrincipalRef{PrincipalType: "user", PrincipalID: "alice"},
			Delegate:       &PrincipalRef{PrincipalType: "service", PrincipalID: "sv::frontend::api-1"},
			IssuedBy:       &PrincipalRef{PrincipalType: "service", PrincipalID: "sv::gateway::admin"},
			RootSubject:    &PrincipalRef{PrincipalType: "user", PrincipalID: "alice"},
			MaxAccessLevel: 20,
			AudienceType:   "session",
			AudienceID:     "session-1",
			ExpiresAt:      now.Add(2 * time.Hour),
			RenewableUntil: now.Add(3 * time.Hour),
			RenewedAt:      ptrTime(now),
			CreatedAt:      now,
		},
	}
	_, router := newTestServer(t, mock)

	body := fmt.Sprintf(`{"expires_at": %q}`, now.Add(2*time.Hour).Format(time.RFC3339))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/acl/authority-grants/grant-1/renew", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestSetACLFallbackPolicy(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"rule_category":"messaging","fallback_access_level":0,"updated_by":"admin"}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/acl/fallback-policy", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestSetACLFallbackPolicy_MissingCategory(t *testing.T) {
	_, router := newTestServer(t, &mockStateProvider{})

	body := `{"updated_by":"admin"}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/acl/fallback-policy", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestCleanupExpiredACLRules(t *testing.T) {
	mock := &mockStateProvider{cleanupExpiredCount: 5}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/acl/cleanup/expired-rules", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if count, _ := resp["count"].(float64); count != 5 {
		t.Errorf("expected count=5, got %v", resp["count"])
	}
}

func TestQueryACLAuditLog(t *testing.T) {
	now := time.Now()
	mock := &mockStateProvider{
		auditLogEntries: []*ACLAuditLogEntryInfo{
			{
				AuditID:       1,
				Timestamp:     now,
				Decision:      "ALLOW",
				PrincipalType: "agent",
				PrincipalID:   "ag::default::foo::bar",
				ResourceType:  "workspace",
				ResourceID:    "default",
				Operation:     "connect",
				GatewayID:     "gw-1",
			},
		},
	}
	_, router := newTestServer(t, mock)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/acl/audit", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if count, _ := resp["count"].(float64); count != 1 {
		t.Errorf("expected count=1, got %v", resp["count"])
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
}
