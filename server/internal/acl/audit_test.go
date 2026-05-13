package acl

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/pkg/models"
)

// --- buildACLMetadata ---

func TestBuildACLMetadata_ContainsRequiredFields(t *testing.T) {
	entry := &AuditLogEntry{
		Decision:        DecisionAllow,
		AccessLevel:     AccessReadWrite,
		FallbackApplied: false,
		Metadata:        map[string]interface{}{},
	}

	raw, err := buildACLMetadata(entry)
	if err != nil {
		t.Fatalf("buildACLMetadata() error = %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("failed to unmarshal metadata JSON: %v", err)
	}

	if m["decision"] != DecisionAllow {
		t.Errorf("metadata[decision] = %v, want %s", m["decision"], DecisionAllow)
	}
	if int(m["access_level"].(float64)) != AccessReadWrite {
		t.Errorf("metadata[access_level] = %v, want %d", m["access_level"], AccessReadWrite)
	}
	if m["fallback_applied"] != false {
		t.Errorf("metadata[fallback_applied] = %v, want false", m["fallback_applied"])
	}
}

func TestBuildACLMetadata_IncludesRuleIDWhenPresent(t *testing.T) {
	ruleID := "rule-xyz-789"
	entry := &AuditLogEntry{
		Decision:    DecisionAllow,
		AccessLevel: AccessRead,
		RuleID:      &ruleID,
		Metadata:    map[string]interface{}{},
	}

	raw, err := buildACLMetadata(entry)
	if err != nil {
		t.Fatalf("buildACLMetadata() error = %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("failed to unmarshal metadata JSON: %v", err)
	}

	if m["rule_id"] != ruleID {
		t.Errorf("metadata[rule_id] = %v, want %s", m["rule_id"], ruleID)
	}
}

func TestBuildACLMetadata_OmitsRuleIDWhenAbsent(t *testing.T) {
	entry := &AuditLogEntry{
		Decision:    DecisionDeny,
		AccessLevel: AccessNone,
		RuleID:      nil,
		Metadata:    map[string]interface{}{},
	}

	raw, err := buildACLMetadata(entry)
	if err != nil {
		t.Fatalf("buildACLMetadata() error = %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("failed to unmarshal metadata JSON: %v", err)
	}

	if _, exists := m["rule_id"]; exists {
		t.Error("rule_id should be absent when RuleID is nil")
	}
}

func TestBuildACLMetadata_PreservesExtraMetadataFields(t *testing.T) {
	entry := &AuditLogEntry{
		Decision:    DecisionAllow,
		AccessLevel: AccessRead,
		Metadata: map[string]interface{}{
			"custom_key": "custom_value",
			"count":      42,
		},
	}

	raw, err := buildACLMetadata(entry)
	if err != nil {
		t.Fatalf("buildACLMetadata() error = %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("failed to unmarshal metadata JSON: %v", err)
	}

	if m["custom_key"] != "custom_value" {
		t.Errorf("metadata[custom_key] = %v, want custom_value", m["custom_key"])
	}
}

// --- AuditLogger.buildEntry ---

func TestBuildEntry_PopulatesAllFields(t *testing.T) {
	logger := &AuditLogger{gatewayID: "gw-001"}

	decision := &ACLDecision{
		Decision:             DecisionAllow,
		EffectiveAccessLevel: AccessReadWrite,
		FallbackApplied:      true,
	}
	principal := models.Identity{Type: models.PrincipalUser, ID: "alice"}
	sessionID := uuid.New()
	now := time.Now()

	entry := logger.buildEntry(decision, principal, ResourceTypeWorkspace, "prod", "connect", "prod", sessionID)

	if entry.Decision != DecisionAllow {
		t.Errorf("Decision = %q, want ALLOW", entry.Decision)
	}
	if entry.AccessLevel != AccessReadWrite {
		t.Errorf("AccessLevel = %d, want %d", entry.AccessLevel, AccessReadWrite)
	}
	if entry.PrincipalType != PrincipalTypeUser {
		t.Errorf("PrincipalType = %q, want %s", entry.PrincipalType, PrincipalTypeUser)
	}
	if entry.PrincipalID != "alice" {
		t.Errorf("PrincipalID = %q, want alice", entry.PrincipalID)
	}
	if entry.SubjectType != PrincipalTypeUser {
		t.Errorf("SubjectType = %q, want %s", entry.SubjectType, PrincipalTypeUser)
	}
	if entry.SubjectID != "alice" {
		t.Errorf("SubjectID = %q, want alice", entry.SubjectID)
	}
	if entry.RootSubjectType != PrincipalTypeUser {
		t.Errorf("RootSubjectType = %q, want %s", entry.RootSubjectType, PrincipalTypeUser)
	}
	if entry.RootSubjectID != "alice" {
		t.Errorf("RootSubjectID = %q, want alice", entry.RootSubjectID)
	}
	if entry.AuthorityMode != "direct" {
		t.Errorf("AuthorityMode = %q, want direct", entry.AuthorityMode)
	}
	if entry.ResourceType != ResourceTypeWorkspace {
		t.Errorf("ResourceType = %q, want %s", entry.ResourceType, ResourceTypeWorkspace)
	}
	if entry.ResourceID != "prod" {
		t.Errorf("ResourceID = %q, want prod", entry.ResourceID)
	}
	if entry.Operation != "connect" {
		t.Errorf("Operation = %q, want connect", entry.Operation)
	}
	if entry.Workspace != "prod" {
		t.Errorf("Workspace = %q, want prod", entry.Workspace)
	}
	if entry.GatewayID != "gw-001" {
		t.Errorf("GatewayID = %q, want gw-001", entry.GatewayID)
	}
	if entry.SessionID != sessionID {
		t.Errorf("SessionID = %v, want %v", entry.SessionID, sessionID)
	}
	if !entry.FallbackApplied {
		t.Error("expected FallbackApplied=true")
	}
	if entry.Timestamp.Before(now.Add(-time.Second)) {
		t.Error("Timestamp should be approximately now")
	}
}

func TestBuildEntry_SetsRuleIDFromDecision(t *testing.T) {
	logger := &AuditLogger{gatewayID: "gw-test"}
	ruleID := "rule-123"
	decision := &ACLDecision{
		Decision:    DecisionAllow,
		RuleApplied: &ACLRule{RuleID: ruleID},
	}
	principal := models.Identity{Type: models.PrincipalAgent, ID: "agent-x"}

	entry := logger.buildEntry(decision, principal, ResourceTypeAgent, "agent-x", "connect", "ws", uuid.New())

	if entry.RuleID == nil {
		t.Fatal("expected RuleID to be set")
	}
	if *entry.RuleID != ruleID {
		t.Errorf("*RuleID = %q, want %s", *entry.RuleID, ruleID)
	}
}

func TestBuildEntry_RuleIDNilWhenNoRuleApplied(t *testing.T) {
	logger := &AuditLogger{gatewayID: "gw-test"}
	decision := &ACLDecision{Decision: DecisionDeny, RuleApplied: nil}
	principal := models.Identity{Type: models.PrincipalUser, ID: "user-z"}

	entry := logger.buildEntry(decision, principal, ResourceTypeWorkspace, "ws", "op", "ws", uuid.New())

	if entry.RuleID != nil {
		t.Error("expected RuleID to be nil when no rule was applied")
	}
}

func TestBuildEntry_MetadataMapInitialised(t *testing.T) {
	logger := &AuditLogger{gatewayID: "gw-test"}
	decision := &ACLDecision{Decision: DecisionDeny}
	principal := models.Identity{Type: models.PrincipalUser, ID: "u"}

	entry := logger.buildEntry(decision, principal, "workspace", "w", "op", "w", uuid.New())

	if entry.Metadata == nil {
		t.Error("expected Metadata map to be initialized, got nil")
	}
}

func TestBuildEntry_UsesCanonicalPrincipalID(t *testing.T) {
	logger := &AuditLogger{gatewayID: "gw-test"}
	decision := &ACLDecision{Decision: DecisionAllow}
	principal := models.Identity{
		Type:           models.PrincipalService,
		Implementation: "frontend-api",
		Specifier:      "pod-1",
	}

	entry := logger.buildEntry(decision, principal, ResourceTypeWorkspace, "ws", "read", "ws", uuid.New())

	if entry.PrincipalID != "sv::frontend-api::pod-1" {
		t.Errorf("PrincipalID = %q, want %q", entry.PrincipalID, "sv::frontend-api::pod-1")
	}
	if entry.PrincipalType != PrincipalTypeService {
		t.Errorf("PrincipalType = %q, want %s", entry.PrincipalType, PrincipalTypeService)
	}
}
