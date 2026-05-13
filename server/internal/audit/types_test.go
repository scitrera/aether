package audit

import (
	"testing"

	"github.com/google/uuid"
)

func TestValidateEventType(t *testing.T) {
	valid := []string{
		EventTypeConnection, EventTypeAuth, EventTypeMessage,
		EventTypeKV, EventTypeAdmin, EventTypeACL,
	}

	for _, et := range valid {
		if err := ValidateEventType(et); err != nil {
			t.Errorf("ValidateEventType(%q) = %v, want nil", et, err)
		}
	}

	invalid := []string{"", "invalid", "CONNECTION", "unknown"}
	for _, et := range invalid {
		if err := ValidateEventType(et); err == nil {
			t.Errorf("ValidateEventType(%q) = nil, want error", et)
		}
	}
}

func TestValidateVerbosityLevel(t *testing.T) {
	valid := []string{VerbosityLow, VerbosityMedium, VerbosityHigh}
	for _, v := range valid {
		if err := ValidateVerbosityLevel(v); err != nil {
			t.Errorf("ValidateVerbosityLevel(%q) = %v, want nil", v, err)
		}
	}

	invalid := []string{"", "extreme", "LOW", "unknown"}
	for _, v := range invalid {
		if err := ValidateVerbosityLevel(v); err == nil {
			t.Errorf("ValidateVerbosityLevel(%q) = nil, want error", v)
		}
	}
}

func TestEventTypeName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{EventTypeConnection, "Connection"},
		{EventTypeAuth, "Authentication"},
		{EventTypeMessage, "Message"},
		{EventTypeKV, "Key-Value"},
		{EventTypeAdmin, "Administrative"},
		{EventTypeACL, "Access Control"},
		{"unknown", "Unknown"},
	}

	for _, tt := range tests {
		got := EventTypeName(tt.input)
		if got != tt.want {
			t.Errorf("EventTypeName(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestVerbosityLevelName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{VerbosityLow, "Low"},
		{VerbosityMedium, "Medium"},
		{VerbosityHigh, "High"},
		{"other", "Unknown"},
	}

	for _, tt := range tests {
		got := VerbosityLevelName(tt.input)
		if got != tt.want {
			t.Errorf("VerbosityLevelName(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestShouldIncludeMessageContent(t *testing.T) {
	if ShouldIncludeMessageContent(VerbosityLow) {
		t.Error("low verbosity should not include content")
	}
	if ShouldIncludeMessageContent(VerbosityMedium) {
		t.Error("medium verbosity should not include content")
	}
	if !ShouldIncludeMessageContent(VerbosityHigh) {
		t.Error("high verbosity should include content")
	}
}

func TestShouldIncludeMessageMetadata(t *testing.T) {
	if ShouldIncludeMessageMetadata(VerbosityLow) {
		t.Error("low verbosity should not include metadata")
	}
	if !ShouldIncludeMessageMetadata(VerbosityMedium) {
		t.Error("medium verbosity should include metadata")
	}
	if !ShouldIncludeMessageMetadata(VerbosityHigh) {
		t.Error("high verbosity should include metadata")
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if !cfg.Enabled {
		t.Error("default config should be enabled")
	}
	if cfg.VerbosityLevel != VerbosityLow {
		t.Errorf("default verbosity = %q, want %q", cfg.VerbosityLevel, VerbosityLow)
	}
	if cfg.BatchSize != DefaultBatchSize {
		t.Errorf("default batch size = %d, want %d", cfg.BatchSize, DefaultBatchSize)
	}
	if cfg.FlushPeriod != DefaultFlushPeriod {
		t.Errorf("default flush period = %v, want %v", cfg.FlushPeriod, DefaultFlushPeriod)
	}
	if cfg.RetentionDays != DefaultRetentionDays {
		t.Errorf("default retention = %d, want %d", cfg.RetentionDays, DefaultRetentionDays)
	}
	if cfg.ChannelBuffer != DefaultChannelBuffer {
		t.Errorf("default channel buffer = %d, want %d", cfg.ChannelBuffer, DefaultChannelBuffer)
	}
	if len(cfg.EnabledEventTypes) != 7 {
		t.Errorf("default enabled event types = %d, want 7", len(cfg.EnabledEventTypes))
	}
}

func TestIsEventTypeEnabled(t *testing.T) {
	cfg := &Config{
		Enabled:           true,
		EnabledEventTypes: []string{EventTypeConnection, EventTypeAuth},
	}

	if !cfg.IsEventTypeEnabled(EventTypeConnection) {
		t.Error("connection should be enabled")
	}
	if !cfg.IsEventTypeEnabled(EventTypeAuth) {
		t.Error("auth should be enabled")
	}
	if cfg.IsEventTypeEnabled(EventTypeMessage) {
		t.Error("message should not be enabled")
	}
}

func TestIsEventTypeEnabled_Disabled(t *testing.T) {
	cfg := &Config{Enabled: false, EnabledEventTypes: []string{EventTypeConnection}}

	if cfg.IsEventTypeEnabled(EventTypeConnection) {
		t.Error("nothing should be enabled when config is disabled")
	}
}

func TestIsEventTypeEnabled_EmptyList(t *testing.T) {
	cfg := &Config{Enabled: true, EnabledEventTypes: []string{}}

	if !cfg.IsEventTypeEnabled(EventTypeConnection) {
		t.Error("empty list should mean all types enabled")
	}
}

func TestNewConnectionEvent(t *testing.T) {
	sid := uuid.New()
	e := NewConnectionEvent("user", "alice", OpConnectionEstablished, sid, true, "", nil)

	if e.EventType != EventTypeConnection {
		t.Errorf("EventType = %q, want %q", e.EventType, EventTypeConnection)
	}
	if e.ActorID != "alice" {
		t.Errorf("ActorID = %q, want alice", e.ActorID)
	}
	if e.ResourceType != ResourceTypeSession {
		t.Errorf("ResourceType = %q, want %q", e.ResourceType, ResourceTypeSession)
	}
	if e.SessionID != sid {
		t.Errorf("SessionID mismatch")
	}
	if !e.Success {
		t.Error("expected success=true")
	}
	if e.Metadata == nil {
		t.Error("metadata should be initialized")
	}
}

func TestNewAuthEvent(t *testing.T) {
	sid := uuid.New()
	e := NewAuthEvent("agent", "bot-1", OpAuthTokenValidation, "prod", sid, false, "bad token", nil)

	if e.EventType != EventTypeAuth {
		t.Errorf("EventType = %q, want %q", e.EventType, EventTypeAuth)
	}
	if e.Workspace != "prod" {
		t.Errorf("Workspace = %q, want prod", e.Workspace)
	}
	if e.Success {
		t.Error("expected success=false")
	}
	if e.ErrorMessage != "bad token" {
		t.Errorf("ErrorMessage = %q, want 'bad token'", e.ErrorMessage)
	}
}

func TestNewMessageEvent(t *testing.T) {
	sid := uuid.New()
	e := NewMessageEvent("user", "alice", OpMessageRouted, "ag::prod::worker::1", "prod", sid, true, "", map[string]interface{}{"size": 42})

	if e.EventType != EventTypeMessage {
		t.Errorf("EventType = %q", e.EventType)
	}
	if e.ResourceType != ResourceTypeTopic {
		t.Errorf("ResourceType = %q, want %q", e.ResourceType, ResourceTypeTopic)
	}
	if e.ResourceID != "ag::prod::worker::1" {
		t.Errorf("ResourceID = %q", e.ResourceID)
	}
	if e.Metadata["size"] != 42 {
		t.Errorf("Metadata[size] = %v, want 42", e.Metadata["size"])
	}
}

func TestNewKVEvent(t *testing.T) {
	sid := uuid.New()
	e := NewKVEvent("user", "alice", OpKVPut, "config/key", "dev", sid, true, "", nil)

	if e.EventType != EventTypeKV {
		t.Errorf("EventType = %q", e.EventType)
	}
	if e.ResourceType != ResourceTypeKVKey {
		t.Errorf("ResourceType = %q, want %q", e.ResourceType, ResourceTypeKVKey)
	}
	if e.ResourceID != "config/key" {
		t.Errorf("ResourceID = %q", e.ResourceID)
	}
}

func TestNewAdminEvent(t *testing.T) {
	sid := uuid.New()
	e := NewAdminEvent("user", "admin", OpAdminSessionDisconnect, ResourceTypeSession, "session-123", "prod", sid, true, "", nil)

	if e.EventType != EventTypeAdmin {
		t.Errorf("EventType = %q", e.EventType)
	}
	if e.ResourceType != ResourceTypeSession {
		t.Errorf("ResourceType = %q", e.ResourceType)
	}
	if e.ResourceID != "session-123" {
		t.Errorf("ResourceID = %q", e.ResourceID)
	}
}
