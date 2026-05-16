package tasks

import (
	"encoding/json"
	"testing"
	"time"
)

// TestWaitSpec_HibernationDescriptorRoundTrip verifies that a WaitSpec carrying
// a non-nil HibernationDescriptor marshals to JSON and unmarshals back without
// data loss. This guards the JSONB-column persistence used by both the
// postgres and sqlite stores.
func TestWaitSpec_HibernationDescriptorRoundTrip(t *testing.T) {
	wakeAt := time.Now().Add(time.Hour).UnixMilli()
	original := WaitSpec{
		Reason:              WaitReasonHibernation,
		ScheduledWakeUnixMs: wakeAt,
		TimeoutMs:           7_200_000,
		Hibernation: &HibernationDescriptor{
			CheckpointKey:    "ckpt-abc-123",
			ResumeSessionID:  "sess-xyz-789",
			WakeEventTypes:   []string{"user_message", "external_signal"},
			EscalationPolicy: "retry",
		},
	}

	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	var got WaitSpec
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}

	if got.Reason != WaitReasonHibernation {
		t.Errorf("Reason = %q, want %q", got.Reason, WaitReasonHibernation)
	}
	if got.ScheduledWakeUnixMs != wakeAt {
		t.Errorf("ScheduledWakeUnixMs = %d, want %d", got.ScheduledWakeUnixMs, wakeAt)
	}
	if got.TimeoutMs != 7_200_000 {
		t.Errorf("TimeoutMs = %d, want 7_200_000", got.TimeoutMs)
	}
	if got.Hibernation == nil {
		t.Fatal("Hibernation = nil after roundtrip, want non-nil")
	}
	if got.Hibernation.CheckpointKey != "ckpt-abc-123" {
		t.Errorf("CheckpointKey = %q, want %q", got.Hibernation.CheckpointKey, "ckpt-abc-123")
	}
	if got.Hibernation.ResumeSessionID != "sess-xyz-789" {
		t.Errorf("ResumeSessionID = %q, want %q", got.Hibernation.ResumeSessionID, "sess-xyz-789")
	}
	if len(got.Hibernation.WakeEventTypes) != 2 ||
		got.Hibernation.WakeEventTypes[0] != "user_message" ||
		got.Hibernation.WakeEventTypes[1] != "external_signal" {
		t.Errorf("WakeEventTypes = %v, want [user_message external_signal]", got.Hibernation.WakeEventTypes)
	}
	if got.Hibernation.EscalationPolicy != "retry" {
		t.Errorf("EscalationPolicy = %q, want %q", got.Hibernation.EscalationPolicy, "retry")
	}
}

// TestWaitSpec_NilHibernationRoundTrip verifies that a WaitSpec without a
// Hibernation descriptor (the common case for non-hibernation pauses) survives
// a marshal/unmarshal cycle with Hibernation = nil. This is the backward-compat
// shape rows persisted before Phase 3 will exhibit.
func TestWaitSpec_NilHibernationRoundTrip(t *testing.T) {
	original := WaitSpec{
		Reason:    WaitReasonDependency,
		DependsOn: []string{"t-1"},
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var got WaitSpec
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if got.Hibernation != nil {
		t.Errorf("Hibernation = %+v, want nil", got.Hibernation)
	}
}

// TestWaitSpec_PreHibernationJSON verifies that JSON serialized before the
// Hibernation field existed deserializes cleanly with Hibernation = nil. This
// is the migration story for existing rows.
func TestWaitSpec_PreHibernationJSON(t *testing.T) {
	legacy := `{"reason":"hibernation","scheduled_wake_unix_ms":1700000000000}`
	var got WaitSpec
	if err := json.Unmarshal([]byte(legacy), &got); err != nil {
		t.Fatalf("json.Unmarshal legacy: %v", err)
	}
	if got.Reason != WaitReasonHibernation {
		t.Errorf("Reason = %q, want %q", got.Reason, WaitReasonHibernation)
	}
	if got.ScheduledWakeUnixMs != 1700000000000 {
		t.Errorf("ScheduledWakeUnixMs = %d, want 1700000000000", got.ScheduledWakeUnixMs)
	}
	if got.Hibernation != nil {
		t.Errorf("Hibernation = %+v, want nil for legacy row", got.Hibernation)
	}
}

// TestMetadataKeyHibernationConstants sanity-checks the reserved metadata
// keys used for hibernation handoff (Stage B will read/write these in the
// waker / DeliverQueuedTasks path).
func TestMetadataKeyHibernationConstants(t *testing.T) {
	if MetadataKeyHibernationCheckpointKey != "_hibernation_checkpoint_key" {
		t.Errorf("MetadataKeyHibernationCheckpointKey = %q, want %q",
			MetadataKeyHibernationCheckpointKey, "_hibernation_checkpoint_key")
	}
	if MetadataKeyHibernationResumeSessionID != "_hibernation_resume_session_id" {
		t.Errorf("MetadataKeyHibernationResumeSessionID = %q, want %q",
			MetadataKeyHibernationResumeSessionID, "_hibernation_resume_session_id")
	}
	// Reserved keys must use the "_" prefix denoting server-managed metadata.
	if MetadataKeyHibernationCheckpointKey[0] != '_' {
		t.Errorf("MetadataKeyHibernationCheckpointKey should start with '_' prefix")
	}
	if MetadataKeyHibernationResumeSessionID[0] != '_' {
		t.Errorf("MetadataKeyHibernationResumeSessionID should start with '_' prefix")
	}
}
