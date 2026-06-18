package a2abridge

import (
	"errors"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

func TestEnvelope_RequestRoundTrip(t *testing.T) {
	req := &a2a.SendMessageRequest{
		Tenant: "a2a-prod",
		Message: a2a.NewMessage(
			a2a.MessageRoleUser,
			a2a.NewTextPart("hello"),
			a2a.NewDataPart(map[string]any{"k": "v"}),
		),
		Metadata: map[string]any{"trace_id": "abc"},
	}
	data, err := MarshalRequest("req-1", req)
	if err != nil {
		t.Fatalf("MarshalRequest: %v", err)
	}

	env, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if env.Kind != KindRequest {
		t.Errorf("Kind: want %q, got %q", KindRequest, env.Kind)
	}
	if env.ReqID != "req-1" {
		t.Errorf("ReqID: want req-1, got %q", env.ReqID)
	}
	if env.Request == nil || env.Request.Message == nil {
		t.Fatalf("Request not populated")
	}
	if env.Request.Tenant != "a2a-prod" {
		t.Errorf("Tenant: want a2a-prod, got %q", env.Request.Tenant)
	}
	if len(env.Request.Message.Parts) != 2 {
		t.Fatalf("Parts: want 2, got %d", len(env.Request.Message.Parts))
	}
	if env.Request.Message.Parts[0].Text() != "hello" {
		t.Errorf("text part: want hello, got %q", env.Request.Message.Parts[0].Text())
	}
}

func TestEnvelope_EventRoundTrip_Message(t *testing.T) {
	msg := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("hi back"))
	data, err := MarshalEvent("req-2", msg, true)
	if err != nil {
		t.Fatalf("MarshalEvent: %v", err)
	}
	env, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if env.Kind != KindEvent {
		t.Fatalf("Kind: want %q, got %q", KindEvent, env.Kind)
	}
	if !env.Terminal {
		t.Errorf("Terminal: want true")
	}
	got, ok := env.Event.Event.(*a2a.Message)
	if !ok {
		t.Fatalf("Event: want *a2a.Message, got %T", env.Event.Event)
	}
	if got.Parts[0].Text() != "hi back" {
		t.Errorf("text: want hi back, got %q", got.Parts[0].Text())
	}
}

func TestEnvelope_EventRoundTrip_Task(t *testing.T) {
	task := a2a.NewSubmittedTask(
		a2a.TaskInfo{TaskID: "t-1", ContextID: "ctx-1"},
		a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("start")),
	)
	data, err := MarshalEvent("req-3", task, false)
	if err != nil {
		t.Fatalf("MarshalEvent: %v", err)
	}
	env, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	got, ok := env.Event.Event.(*a2a.Task)
	if !ok {
		t.Fatalf("want *a2a.Task, got %T", env.Event.Event)
	}
	if got.ID != "t-1" || got.ContextID != "ctx-1" {
		t.Errorf("task ids: got id=%q ctx=%q", got.ID, got.ContextID)
	}
	if got.Status.State != a2a.TaskStateSubmitted {
		t.Errorf("state: want submitted, got %q", got.Status.State)
	}
}

func TestEnvelope_EventRoundTrip_StatusUpdate(t *testing.T) {
	evt := a2a.NewStatusUpdateEvent(
		a2a.TaskInfo{TaskID: "t-2", ContextID: "ctx-2"},
		a2a.TaskStateWorking,
		nil,
	)
	data, err := MarshalEvent("req-4", evt, false)
	if err != nil {
		t.Fatalf("MarshalEvent: %v", err)
	}
	env, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	got, ok := env.Event.Event.(*a2a.TaskStatusUpdateEvent)
	if !ok {
		t.Fatalf("want *a2a.TaskStatusUpdateEvent, got %T", env.Event.Event)
	}
	if got.TaskID != "t-2" || got.Status.State != a2a.TaskStateWorking {
		t.Errorf("statusUpdate fields off: %+v", got)
	}
}

func TestEnvelope_EventRoundTrip_ArtifactUpdate(t *testing.T) {
	evt := a2a.NewArtifactEvent(
		a2a.TaskInfo{TaskID: "t-3", ContextID: "ctx-3"},
		a2a.NewTextPart("artifact-bytes"),
	)
	data, err := MarshalEvent("req-5", evt, false)
	if err != nil {
		t.Fatalf("MarshalEvent: %v", err)
	}
	env, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	got, ok := env.Event.Event.(*a2a.TaskArtifactUpdateEvent)
	if !ok {
		t.Fatalf("want *a2a.TaskArtifactUpdateEvent, got %T", env.Event.Event)
	}
	if got.TaskID != "t-3" {
		t.Errorf("artifactUpdate TaskID: got %q", got.TaskID)
	}
	if got.Artifact == nil || len(got.Artifact.Parts) != 1 {
		t.Fatalf("artifact parts not preserved")
	}
}

func TestEnvelope_Error(t *testing.T) {
	data, err := MarshalError("req-6", "unknown_agent", "no agent ag::test::echo::1")
	if err != nil {
		t.Fatalf("MarshalError: %v", err)
	}
	env, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if env.Kind != KindError || env.Error == nil {
		t.Fatalf("error envelope not populated: %+v", env)
	}
	if !env.Terminal {
		t.Errorf("error envelopes must be terminal")
	}
	if !strings.Contains(env.Error.Error(), "unknown_agent") {
		t.Errorf("Error(): %q", env.Error.Error())
	}
}

func TestEnvelope_Validation(t *testing.T) {
	if _, err := MarshalRequest("", &a2a.SendMessageRequest{}); err == nil {
		t.Errorf("MarshalRequest empty reqID: expected error")
	}
	if _, err := MarshalRequest("r", nil); err == nil {
		t.Errorf("MarshalRequest nil req: expected error")
	}
	if _, err := MarshalEvent("", a2a.NewMessage(a2a.MessageRoleAgent), false); err == nil {
		t.Errorf("MarshalEvent empty reqID: expected error")
	}
	if _, err := MarshalEvent("r", nil, false); err == nil {
		t.Errorf("MarshalEvent nil event: expected error")
	}
	if _, err := MarshalError("r", "", "m"); err == nil {
		t.Errorf("MarshalError empty code: expected error")
	}

	if _, err := Unmarshal(nil); err == nil {
		t.Errorf("Unmarshal empty: expected error")
	}
	if _, err := Unmarshal([]byte(`{"v":"99","kind":"event"}`)); err == nil {
		t.Errorf("Unmarshal wrong version: expected error")
	}
	if _, err := Unmarshal([]byte(`{"v":"1","kind":"event"}`)); err == nil {
		t.Errorf("Unmarshal event without body: expected error")
	}
	if _, err := Unmarshal([]byte(`{"v":"1","kind":"request"}`)); err == nil {
		t.Errorf("Unmarshal request without body: expected error")
	}
	if _, err := Unmarshal([]byte(`{"v":"1","kind":"weird"}`)); err == nil {
		t.Errorf("Unmarshal unknown kind: expected error")
	}
}

func TestEnvelopeError_NilSafe(t *testing.T) {
	var e *EnvelopeError
	// Should not panic
	_ = e.Error()
	var asErr error = (*EnvelopeError)(nil)
	if errors.Is(asErr, ErrUnknownTenant) {
		t.Errorf("nil EnvelopeError should not match ErrUnknownTenant")
	}
}
