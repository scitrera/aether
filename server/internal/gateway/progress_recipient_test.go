package gateway

import (
	"context"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/circuitbreaker"
	"github.com/scitrera/aether/pkg/models"
	"google.golang.org/protobuf/proto"
)

// newProgressTestServer builds a minimal GatewayServer for
// handleProgressReport tests. ACL / taskStore are nil so heartbeat +
// authority-grant side effects are skipped, letting us focus on the
// publish-topic routing decision.
func newProgressTestServer(router *mockMessageRouter) *GatewayServer {
	s := newTestGatewayWithMocks(
		newMockSessionManager(),
		router,
		newMockKVReadWriter(),
		newMockCheckpointManager(),
	)
	s.publishBreaker = circuitbreaker.New("test-progress-pub",
		circuitbreaker.WithMaxFailures(100),
	)
	return s
}

func newProgressTestClient(identity models.Identity) *ClientSession {
	return &ClientSession{
		ID:            "progress-test-session",
		Identity:      identity,
		subscriptions: make(map[string]func()),
	}
}

// TestHandleProgressReport_RoutesToUserProgressTopic verifies that when the
// report's Recipient is a window-specific user identity (us::{user}::{window}),
// handleProgressReport publishes to the collapsed per-user progress topic
// pg::us::{user} (window-level filtering happens at delivery time). The
// Recipient field is preserved on the ProgressUpdate so each window's filter
// handler can decide whether to deliver.
func TestHandleProgressReport_RoutesToUserProgressTopic(t *testing.T) {
	router := newMockMessageRouter()
	s := newProgressTestServer(router)

	sender := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "_apps",
		Implementation: "CoworkAgent",
		Specifier:      "dev@example.com",
	}
	client := newProgressTestClient(sender)

	report := &pb.ProgressReport{
		TaskId:    "task-1",
		State:     "running",
		Recipient: "us::dev@example.com::win-abc",
		RequestId: "req-1",
		Kind:      pb.ProgressKind_PROGRESS_KIND_CHAT,
		Metadata:  map[string]string{"thread_id": "chat-t1"},
	}

	s.handleProgressReport(context.Background(), client, report)

	router.mu.Lock()
	published := append([]publishedMsg(nil), router.publishedMessages...)
	router.mu.Unlock()

	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}
	wantTopic := "pg::us::dev@example.com"
	if published[0].topic != wantTopic {
		t.Errorf("publish topic = %q, want %q (per-user collapsed topic)", published[0].topic, wantTopic)
	}

	var update pb.ProgressUpdate
	if err := proto.Unmarshal(published[0].payload, &update); err != nil {
		t.Fatalf("unmarshal ProgressUpdate: %v", err)
	}
	if update.Kind != pb.ProgressKind_PROGRESS_KIND_CHAT {
		t.Errorf("update.Kind = %v, want CHAT", update.Kind)
	}
	if update.Recipient != report.Recipient {
		t.Errorf("update.Recipient = %q, want %q (preserved for delivery-time filter)",
			update.Recipient, report.Recipient)
	}
	if update.Metadata["thread_id"] != "chat-t1" {
		t.Errorf("metadata.thread_id = %q, want chat-t1", update.Metadata["thread_id"])
	}
}

// TestHandleProgressReport_BareUserRecipientPublishesToUserTopic verifies that
// a bare user-level recipient (us::{user}, no window specifier) routes to the
// same pg::us::{user} topic as the window-specific form. The filter at delivery
// time uses prefix-match so every one of the user's windows receives the update.
func TestHandleProgressReport_BareUserRecipientPublishesToUserTopic(t *testing.T) {
	router := newMockMessageRouter()
	s := newProgressTestServer(router)

	sender := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "_apps",
		Implementation: "CoworkAgent",
		Specifier:      "dev@example.com",
	}
	client := newProgressTestClient(sender)

	report := &pb.ProgressReport{
		TaskId:    "task-1",
		State:     "running",
		Recipient: "us::dev@example.com", // bare user, no window
		Kind:      pb.ProgressKind_PROGRESS_KIND_APP,
	}

	s.handleProgressReport(context.Background(), client, report)

	router.mu.Lock()
	published := append([]publishedMsg(nil), router.publishedMessages...)
	router.mu.Unlock()

	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}
	wantTopic := "pg::us::dev@example.com"
	if published[0].topic != wantTopic {
		t.Errorf("publish topic = %q, want %q", published[0].topic, wantTopic)
	}
}

// TestHandleProgressReport_BroadcastFallback verifies that when the report
// has no recipient (or a non-user recipient), the legacy behaviour holds:
// publish to pg::{sender.workspace}. This preserves parent-agent /
// orchestrator consumption of task-kind progress on the broadcast stream.
func TestHandleProgressReport_BroadcastFallback(t *testing.T) {
	router := newMockMessageRouter()
	s := newProgressTestServer(router)

	sender := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "prod",
		Implementation: "worker",
		Specifier:      "v1",
	}
	client := newProgressTestClient(sender)

	report := &pb.ProgressReport{
		TaskId: "task-2",
		State:  "running",
		Kind:   pb.ProgressKind_PROGRESS_KIND_TASK,
	}

	s.handleProgressReport(context.Background(), client, report)

	router.mu.Lock()
	published := append([]publishedMsg(nil), router.publishedMessages...)
	router.mu.Unlock()

	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}
	wantTopic := "pg::prod"
	if published[0].topic != wantTopic {
		t.Errorf("publish topic = %q, want %q", published[0].topic, wantTopic)
	}

	var update pb.ProgressUpdate
	if err := proto.Unmarshal(published[0].payload, &update); err != nil {
		t.Fatalf("unmarshal ProgressUpdate: %v", err)
	}
	if update.Kind != pb.ProgressKind_PROGRESS_KIND_TASK {
		t.Errorf("update.Kind = %v, want TASK", update.Kind)
	}
	if update.Workspace != "prod" {
		t.Errorf("update.Workspace = %q, want prod", update.Workspace)
	}
}

// TestIsBareUserRecipientMatch covers the prefix-match helper that turns a
// bare us::{user} recipient into "deliver to every window of that user".
func TestIsBareUserRecipientMatch(t *testing.T) {
	cases := []struct {
		name      string
		recipient string
		myTopic   string
		want      bool
	}{
		{"bare user matches own window", "us::alice", "us::alice::win-1", true},
		{"bare user matches another window of same user", "us::alice", "us::alice::win-2", true},
		{"bare user does NOT match different user's window", "us::bob", "us::alice::win-1", false},
		{"window-specific recipient is not a prefix match", "us::alice::win-1", "us::alice::win-1", false},
		{"agent recipient does not match user topic", "ag::prod::svc::v1", "us::alice::win-1", false},
		{"empty recipient is not a bare-user match", "", "us::alice::win-1", false},
		{"bare user does not match an agent topic", "us::alice", "ag::prod::svc::v1", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := isBareUserRecipientMatch(c.recipient, c.myTopic)
			if got != c.want {
				t.Errorf("isBareUserRecipientMatch(%q, %q) = %v, want %v",
					c.recipient, c.myTopic, got, c.want)
			}
		})
	}
}

// TestBareUserRecipientID covers the parser that extracts the user ID from a
// bare us::{user} recipient (which ParseIdentity intentionally rejects).
func TestBareUserRecipientID(t *testing.T) {
	cases := []struct {
		name      string
		recipient string
		wantID    string
		wantOK    bool
	}{
		{"bare user", "us::alice", "alice", true},
		{"bare user with dot in id", "us::dev@example.com", "dev@example.com", true},
		{"window-specific is rejected", "us::alice::win-1", "", false},
		{"agent identity is rejected", "ag::prod::svc::v1", "", false},
		{"empty string", "", "", false},
		{"only prefix", "us::", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id, ok := bareUserRecipientID(c.recipient)
			if id != c.wantID || ok != c.wantOK {
				t.Errorf("bareUserRecipientID(%q) = (%q, %v), want (%q, %v)",
					c.recipient, id, ok, c.wantID, c.wantOK)
			}
		})
	}
}

// TestHandleProgressReport_NonUserRecipientUsesBroadcast verifies that a
// non-user recipient (e.g. an agent identity) still uses the broadcast path
// with the recipient stamped for server-side filtering — the user-progress
// routing branch ONLY fires for us:: recipients.
func TestHandleProgressReport_NonUserRecipientUsesBroadcast(t *testing.T) {
	router := newMockMessageRouter()
	s := newProgressTestServer(router)

	sender := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "prod",
		Implementation: "worker",
		Specifier:      "v1",
	}
	client := newProgressTestClient(sender)

	report := &pb.ProgressReport{
		TaskId:    "task-3",
		State:     "running",
		Recipient: "ag::prod::parent::main",
	}

	s.handleProgressReport(context.Background(), client, report)

	router.mu.Lock()
	published := append([]publishedMsg(nil), router.publishedMessages...)
	router.mu.Unlock()

	if len(published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(published))
	}
	wantTopic := "pg::prod"
	if published[0].topic != wantTopic {
		t.Errorf("publish topic = %q, want %q (broadcast fallback)", published[0].topic, wantTopic)
	}

	var update pb.ProgressUpdate
	if err := proto.Unmarshal(published[0].payload, &update); err != nil {
		t.Fatalf("unmarshal ProgressUpdate: %v", err)
	}
	if update.Recipient != report.Recipient {
		t.Errorf("update.Recipient = %q, want %q (preserved for server-side filter)",
			update.Recipient, report.Recipient)
	}
}
