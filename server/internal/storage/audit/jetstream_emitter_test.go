package audit_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/google/uuid"

	auditstore "github.com/scitrera/aether/internal/storage/audit"
)

// ---------------------------------------------------------------------------
// Embedded NATS + JetStream helper
// ---------------------------------------------------------------------------

func startTestJSForAudit(t *testing.T) (jetstream.JetStream, func()) {
	t.Helper()

	opts := &natsserver.Options{
		Port:               -1,
		JetStream:          true,
		StoreDir:           t.TempDir(),
		JetStreamMaxMemory: 64 * 1024 * 1024,
		JetStreamMaxStore:  256 * 1024 * 1024,
		NoSigs:             true,
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("nats server new: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		t.Fatal("nats server not ready")
	}

	conn, err := natsgo.Connect("", natsgo.InProcessServer(srv))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("nats connect: %v", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		srv.Shutdown()
		t.Fatalf("jetstream new: %v", err)
	}

	stop := func() {
		_ = conn.Drain()
		conn.Close()
		srv.Shutdown()
		srv.WaitForShutdown()
	}
	return js, stop
}

// ---------------------------------------------------------------------------
// fakeInnerStore — minimal Store implementation that records LogEvent calls
// ---------------------------------------------------------------------------

type fakeInnerStore struct {
	mu     sync.Mutex
	events []*auditstore.Event
	config *auditstore.Config
}

func newFakeInnerStore() *fakeInnerStore {
	return &fakeInnerStore{
		config: auditstore.DefaultConfig(),
	}
}

func (f *fakeInnerStore) LogEvent(_ context.Context, event *auditstore.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
}

func (f *fakeInnerStore) LogEventSync(_ context.Context, event *auditstore.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
	return nil
}

func (f *fakeInnerStore) Close() error { return nil }

func (f *fakeInnerStore) QueryAuditLog(_ context.Context, _ auditstore.EventFilter) ([]*auditstore.Event, error) {
	return nil, nil
}

func (f *fakeInnerStore) CleanupOldLogs(_ context.Context, _ int) (int64, error) {
	return 0, nil
}

func (f *fakeInnerStore) GetConfig() *auditstore.Config {
	return f.config
}

func (f *fakeInnerStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}

// Compile-time: fakeInnerStore satisfies auditstore.Store.
var _ auditstore.Store = (*fakeInnerStore)(nil)

// ---------------------------------------------------------------------------
// Helper: consume one message from a JetStream subject
// ---------------------------------------------------------------------------

// consumeOne creates an ephemeral push consumer on the given subject filter and
// waits up to timeout for one message. Returns the raw payload or fails the test.
func consumeOne(t *testing.T, js jetstream.JetStream, streamName, filterSubject string, timeout time.Duration) []byte {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cons, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		FilterSubject: filterSubject,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("consumeOne: create consumer on %q: %v", filterSubject, err)
	}

	msg, err := cons.Next(jetstream.FetchMaxWait(timeout))
	if err != nil {
		t.Fatalf("consumeOne: fetch from %q: %v", filterSubject, err)
	}
	_ = msg.Ack()
	return msg.Data()
}

// ---------------------------------------------------------------------------
// Test 1: LogEvent publishes to stream
// ---------------------------------------------------------------------------

func TestJetStreamAuditEmitter_LogEvent_PublishesToStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream integration test in short mode")
	}

	js, stop := startTestJSForAudit(t)
	defer stop()

	inner := newFakeInnerStore()
	ctx := context.Background()

	emitter, err := auditstore.NewJetStreamAuditEmitter(ctx, inner, js, 1, nil)
	if err != nil {
		t.Fatalf("NewJetStreamAuditEmitter: %v", err)
	}
	defer emitter.Close()

	event := &auditstore.Event{
		AuditID:   0,
		Timestamp: time.Now().UTC(),
		EventType: auditstore.EventTypeConnection,
		ActorType: "agent",
		ActorID:   "test-agent",
		Workspace: "test-ws",
		SessionID: uuid.New(),
		Success:   true,
		Metadata:  map[string]interface{}{"k": "v"},
	}

	emitter.LogEvent(ctx, event)

	// Allow the async goroutine to publish.
	time.Sleep(200 * time.Millisecond)

	// The inner must have been called.
	if got := inner.callCount(); got != 1 {
		t.Errorf("inner.LogEvent call count = %d, want 1", got)
	}

	// Consume the message from JetStream.
	payload := consumeOne(t, js, "audit", "audit.test-ws.connection", 5*time.Second)

	var got auditstore.Event
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.EventType != event.EventType {
		t.Errorf("EventType = %q, want %q", got.EventType, event.EventType)
	}
	if got.ActorID != event.ActorID {
		t.Errorf("ActorID = %q, want %q", got.ActorID, event.ActorID)
	}
	if got.Workspace != event.Workspace {
		t.Errorf("Workspace = %q, want %q", got.Workspace, event.Workspace)
	}
}

// ---------------------------------------------------------------------------
// Test 2: Distinct event types land on distinct subjects
// ---------------------------------------------------------------------------

func TestJetStreamAuditEmitter_DistinctEventTypes_DistinctSubjects(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream integration test in short mode")
	}

	js, stop := startTestJSForAudit(t)
	defer stop()

	inner := newFakeInnerStore()
	ctx := context.Background()

	emitter, err := auditstore.NewJetStreamAuditEmitter(ctx, inner, js, 1, nil)
	if err != nil {
		t.Fatalf("NewJetStreamAuditEmitter: %v", err)
	}
	defer emitter.Close()

	ws := "myworkspace"
	events := []struct {
		eventType      string
		expectedSuffix string
	}{
		{auditstore.EventTypeAuth, "auth"},
		{auditstore.EventTypeMessage, "message"},
		{auditstore.EventTypeKV, "kv"},
	}

	for _, tc := range events {
		ev := &auditstore.Event{
			Timestamp: time.Now().UTC(),
			EventType: tc.eventType,
			ActorType: "user",
			ActorID:   "u1",
			Workspace: ws,
			SessionID: uuid.New(),
			Success:   true,
			Metadata:  map[string]interface{}{},
		}
		// Use LogEventSync so we don't need to sleep.
		if err := emitter.LogEventSync(ctx, ev); err != nil {
			t.Fatalf("LogEventSync(%s): %v", tc.eventType, err)
		}
	}

	// Each event type should be on its own subject.
	for _, tc := range events {
		filterSubject := "audit." + ws + "." + tc.expectedSuffix
		payload := consumeOne(t, js, "audit", filterSubject, 5*time.Second)

		var got auditstore.Event
		if err := json.Unmarshal(payload, &got); err != nil {
			t.Fatalf("unmarshal payload for %s: %v", tc.eventType, err)
		}
		if got.EventType != tc.eventType {
			t.Errorf("subject %s: EventType = %q, want %q", filterSubject, got.EventType, tc.eventType)
		}
	}
}

// ---------------------------------------------------------------------------
// Test 3: Publish failure does not block inner write
// ---------------------------------------------------------------------------

func TestJetStreamAuditEmitter_PublishFailure_DoesNotBlockInner(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream integration test in short mode")
	}

	js, stop := startTestJSForAudit(t)

	inner := newFakeInnerStore()
	ctx := context.Background()

	emitter, err := auditstore.NewJetStreamAuditEmitter(ctx, inner, js, 1, nil)
	if err != nil {
		t.Fatalf("NewJetStreamAuditEmitter: %v", err)
	}

	// Shut down NATS to force publish failure.
	stop()

	event := &auditstore.Event{
		Timestamp: time.Now().UTC(),
		EventType: auditstore.EventTypeAdmin,
		ActorType: "service",
		ActorID:   "gw-1",
		Workspace: "ws",
		SessionID: uuid.New(),
		Success:   true,
		Metadata:  map[string]interface{}{},
	}

	// LogEventSync must return nil (inner write succeeded) even though JetStream publish fails.
	if err := emitter.LogEventSync(ctx, event); err != nil {
		t.Errorf("LogEventSync returned error after JS shutdown: %v", err)
	}

	if got := inner.callCount(); got != 1 {
		t.Errorf("inner.LogEventSync call count = %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// Test 4: Empty workspace uses _system fallback
// ---------------------------------------------------------------------------

func TestJetStreamAuditEmitter_EmptyWorkspace_UsesSystemFallback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping JetStream integration test in short mode")
	}

	js, stop := startTestJSForAudit(t)
	defer stop()

	inner := newFakeInnerStore()
	ctx := context.Background()

	emitter, err := auditstore.NewJetStreamAuditEmitter(ctx, inner, js, 1, nil)
	if err != nil {
		t.Fatalf("NewJetStreamAuditEmitter: %v", err)
	}
	defer emitter.Close()

	event := &auditstore.Event{
		Timestamp: time.Now().UTC(),
		EventType: auditstore.EventTypeConnection,
		ActorType: "service",
		ActorID:   "system",
		Workspace: "", // deliberately empty
		SessionID: uuid.New(),
		Success:   true,
		Metadata:  map[string]interface{}{},
	}

	if err := emitter.LogEventSync(ctx, event); err != nil {
		t.Fatalf("LogEventSync: %v", err)
	}

	// Subject must be audit._5F_system.connection
	// (natscodec escapes '_' as '_5F_', so "_system" → "_5F_system")
	payload := consumeOne(t, js, "audit", "audit._5F_system.connection", 5*time.Second)

	var got auditstore.Event
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.ActorID != event.ActorID {
		t.Errorf("ActorID = %q, want %q", got.ActorID, event.ActorID)
	}
}
