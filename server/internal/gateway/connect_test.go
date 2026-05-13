package gateway

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/checkpoint"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/pkg/models"
)

// ---------------------------------------------------------------------------
// Mock implementations of the 4 gateway interfaces
// ---------------------------------------------------------------------------

// mockSessionManager implements SessionManager.
type mockSessionManager struct {
	mu                  sync.Mutex
	acquireResult       bool
	acquireResumed      bool
	acquireForced       bool
	acquireErr          error
	releaseErr          error
	refreshResult       bool
	refreshErr          error
	registerErr         error
	serviceInstances    []string
	serviceInstancesErr error
	tunnelPins          map[string]string
	requestPins         map[string]string
	setPinErr           error
	getPinErr           error
	setRequestPinErr    error
	getRequestPinErr    error
	sessionIdentity     models.Identity
	sessionIdentityErr  error
	sessionGateway      string
	sessionGatewayErr   error
	unregisterErr       error
	refreshSessErr      error
	isActiveResult      bool
	isActiveErr         error

	// call tracking
	acquireCalls       []acquireCall
	releaseCalls       []releaseCall
	registerCalls      []registerCall
	unregisterCalls    []string // session IDs
	sessionGatewayLook []models.Identity
}

type registerCall struct {
	identity  models.Identity
	sessionID string
	gatewayID string
}

type acquireCall struct {
	identity        models.Identity
	sessionID       string
	resumeSessionID string
}

type releaseCall struct {
	identity  models.Identity
	sessionID string
}

func newMockSessionManager() *mockSessionManager {
	return &mockSessionManager{
		acquireResult:  true,
		acquireResumed: false,
		refreshResult:  true,
		isActiveResult: true,
	}
}

func (m *mockSessionManager) AcquireOrResumeLock(_ context.Context, identity models.Identity, sessionID, resumeSessionID string, _ int64) (bool, bool, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acquireCalls = append(m.acquireCalls, acquireCall{identity, sessionID, resumeSessionID})
	return m.acquireResult, m.acquireResumed, m.acquireForced, m.acquireErr
}

func (m *mockSessionManager) ReleaseLock(_ context.Context, identity models.Identity, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseCalls = append(m.releaseCalls, releaseCall{identity, sessionID})
	return m.releaseErr
}

func (m *mockSessionManager) RefreshLock(_ context.Context, _ models.Identity, _ string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.refreshResult, m.refreshErr
}

func (m *mockSessionManager) RefreshLockAndSession(_ context.Context, _ models.Identity, _ string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.refreshResult, m.refreshErr
}

func (m *mockSessionManager) RegisterSession(_ context.Context, identity models.Identity, sessionID, gatewayID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registerCalls = append(m.registerCalls, registerCall{identity, sessionID, gatewayID})
	return m.registerErr
}

func (m *mockSessionManager) GetSessionIdentity(_ context.Context, _ string) (models.Identity, error) {
	return m.sessionIdentity, m.sessionIdentityErr
}

func (m *mockSessionManager) GetSessionGateway(_ context.Context, identity models.Identity) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessionGatewayLook = append(m.sessionGatewayLook, identity)
	return m.sessionGateway, m.sessionGatewayErr
}

func (m *mockSessionManager) UnregisterSession(_ context.Context, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unregisterCalls = append(m.unregisterCalls, sessionID)
	return m.unregisterErr
}

func (m *mockSessionManager) RefreshSession(_ context.Context, _ string) error {
	return m.refreshSessErr
}

func (m *mockSessionManager) IsActive(_ context.Context, _ string) (bool, error) {
	return m.isActiveResult, m.isActiveErr
}

// FindHealthyServiceInstances returns whatever the test fixture pre-populated
// in serviceInstances; mockServiceInstancesErr lets a test simulate a scan
// failure. The TTL filter is ignored — tests that need TTL semantics use the
// real SessionRegistry against miniredis.
func (m *mockSessionManager) FindHealthyServiceInstances(_ context.Context, _ string, _ time.Duration) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.serviceInstancesErr != nil {
		return nil, m.serviceInstancesErr
	}
	out := make([]string, len(m.serviceInstances))
	copy(out, m.serviceInstances)
	return out, nil
}

func (m *mockSessionManager) SetTunnelPin(_ context.Context, tunnelID, identity string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.tunnelPins == nil {
		m.tunnelPins = make(map[string]string)
	}
	if m.setPinErr != nil {
		return m.setPinErr
	}
	m.tunnelPins[tunnelID] = identity
	return nil
}

func (m *mockSessionManager) GetTunnelPin(_ context.Context, tunnelID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getPinErr != nil {
		return "", m.getPinErr
	}
	return m.tunnelPins[tunnelID], nil
}

func (m *mockSessionManager) RefreshTunnelPin(_ context.Context, _ string, _ time.Duration) error {
	return nil
}

func (m *mockSessionManager) DeleteTunnelPin(_ context.Context, tunnelID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tunnelPins, tunnelID)
	return nil
}

func (m *mockSessionManager) SetRequestPin(_ context.Context, requestID, pinValue string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.requestPins == nil {
		m.requestPins = make(map[string]string)
	}
	if m.setRequestPinErr != nil {
		return m.setRequestPinErr
	}
	m.requestPins[requestID] = pinValue
	return nil
}

func (m *mockSessionManager) GetRequestPin(_ context.Context, requestID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getRequestPinErr != nil {
		return "", m.getRequestPinErr
	}
	return m.requestPins[requestID], nil
}

func (m *mockSessionManager) RefreshRequestPin(_ context.Context, _ string, _ time.Duration) error {
	return nil
}

func (m *mockSessionManager) DeleteRequestPin(_ context.Context, requestID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.requestPins, requestID)
	return nil
}

// mockMessageRouter implements MessageRouter.
type mockMessageRouter struct {
	mu                     sync.Mutex
	publishedMessages      []publishedMsg
	subscribedTopics       []string
	exclusiveSubscriptions map[string]string // topic -> consumerName
	subscribeErr           error
}

type publishedMsg struct {
	topic   string
	payload []byte
}

func newMockMessageRouter() *mockMessageRouter {
	return &mockMessageRouter{
		exclusiveSubscriptions: make(map[string]string),
	}
}

func (m *mockMessageRouter) Publish(_ context.Context, topic string, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishedMessages = append(m.publishedMessages, publishedMsg{topic, payload})
	return nil
}

func (m *mockMessageRouter) Subscribe(topic string, _ func([]byte)) (func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.subscribeErr != nil {
		return nil, m.subscribeErr
	}
	m.subscribedTopics = append(m.subscribedTopics, topic)
	return func() {}, nil
}

func (m *mockMessageRouter) SubscribeExclusive(topic string, consumerName string, _ func([]byte)) (func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.subscribeErr != nil {
		return nil, m.subscribeErr
	}
	m.exclusiveSubscriptions[topic] = consumerName
	return func() {}, nil
}

func (m *mockMessageRouter) SubscribeExclusiveFromNow(topic string, consumerName string, handler func([]byte)) (func(), error) {
	return m.SubscribeExclusive(topic, consumerName, handler)
}

func (m *mockMessageRouter) SubscribeExclusiveFromTimestamp(topic string, consumerName string, _ int64, handler func([]byte)) (func(), error) {
	return m.SubscribeExclusive(topic, consumerName, handler)
}

func (m *mockMessageRouter) hasSharedTopic(topic string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.subscribedTopics {
		if t == topic {
			return true
		}
	}
	return false
}

func (m *mockMessageRouter) hasExclusiveTopic(topic string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.exclusiveSubscriptions[topic]
	return ok
}

// mockKVReadWriter implements KVReadWriter.
type mockKVReadWriter struct {
	mu       sync.Mutex
	listData map[string]string
	getErr   error
	setErr   error
	delErr   error
	listErr  error
}

func newMockKVReadWriter() *mockKVReadWriter {
	return &mockKVReadWriter{
		listData: make(map[string]string),
	}
}

func (m *mockKVReadWriter) Get(_ context.Context, _ models.Identity, _ kv.KVScope, _ string, _ string, _ string) (string, error) {
	return "", m.getErr
}

func (m *mockKVReadWriter) Set(_ context.Context, _ models.Identity, _ kv.KVScope, _ string, _ string, _ string, _ string, _ time.Duration) error {
	return m.setErr
}

func (m *mockKVReadWriter) Delete(_ context.Context, _ models.Identity, _ kv.KVScope, _ string, _ string, _ string) error {
	return m.delErr
}

func (m *mockKVReadWriter) List(_ context.Context, _ models.Identity, _ kv.KVScope, _ string, _ string) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listErr != nil {
		return nil, m.listErr
	}
	result := make(map[string]string, len(m.listData))
	for k, v := range m.listData {
		result[k] = v
	}
	return result, nil
}

func (m *mockKVReadWriter) ListPaginated(_ context.Context, _ models.Identity, _ kv.KVScope, _ string, _ string, _ *kv.ListOptions) (*kv.ListResult, error) {
	return &kv.ListResult{}, nil
}

func (m *mockKVReadWriter) Increment(_ context.Context, _ models.Identity, _ kv.KVScope, _ string, _ string, _ string) (int64, error) {
	return 0, nil
}

func (m *mockKVReadWriter) Decrement(_ context.Context, _ models.Identity, _ kv.KVScope, _ string, _ string, _ string) (int64, error) {
	return 0, nil
}

func (m *mockKVReadWriter) IncrementIf(_ context.Context, _ models.Identity, _ kv.KVScope, _ string, _ string, _ string, _ int64, _ int64) (int64, bool, error) {
	return 0, true, nil
}

func (m *mockKVReadWriter) DecrementIf(_ context.Context, _ models.Identity, _ kv.KVScope, _ string, _ string, _ string, _ int64, _ int64) (int64, bool, error) {
	return 0, true, nil
}

// mockCheckpointManager implements CheckpointManager.
type mockCheckpointManager struct {
	mu        sync.Mutex
	saveErr   error
	loadErr   error
	deleteErr error
	listErr   error
	savedData map[string][]byte // key -> data
}

func newMockCheckpointManager() *mockCheckpointManager {
	return &mockCheckpointManager{
		savedData: make(map[string][]byte),
	}
}

func (m *mockCheckpointManager) Save(_ context.Context, _ models.Identity, key string, data []byte, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return m.saveErr
	}
	m.savedData[key] = data
	return nil
}

func (m *mockCheckpointManager) Load(_ context.Context, _ models.Identity, key string) (*checkpoint.Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.loadErr != nil {
		return nil, m.loadErr
	}
	data, ok := m.savedData[key]
	if !ok {
		return nil, nil
	}
	return &checkpoint.Checkpoint{Data: data, SavedAt: time.Now()}, nil
}

func (m *mockCheckpointManager) Delete(_ context.Context, _ models.Identity, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.savedData, key)
	return nil
}

func (m *mockCheckpointManager) List(_ context.Context, _ models.Identity) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listErr != nil {
		return nil, m.listErr
	}
	keys := make([]string, 0, len(m.savedData))
	for k := range m.savedData {
		keys = append(keys, k)
	}
	return keys, nil
}

// ---------------------------------------------------------------------------
// Helper: build a minimal GatewayServer with mock dependencies
// ---------------------------------------------------------------------------

func newTestGatewayWithMocks(sessions *mockSessionManager, router *mockMessageRouter, kvStore *mockKVReadWriter, checkpoints *mockCheckpointManager) *GatewayServer {
	s := &GatewayServer{
		sessions:      sessions,
		router:        router,
		kv:            kvStore,
		checkpoints:   checkpoints,
		gatewayID:     "test-gateway",
		authHandler:   newAuthHandler(nil, false, MTLSModeStrict, nil, nil),
		quotaEnforcer: newQuotaEnforcer(100, 200),
	}
	return s
}

// ---------------------------------------------------------------------------
// TestNewGatewayServer - verify constructor wires dependencies
// ---------------------------------------------------------------------------

func TestNewGatewayServer_StoresGatewayID(t *testing.T) {
	sessions := newMockSessionManager()
	router := newMockMessageRouter()
	kvStore := newMockKVReadWriter()
	checkpoints := newMockCheckpointManager()

	s := newTestGatewayWithMocks(sessions, router, kvStore, checkpoints)

	if s.gatewayID != "test-gateway" {
		t.Errorf("expected gatewayID 'test-gateway', got %q", s.gatewayID)
	}
}

func TestNewGatewayServer_SessionsFieldSet(t *testing.T) {
	sessions := newMockSessionManager()
	router := newMockMessageRouter()
	kvStore := newMockKVReadWriter()
	checkpoints := newMockCheckpointManager()

	s := newTestGatewayWithMocks(sessions, router, kvStore, checkpoints)

	if s.sessions == nil {
		t.Error("expected sessions to be set, got nil")
	}
}

func TestNewGatewayServer_RouterFieldSet(t *testing.T) {
	sessions := newMockSessionManager()
	router := newMockMessageRouter()
	kvStore := newMockKVReadWriter()
	checkpoints := newMockCheckpointManager()

	s := newTestGatewayWithMocks(sessions, router, kvStore, checkpoints)

	if s.router == nil {
		t.Error("expected router to be set, got nil")
	}
}

func TestNewGatewayServer_KVFieldSet(t *testing.T) {
	sessions := newMockSessionManager()
	router := newMockMessageRouter()
	kvStore := newMockKVReadWriter()
	checkpoints := newMockCheckpointManager()

	s := newTestGatewayWithMocks(sessions, router, kvStore, checkpoints)

	if s.kv == nil {
		t.Error("expected kv to be set, got nil")
	}
}

func TestNewGatewayServer_CheckpointsFieldSet(t *testing.T) {
	sessions := newMockSessionManager()
	router := newMockMessageRouter()
	kvStore := newMockKVReadWriter()
	checkpoints := newMockCheckpointManager()

	s := newTestGatewayWithMocks(sessions, router, kvStore, checkpoints)

	if s.checkpoints == nil {
		t.Error("expected checkpoints to be set, got nil")
	}
}

func TestWithCheckpointDefaultTTL_SetsField(t *testing.T) {
	sessions := newMockSessionManager()
	router := newMockMessageRouter()
	kvStore := newMockKVReadWriter()
	checkpoints := newMockCheckpointManager()

	s := newTestGatewayWithMocks(sessions, router, kvStore, checkpoints)
	WithCheckpointDefaultTTL(5 * time.Minute)(s)

	if s.checkpointDefaultTTL != 5*time.Minute {
		t.Errorf("expected checkpointDefaultTTL 5m, got %v", s.checkpointDefaultTTL)
	}
}

func TestWithMessageRateLimit_SetsFields(t *testing.T) {
	sessions := newMockSessionManager()
	router := newMockMessageRouter()
	kvStore := newMockKVReadWriter()
	checkpoints := newMockCheckpointManager()

	s := newTestGatewayWithMocks(sessions, router, kvStore, checkpoints)
	WithMessageRateLimit(50.0, 100)(s)

	if s.quotaEnforcer.messageRateLimit != 50.0 {
		t.Errorf("expected messageRateLimit 50.0, got %v", s.quotaEnforcer.messageRateLimit)
	}
	if s.quotaEnforcer.messageRateBurst != 100 {
		t.Errorf("expected messageRateBurst 100, got %v", s.quotaEnforcer.messageRateBurst)
	}
}

// ---------------------------------------------------------------------------
// TestClientSession_SafeSend - mutex-protected send
// ---------------------------------------------------------------------------

// mockStream is a fake AetherGateway_ConnectServer that records sent messages.
type mockStream struct {
	pb.AetherGateway_ConnectServer // embed for interface satisfaction; unused methods panic if called
	mu                             sync.Mutex
	sent                           []*pb.DownstreamMessage
	sendErr                        error
}

func (s *mockStream) Send(msg *pb.DownstreamMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sent = append(s.sent, msg)
	return nil
}

func (s *mockStream) sentCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

func TestClientSession_SafeSend_DeliversSingleMessage(t *testing.T) {
	stream := &mockStream{}
	client := &ClientSession{
		ID:            "sess-1",
		Identity:      models.Identity{Type: models.PrincipalAgent},
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	msg := &pb.DownstreamMessage{}
	if err := client.SafeSend(msg); err != nil {
		t.Fatalf("SafeSend returned unexpected error: %v", err)
	}

	if stream.sentCount() != 1 {
		t.Errorf("expected 1 sent message, got %d", stream.sentCount())
	}
}

func TestClientSession_SafeSend_PropagatesStreamError(t *testing.T) {
	stream := &mockStream{sendErr: context.Canceled}
	client := &ClientSession{
		ID:            "sess-err",
		Identity:      models.Identity{Type: models.PrincipalAgent},
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	err := client.SafeSend(&pb.DownstreamMessage{})
	if err == nil {
		t.Error("expected error from SafeSend, got nil")
	}
}

func TestClientSession_SafeSend_ConcurrentSendsDoNotRace(t *testing.T) {
	stream := &mockStream{}
	client := &ClientSession{
		ID:            "sess-concurrent",
		Identity:      models.Identity{Type: models.PrincipalAgent},
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_ = client.SafeSend(&pb.DownstreamMessage{})
		}()
	}
	wg.Wait()

	if stream.sentCount() != goroutines {
		t.Errorf("expected %d sent messages, got %d", goroutines, stream.sentCount())
	}
}

// ---------------------------------------------------------------------------
// TestClientSession_SubscriptionManagement
// ---------------------------------------------------------------------------

func TestClientSession_AddSubscription_RecordsTopic(t *testing.T) {
	client := &ClientSession{
		ID:            "sess-sub",
		subscriptions: make(map[string]func()),
	}

	called := false
	client.AddSubscription("ag::ws1::impl::spec", func() { called = true })

	if !client.HasSubscription("ag::ws1::impl::spec") {
		t.Error("expected HasSubscription to return true after AddSubscription")
	}
	_ = called // unsubscribe not yet invoked
}

func TestClientSession_HasSubscription_ReturnsFalseForUnknownTopic(t *testing.T) {
	client := &ClientSession{
		ID:            "sess-sub",
		subscriptions: make(map[string]func()),
	}

	if client.HasSubscription("ag::ws1::impl::spec") {
		t.Error("expected HasSubscription to return false for unregistered topic")
	}
}

func TestClientSession_RemoveSubscription_InvokesUnsubscribeFunc(t *testing.T) {
	client := &ClientSession{
		ID:            "sess-sub",
		subscriptions: make(map[string]func()),
	}

	unsubCalled := false
	client.AddSubscription("ga::ws1", func() { unsubCalled = true })
	client.RemoveSubscription("ga::ws1")

	if !unsubCalled {
		t.Error("expected unsubscribe function to be called on RemoveSubscription")
	}
	if client.HasSubscription("ga::ws1") {
		t.Error("expected topic to be removed from subscriptions after RemoveSubscription")
	}
}

func TestClientSession_RemoveSubscription_NoopForUnknownTopic(t *testing.T) {
	// Should not panic for a topic that was never added.
	client := &ClientSession{
		ID:            "sess-sub",
		subscriptions: make(map[string]func()),
	}
	// Should not panic
	client.RemoveSubscription("nonexistent.topic")
}

func TestClientSession_UnsubscribeAll_InvokesAllUnsubscribeFuncs(t *testing.T) {
	client := &ClientSession{
		ID:            "sess-sub",
		subscriptions: make(map[string]func()),
	}

	calls := make(map[string]bool)
	topics := []string{"ag::ws1::impl::spec", "ga::ws1", "gu::ws1"}
	for _, topic := range topics {
		t := topic // capture
		calls[t] = false
		client.AddSubscription(t, func() { calls[t] = true })
	}

	client.UnsubscribeAll()

	for _, topic := range topics {
		if !calls[topic] {
			t.Errorf("expected unsubscribe to be called for topic %q", topic)
		}
		if client.HasSubscription(topic) {
			t.Errorf("expected topic %q to be removed after UnsubscribeAll", topic)
		}
	}
}

func TestClientSession_UnsubscribeAll_IdempotentOnEmptySubscriptions(t *testing.T) {
	client := &ClientSession{
		ID:            "sess-empty",
		subscriptions: make(map[string]func()),
	}
	// Should not panic on empty subscriptions map
	client.UnsubscribeAll()
}

func TestClientSession_AddSubscription_InitializesNilMap(t *testing.T) {
	// subscriptions map not pre-initialized - AddSubscription must initialize it.
	client := &ClientSession{
		ID: "sess-nil-map",
	}

	client.AddSubscription("metric::prod", func() {})

	if !client.HasSubscription("metric::prod") {
		t.Error("expected HasSubscription to return true after AddSubscription on nil map")
	}
}

// ---------------------------------------------------------------------------
// TestConnectionState - verify connectionState struct fields
// ---------------------------------------------------------------------------

func TestConnectionState_Fields(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	identity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "ws1",
		Implementation: "impl",
		Specifier:      "spec",
	}

	cs := &connectionState{
		identity:         identity,
		sessionID:        "test-session-id",
		sessionCtx:       ctx,
		sessionCancel:    cancel,
		associatedTaskID: "task-123",
		resumed:          false,
	}

	if cs.identity.Type != models.PrincipalAgent {
		t.Errorf("expected PrincipalAgent, got %s", cs.identity.Type)
	}
	if cs.sessionID != "test-session-id" {
		t.Errorf("expected session ID 'test-session-id', got %q", cs.sessionID)
	}
	if cs.associatedTaskID != "task-123" {
		t.Errorf("expected associatedTaskID 'task-123', got %q", cs.associatedTaskID)
	}
	if cs.resumed {
		t.Error("expected resumed=false")
	}
}

func TestConnectionState_CancelTerminatesContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cs := &connectionState{
		sessionCtx:    ctx,
		sessionCancel: cancel,
	}

	cs.sessionCancel()

	select {
	case <-cs.sessionCtx.Done():
		// expected
	default:
		t.Error("expected context to be done after sessionCancel()")
	}
}

// ---------------------------------------------------------------------------
// TestRollbackSession - verify lock released and session unregistered
// ---------------------------------------------------------------------------

func TestRollbackSession_CallsUnregisterAndRelease(t *testing.T) {
	sessions := newMockSessionManager()
	router := newMockMessageRouter()
	kvStore := newMockKVReadWriter()
	checkpoints := newMockCheckpointManager()

	s := newTestGatewayWithMocks(sessions, router, kvStore, checkpoints)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	identity := models.Identity{
		Type:      models.PrincipalAgent,
		Workspace: "ws1",
	}

	cs := &connectionState{
		identity:      identity,
		sessionID:     "rollback-session-id",
		sessionCtx:    ctx,
		sessionCancel: cancel,
	}

	// Store in active streams to verify it gets deleted
	s.activeStreams.Store(cs.sessionID, &ClientSession{ID: cs.sessionID})
	s.identityIndex.Store(identity.String(), cs.sessionID)

	s.rollbackSession(cs)

	sessions.mu.Lock()
	unregLen := len(sessions.unregisterCalls)
	releaseLen := len(sessions.releaseCalls)
	sessions.mu.Unlock()

	if unregLen == 0 {
		t.Error("expected UnregisterSession to be called during rollback")
	}
	if releaseLen == 0 {
		t.Error("expected ReleaseLock to be called during rollback")
	}
}

func TestRollbackSession_RemovesFromActiveStreams(t *testing.T) {
	sessions := newMockSessionManager()
	router := newMockMessageRouter()
	kvStore := newMockKVReadWriter()
	checkpoints := newMockCheckpointManager()

	s := newTestGatewayWithMocks(sessions, router, kvStore, checkpoints)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	identity := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	cs := &connectionState{
		identity:      identity,
		sessionID:     "stream-session-id",
		sessionCtx:    ctx,
		sessionCancel: cancel,
	}

	s.activeStreams.Store(cs.sessionID, &ClientSession{ID: cs.sessionID})
	s.identityIndex.Store(identity.String(), cs.sessionID)

	s.rollbackSession(cs)

	if _, ok := s.activeStreams.Load(cs.sessionID); ok {
		t.Error("expected session to be removed from activeStreams after rollback")
	}
	if _, ok := s.identityIndex.Load(identity.String()); ok {
		t.Error("expected identity to be removed from identityIndex after rollback")
	}
}

// ---------------------------------------------------------------------------
// TestSendBaselineConfig - verify KV snapshot sent to agents/tasks, not users
// ---------------------------------------------------------------------------

func TestSendBaselineConfig_SkipsNonAgentPrincipals(t *testing.T) {
	principals := []models.PrincipalType{
		models.PrincipalUser,
		models.PrincipalOrchestrator,
		models.PrincipalWorkflowEngine,
		models.PrincipalMetricsBridge,
	}

	for _, pt := range principals {
		t.Run(string(pt), func(t *testing.T) {
			kvStore := newMockKVReadWriter()
			s := &GatewayServer{kv: kvStore}

			stream := &mockStream{}
			client := &ClientSession{
				Identity:      models.Identity{Type: pt, Workspace: "ws1"},
				Stream:        stream,
				subscriptions: make(map[string]func()),
			}

			s.sendBaselineConfig(context.Background(), client)

			// kv.List should not be called - no message sent
			if stream.sentCount() != 0 {
				t.Errorf("expected 0 messages sent for %s, got %d", pt, stream.sentCount())
			}
		})
	}
}

func TestSendBaselineConfig_SendsConfigSnapshotToAgent(t *testing.T) {
	kvStore := newMockKVReadWriter()
	kvStore.listData = map[string]string{"config_key": "config_value"}

	s := &GatewayServer{kv: kvStore}

	stream := &mockStream{}
	client := &ClientSession{
		Identity: models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "ws1",
			Implementation: "impl",
			Specifier:      "spec",
		},
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	s.sendBaselineConfig(context.Background(), client)

	if stream.sentCount() == 0 {
		t.Error("expected at least 1 message sent for agent, got 0")
	}

	stream.mu.Lock()
	msg := stream.sent[0]
	stream.mu.Unlock()

	if msg.GetConfig() == nil {
		t.Error("expected ConfigSnapshot payload in sent message, got nil")
	}
}

// TestSendBaselineConfig_PopulatesExclusiveKVFields verifies that the server
// populates WorkspaceExclusiveKv and GlobalExclusiveKv in the ConfigSnapshot
// and does NOT populate the legacy Kv/GlobalKv fields (which were removed from
// the send path in the KV scope revamp).
func TestSendBaselineConfig_PopulatesExclusiveKVFields(t *testing.T) {
	kvStore := newMockKVReadWriter()
	// The mock List always returns listData regardless of scope, so seeding it
	// simulates both the workspace-exclusive and global-exclusive list calls.
	kvStore.listData = map[string]string{"agent_setting": "on"}

	s := &GatewayServer{kv: kvStore}

	stream := &mockStream{}
	client := &ClientSession{
		Identity: models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "ws1",
			Implementation: "impl",
			Specifier:      "spec",
		},
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	s.sendBaselineConfig(context.Background(), client)

	if stream.sentCount() == 0 {
		t.Fatal("expected at least 1 message sent, got 0")
	}

	stream.mu.Lock()
	msg := stream.sent[0]
	stream.mu.Unlock()

	cfg := msg.GetConfig()
	if cfg == nil {
		t.Fatal("expected ConfigSnapshot payload, got nil")
	}

	// WorkspaceExclusiveKv and GlobalExclusiveKv must be populated.
	if len(cfg.WorkspaceExclusiveKv) == 0 {
		t.Error("expected WorkspaceExclusiveKv to be populated in ConfigSnapshot")
	}
	if len(cfg.GlobalExclusiveKv) == 0 {
		t.Error("expected GlobalExclusiveKv to be populated in ConfigSnapshot")
	}

	// Legacy Kv field must NOT be populated (deprecated, intentionally omitted).
	if len(cfg.Kv) != 0 {
		t.Errorf("expected legacy Kv field to be empty, got %d entries", len(cfg.Kv))
	}
	if len(cfg.GlobalKv) != 0 {
		t.Errorf("expected legacy GlobalKv field to be empty, got %d entries", len(cfg.GlobalKv))
	}
}

func TestSendBaselineConfig_SendsConfigSnapshotToTask(t *testing.T) {
	kvStore := newMockKVReadWriter()

	s := &GatewayServer{kv: kvStore}

	stream := &mockStream{}
	client := &ClientSession{
		Identity: models.Identity{
			Type:           models.PrincipalTask,
			Workspace:      "ws1",
			Implementation: "batch",
			Specifier:      "job-1",
		},
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	s.sendBaselineConfig(context.Background(), client)

	if stream.sentCount() == 0 {
		t.Error("expected at least 1 message sent for task, got 0")
	}
}

func TestSendBaselineConfig_KVErrorDoesNotSendMessage(t *testing.T) {
	kvStore := newMockKVReadWriter()
	kvStore.listErr = context.DeadlineExceeded

	s := &GatewayServer{kv: kvStore}

	stream := &mockStream{}
	client := &ClientSession{
		Identity: models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "ws1",
			Implementation: "impl",
			Specifier:      "spec",
		},
		Stream:        stream,
		subscriptions: make(map[string]func()),
	}

	// Should not panic even if KV List returns error
	s.sendBaselineConfig(context.Background(), client)
	// No assertion on sent count - the function logs and returns on error
}

// ---------------------------------------------------------------------------
// TestEnforceTopicPermissions_CrossWorkspace - cross-workspace pass-through
// ---------------------------------------------------------------------------
//
// The transport layer (enforceTopicPermissions) USED to unconditionally
// reject cross-workspace sends. That rule was over-broad and prevented
// legitimate operator-coordination patterns where the caller had explicit
// ACL grant on the target workspace (e.g., CoworkAgent in `_apps`
// pushing config to its spawned sidecar in `_sandbox`). The check moved
// downstream to checkMessageSendWith{Authority,Delegation}, which gate
// on ACL grants for the target workspace.
//
// These tests now assert the transport layer LETS the call through —
// per-test ACL setup in routing_*_test.go covers the actual deny path.

func TestEnforceTopicPermissions_CrossWorkspace_TbTopicAllowedAtTransport(t *testing.T) {
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	err := enforceTopicPermissions(sender, "tb::ws2::impl")
	if err != nil {
		t.Errorf("transport must not reject cross-workspace tb send (ACL is the gate now); got: %v", err)
	}
}

func TestEnforceTopicPermissions_CrossWorkspace_TaTopicAllowedAtTransport(t *testing.T) {
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	err := enforceTopicPermissions(sender, "ta::ws2::impl::id")
	if err != nil {
		t.Errorf("transport must not reject cross-workspace ta send (ACL is the gate now); got: %v", err)
	}
}

func TestEnforceTopicPermissions_CrossWorkspace_EventTopicAllowedAtTransport(t *testing.T) {
	sender := models.Identity{Type: models.PrincipalAgent, Workspace: "ws1"}
	err := enforceTopicPermissions(sender, "event::ws2")
	if err != nil {
		t.Errorf("transport must not reject cross-workspace event send (ACL is the gate now); got: %v", err)
	}
	// Same workspace should still be allowed.
	err = enforceTopicPermissions(sender, "event::ws1")
	if err != nil {
		t.Errorf("expected no error for same-workspace event topic, got: %v", err)
	}
}

func TestEnforceTopicPermissions_WorkflowEngine_SendsToAnyTopicType(t *testing.T) {
	// WorkflowEngine has no principal-level restrictions (event.*, metric.*, us.*, etc. all allowed).
	// Use same workspace to avoid cross-workspace guard, which applies to all principals.
	sender := models.Identity{Type: models.PrincipalWorkflowEngine, Workspace: "ws1"}
	topics := []string{
		"ag::ws1::impl::spec",
		"tu::ws1::impl::spec",
		"event::ws1",
		"metric::ws1",
		"us::user1::win1",
		"ga::ws1",
		"gu::ws1",
	}
	for _, topic := range topics {
		err := enforceTopicPermissions(sender, topic)
		if err != nil {
			t.Errorf("expected WorkflowEngine to send to %q without error, got: %v", topic, err)
		}
	}
}

// ---------------------------------------------------------------------------
// TestSetupClientSubscriptions_Agent / User / Orchestrator (additional coverage)
// ---------------------------------------------------------------------------

func TestSetupClientSubscriptions_AgentGetsIdentityAndBroadcastTopics(t *testing.T) {
	identity := models.Identity{
		Type:           models.PrincipalAgent,
		Workspace:      "staging",
		Implementation: "worker",
		Specifier:      "v2",
	}

	router := newMockMessageRouter()
	s := &GatewayServer{router: router}
	client := newTestClient(identity)

	if err := s.setupClientSubscriptions(client); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !router.hasExclusiveTopic("ag::staging::worker::v2") {
		t.Error("expected exclusive subscription for agent identity topic ag.staging.worker.v2")
	}
	if !router.hasSharedTopic("ga::staging") {
		t.Error("expected shared subscription for global agent broadcast ga.staging")
	}
}

func TestSetupClientSubscriptions_UserGetsWindowAndWorkspaceTopics(t *testing.T) {
	identity := models.Identity{
		Type:      models.PrincipalUser,
		ID:        "bob",
		Specifier: "win-42",
		Workspace: "workspace-x",
	}

	router := newMockMessageRouter()
	s := &GatewayServer{router: router}
	client := newTestClient(identity)

	if err := s.setupClientSubscriptions(client); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !router.hasExclusiveTopic("us::bob::win-42") {
		t.Error("expected exclusive subscription for user window topic us.bob.win-42")
	}
	if !router.hasSharedTopic("gu::workspace-x") {
		t.Error("expected shared subscription for global user topic gu.workspace-x")
	}
	if !router.hasSharedTopic("uw::bob::workspace-x") {
		t.Error("expected shared subscription for user-workspace topic uw.bob.workspace-x")
	}
}

func TestSetupClientSubscriptions_OrchestratorGetsNoSubscriptions(t *testing.T) {
	identity := models.Identity{
		Type:           models.PrincipalOrchestrator,
		Implementation: "k8s",
		Specifier:      "primary",
	}

	router := newMockMessageRouter()
	s := &GatewayServer{router: router}
	client := newTestClient(identity)

	if err := s.setupClientSubscriptions(client); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	router.mu.Lock()
	exclusiveCount := len(router.exclusiveSubscriptions)
	sharedCount := len(router.subscribedTopics)
	router.mu.Unlock()

	if exclusiveCount != 0 {
		t.Errorf("expected 0 exclusive subscriptions for orchestrator, got %d", exclusiveCount)
	}
	if sharedCount != 0 {
		t.Errorf("expected 0 shared subscriptions for orchestrator, got %d", sharedCount)
	}
}

// ---------------------------------------------------------------------------
// TestShouldOrchestrate - internal routing helper
// ---------------------------------------------------------------------------

func TestShouldOrchestrate_AgentTopicReturnsTrue(t *testing.T) {
	s := &GatewayServer{}
	if !s.shouldOrchestrate("ag::ws1::impl::spec") {
		t.Error("expected shouldOrchestrate=true for ag.* topic")
	}
}

func TestShouldOrchestrate_UniqueTaskTopicReturnsTrue(t *testing.T) {
	s := &GatewayServer{}
	if !s.shouldOrchestrate("tu::ws1::impl::spec") {
		t.Error("expected shouldOrchestrate=true for tu.* topic")
	}
}

func TestShouldOrchestrate_UserTopicReturnsFalse(t *testing.T) {
	s := &GatewayServer{}
	if s.shouldOrchestrate("us::user1::win1") {
		t.Error("expected shouldOrchestrate=false for us.* topic")
	}
}

func TestShouldOrchestrate_EventTopicReturnsFalse(t *testing.T) {
	s := &GatewayServer{}
	if s.shouldOrchestrate("event::ws1") {
		t.Error("expected shouldOrchestrate=false for event.* topic")
	}
}

func TestShouldOrchestrate_ShortTopicReturnsFalse(t *testing.T) {
	s := &GatewayServer{}
	// Topics shorter than 4 chars cannot match "ag::" or "tu::"
	if s.shouldOrchestrate("ag") {
		t.Error("expected shouldOrchestrate=false for short topic 'ag'")
	}
}

// ---------------------------------------------------------------------------
// TestMTLSMode - identity.go helpers
// ---------------------------------------------------------------------------

func TestMTLSMode_StrictIsValid(t *testing.T) {
	if !MTLSModeStrict.IsValid() {
		t.Error("expected MTLSModeStrict to be valid")
	}
}

func TestMTLSMode_RelaxedIsValid(t *testing.T) {
	if !MTLSModeRelaxed.IsValid() {
		t.Error("expected MTLSModeRelaxed to be valid")
	}
}

func TestMTLSMode_EmptyStringIsInvalid(t *testing.T) {
	if MTLSMode("").IsValid() {
		t.Error("expected empty MTLSMode to be invalid")
	}
}

func TestMTLSMode_UnknownValueIsInvalid(t *testing.T) {
	if MTLSMode("permissive").IsValid() {
		t.Error("expected 'permissive' MTLSMode to be invalid")
	}
}

func TestDefaultMTLSConfig_RequiredIsTrueAndModeIsStrict(t *testing.T) {
	cfg := DefaultMTLSConfig()
	if !cfg.Required {
		t.Error("expected DefaultMTLSConfig Required=true")
	}
	if cfg.Mode != MTLSModeStrict {
		t.Errorf("expected DefaultMTLSConfig Mode=strict, got %s", cfg.Mode)
	}
}
