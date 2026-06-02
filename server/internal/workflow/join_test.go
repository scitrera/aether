package workflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/aether/sdk/go/coord"
)

// ---- in-memory fakes (no gateway / KV backend needed) ----

type fakeCounter struct {
	mu sync.Mutex
	m  map[string]int64
}

func newFakeCounter() *fakeCounter { return &fakeCounter{m: map[string]int64{}} }

func (c *fakeCounter) IncrementIf(_ context.Context, key string, delta, ceiling int64) (int64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	proposed := c.m[key] + delta
	if proposed > ceiling {
		return c.m[key], false, nil
	}
	c.m[key] = proposed
	return proposed, true, nil
}

func (c *fakeCounter) DecrementIf(_ context.Context, key string, delta, floor int64) (int64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	proposed := c.m[key] - delta
	if proposed < floor {
		return c.m[key], false, nil
	}
	c.m[key] = proposed
	return proposed, true, nil
}

type fakeLocker struct {
	mu sync.Mutex
	m  map[string]string
}

func newFakeLocker() *fakeLocker { return &fakeLocker{m: map[string]string{}} }

func (l *fakeLocker) TryAcquire(_ context.Context, key, owner string, _ time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.m[key]; ok {
		return false, nil
	}
	l.m[key] = owner
	return true, nil
}

func (l *fakeLocker) Refresh(_ context.Context, key, owner string, _ time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.m[key] == owner {
		return true, nil
	}
	return false, nil
}

func (l *fakeLocker) Release(_ context.Context, key, owner string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.m[key] == owner {
		delete(l.m, key)
		return true, nil
	}
	return false, nil
}

func (l *fakeLocker) Peek(_ context.Context, key string) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.m[key], nil
}

type fakeJoinStore struct {
	mu   sync.Mutex
	rows map[string]*Join
	seq  int64
}

func newFakeJoinStore() *fakeJoinStore { return &fakeJoinStore{rows: map[string]*Join{}} }

func joinKey(name, ws, corr string) string { return name + "|" + ws + "|" + corr }

func (s *fakeJoinStore) byID(id int64) *Join {
	for _, r := range s.rows {
		if r.ID == id {
			return r
		}
	}
	return nil
}

func (s *fakeJoinStore) EnsureJoin(_ context.Context, j *Join) (*Join, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := joinKey(j.JoinName, j.Workspace, j.CorrelationKey)
	if ex, ok := s.rows[k]; ok {
		cp := *ex
		return &cp, nil
	}
	s.seq++
	cp := *j
	cp.ID = s.seq
	if cp.Status == "" {
		cp.Status = JoinStatusOpen
	}
	s.rows[k] = &cp
	out := cp
	return &out, nil
}

func (s *fakeJoinStore) GetJoin(_ context.Context, name, ws, corr string) (*Join, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.rows[joinKey(name, ws, corr)]; ok {
		cp := *r
		return &cp, nil
	}
	return nil, nil
}

func (s *fakeJoinStore) UpdateJoinArrived(_ context.Context, id, n int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.byID(id); r != nil {
		r.ArrivedCount = n
	}
	return nil
}

func (s *fakeJoinStore) SetJoinExpected(_ context.Context, id, expected int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.byID(id); r != nil {
		e := expected
		r.ExpectedCount = &e
	}
	return nil
}

func (s *fakeJoinStore) SetJoinDirty(_ context.Context, id int64, dirty bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.byID(id); r != nil {
		r.Dirty = dirty
	}
	return nil
}

func (s *fakeJoinStore) MarkJoinTerminal(_ context.Context, id int64, status string, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.byID(id); r != nil {
		r.Status = status
	}
	return nil
}

type fakeDispatcher struct {
	mu         sync.Mutex
	dispatched []*ActionDef
	emitted    []*ActionDef
}

func (d *fakeDispatcher) DispatchAction(a *ActionDef) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dispatched = append(d.dispatched, a)
	return nil
}

func (d *fakeDispatcher) EmitEvent(a *ActionDef) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.emitted = append(d.emitted, a)
	return nil
}

func (d *fakeDispatcher) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.dispatched)
}

// Compile-time conformance for the fakes.
var (
	_ coord.Counter    = (*fakeCounter)(nil)
	_ coord.Locker     = (*fakeLocker)(nil)
	_ joinStore        = (*fakeJoinStore)(nil)
	_ actionDispatcher = (*fakeDispatcher)(nil)
)

func newTestJoinEngine() (*JoinEngine, *fakeDispatcher, *fakeJoinStore) {
	disp := &fakeDispatcher{}
	store := newFakeJoinStore()
	je := NewJoinEngine(store, NewExprEngine(64), disp, newFakeCounter(), newFakeLocker(), "default")
	return je, disp, store
}

func env(input map[string]any, ws string) map[string]any {
	return map[string]any{
		"input":  input,
		"source": map[string]any{"workspace": ws},
	}
}

// ---- tests ----

func TestJoinCount_FiresOnceWhenComplete(t *testing.T) {
	je, disp, _ := newTestJoinEngine()
	ctx := context.Background()
	spec := &JoinSpec{
		Name:           "barrier",
		Mode:           JoinModeCount,
		CorrelationKey: "input.batch",
		ExpectedCount:  "2",
		OnComplete:     &ActionDef{Type: "create_task", TaskType: "kb"},
	}
	eA := env(map[string]any{"batch": "A"}, "ws")

	if err := je.HandleArrival(ctx, spec, eA, "member", "ws"); err != nil {
		t.Fatalf("arrival 1: %v", err)
	}
	if disp.count() != 0 {
		t.Fatalf("after 1/2 arrivals: dispatched=%d, want 0", disp.count())
	}
	if err := je.HandleArrival(ctx, spec, eA, "member", "ws"); err != nil {
		t.Fatalf("arrival 2: %v", err)
	}
	if disp.count() != 1 {
		t.Fatalf("after 2/2 arrivals: dispatched=%d, want 1", disp.count())
	}
	// A third (duplicate/late) member must not fire again — fire-marker gate.
	if err := je.HandleArrival(ctx, spec, eA, "member", "ws"); err != nil {
		t.Fatalf("arrival 3: %v", err)
	}
	if disp.count() != 1 {
		t.Fatalf("after 3rd arrival: dispatched=%d, want 1 (exactly-once)", disp.count())
	}
}

func TestJoinCount_CorrelationIsolation(t *testing.T) {
	je, disp, _ := newTestJoinEngine()
	ctx := context.Background()
	spec := &JoinSpec{
		Name:           "barrier",
		Mode:           JoinModeCount,
		CorrelationKey: "input.batch",
		ExpectedCount:  "2",
		OnComplete:     &ActionDef{Type: "create_task", TaskType: "kb"},
	}
	// One member each for two different batches: neither completes.
	_ = je.HandleArrival(ctx, spec, env(map[string]any{"batch": "A"}, "ws"), "member", "ws")
	_ = je.HandleArrival(ctx, spec, env(map[string]any{"batch": "B"}, "ws"), "member", "ws")
	if disp.count() != 0 {
		t.Fatalf("cross-batch arrivals fired prematurely: dispatched=%d, want 0", disp.count())
	}
	// Complete batch A only.
	_ = je.HandleArrival(ctx, spec, env(map[string]any{"batch": "A"}, "ws"), "member", "ws")
	if disp.count() != 1 {
		t.Fatalf("batch A completion: dispatched=%d, want 1", disp.count())
	}
}

func TestJoinCount_DedupDropsRepeats(t *testing.T) {
	je, disp, _ := newTestJoinEngine()
	ctx := context.Background()
	spec := &JoinSpec{
		Name:           "barrier",
		Mode:           JoinModeCount,
		CorrelationKey: "input.batch",
		ExpectedCount:  "3",
		DedupKey:       "input.id",
		OnComplete:     &ActionDef{Type: "create_task", TaskType: "kb"},
	}
	mk := func(id string) map[string]any { return map[string]any{"batch": "A", "id": id} }

	_ = je.HandleArrival(ctx, spec, env(mk("m1"), "ws"), "member", "ws")
	_ = je.HandleArrival(ctx, spec, env(mk("m1"), "ws"), "member", "ws") // duplicate — dropped
	_ = je.HandleArrival(ctx, spec, env(mk("m2"), "ws"), "member", "ws")
	if disp.count() != 0 {
		t.Fatalf("after m1,m1(dup),m2: dispatched=%d, want 0 (only 2 distinct)", disp.count())
	}
	_ = je.HandleArrival(ctx, spec, env(mk("m3"), "ws"), "member", "ws") // 3rd distinct → fire
	if disp.count() != 1 {
		t.Fatalf("after 3 distinct members: dispatched=%d, want 1", disp.count())
	}
}

func TestJoinCount_DynamicArmingAfterMembers(t *testing.T) {
	je, disp, _ := newTestJoinEngine()
	ctx := context.Background()
	spec := &JoinSpec{
		Name:           "barrier",
		Mode:           JoinModeCount,
		CorrelationKey: "input.batch",
		ExpectedCount:  "input.expected_count",
		ArmOnEvent:     "manifest",
		OnComplete:     &ActionDef{Type: "create_task", TaskType: "kb"},
	}
	// Two members arrive before the expected count is known.
	_ = je.HandleArrival(ctx, spec, env(map[string]any{"batch": "A"}, "ws"), "member", "ws")
	_ = je.HandleArrival(ctx, spec, env(map[string]any{"batch": "A"}, "ws"), "member", "ws")
	if disp.count() != 0 {
		t.Fatalf("before arming: dispatched=%d, want 0", disp.count())
	}
	// The manifest arms expected=2; the count already satisfies it → fire now.
	_ = je.HandleArrival(ctx, spec, env(map[string]any{"batch": "A", "expected_count": 2}, "ws"), "manifest", "ws")
	if disp.count() != 1 {
		t.Fatalf("after arming with count satisfied: dispatched=%d, want 1", disp.count())
	}
}

func TestJoinCoalesce_FirstFiresRestMarkDirty(t *testing.T) {
	je, disp, store := newTestJoinEngine()
	ctx := context.Background()
	spec := &JoinSpec{
		Name:           "coalesce-kb",
		Mode:           JoinModeCoalesce,
		CorrelationKey: "input.ws",
		OnComplete:     &ActionDef{Type: "create_task", TaskType: "kb"},
	}
	e := env(map[string]any{"ws": "tenant1"}, "ws")

	_ = je.HandleArrival(ctx, spec, e, "ingest", "ws") // first → fire
	_ = je.HandleArrival(ctx, spec, e, "ingest", "ws") // within active window → dirty
	_ = je.HandleArrival(ctx, spec, e, "ingest", "ws") // still dirty
	if disp.count() != 1 {
		t.Fatalf("coalesce: dispatched=%d, want 1", disp.count())
	}
	row, _ := store.GetJoin(ctx, "coalesce-kb", "ws", "tenant1")
	if row == nil || !row.Dirty {
		t.Fatalf("coalesce: expected dirty=true after coalesced arrivals, got row=%+v", row)
	}
}
