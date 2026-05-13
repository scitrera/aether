package aether

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// resolveFirstPendingAuthorityGrant drains the request queue and resolves
// the first pending authority-grant request with the given response.
func resolveFirstPendingAuthorityGrant(client *BaseClient, resp *pb.AuthorityGrantResponse) {
	time.Sleep(10 * time.Millisecond)
	<-client.RequestQueue()
	client.pendingAuthorityGrantRequests.Range(func(key, val any) bool {
		ch := val.(chan *pb.AuthorityGrantResponse)
		client.pendingAuthorityGrantRequests.Delete(key)
		ch <- resp
		return false
	})
}

// =============================================================================
// AuthorityGrantOps Tests
// =============================================================================

func TestAuthorityGrantOps_Exchange(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go resolveFirstPendingAuthorityGrant(client, &pb.AuthorityGrantResponse{
		Success: true,
		Grant: &pb.ACLAuthorityGrantInfo{
			GrantId:   "g-1",
			ExpiresAt: time.Now().Add(time.Hour).Unix(),
		},
	})

	resp, err := client.AuthorityGrants().Exchange(context.Background(), "sess-1", ExchangeOpts{
		AudienceType: "service",
		AudienceID:   "memorylayer",
	})
	if err != nil {
		t.Fatalf("Exchange() error = %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatal("Exchange() should be successful")
	}
	if resp.GetGrant().GetGrantId() != "g-1" {
		t.Errorf("Grant.GrantId = %q, want g-1", resp.GetGrant().GetGrantId())
	}
}

func TestAuthorityGrantOps_DeriveForTarget_QueuesCorrectOp(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	if err := client.AuthorityGrants().SendOp(&pb.AuthorityGrantOperation{
		Op: pb.AuthorityGrantOperation_DERIVE_FOR_TARGET,
		DeriveForTargetRequest: &pb.AuthorityGrantDeriveForTargetRequest{
			ParentGrantId: "g-parent",
			Target:        &pb.PrincipalRef{PrincipalType: "task", PrincipalId: "tsk-1"},
		},
	}); err != nil {
		t.Fatalf("SendOp() error = %v", err)
	}

	select {
	case msg := <-client.RequestQueue():
		op := msg.GetAuthorityGrantOp()
		if op == nil {
			t.Fatal("Expected AuthorityGrantOperation in queue")
		}
		if op.Op != pb.AuthorityGrantOperation_DERIVE_FOR_TARGET {
			t.Errorf("Op = %v, want DERIVE_FOR_TARGET", op.Op)
		}
		if op.GetDeriveForTargetRequest().GetParentGrantId() != "g-parent" {
			t.Errorf("ParentGrantId = %q", op.GetDeriveForTargetRequest().GetParentGrantId())
		}
	default:
		t.Error("Message should be queued")
	}
}

func TestAuthorityGrantOps_Renew_PassesExtendSeconds(t *testing.T) {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)

	go func() {
		time.Sleep(10 * time.Millisecond)
		msg := <-client.RequestQueue()
		op := msg.GetAuthorityGrantOp()
		if op == nil || op.Op != pb.AuthorityGrantOperation_RENEW {
			return
		}
		if op.GetRenewRequest().GetExtendSeconds() != 600 {
			return
		}
		client.pendingAuthorityGrantRequests.Range(func(key, val any) bool {
			ch := val.(chan *pb.AuthorityGrantResponse)
			client.pendingAuthorityGrantRequests.Delete(key)
			ch <- &pb.AuthorityGrantResponse{Success: true}
			return false
		})
	}()

	resp, err := client.AuthorityGrants().Renew(context.Background(), "g-1", 0, 600)
	if err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if !resp.GetSuccess() {
		t.Error("Renew() should be successful (server might have rejected extend_seconds value)")
	}
}

// =============================================================================
// Cache Tests
// =============================================================================

// cacheTestHarness wraps the operation stub injection. Because
// AuthorityGrantCache calls AuthorityGrantOps.Exchange/DeriveForTarget/Revoke
// directly, we replace those methods at test time by intercepting the
// underlying SendOpSync via a goroutine that drains the request queue.
type cacheTestHarness struct {
	t             *testing.T
	client        *BaseClient
	mu            sync.Mutex
	exchangeQueue []*pb.AuthorityGrantResponse
	deriveQueue   []*pb.AuthorityGrantResponse
	revokeQueue   []*pb.AuthorityGrantResponse
	exchangeCount int32
	deriveCount   int32
	revokeIDs     []string
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

func newCacheHarness(t *testing.T) *cacheTestHarness {
	cfg := BaseClientConfig{ServerAddr: TestServerAddr}
	client, err := NewBaseClient(cfg)
	if err != nil {
		t.Fatalf("NewBaseClient() error = %v", err)
	}
	client.running.Store(true)
	h := &cacheTestHarness{
		t:      t,
		client: client,
		stopCh: make(chan struct{}),
	}
	h.wg.Add(1)
	go h.run()
	return h
}

func (h *cacheTestHarness) run() {
	defer h.wg.Done()
	for {
		select {
		case <-h.stopCh:
			return
		case msg := <-h.client.RequestQueue():
			op := msg.GetAuthorityGrantOp()
			if op == nil {
				continue
			}
			var resp *pb.AuthorityGrantResponse
			h.mu.Lock()
			switch op.Op {
			case pb.AuthorityGrantOperation_EXCHANGE:
				atomic.AddInt32(&h.exchangeCount, 1)
				if len(h.exchangeQueue) > 0 {
					resp = h.exchangeQueue[0]
					h.exchangeQueue = h.exchangeQueue[1:]
				}
			case pb.AuthorityGrantOperation_DERIVE_FOR_TARGET:
				atomic.AddInt32(&h.deriveCount, 1)
				if len(h.deriveQueue) > 0 {
					resp = h.deriveQueue[0]
					h.deriveQueue = h.deriveQueue[1:]
				}
			case pb.AuthorityGrantOperation_REVOKE:
				h.revokeIDs = append(h.revokeIDs, op.GetGrantId())
				if len(h.revokeQueue) > 0 {
					resp = h.revokeQueue[0]
					h.revokeQueue = h.revokeQueue[1:]
				} else {
					resp = &pb.AuthorityGrantResponse{Success: true}
				}
			}
			h.mu.Unlock()
			if resp != nil {
				resp.RequestId = op.GetRequestId()
				h.client.pendingAuthorityGrantRequests.Resolve(op.GetRequestId(), resp)
			}
		}
	}
}

func (h *cacheTestHarness) close() {
	close(h.stopCh)
	h.wg.Wait()
}

func (h *cacheTestHarness) queueExchange(resps ...*pb.AuthorityGrantResponse) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.exchangeQueue = append(h.exchangeQueue, resps...)
}

func (h *cacheTestHarness) queueDerive(resps ...*pb.AuthorityGrantResponse) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.deriveQueue = append(h.deriveQueue, resps...)
}

func (h *cacheTestHarness) revokes() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.revokeIDs))
	copy(out, h.revokeIDs)
	return out
}

func grant(id, root string, expiresAt int64) *pb.ACLAuthorityGrantInfo {
	return &pb.ACLAuthorityGrantInfo{
		GrantId:     id,
		RootGrantId: root,
		ExpiresAt:   expiresAt,
	}
}

func TestAuthorityGrantCache_HitWithoutSecondExchange(t *testing.T) {
	h := newCacheHarness(t)
	defer h.close()

	expiresAt := time.Now().Add(time.Hour).Unix()
	h.queueExchange(&pb.AuthorityGrantResponse{
		Success: true,
		Grant:   grant("g-1", "g-1", expiresAt),
	})

	cache := h.client.MakeAuthorityCache()

	g1, err := cache.GetOrExchange(context.Background(), "sess-1", "service", "memorylayer", ExchangeOpts{})
	if err != nil {
		t.Fatalf("first GetOrExchange error = %v", err)
	}
	if g1.GetGrantId() != "g-1" {
		t.Errorf("got grant %q, want g-1", g1.GetGrantId())
	}

	g2, err := cache.GetOrExchange(context.Background(), "sess-1", "service", "memorylayer", ExchangeOpts{})
	if err != nil {
		t.Fatalf("second GetOrExchange error = %v", err)
	}
	if g2 != g1 {
		t.Error("cache should serve the same grant pointer on hit")
	}

	if got := atomic.LoadInt32(&h.exchangeCount); got != 1 {
		t.Errorf("exchangeCount = %d, want 1 (cache should serve from cache)", got)
	}
}

func TestAuthorityGrantCache_SoftRenewBeforeExpiry(t *testing.T) {
	h := newCacheHarness(t)
	defer h.close()

	now := time.Unix(1_700_000_000, 0)
	clock := now
	clockMu := sync.Mutex{}

	expiresAtFirst := now.Add(time.Minute).Unix() // 60s in future
	expiresAtSecond := now.Add(time.Hour).Unix()
	h.queueExchange(
		&pb.AuthorityGrantResponse{Success: true, Grant: grant("g-old", "g-old", expiresAtFirst)},
		&pb.AuthorityGrantResponse{Success: true, Grant: grant("g-new", "g-new", expiresAtSecond)},
	)

	cache := h.client.MakeAuthorityCache(
		WithSoftRenewSkew(45*time.Second),
		WithClock(func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			return clock
		}),
	)

	g1, err := cache.GetOrExchange(context.Background(), "sess", "", "", ExchangeOpts{})
	if err != nil {
		t.Fatalf("first GetOrExchange error = %v", err)
	}
	if g1.GetGrantId() != "g-old" {
		t.Errorf("first grant = %q, want g-old", g1.GetGrantId())
	}

	// Advance clock to within the soft-renew window: expiresAt - 30s,
	// which is past the 45s skew. The cache should re-exchange.
	clockMu.Lock()
	clock = clock.Add(31 * time.Second)
	clockMu.Unlock()

	g2, err := cache.GetOrExchange(context.Background(), "sess", "", "", ExchangeOpts{})
	if err != nil {
		t.Fatalf("second GetOrExchange error = %v", err)
	}
	if g2.GetGrantId() != "g-new" {
		t.Errorf("second grant = %q, want g-new (soft renew)", g2.GetGrantId())
	}
	if got := atomic.LoadInt32(&h.exchangeCount); got != 2 {
		t.Errorf("exchangeCount = %d, want 2", got)
	}
}

func TestAuthorityGrantCache_RevocationInvalidatesByGrantID(t *testing.T) {
	h := newCacheHarness(t)
	defer h.close()

	expiresAt := time.Now().Add(time.Hour).Unix()
	h.queueExchange(
		&pb.AuthorityGrantResponse{Success: true, Grant: grant("g-1", "g-root", expiresAt)},
		&pb.AuthorityGrantResponse{Success: true, Grant: grant("g-2", "g-root", expiresAt)},
	)

	cache := h.client.MakeAuthorityCache()

	if _, err := cache.GetOrExchange(context.Background(), "sess", "", "", ExchangeOpts{}); err != nil {
		t.Fatalf("first GetOrExchange error = %v", err)
	}

	// Server pushes a revocation for g-1.
	dropped := cache.HandleRevocationEvent(&pb.AuthorityGrantRevocation{GrantId: "g-1"})
	if dropped != 1 {
		t.Errorf("dropped = %d, want 1", dropped)
	}

	// Next call should re-exchange.
	g, err := cache.GetOrExchange(context.Background(), "sess", "", "", ExchangeOpts{})
	if err != nil {
		t.Fatalf("re-exchange error = %v", err)
	}
	if g.GetGrantId() != "g-2" {
		t.Errorf("post-revoke grant = %q, want g-2", g.GetGrantId())
	}
}

func TestAuthorityGrantCache_RevocationCascadeByRootGrantID(t *testing.T) {
	h := newCacheHarness(t)
	defer h.close()

	expiresAt := time.Now().Add(time.Hour).Unix()
	// Two distinct grants share a common root.
	h.queueExchange(
		&pb.AuthorityGrantResponse{Success: true, Grant: grant("g-1", "g-root", expiresAt)},
	)
	h.queueDerive(
		&pb.AuthorityGrantResponse{Success: true, Grant: grant("g-2", "g-root", expiresAt)},
	)

	cache := h.client.MakeAuthorityCache()

	if _, err := cache.GetOrExchange(context.Background(), "sess", "", "", ExchangeOpts{}); err != nil {
		t.Fatalf("GetOrExchange error = %v", err)
	}
	if _, err := cache.DeriveForTask(context.Background(), "g-1", "tsk-1", DeriveForTargetOpts{}); err != nil {
		t.Fatalf("DeriveForTask error = %v", err)
	}

	if cache.Stats().Size != 2 {
		t.Errorf("cache size = %d, want 2", cache.Stats().Size)
	}

	// Revoke the root grant — both children should be invalidated.
	dropped := cache.HandleRevocationEvent(&pb.AuthorityGrantRevocation{
		GrantId:     "g-root",
		RootGrantId: "g-root",
		Cascade:     true,
	})
	if dropped != 2 {
		t.Errorf("cascade dropped = %d, want 2", dropped)
	}
	if cache.Stats().Size != 0 {
		t.Errorf("cache size after cascade = %d, want 0", cache.Stats().Size)
	}
}

func TestAuthorityGrantCache_DeriveForTaskIdempotent(t *testing.T) {
	h := newCacheHarness(t)
	defer h.close()

	expiresAt := time.Now().Add(time.Hour).Unix()
	h.queueDerive(&pb.AuthorityGrantResponse{
		Success: true,
		Grant:   grant("g-derived", "g-root", expiresAt),
	})

	cache := h.client.MakeAuthorityCache()

	g1, err := cache.DeriveForTask(context.Background(), "g-parent", "tsk-1", DeriveForTargetOpts{
		AudienceType: "service",
		AudienceID:   "memorylayer",
	})
	if err != nil {
		t.Fatalf("first DeriveForTask error = %v", err)
	}

	g2, err := cache.DeriveForTask(context.Background(), "g-parent", "tsk-1", DeriveForTargetOpts{
		AudienceType: "service",
		AudienceID:   "memorylayer",
	})
	if err != nil {
		t.Fatalf("second DeriveForTask error = %v", err)
	}
	if g1 != g2 {
		t.Error("idempotent DeriveForTask should return cached grant on second call")
	}
	if got := atomic.LoadInt32(&h.deriveCount); got != 1 {
		t.Errorf("deriveCount = %d, want 1 (cache should serve repeat)", got)
	}
}

func TestAuthorityGrantCache_RevokeAllRevokesEachGrant(t *testing.T) {
	h := newCacheHarness(t)
	defer h.close()

	expiresAt := time.Now().Add(time.Hour).Unix()
	h.queueExchange(
		&pb.AuthorityGrantResponse{Success: true, Grant: grant("g-a", "g-a", expiresAt)},
	)
	h.queueDerive(
		&pb.AuthorityGrantResponse{Success: true, Grant: grant("g-b", "g-a", expiresAt)},
	)

	cache := h.client.MakeAuthorityCache()

	if _, err := cache.GetOrExchange(context.Background(), "sess-a", "svc", "x", ExchangeOpts{}); err != nil {
		t.Fatalf("Exchange error = %v", err)
	}
	if _, err := cache.DeriveForTask(context.Background(), "g-a", "tsk-1", DeriveForTargetOpts{}); err != nil {
		t.Fatalf("Derive error = %v", err)
	}

	if err := cache.RevokeAll(context.Background()); err != nil {
		t.Fatalf("RevokeAll() error = %v", err)
	}

	revoked := h.revokes()
	if len(revoked) != 2 {
		t.Errorf("revoke calls = %v, want 2 calls", revoked)
	}
	got := make(map[string]bool)
	for _, id := range revoked {
		got[id] = true
	}
	if !got["g-a"] || !got["g-b"] {
		t.Errorf("expected both g-a and g-b revoked, got %v", revoked)
	}
	if cache.Stats().Size != 0 {
		t.Errorf("cache size after revoke-all = %d, want 0", cache.Stats().Size)
	}
}

func TestAuthorityGrantCache_HandleRevocationDispatchedFromDownstream(t *testing.T) {
	h := newCacheHarness(t)
	defer h.close()

	expiresAt := time.Now().Add(time.Hour).Unix()
	h.queueExchange(&pb.AuthorityGrantResponse{
		Success: true,
		Grant:   grant("g-x", "g-x", expiresAt),
	})

	cache := h.client.MakeAuthorityCache()
	if _, err := cache.GetOrExchange(context.Background(), "sess", "", "", ExchangeOpts{}); err != nil {
		t.Fatalf("GetOrExchange error = %v", err)
	}

	// Simulate the gateway pushing a revocation through dispatchResponse.
	if err := h.client.dispatchResponse(context.Background(), &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_AuthorityGrantRevocation{
			AuthorityGrantRevocation: &pb.AuthorityGrantRevocation{
				GrantId: "g-x",
				Reason:  "test",
			},
		},
	}); err != nil {
		t.Fatalf("dispatchResponse() error = %v", err)
	}

	if cache.Stats().Size != 0 {
		t.Errorf("cache size after dispatched revocation = %d, want 0", cache.Stats().Size)
	}
}

func TestAuthorityGrantCache_CloseDeregisters(t *testing.T) {
	h := newCacheHarness(t)
	defer h.close()

	cache := h.client.MakeAuthorityCache()
	cache.Close()

	// After Close the cache no longer receives revocation events; pre-close
	// cache had no entries so we're really just checking Close() is safe.
	cache.Close() // double-close should be a no-op
	if cache.Stats().Size != 0 {
		t.Error("cache should be empty")
	}
}

// =============================================================================
// High-level helpers (Phase 4)
// =============================================================================

func TestAuthorityGrantCache_IsValidPredicate(t *testing.T) {
	h := newCacheHarness(t)
	defer h.close()

	expiresAt := time.Now().Add(time.Hour).Unix()
	h.queueExchange(&pb.AuthorityGrantResponse{
		Success: true,
		Grant:   grant("g-1", "g-1", expiresAt),
	})

	cache := h.client.MakeAuthorityCache()
	if cache.IsValid("g-1") {
		t.Error("IsValid should be false before seeding")
	}
	if _, err := cache.GetOrExchange(context.Background(), "sess", "task", "t1", ExchangeOpts{}); err != nil {
		t.Fatalf("GetOrExchange error = %v", err)
	}
	if !cache.IsValid("g-1") {
		t.Error("IsValid should be true after seeding")
	}
	if cache.IsValid("") {
		t.Error("IsValid(\"\") must be false")
	}
	if cache.IsValid("g-missing") {
		t.Error("IsValid for unknown grant must be false")
	}
}

func TestAuthorityGrantCache_ListActiveSnapshotsAndDedupes(t *testing.T) {
	h := newCacheHarness(t)
	defer h.close()

	expiresAt := time.Now().Add(time.Hour).Unix()
	h.queueExchange(
		&pb.AuthorityGrantResponse{Success: true, Grant: grant("g-1", "g-1", expiresAt)},
		&pb.AuthorityGrantResponse{Success: true, Grant: grant("g-2", "g-2", expiresAt)},
	)
	cache := h.client.MakeAuthorityCache()
	if _, err := cache.GetOrExchange(context.Background(), "sess-a", "task", "t1", ExchangeOpts{}); err != nil {
		t.Fatalf("GetOrExchange(a) error = %v", err)
	}
	if _, err := cache.GetOrExchange(context.Background(), "sess-b", "task", "t2", ExchangeOpts{}); err != nil {
		t.Fatalf("GetOrExchange(b) error = %v", err)
	}

	active := cache.ListActive()
	if len(active) != 2 {
		t.Fatalf("ListActive returned %d, want 2 (got %+v)", len(active), active)
	}
	got := make(map[string]bool, len(active))
	for _, g := range active {
		got[g.GetGrantId()] = true
	}
	if !got["g-1"] || !got["g-2"] {
		t.Errorf("ListActive missing entries: %v", got)
	}
}

func TestAuthorityGrantCache_RevokeLocalDropsWithoutGatewayCall(t *testing.T) {
	h := newCacheHarness(t)
	defer h.close()

	expiresAt := time.Now().Add(time.Hour).Unix()
	h.queueExchange(&pb.AuthorityGrantResponse{
		Success: true,
		Grant:   grant("g-1", "g-1", expiresAt),
	})
	cache := h.client.MakeAuthorityCache()
	if _, err := cache.GetOrExchange(context.Background(), "sess", "task", "t1", ExchangeOpts{}); err != nil {
		t.Fatalf("GetOrExchange error = %v", err)
	}
	dropped := cache.RevokeLocal("g-1")
	if dropped != 1 {
		t.Errorf("RevokeLocal dropped = %d, want 1", dropped)
	}
	if cache.Stats().Size != 0 {
		t.Errorf("cache size after RevokeLocal = %d, want 0", cache.Stats().Size)
	}
	if calls := h.revokes(); len(calls) != 0 {
		t.Errorf("RevokeLocal must not invoke server-side revoke; got %v", calls)
	}
}

func TestAuthorityGrantCache_RefreshForceDropsAndReExchanges(t *testing.T) {
	h := newCacheHarness(t)
	defer h.close()

	expiresAt := time.Now().Add(time.Hour).Unix()
	h.queueExchange(
		&pb.AuthorityGrantResponse{Success: true, Grant: grant("g-old", "g-old", expiresAt)},
		&pb.AuthorityGrantResponse{Success: true, Grant: grant("g-new", "g-new", expiresAt)},
	)
	cache := h.client.MakeAuthorityCache()

	first, err := cache.GetOrExchange(context.Background(), "sess", "task", "t1", ExchangeOpts{})
	if err != nil {
		t.Fatalf("GetOrExchange error = %v", err)
	}
	if first.GetGrantId() != "g-old" {
		t.Fatalf("first grant = %q, want g-old", first.GetGrantId())
	}

	refreshed, err := cache.Refresh(context.Background(), "g-old", ExchangeOpts{})
	if err != nil {
		t.Fatalf("Refresh error = %v", err)
	}
	if refreshed == nil || refreshed.GetGrantId() != "g-new" {
		t.Fatalf("Refresh returned %+v, want grant g-new", refreshed)
	}
	if cache.IsValid("g-old") {
		t.Error("old grant should be evicted after Refresh")
	}
	if !cache.IsValid("g-new") {
		t.Error("new grant should be present after Refresh")
	}
}

func TestAuthorityGrantCache_RefreshUnknownGrantReturnsNil(t *testing.T) {
	h := newCacheHarness(t)
	defer h.close()

	cache := h.client.MakeAuthorityCache()
	g, err := cache.Refresh(context.Background(), "", ExchangeOpts{})
	if err != nil || g != nil {
		t.Errorf("Refresh(\"\") = (%v, %v), want (nil, nil)", g, err)
	}
	g, err = cache.Refresh(context.Background(), "never-cached", ExchangeOpts{})
	if err != nil || g != nil {
		t.Errorf("Refresh(unknown) = (%v, %v), want (nil, nil)", g, err)
	}
}

func TestAuthorityGrantCache_RefreshSkipsDerivedEntries(t *testing.T) {
	h := newCacheHarness(t)
	defer h.close()

	expiresAt := time.Now().Add(time.Hour).Unix()
	h.queueDerive(&pb.AuthorityGrantResponse{
		Success: true,
		Grant:   grant("g-derived", "g-root", expiresAt),
	})
	cache := h.client.MakeAuthorityCache()
	if _, err := cache.DeriveForTask(context.Background(), "g-parent", "tsk-1", DeriveForTargetOpts{}); err != nil {
		t.Fatalf("DeriveForTask error = %v", err)
	}
	g, err := cache.Refresh(context.Background(), "g-derived", ExchangeOpts{})
	if err != nil || g != nil {
		t.Errorf("Refresh on derived entry should return (nil, nil); got (%v, %v)", g, err)
	}
	if cache.IsValid("g-derived") {
		t.Error("derived grant should be dropped after Refresh attempt")
	}
}
