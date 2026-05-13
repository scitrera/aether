//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/proxysidecar"
	"github.com/scitrera/aether/pkg/identityheaders"
)

// ---------------------------------------------------------------------------
// 1. GET / POST happy path
// ---------------------------------------------------------------------------

func TestPhase1_GET_HappyPath(t *testing.T) {
	t.Parallel()

	be := newEchoBackend(t, "be-a")
	h := newHarness()
	h.addTerminator(t, "memorylayer", "default", be.srv.URL, "tenant-test", proxysidecar.HeaderModeStrict)

	caller := userCaller("alice", "win-1")
	req := &pb.ProxyHttpRequest{
		RequestId:   "r-get-1",
		TargetTopic: "sv::memorylayer::default",
		Method:      "GET",
		Path:        "/v1/items",
		Headers: map[string]string{
			"User-Agent":                        "phase1-test",
			identityheaders.HeaderUserID:        "alice",
			identityheaders.HeaderPrincipalType: "User",
		},
	}
	// Append a query string to assert it round-trips.
	req.Path = "/v1/items?limit=10&q=foo"

	resp, body, err := h.proxyHTTP(context.Background(), caller, req)
	if err != nil {
		t.Fatalf("harness.proxyHTTP: %v", err)
	}
	if status := expectOK(t, resp); status != 200 {
		t.Errorf("status: got %d, want 200", status)
	}
	if got := resp.Headers["Content-Type"]; got != "application/json" {
		t.Errorf("Content-Type: got %q, want %q", got, "application/json")
	}
	if got := resp.Headers["X-Backend-Tag"]; got != "be-a" {
		t.Errorf("X-Backend-Tag: got %q, want %q", got, "be-a")
	}
	if !strings.Contains(string(body), `"path":"/v1/items"`) {
		t.Errorf("expected path in echo body, got %q", string(body))
	}
	if !strings.Contains(string(body), `"query":"limit=10&q=foo"`) {
		t.Errorf("expected query string preserved, got %q", string(body))
	}

	// Backend must have observed the User-Agent and minted X-Auth headers.
	rec := be.lastRequest()
	if rec == nil {
		t.Fatal("backend never recorded a request")
	}
	if got := rec.Headers.Get("User-Agent"); got != "phase1-test" {
		t.Errorf("backend User-Agent: got %q, want %q", got, "phase1-test")
	}
	if got := rec.Headers.Get(identityheaders.HeaderTenantID); got != "tenant-test" {
		t.Errorf("backend X-Auth-Tenant-ID: got %q, want %q", got, "tenant-test")
	}
	if got := rec.Headers.Get(identityheaders.HeaderUserID); got != "alice" {
		t.Errorf("backend X-Auth-User-ID: got %q, want %q", got, "alice")
	}
	if got := rec.Headers.Get(identityheaders.HeaderAuthorityMode); got != identityheaders.AuthorityModeDirect {
		t.Errorf("backend X-Auth-Authority-Mode: got %q, want %q",
			got, identityheaders.AuthorityModeDirect)
	}
}

func TestPhase1_POST_HappyPath_PreservesContentType(t *testing.T) {
	t.Parallel()

	be := newEchoBackend(t, "be-post")
	h := newHarness()
	h.addTerminator(t, "memorylayer", "p1", be.srv.URL, "tenant-test", proxysidecar.HeaderModeStrict)

	caller := userCaller("alice", "win-1")
	body := []byte(`{"hello":"world"}`)

	req := &pb.ProxyHttpRequest{
		RequestId:   "r-post-1",
		TargetTopic: "sv::memorylayer::p1",
		Method:      "POST",
		Path:        "/echo-bytes",
		Headers: map[string]string{
			"Content-Type":                      "application/json",
			identityheaders.HeaderUserID:        "alice",
			identityheaders.HeaderPrincipalType: "User",
		},
		Body: body,
	}

	resp, respBody, err := h.proxyHTTP(context.Background(), caller, req)
	if err != nil {
		t.Fatalf("harness.proxyHTTP: %v", err)
	}
	if status := expectOK(t, resp); status != 200 {
		t.Fatalf("status: got %d, want 200", status)
	}
	if string(respBody) != string(body) {
		t.Errorf("response body: got %q, want echo of %q", string(respBody), string(body))
	}
	rec := be.lastRequest()
	if rec == nil {
		t.Fatal("backend never recorded a request")
	}
	if rec.ContentType != "application/json" {
		t.Errorf("backend Content-Type: got %q, want application/json", rec.ContentType)
	}
	if string(rec.Body) != string(body) {
		t.Errorf("backend body: got %q, want %q", string(rec.Body), string(body))
	}
}

// TestPhase1_POST_LargeChunkedBody verifies a >256 KB body round-trips
// end-to-end. The harness assembles the body up-front and hands it to the
// terminator in one HandleProxyRequest call (the same way the gateway does
// after re-assembling chunked input).
func TestPhase1_POST_LargeChunkedBody(t *testing.T) {
	t.Parallel()

	be := newEchoBackend(t, "be-big")
	h := newHarness()
	h.addTerminator(t, "memorylayer", "big", be.srv.URL, "tenant-test", proxysidecar.HeaderModeStrict)

	// 768 KiB body — comfortably above the 256 KiB chunk threshold used by
	// the Python SDK's proxy layer.
	body := make([]byte, 768<<10)
	if _, err := rand.Read(body); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	req := &pb.ProxyHttpRequest{
		RequestId:   "r-big",
		TargetTopic: "sv::memorylayer::big",
		Method:      "POST",
		Path:        "/echo-bytes",
		Headers: map[string]string{
			"Content-Type":                      "application/octet-stream",
			identityheaders.HeaderUserID:        "alice",
			identityheaders.HeaderPrincipalType: "User",
		},
		Body: body,
	}

	resp, respBody, err := h.proxyHTTP(context.Background(), userCaller("alice", "w"), req)
	if err != nil {
		t.Fatalf("harness.proxyHTTP: %v", err)
	}
	if status := expectOK(t, resp); status != 200 {
		t.Fatalf("status: got %d, want 200", status)
	}
	if len(respBody) != len(body) {
		t.Fatalf("body length: got %d, want %d", len(respBody), len(body))
	}
	if string(respBody) != string(body) {
		t.Error("body contents did not survive round-trip")
	}
	rec := be.lastRequest()
	if rec == nil || len(rec.Body) != len(body) {
		t.Fatalf("backend body length mismatch: got %d, want %d",
			len(rec.Body), len(body))
	}
}

// ---------------------------------------------------------------------------
// 2. ACL deny path
// ---------------------------------------------------------------------------

func TestPhase1_ACLDeny_NoGrant(t *testing.T) {
	t.Parallel()

	be := newEchoBackend(t, "be-acl")
	h := newHarness()
	h.addTerminator(t, "memorylayer", "acl", be.srv.URL, "tenant-test", proxysidecar.HeaderModeStrict)

	// Allow only callers in workspace `prod`. A user is a non-workspaced
	// principal here; deny.
	h.setACL(func(caller, target string) bool {
		return strings.HasPrefix(caller, "ag::prod::")
	})

	req := &pb.ProxyHttpRequest{
		RequestId:   "r-acl",
		TargetTopic: "sv::memorylayer::acl",
		Method:      "GET",
		Path:        "/v1/anything",
		Headers: map[string]string{
			identityheaders.HeaderUserID:        "carol",
			identityheaders.HeaderPrincipalType: "User",
		},
	}
	resp, _, err := h.proxyHTTP(context.Background(), userCaller("carol", "w"), req)
	if err != nil {
		t.Fatalf("harness.proxyHTTP: %v", err)
	}
	expectError(t, resp, pb.ProxyError_ACL_DENIED)

	// Backend must not have been invoked.
	if c := be.requestCount(); c != 0 {
		t.Errorf("expected 0 backend requests on ACL deny, got %d", c)
	}
}

func TestPhase1_ACLDeny_NonServiceTarget(t *testing.T) {
	t.Parallel()

	h := newHarness()
	req := &pb.ProxyHttpRequest{
		RequestId:   "r-bad",
		TargetTopic: "ag::ws::other::v1", // not sv::*
	}
	resp, _, err := h.proxyHTTP(context.Background(), userCaller("alice", "w"), req)
	if err != nil {
		t.Fatalf("harness.proxyHTTP: %v", err)
	}
	expectError(t, resp, pb.ProxyError_ACL_DENIED)
}

// ---------------------------------------------------------------------------
// 3. OBO header injection — byte-equal to auth-proxy
// ---------------------------------------------------------------------------

func TestPhase1_OBO_HeadersByteEqualToAuthProxy(t *testing.T) {
	t.Parallel()

	be := newEchoBackend(t, "be-obo")
	h := newHarness()
	h.addTerminator(t, "memorylayer", "obo", be.srv.URL, "tenant-test", proxysidecar.HeaderModeStrict)

	// Configure a stub authority resolver that issues the same authority the
	// auth-proxy would resolve for this grant. We then assert that the headers
	// the backend receives match (byte for byte) what identityheaders.Mint
	// would emit for the same Identity{Authority: ...}.
	resolver := newStubResolver()
	authority := &identityheaders.AuthenticatedAuthority{
		ActorType:       "Service",
		ActorID:         "sv::memorylayer::caller",
		GrantID:         "grant-test-1",
		SubjectType:     "User",
		SubjectID:       "alice",
		RootSubjectType: "User",
		RootSubjectID:   "alice",
		AudienceType:    "Service",
		AudienceID:      "sv::memorylayer::obo",
		MaxAccessLevel:  3,
		WorkspaceScope:  []string{"prod", "staging"},
	}
	resolver.setAuthority(authority)

	// Wire the resolver into the terminator. WithAuthorityResolver applies it
	// to all backends.
	for _, e := range h.terminators {
		e.t.WithAuthorityResolver(resolver)
	}

	// Caller hands an OBO authorization context exactly like the Python SDK's
	// proxy_http() shorthand would emit.
	req := &pb.ProxyHttpRequest{
		RequestId:   "r-obo",
		TargetTopic: "sv::memorylayer::obo",
		Method:      "GET",
		Path:        "/me",
		Headers: map[string]string{
			// Caller identity headers (the gateway forwards these as the
			// authoritative actor identity to the sidecar).
			identityheaders.HeaderUserID:        "sv::memorylayer::caller",
			identityheaders.HeaderPrincipalType: "Service",
		},
		Authorization: &pb.AuthorizationContext{
			AuthorityMode: identityheaders.AuthorityModeOnBehalfOf,
			Subject: &pb.PrincipalRef{
				PrincipalType: "User",
				PrincipalId:   "alice",
			},
			GrantId: "grant-test-1",
		},
	}

	resp, _, err := h.proxyHTTP(context.Background(), agentCaller("prod", "memorylayer", "caller"), req)
	if err != nil {
		t.Fatalf("harness.proxyHTTP: %v", err)
	}
	expectOK(t, resp)

	rec := be.lastRequest()
	if rec == nil {
		t.Fatal("backend never recorded a request")
	}

	// Compute the byte-level reference using identityheaders.Mint with the
	// resolved authority — exactly what the auth-proxy's InjectHeaders does.
	expected := identityheaders.Mint(context.Background(), "tenant-test", identityheaders.Identity{
		// In OBO mode the actor identity (the "caller") is what the
		// sidecar received via its caller-headers map; the authority overlay
		// fills in the subject/grant fields.
		UserID:        "sv::memorylayer::caller",
		PrincipalType: "Service",
		Authority:     authority,
	})

	// All trusted X-Auth-* headers must agree byte-for-byte.
	checked := []string{
		identityheaders.HeaderTenantID,
		identityheaders.HeaderUserID,
		identityheaders.HeaderPrincipalType,
		identityheaders.HeaderActorType,
		identityheaders.HeaderActorID,
		identityheaders.HeaderAuthorityMode,
		identityheaders.HeaderGrantID,
		identityheaders.HeaderSubjectType,
		identityheaders.HeaderSubjectID,
		identityheaders.HeaderRootSubjectType,
		identityheaders.HeaderRootSubjectID,
		identityheaders.HeaderAudienceType,
		identityheaders.HeaderAudienceID,
		identityheaders.HeaderMaxAccessLevel,
		identityheaders.HeaderWorkspaceScope,
	}
	for _, h := range checked {
		want := expected.Get(h)
		got := rec.Headers.Get(h)
		if got != want {
			t.Errorf("%s: sidecar minted %q, auth-proxy mints %q (mismatch breaks byte-equivalence)",
				h, got, want)
		}
	}

	// And authority-mode must reflect OBO.
	if got := rec.Headers.Get(identityheaders.HeaderAuthorityMode); got != identityheaders.AuthorityModeOnBehalfOf {
		t.Errorf("authority-mode: got %q, want %q", got, identityheaders.AuthorityModeOnBehalfOf)
	}
}

// ---------------------------------------------------------------------------
// 4. Wildcard sv::{impl} fan-out across two instances + offline / online
// ---------------------------------------------------------------------------

func TestPhase1_Wildcard_TwoInstances_FanOut_AndOfflineRouting(t *testing.T) {
	t.Parallel()

	beA := newEchoBackend(t, "be-A")
	beB := newEchoBackend(t, "be-B")
	h := newHarness()
	a := h.addTerminator(t, "memorylayer", "a", beA.srv.URL, "tenant-test", proxysidecar.HeaderModeStrict)
	b := h.addTerminator(t, "memorylayer", "b", beB.srv.URL, "tenant-test", proxysidecar.HeaderModeStrict)

	caller := userCaller("alice", "w")
	const N = 60

	send := func(rid string) (*pb.ProxyHttpResponse, []byte) {
		req := &pb.ProxyHttpRequest{
			RequestId:   rid,
			TargetTopic: "sv::memorylayer", // bare wildcard
			Method:      "GET",
			Path:        "/v1/ping",
			Headers: map[string]string{
				identityheaders.HeaderUserID:        "alice",
				identityheaders.HeaderPrincipalType: "User",
			},
		}
		resp, body, err := h.proxyHTTP(context.Background(), caller, req)
		if err != nil {
			t.Fatalf("proxyHTTP: %v", err)
		}
		return resp, body
	}

	// Both online — distribution should touch both buckets.
	for i := 0; i < N; i++ {
		send("r-w-" + iToA(i))
	}
	if a.hits.Load() == 0 || b.hits.Load() == 0 {
		t.Errorf("expected both instances to receive traffic, got a=%d b=%d",
			a.hits.Load(), b.hits.Load())
	}
	if total := a.hits.Load() + b.hits.Load(); total != N {
		t.Errorf("total hits across instances: got %d, want %d", total, N)
	}

	// Take instance B offline; subsequent requests must all land on A.
	b.online.Store(false)
	preOfflineA := a.hits.Load()
	preOfflineB := b.hits.Load()
	for i := 0; i < 30; i++ {
		send("r-off-" + iToA(i))
	}
	if got := b.hits.Load() - preOfflineB; got != 0 {
		t.Errorf("expected B to receive no traffic while offline, got %d new hits", got)
	}
	if got := a.hits.Load() - preOfflineA; got != 30 {
		t.Errorf("expected A to receive 30 hits while B offline, got %d", got)
	}

	// Bring B back online; both should be reachable again.
	b.online.Store(true)
	preBackA := a.hits.Load()
	preBackB := b.hits.Load()
	for i := 0; i < 60; i++ {
		send("r-back-" + iToA(i))
	}
	if got := b.hits.Load() - preBackB; got == 0 {
		t.Errorf("expected B to receive traffic after coming back online, got 0")
	}
	if got := a.hits.Load() - preBackA; got == 0 {
		t.Errorf("expected A to keep receiving traffic when both online, got 0")
	}
}

// ---------------------------------------------------------------------------
// 5. Idle timeout path
// ---------------------------------------------------------------------------

func TestPhase1_IdleTimeout_TerminatedRequestReturnsTimeout(t *testing.T) {
	t.Parallel()

	be := newEchoBackend(t, "be-slow")
	h := newHarness()
	h.addTerminatorWithIdle(t, "memorylayer", "slow", be.srv.URL, "tenant-test",
		proxysidecar.HeaderModeStrict, 250) // 250 ms idle timeout on backend

	caller := userCaller("alice", "w")
	req := &pb.ProxyHttpRequest{
		RequestId:   "r-slow",
		TargetTopic: "sv::memorylayer::slow",
		Method:      "GET",
		Path:        "/slow", // backend sleeps 2s on this path
		// Per-request timeout shorter than the backend sleep but >= idle.
		TimeoutMs: 500,
		Headers: map[string]string{
			identityheaders.HeaderUserID:        "alice",
			identityheaders.HeaderPrincipalType: "User",
		},
	}

	start := time.Now()
	resp, _, err := h.proxyHTTP(context.Background(), caller, req)
	if err != nil {
		t.Fatalf("proxyHTTP: %v", err)
	}
	elapsed := time.Since(start)
	expectError(t, resp, pb.ProxyError_TIMEOUT)
	if elapsed > 1500*time.Millisecond {
		t.Errorf("expected idle timeout to short-circuit dispatch (<1.5s), elapsed %s", elapsed)
	}
}

// ---------------------------------------------------------------------------
// 6. Sidecar unavailable: target with no instances
// ---------------------------------------------------------------------------

func TestPhase1_SidecarUnavailable_NoInstances(t *testing.T) {
	t.Parallel()

	h := newHarness()
	// No terminators registered for sv::ghost.
	req := &pb.ProxyHttpRequest{
		RequestId:   "r-empty",
		TargetTopic: "sv::ghost",
		Method:      "GET",
		Path:        "/anything",
	}
	resp, _, err := h.proxyHTTP(context.Background(), userCaller("alice", "w"), req)
	if err != nil {
		t.Fatalf("proxyHTTP: %v", err)
	}
	expectError(t, resp, pb.ProxyError_SIDECAR_UNAVAILABLE)
}

func TestPhase1_SidecarUnavailable_AllInstancesOffline(t *testing.T) {
	t.Parallel()

	be := newEchoBackend(t, "be-off")
	h := newHarness()
	e := h.addTerminator(t, "memorylayer", "only", be.srv.URL, "tenant-test", proxysidecar.HeaderModeStrict)
	e.online.Store(false)

	req := &pb.ProxyHttpRequest{
		RequestId:   "r-all-off",
		TargetTopic: "sv::memorylayer",
		Method:      "GET",
		Path:        "/x",
	}
	resp, _, err := h.proxyHTTP(context.Background(), userCaller("alice", "w"), req)
	if err != nil {
		t.Fatalf("proxyHTTP: %v", err)
	}
	expectError(t, resp, pb.ProxyError_SIDECAR_UNAVAILABLE)
}

// ---------------------------------------------------------------------------
// 7. Concrete sv::{impl}::{spec} routing (no wildcard)
// ---------------------------------------------------------------------------

func TestPhase1_ConcreteAddress_RoutesToExactInstance(t *testing.T) {
	t.Parallel()

	beA := newEchoBackend(t, "be-A")
	beB := newEchoBackend(t, "be-B")
	h := newHarness()
	h.addTerminator(t, "memorylayer", "a", beA.srv.URL, "tenant-test", proxysidecar.HeaderModeStrict)
	h.addTerminator(t, "memorylayer", "b", beB.srv.URL, "tenant-test", proxysidecar.HeaderModeStrict)

	for i := 0; i < 10; i++ {
		req := &pb.ProxyHttpRequest{
			RequestId:   "r-conc-" + iToA(i),
			TargetTopic: "sv::memorylayer::a",
			Method:      "GET",
			Path:        "/v1/ping",
			Headers: map[string]string{
				identityheaders.HeaderUserID:        "alice",
				identityheaders.HeaderPrincipalType: "User",
			},
		}
		resp, _, err := h.proxyHTTP(context.Background(), userCaller("alice", "w"), req)
		if err != nil {
			t.Fatalf("proxyHTTP: %v", err)
		}
		expectOK(t, resp)
	}
	if a, b := beA.requestCount(), beB.requestCount(); a != 10 || b != 0 {
		t.Errorf("concrete routing: a=%d b=%d, want a=10 b=0", a, b)
	}
}
