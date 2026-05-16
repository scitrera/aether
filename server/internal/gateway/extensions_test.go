package gateway

import (
	"context"
	"strings"
	"testing"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/models"
)

// fakeAgentExtensionLookup implements agentDeclaredLookup for tests. It
// returns a fixed slice of URIs for a known implementation; everything else
// returns nil.
type fakeAgentExtensionLookup struct {
	impl string
	uris []string
}

func (f fakeAgentExtensionLookup) AgentExtensions(_ context.Context, implementation string) []string {
	if implementation == f.impl {
		return f.uris
	}
	return nil
}

// withKnownExtensions swaps KnownExtensions for the duration of the test
// and restores the original mapping on cleanup. Phase 6 ships with
// KnownExtensions empty by default; the negotiator tests pin specific
// URIs so the assertions don't depend on which extensions later phases
// happen to bless.
func withKnownExtensions(t *testing.T, fresh map[string]bool) {
	t.Helper()
	orig := KnownExtensions
	KnownExtensions = fresh
	t.Cleanup(func() { KnownExtensions = orig })
}

func TestExtensionNegotiation_AllSupportedRoundtrip(t *testing.T) {
	withKnownExtensions(t, map[string]bool{
		"https://example.com/ext-a": true,
		"https://example.com/ext-b": true,
	})

	decls := []*pb.ExtensionDeclaration{
		{Uri: "https://example.com/ext-a", Version: "1.0"},
		{Uri: "https://example.com/ext-b"},
	}

	res := negotiateExtensions(
		context.Background(),
		decls,
		models.Identity{Type: models.PrincipalUser},
		nil,
	)

	if res.rejectURI != "" {
		t.Fatalf("unexpected reject: %s (%s)", res.rejectURI, res.rejectReason)
	}
	if len(res.negotiated) != 2 {
		t.Fatalf("want 2 negotiated entries, got %d", len(res.negotiated))
	}
	for _, nx := range res.negotiated {
		if !nx.Supported {
			t.Errorf("uri %q should be supported", nx.Uri)
		}
		if nx.RejectionReason != "" {
			t.Errorf("uri %q: rejection_reason should be empty, got %q", nx.Uri, nx.RejectionReason)
		}
	}
	if v, ok := res.activeURIs["https://example.com/ext-a"]; !ok || v != "1.0" {
		t.Errorf("activeURIs should record ext-a version 1.0, got %q (ok=%v)", v, ok)
	}
	if len(res.serverSupported) != 0 {
		t.Errorf("server_supported should be empty when client declared everything; got %v", res.serverSupported)
	}
}

func TestExtensionNegotiation_RequiredAndUnsupportedRejects(t *testing.T) {
	withKnownExtensions(t, map[string]bool{
		"https://example.com/ext-known": true,
	})

	decls := []*pb.ExtensionDeclaration{
		{Uri: "https://example.com/ext-unknown", Required: true},
		{Uri: "https://example.com/ext-known"},
	}

	res := negotiateExtensions(
		context.Background(),
		decls,
		models.Identity{Type: models.PrincipalUser},
		nil,
	)

	if res.rejectURI != "https://example.com/ext-unknown" {
		t.Fatalf("want rejectURI=ext-unknown, got %q", res.rejectURI)
	}
	if !strings.Contains(res.rejectReason, "ext-unknown") {
		t.Errorf("rejectReason should mention the failing URI, got %q", res.rejectReason)
	}
	// Both declarations are still surfaced so the client can introspect.
	if len(res.negotiated) != 2 {
		t.Fatalf("want 2 negotiated entries even on reject, got %d", len(res.negotiated))
	}
	var sawUnknown, sawKnown bool
	for _, nx := range res.negotiated {
		switch nx.Uri {
		case "https://example.com/ext-unknown":
			sawUnknown = true
			if nx.Supported {
				t.Errorf("ext-unknown should be unsupported")
			}
			if nx.RejectionReason == "" {
				t.Errorf("ext-unknown should carry a rejection reason")
			}
		case "https://example.com/ext-known":
			sawKnown = true
			if !nx.Supported {
				t.Errorf("ext-known should be supported")
			}
		}
	}
	if !sawUnknown || !sawKnown {
		t.Errorf("missing expected negotiated entries (unknown=%v, known=%v)", sawUnknown, sawKnown)
	}
}

func TestExtensionNegotiation_NonRequiredAndUnsupportedNegotiates(t *testing.T) {
	withKnownExtensions(t, map[string]bool{}) // server supports nothing

	decls := []*pb.ExtensionDeclaration{
		{Uri: "https://example.com/optional-ext"}, // required=false
	}

	res := negotiateExtensions(
		context.Background(),
		decls,
		models.Identity{Type: models.PrincipalUser},
		nil,
	)

	if res.rejectURI != "" {
		t.Fatalf("non-required unsupported extension must not reject; got rejectURI=%q", res.rejectURI)
	}
	if len(res.negotiated) != 1 {
		t.Fatalf("want 1 negotiated entry, got %d", len(res.negotiated))
	}
	nx := res.negotiated[0]
	if nx.Supported {
		t.Errorf("uri should be unsupported")
	}
	if nx.RejectionReason == "" {
		t.Errorf("uri should carry a rejection reason")
	}
	if len(res.activeURIs) != 0 {
		t.Errorf("activeURIs should be empty when nothing was supported, got %v", res.activeURIs)
	}
}

func TestExtensionNegotiation_HeaderUnionsWithProto(t *testing.T) {
	withKnownExtensions(t, map[string]bool{
		"https://example.com/proto-ext":  true,
		"https://example.com/header-ext": true,
	})

	// Simulate the union the connect-time path performs: proto-declared
	// extensions plus header-derived ExtensionDeclaration values.
	hdr := parseExtensionMetadataHeader([]string{"https://example.com/header-ext, https://example.com/header-ext2"})
	if len(hdr) != 2 {
		t.Fatalf("expected 2 header-derived declarations, got %d", len(hdr))
	}
	for _, d := range hdr {
		if d.Required {
			t.Errorf("header-derived declarations must always be non-required, got %q required", d.Uri)
		}
	}

	decls := append([]*pb.ExtensionDeclaration{
		{Uri: "https://example.com/proto-ext", Required: true},
	}, hdr...)

	res := negotiateExtensions(
		context.Background(),
		decls,
		models.Identity{Type: models.PrincipalUser},
		nil,
	)

	if res.rejectURI != "" {
		t.Fatalf("unexpected reject: %s (%s)", res.rejectURI, res.rejectReason)
	}
	if len(res.negotiated) != 3 {
		t.Fatalf("expected 3 negotiated entries (proto + 2 header), got %d", len(res.negotiated))
	}
	want := map[string]bool{
		"https://example.com/proto-ext":   true,
		"https://example.com/header-ext":  true,
		"https://example.com/header-ext2": false, // not in KnownExtensions
	}
	for _, nx := range res.negotiated {
		expected, ok := want[nx.Uri]
		if !ok {
			t.Errorf("unexpected URI in negotiated set: %q", nx.Uri)
			continue
		}
		if nx.Supported != expected {
			t.Errorf("uri %q: supported=%v, want %v", nx.Uri, nx.Supported, expected)
		}
	}
}

func TestExtensionNegotiation_AgentDeclaredExtension(t *testing.T) {
	// Empty KnownExtensions — only the agent's own declaration should
	// widen the supported set, and only for that agent.
	withKnownExtensions(t, map[string]bool{})

	agentURI := "https://example.com/agent-private-ext"
	lookup := fakeAgentExtensionLookup{
		impl: "my-impl",
		uris: []string{agentURI},
	}

	decls := []*pb.ExtensionDeclaration{
		{Uri: agentURI, Required: true},
	}

	// Agent identity matching the lookup's implementation: the URI should
	// negotiate as supported even though it's not in KnownExtensions.
	res := negotiateExtensions(
		context.Background(),
		decls,
		models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      "ws1",
			Implementation: "my-impl",
			Specifier:      "spec1",
		},
		lookup,
	)
	if res.rejectURI != "" {
		t.Fatalf("agent-declared extension should be accepted, got reject %s", res.rejectURI)
	}
	if len(res.negotiated) != 1 || !res.negotiated[0].Supported {
		t.Fatalf("expected agent-declared URI to be supported; got negotiated=%+v", res.negotiated)
	}

	// Non-agent (e.g. user) caller with the same lookup wired in should
	// NOT inherit the agent's declarations — they're scoped to agent
	// sessions on that implementation. Required URI -> reject.
	res2 := negotiateExtensions(
		context.Background(),
		decls,
		models.Identity{Type: models.PrincipalUser},
		lookup,
	)
	if res2.rejectURI != agentURI {
		t.Fatalf("non-agent caller should reject required unknown URI; got rejectURI=%q", res2.rejectURI)
	}
}

func TestParseExtensionMetadataHeader_TrimsAndDedupes(t *testing.T) {
	// Multiple header values, commas with whitespace, empty entries, and
	// duplicates — parseExtensionMetadataHeader preserves duplicates
	// (negotiation handles them as separate declarations).
	values := []string{
		"  https://a.example/one  ,  ,https://a.example/two",
		"https://a.example/three",
	}
	got := parseExtensionMetadataHeader(values)
	if len(got) != 3 {
		t.Fatalf("expected 3 declarations, got %d", len(got))
	}
	wantURIs := []string{
		"https://a.example/one",
		"https://a.example/two",
		"https://a.example/three",
	}
	for i, d := range got {
		if d.Uri != wantURIs[i] {
			t.Errorf("[%d]: got %q, want %q", i, d.Uri, wantURIs[i])
		}
		if d.Required {
			t.Errorf("[%d]: header-derived declarations must be non-required", i)
		}
	}
}

func TestIsExtensionKnown_TablePoint(t *testing.T) {
	// Sanity check: IsExtensionKnown reads KnownExtensions directly, so
	// once we swap in a fresh map the helper picks up the new entries.
	withKnownExtensions(t, map[string]bool{"https://x.example/y": true})
	if !IsExtensionKnown("https://x.example/y") {
		t.Errorf("IsExtensionKnown should return true for an entry we just added")
	}
	if IsExtensionKnown("https://nope.example/") {
		t.Errorf("IsExtensionKnown should return false for an unmapped URI")
	}
}
