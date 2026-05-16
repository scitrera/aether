package registry

import (
	"testing"
)

func TestPrefixIndex_LookupExactAndPrefix(t *testing.T) {
	idx := NewPrefixIndex()
	idx.Set("chat-agent", []AgentResourceSchemaEntry{
		{ResourceTypePrefix: "chat/"},
	})
	idx.Set("doc-agent", []AgentResourceSchemaEntry{
		{ResourceTypePrefix: "docmgmt/document/"},
	})

	cases := []struct {
		name      string
		input     string
		wantImpl  string
		wantPref  string
		wantFound bool
	}{
		{"exact match", "chat/", "chat-agent", "chat/", true},
		{"trailing-slash variant", "chat", "chat-agent", "chat/", true},
		{"under chat prefix", "chat/session-1", "chat-agent", "chat/", true},
		{"deep nested under doc", "docmgmt/document/abc/xyz", "doc-agent", "docmgmt/document/", true},
		{"unknown family", "kvscope/global", "", "", false},
		{"empty input", "", "", "", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			impl, prefix, ok := idx.Lookup(c.input)
			if impl != c.wantImpl || prefix != c.wantPref || ok != c.wantFound {
				t.Fatalf("Lookup(%q) = (%q, %q, %v), want (%q, %q, %v)",
					c.input, impl, prefix, ok, c.wantImpl, c.wantPref, c.wantFound)
			}
		})
	}
}

func TestPrefixIndex_SetReleasesDroppedPrefixes(t *testing.T) {
	idx := NewPrefixIndex()
	idx.Set("agent-x", []AgentResourceSchemaEntry{
		{ResourceTypePrefix: "alpha/"},
		{ResourceTypePrefix: "beta/"},
	})
	if impl, _, ok := idx.Lookup("alpha/thing"); !ok || impl != "agent-x" {
		t.Fatalf("alpha lookup before update: impl=%q ok=%v, want agent-x true", impl, ok)
	}

	// Re-Set with a narrower schema: drop "alpha", keep "beta", add "gamma".
	idx.Set("agent-x", []AgentResourceSchemaEntry{
		{ResourceTypePrefix: "beta/"},
		{ResourceTypePrefix: "gamma/"},
	})

	if _, _, ok := idx.Lookup("alpha/thing"); ok {
		t.Fatalf("alpha lookup after drop: ok=true, want false")
	}
	if impl, _, ok := idx.Lookup("beta/thing"); !ok || impl != "agent-x" {
		t.Fatalf("beta lookup: impl=%q ok=%v, want agent-x true", impl, ok)
	}
	if impl, _, ok := idx.Lookup("gamma/thing"); !ok || impl != "agent-x" {
		t.Fatalf("gamma lookup: impl=%q ok=%v, want agent-x true", impl, ok)
	}
}

func TestPrefixIndex_Delete(t *testing.T) {
	idx := NewPrefixIndex()
	idx.Set("agent-a", []AgentResourceSchemaEntry{{ResourceTypePrefix: "shared/"}})
	idx.Set("agent-b", []AgentResourceSchemaEntry{{ResourceTypePrefix: "other/"}})

	idx.Delete("agent-a")
	if _, _, ok := idx.Lookup("shared/thing"); ok {
		t.Fatalf("shared lookup after delete: ok=true, want false")
	}
	if impl, _, ok := idx.Lookup("other/thing"); !ok || impl != "agent-b" {
		t.Fatalf("other lookup after agent-a delete: impl=%q ok=%v, want agent-b true", impl, ok)
	}
}

func TestPrefixIndex_RebuildFromRegistrations(t *testing.T) {
	idx := NewPrefixIndex()
	// Pre-populate with stale data that should be cleared.
	idx.Set("ghost", []AgentResourceSchemaEntry{{ResourceTypePrefix: "ghost/"}})

	all := []*AgentRegistration{
		{
			Implementation: "live-a",
			ResourceSchema: []AgentResourceSchemaEntry{{ResourceTypePrefix: "live-a/"}},
		},
		{
			Implementation: "live-b",
			ResourceSchema: []AgentResourceSchemaEntry{
				{ResourceTypePrefix: "live-b/r1/"},
				{ResourceTypePrefix: "live-b/r2/"},
			},
		},
	}
	idx.Rebuild(all)

	if _, _, ok := idx.Lookup("ghost/thing"); ok {
		t.Fatalf("ghost lookup after Rebuild: ok=true, want false")
	}
	if impl, _, ok := idx.Lookup("live-a/x"); !ok || impl != "live-a" {
		t.Fatalf("live-a lookup: impl=%q ok=%v, want live-a true", impl, ok)
	}
	if impl, _, ok := idx.Lookup("live-b/r1/x"); !ok || impl != "live-b" {
		t.Fatalf("live-b/r1 lookup: impl=%q ok=%v, want live-b true", impl, ok)
	}
	if impl, _, ok := idx.Lookup("live-b/r2/x"); !ok || impl != "live-b" {
		t.Fatalf("live-b/r2 lookup: impl=%q ok=%v, want live-b true", impl, ok)
	}
}

func TestPrefixIndex_NilReceiver(t *testing.T) {
	var idx *PrefixIndex // nil
	if _, _, ok := idx.Lookup("anything"); ok {
		t.Fatalf("nil Lookup: ok=true, want false")
	}
	// Should not panic.
	idx.Set("x", []AgentResourceSchemaEntry{{ResourceTypePrefix: "p/"}})
	idx.Delete("x")
	idx.Rebuild(nil)
}
