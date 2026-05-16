package registry

import (
	"strings"
	"sync"
)

// PrefixIndex maps an agent-declared resource_type_prefix to the implementation
// name that registered it. It is the in-memory routing table consulted by ACL
// CheckAccess (Phase 5 Stage B) to attribute resource access to the owning
// agent.
//
// The index is refreshed on every Register / Delete that touches the
// agent_registry table, plus a full Rebuild at gateway startup. It is NOT a
// source of truth: the agent_registry table is. If the index drifts (e.g.
// after a partial failure that committed the DB write but failed to call
// Set/Delete), audit attribution may be stale until the next Rebuild —
// CheckAccess decisions themselves remain correct because attribution is
// advisory and ACL decisions never depend on it.
//
// Thread-safe via an RWMutex. Lookups happen in the hot CheckAccess path, so
// reads are concurrent. Writes (Set/Delete/Rebuild) take the write lock briefly
// to swap the map shape.
type PrefixIndex struct {
	mu sync.RWMutex
	// prefixes maps resource_type_prefix -> implementation. The key is the
	// exact string declared in AgentResourceSchemaEntry.ResourceTypePrefix
	// (e.g. "chat/", "docmgmt/document"). Trailing slash semantics match
	// what the agent declared; Lookup tolerates both forms.
	prefixes map[string]string

	// watchMu guards watchActive separately from the prefix map so the
	// hot CheckAccess path never serializes against the watch lifecycle
	// state. Acquired only in StartJetStreamWatch / IsWatchActive /
	// runWatchLoop teardown — never on Lookup.
	watchMu sync.RWMutex
	// watchActive is true between a successful StartJetStreamWatch return
	// and the parent ctx's cancellation. Callers (gateway periodic
	// Rebuild scheduler) consult IsWatchActive to suppress redundant
	// DB-driven Rebuilds when JetStream is the live propagation channel.
	watchActive bool
}

// NewPrefixIndex returns an empty PrefixIndex ready for use.
func NewPrefixIndex() *PrefixIndex {
	return &PrefixIndex{prefixes: make(map[string]string)}
}

// Lookup resolves a resource_type string (e.g. the resourceType argument to
// CheckAccess) to the owning agent implementation. It checks for an exact
// match first, then progressively trims trailing path segments separated by
// "/" until a prefix match is found or the string is exhausted.
//
// The matchedPrefix return is the key from the underlying map that matched —
// callers (audit attribution) record it alongside the implementation so the
// audit row identifies WHICH prefix from the agent's declaration caught the
// access.
//
// Returns ok=false when no registered prefix covers resourceType.
func (p *PrefixIndex) Lookup(resourceType string) (implementation string, matchedPrefix string, ok bool) {
	if p == nil || resourceType == "" {
		return "", "", false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.prefixes) == 0 {
		return "", "", false
	}

	// Exact match first — handles cases like resourceType="chat/" being
	// declared verbatim.
	if impl, found := p.prefixes[resourceType]; found {
		return impl, resourceType, true
	}

	// Also try the trailing-slash variant: a declaration of "chat/" should
	// match resourceType "chat" (declared family wider than the access
	// target). And a declaration of "chat" should match "chat/" too.
	if strings.HasSuffix(resourceType, "/") {
		trimmed := strings.TrimRight(resourceType, "/")
		if impl, found := p.prefixes[trimmed]; found {
			return impl, trimmed, true
		}
	} else {
		if impl, found := p.prefixes[resourceType+"/"]; found {
			return impl, resourceType + "/", true
		}
	}

	// Walk the resourceType right-to-left along "/" boundaries. For
	// resourceType="docmgmt/document/abc", we try in order:
	//   "docmgmt/document/" (prefix form)
	//   "docmgmt/document"  (exact form)
	//   "docmgmt/"
	//   "docmgmt"
	// First hit wins (longest match — natural consequence of right-to-left
	// trimming).
	idx := strings.LastIndex(resourceType, "/")
	for idx > 0 {
		head := resourceType[:idx]
		// Try with trailing slash (prefix declaration).
		if impl, found := p.prefixes[head+"/"]; found {
			return impl, head + "/", true
		}
		// Try without trailing slash (exact declaration).
		if impl, found := p.prefixes[head]; found {
			return impl, head, true
		}
		idx = strings.LastIndex(head, "/")
	}

	return "", "", false
}

// Set replaces all entries currently owned by impl with the supplied schema's
// prefixes. Existing entries pointing at impl are cleared first so a re-Register
// that drops a prefix releases it for future claims. Empty schemas (no
// ResourceSchema declared) effectively clear impl's claims.
//
// Set does NOT validate uniqueness — that runs at the storage layer inside the
// Register transaction. Set assumes its input is already conflict-free; if a
// caller passes overlapping prefixes from different impls, the last write wins
// and a future Rebuild will resolve based on the DB state.
func (p *PrefixIndex) Set(impl string, schema []AgentResourceSchemaEntry) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// Drop any prefix currently pointing at impl so updates that narrow the
	// schema actually release the dropped prefixes.
	for prefix, owner := range p.prefixes {
		if owner == impl {
			delete(p.prefixes, prefix)
		}
	}
	for _, e := range schema {
		if e.ResourceTypePrefix == "" {
			continue
		}
		p.prefixes[e.ResourceTypePrefix] = impl
	}
}

// Delete removes every prefix entry owned by impl. Called by the gateway after
// a successful DELETE of an agent registration.
func (p *PrefixIndex) Delete(impl string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for prefix, owner := range p.prefixes {
		if owner == impl {
			delete(p.prefixes, prefix)
		}
	}
}

// Rebuild replaces the entire index from the supplied list of registrations.
// Called at gateway startup after the registry handle is wired up. Subsequent
// Set / Delete calls keep the index in sync incrementally.
//
// Conflict policy: if two registrations claim the same prefix (which the
// storage-layer uniqueness check should have prevented), the LAST one in the
// `all` slice wins. The storage check is authoritative; Rebuild's tolerance is
// strictly a belt-and-suspenders defense against schema drift.
func (p *PrefixIndex) Rebuild(all []*AgentRegistration) {
	if p == nil {
		return
	}
	fresh := make(map[string]string, 16)
	for _, reg := range all {
		if reg == nil {
			continue
		}
		for _, e := range reg.ResourceSchema {
			if e.ResourceTypePrefix == "" {
				continue
			}
			fresh[e.ResourceTypePrefix] = reg.Implementation
		}
	}
	p.mu.Lock()
	p.prefixes = fresh
	p.mu.Unlock()
}

// Snapshot returns a copy of the current prefix -> implementation map. Used by
// tests; callers in the hot path should use Lookup instead.
func (p *PrefixIndex) Snapshot() map[string]string {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string]string, len(p.prefixes))
	for k, v := range p.prefixes {
		out[k] = v
	}
	return out
}
