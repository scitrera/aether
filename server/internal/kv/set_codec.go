package kv

import (
	"encoding/json"
	"sort"
)

// Member sets on the Badger and JetStream backends have no native set type, so
// they are stored as a JSON array of unique members in a single value. Redis
// uses native SADD/SCARD instead. Keeping the on-disk form defined here ensures
// the two software-emulated backends stay byte-compatible with each other.

// parseMemberSet decodes a stored member-set value into a set map. An empty
// string yields an empty (non-nil) set. A value that is not a JSON array is
// treated as an empty set rather than an error, so a corrupted entry self-heals
// on the next add.
func parseMemberSet(raw string) map[string]struct{} {
	out := make(map[string]struct{})
	if raw == "" {
		return out
	}
	var members []string
	if err := json.Unmarshal([]byte(raw), &members); err != nil {
		return out
	}
	for _, m := range members {
		out[m] = struct{}{}
	}
	return out
}

// encodeMemberSet serializes a set map to a sorted JSON array for deterministic
// storage (stable bytes ease debugging and avoid spurious JetStream revision
// churn).
func encodeMemberSet(set map[string]struct{}) string {
	members := make([]string, 0, len(set))
	for m := range set {
		members = append(members, m)
	}
	sort.Strings(members)
	b, _ := json.Marshal(members)
	return string(b)
}
