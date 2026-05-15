package acl

import "path"

// Exported wrappers for functions that the native sqlite ACL store
// (internal/storage/acl/sqlite) needs to call. The underlying logic
// remains unexported to preserve the existing API surface of this
// package; these thin wrappers exist solely to avoid duplicating the
// code in the sqlite impl.

// RewriteLegacyPermission converts a (resourceType, resourceID) pair from
// the legacy "permission" + "_perm:*" form to the typed "admin"/"capability"
// families. See rewriteLegacyPermission for full doc.
func RewriteLegacyPermission(resourceType, resourceID string) (string, string, bool) {
	return rewriteLegacyPermission(resourceType, resourceID)
}

// ValidateGlobPattern checks that IDs containing glob wildcards have enough
// specificity to prevent overly broad grants. See validateGlobPattern for
// full doc.
func ValidateGlobPattern(id, fieldName string) error {
	return validateGlobPattern(id, fieldName)
}

// GlobMatch wraps path.Match for glob-style pattern matching. Exported so
// the sqlite impl's Casbin evaluation can reuse the same matching semantics.
func GlobMatch(name, pattern string) bool {
	return globMatch(name, pattern)
}

// GlobMatchPath is an alias for path.Match, exported for the sqlite impl's
// constraint-value matching.
func GlobMatchPath(pattern, name string) (bool, error) {
	return path.Match(pattern, name)
}
