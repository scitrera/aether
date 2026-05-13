package gateway

import (
	"testing"

	pb "github.com/scitrera/aether/api/proto"
)

// TestIsUserBoundKVScope verifies that handleKVOp's auto-OBO-promotion guard
// only fires for user-bound KV scopes. Global and workspace scopes must be
// treated as caller-owned — otherwise an agent reading its own global model
// config would be force-promoted to the user's OBO grant path, which is the
// bug that motivated the guard.
//
// After the KV scope revamp there are now eight proto enum values. The four
// user-bound scopes (USER, USER_WORKSPACE, USER_SHARED, USER_WORKSPACE_SHARED)
// must return true; the four non-user scopes (GLOBAL, WORKSPACE,
// GLOBAL_EXCLUSIVE, WORKSPACE_EXCLUSIVE) must return false.
func TestIsUserBoundKVScope(t *testing.T) {
	tests := []struct {
		name  string
		scope pb.KVOperation_Scope
		want  bool
	}{
		// Non-user (caller-owned) scopes
		{"global is caller-owned", pb.KVOperation_GLOBAL, false},
		{"workspace is caller-owned", pb.KVOperation_WORKSPACE, false},
		{"global-exclusive is caller-owned", pb.KVOperation_GLOBAL_EXCLUSIVE, false},
		{"workspace-exclusive is caller-owned", pb.KVOperation_WORKSPACE_EXCLUSIVE, false},
		// User-bound scopes (per-agent)
		{"user is user-bound", pb.KVOperation_USER, true},
		{"user-workspace is user-bound", pb.KVOperation_USER_WORKSPACE, true},
		// User-bound scopes (cross-agent shared)
		{"user-shared is user-bound", pb.KVOperation_USER_SHARED, true},
		{"user-workspace-shared is user-bound", pb.KVOperation_USER_WORKSPACE_SHARED, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isUserBoundKVScope(tc.scope); got != tc.want {
				t.Errorf("isUserBoundKVScope(%v) = %v, want %v", tc.scope, got, tc.want)
			}
		})
	}
}
