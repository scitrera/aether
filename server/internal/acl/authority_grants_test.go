package acl

import (
	"testing"
	"time"
)

func TestAuthorityGrantCanDelegate(t *testing.T) {
	tests := []struct {
		name  string
		grant AuthorityGrant
		want  bool
	}{
		{
			name:  "false when delegation disabled",
			grant: AuthorityGrant{MayDelegate: false, RemainingHops: 1},
			want:  false,
		},
		{
			name:  "false when no hops remain",
			grant: AuthorityGrant{MayDelegate: true, RemainingHops: 0},
			want:  false,
		},
		{
			name:  "true when enabled with remaining hops",
			grant: AuthorityGrant{MayDelegate: true, RemainingHops: 2},
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.grant.CanDelegate(); got != tt.want {
				t.Errorf("CanDelegate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAuthorityGrantValidateActiveAt(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name  string
		grant AuthorityGrant
		want  error
	}{
		{
			name:  "revoked grant fails",
			grant: AuthorityGrant{Revoked: true, ExpiresAt: now.Add(time.Hour)},
			want:  ErrAuthorityGrantRevoked,
		},
		{
			name:  "expired grant fails",
			grant: AuthorityGrant{ExpiresAt: now.Add(-time.Minute)},
			want:  ErrAuthorityGrantExpired,
		},
		{
			name:  "active grant succeeds",
			grant: AuthorityGrant{ExpiresAt: now.Add(time.Hour)},
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.grant.ValidateActiveAt(now); got != tt.want {
				t.Errorf("ValidateActiveAt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringSliceSubset(t *testing.T) {
	tests := []struct {
		name   string
		child  []string
		parent []string
		want   bool
	}{
		{
			name:   "unrestricted parent accepts any child",
			child:  []string{"read", "write"},
			parent: nil,
			want:   true,
		},
		{
			name:   "restricted parent rejects empty child",
			child:  nil,
			parent: []string{"read"},
			want:   false,
		},
		{
			name:   "subset is allowed",
			child:  []string{"read"},
			parent: []string{"read", "write"},
			want:   true,
		},
		{
			name:   "superset is rejected",
			child:  []string{"read", "delete"},
			parent: []string{"read", "write"},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stringSliceSubset(tt.child, tt.parent); got != tt.want {
				t.Errorf("stringSliceSubset() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeRenewedExpiry(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	renewableUntil := now.Add(2 * time.Hour)

	tests := []struct {
		name           string
		opts           RenewAuthorityGrantOpts
		renewableUntil time.Time
		want           time.Time
	}{
		{
			name:           "extend_seconds within window",
			opts:           RenewAuthorityGrantOpts{ExtendSeconds: 600},
			renewableUntil: renewableUntil,
			want:           now.Add(10 * time.Minute),
		},
		{
			name:           "extend_seconds past renewable_until clamps",
			opts:           RenewAuthorityGrantOpts{ExtendSeconds: 99999},
			renewableUntil: renewableUntil,
			want:           renewableUntil,
		},
		{
			name: "extend_seconds wins over expires_at",
			opts: RenewAuthorityGrantOpts{
				ExpiresAt:     now.Add(7 * 24 * time.Hour),
				ExtendSeconds: 60,
			},
			renewableUntil: renewableUntil,
			want:           now.Add(60 * time.Second),
		},
		{
			name:           "absolute expires_at passes through",
			opts:           RenewAuthorityGrantOpts{ExpiresAt: now.Add(30 * time.Minute)},
			renewableUntil: renewableUntil,
			want:           now.Add(30 * time.Minute),
		},
		{
			name:           "zero opts returns zero time",
			opts:           RenewAuthorityGrantOpts{},
			renewableUntil: renewableUntil,
			want:           time.Time{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeRenewedExpiry(tt.opts, tt.renewableUntil, now)
			if !got.Equal(tt.want) {
				t.Fatalf("computeRenewedExpiry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResourceScopeSubset(t *testing.T) {
	tests := []struct {
		name   string
		child  map[string][]string
		parent map[string][]string
		want   bool
	}{
		{
			name:   "unrestricted parent accepts child",
			child:  map[string][]string{"kv_key": []string{"ws.alpha.*"}},
			parent: nil,
			want:   true,
		},
		{
			name:   "restricted parent rejects empty child",
			child:  nil,
			parent: map[string][]string{"kv_key": []string{"ws.alpha.*"}},
			want:   false,
		},
		{
			name:   "subset of same resource type is allowed",
			child:  map[string][]string{"kv_key": []string{"ws.alpha.read"}},
			parent: map[string][]string{"kv_key": []string{"ws.alpha.read", "ws.alpha.write"}},
			want:   true,
		},
		{
			name:   "missing resource type is rejected",
			child:  map[string][]string{"workspace": []string{"alpha"}},
			parent: map[string][]string{"kv_key": []string{"ws.alpha.*"}},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resourceScopeSubset(tt.child, tt.parent); got != tt.want {
				t.Errorf("resourceScopeSubset() = %v, want %v", got, tt.want)
			}
		})
	}
}
