package identityheaders

import "testing"

func TestMatchTunnelTarget(t *testing.T) {
	cases := []struct {
		name        string
		patterns    []string
		backendName string
		protocol    string
		remoteHint  string
		want        bool
	}{
		{
			name:        "empty patterns blanket allow",
			patterns:    nil,
			backendName: "db-prod",
			protocol:    "tcp",
			remoteHint:  "any:5432",
			want:        true,
		},
		{
			name:        "wildcard star pattern",
			patterns:    []string{"*"},
			backendName: "db-prod",
			protocol:    "tcp",
			remoteHint:  "prod-host:5432",
			want:        true,
		},
		{
			name:        "exact backend tcp host:port match",
			patterns:    []string{"db-prod::tcp prod-host:5432"},
			backendName: "db-prod",
			protocol:    "tcp",
			remoteHint:  "prod-host:5432",
			want:        true,
		},
		{
			name:        "backend mismatch denies",
			patterns:    []string{"db-prod::tcp prod-*:5432"},
			backendName: "db-staging",
			protocol:    "tcp",
			remoteHint:  "prod-host:5432",
			want:        false,
		},
		{
			name:        "protocol mismatch denies",
			patterns:    []string{"db-prod::tcp prod-*:5432"},
			backendName: "db-prod",
			protocol:    "udp",
			remoteHint:  "prod-host:5432",
			want:        false,
		},
		{
			name:        "remote hint glob match",
			patterns:    []string{"db-prod::tcp prod-*:5432"},
			backendName: "db-prod",
			protocol:    "tcp",
			remoteHint:  "prod-host:5432",
			want:        true,
		},
		{
			name:        "remote hint glob mismatch",
			patterns:    []string{"db-prod::tcp prod-*:5432"},
			backendName: "db-prod",
			protocol:    "tcp",
			remoteHint:  "prod-host:6543",
			want:        false,
		},
		{
			name:        "wildcard backend with ws protocol and host glob",
			patterns:    []string{"*::ws *.example:443"},
			backendName: "ws-edge",
			protocol:    "ws",
			remoteHint:  "alpha.example:443",
			want:        true,
		},
		{
			name:        "wildcard backend ws protocol mismatch",
			patterns:    []string{"*::ws *.example:443"},
			backendName: "ws-edge",
			protocol:    "tcp",
			remoteHint:  "alpha.example:443",
			want:        false,
		},
		{
			name:        "backend glob match",
			patterns:    []string{"db-*::tcp prod-*:5432"},
			backendName: "db-prod",
			protocol:    "tcp",
			remoteHint:  "prod-host:5432",
			want:        true,
		},
		{
			name:        "protocol upper-case input still matches",
			patterns:    []string{"db-prod::tcp prod-host:5432"},
			backendName: "db-prod",
			protocol:    "TCP",
			remoteHint:  "prod-host:5432",
			want:        true,
		},
		{
			name:        "first pattern denies but second allows",
			patterns:    []string{"db-prod::tcp other:1234", "db-prod::tcp prod-host:5432"},
			backendName: "db-prod",
			protocol:    "tcp",
			remoteHint:  "prod-host:5432",
			want:        true,
		},
		{
			name:        "empty entry skipped, no other matches denies",
			patterns:    []string{"", "db-staging::tcp *"},
			backendName: "db-prod",
			protocol:    "tcp",
			remoteHint:  "prod-host:5432",
			want:        false,
		},
		{
			name:        "malformed pattern (no ::) does not match",
			patterns:    []string{"db-prod tcp prod-host:5432"},
			backendName: "db-prod",
			protocol:    "tcp",
			remoteHint:  "prod-host:5432",
			want:        false,
		},
		{
			name:        "udp protocol allow",
			patterns:    []string{"vpn-relay::udp *:51820"},
			backendName: "vpn-relay",
			protocol:    "udp",
			remoteHint:  "peer-7:51820",
			want:        true,
		},
		{
			name:        "protocol glob admits any",
			patterns:    []string{"db-prod::* prod-host:5432"},
			backendName: "db-prod",
			protocol:    "tcp",
			remoteHint:  "prod-host:5432",
			want:        true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MatchTunnelTarget(tc.patterns, tc.backendName, tc.protocol, tc.remoteHint)
			if got != tc.want {
				t.Fatalf("MatchTunnelTarget(%v, %q, %q, %q) = %v; want %v",
					tc.patterns, tc.backendName, tc.protocol, tc.remoteHint, got, tc.want)
			}
		})
	}
}
