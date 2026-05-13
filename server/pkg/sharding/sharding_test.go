package sharding

import "testing"

func TestShardForWorkspace_StubAlwaysZero(t *testing.T) {
	cases := []struct {
		workspace   string
		totalShards int
	}{
		{"", 1},
		{"_apps", 1},
		{"prod", 4},
		{"some-very-long-workspace-name", 16},
	}
	for _, tc := range cases {
		if got := ShardForWorkspace(tc.workspace, tc.totalShards); got != 0 {
			t.Errorf("ShardForWorkspace(%q, %d) = %d, want 0 (stub)", tc.workspace, tc.totalShards, got)
		}
	}
}

func TestReceiverTopic(t *testing.T) {
	cases := []struct {
		prefix string
		shard  int
		want   string
	}{
		{"event", 0, "event::receiver0"},
		{"metric", 0, "metric::receiver0"},
		{"event", 7, "event::receiver7"},
		{"metric", 12, "metric::receiver12"},
	}
	for _, tc := range cases {
		if got := ReceiverTopic(tc.prefix, tc.shard); got != tc.want {
			t.Errorf("ReceiverTopic(%q, %d) = %q, want %q", tc.prefix, tc.shard, got, tc.want)
		}
	}
}

func TestIsReceiverTopic(t *testing.T) {
	cases := []struct {
		topic string
		want  bool
	}{
		{"event::receiver0", true},
		{"event::receiver1", true},
		{"metric::receiver0", true},
		{"metric::receiver42", true},
		{"event::prod", false},
		{"metric::prod", false},
		{"ag::prod::impl::spec", false},
		{"", false},
		{"event", false},
		{"event::", false},
		// "::receiver0" alone has no prefix, but parts[1] == "receiver0"
		// still matches — treat that pathological case as receiver-shaped.
		{"::receiver0", true},
	}
	for _, tc := range cases {
		if got := IsReceiverTopic(tc.topic); got != tc.want {
			t.Errorf("IsReceiverTopic(%q) = %v, want %v", tc.topic, got, tc.want)
		}
	}
}

func TestTotalShards(t *testing.T) {
	if got := TotalShards(); got != 1 {
		t.Errorf("TotalShards() = %d, want 1 (stub)", got)
	}
}
