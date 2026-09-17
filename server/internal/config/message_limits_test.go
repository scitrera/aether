package config

import (
	"gopkg.in/yaml.v3"
	"testing"
)

func TestConfiguredMessageLimits(t *testing.T) {
	for _, size := range []int{0, 1024, 3 * 1024 * 1024, 8 * 1024 * 1024} {
		q := QuotasConfig{MaxMessagePayloadSize: size}
		want := size
		if want == 0 {
			want = 1024 * 1024
		}
		if q.GetMaxMessagePayloadSize() != want {
			t.Fatalf("payload=%d want=%d", q.GetMaxMessagePayloadSize(), want)
		}
		if got := q.GetGRPCMaxRecvMessageSize(); got < 4*1024*1024 || got < want+64*1024 {
			t.Fatalf("frame limit %d leaves no envelope headroom", got)
		}
	}
	var c Config
	if err := yaml.Unmarshal([]byte("quotas:\n  max_message_payload_size: 8388608\n"), &c); err != nil {
		t.Fatal(err)
	}
	if c.Quotas.GetMaxMessagePayloadSize() != 8*1024*1024 {
		t.Fatal("YAML setting ignored")
	}
	for _, size := range []int{-1, 8*1024*1024 + 1} {
		c.Quotas.MaxMessagePayloadSize = size
		if c.Validate() == nil {
			t.Fatalf("invalid cap %d accepted", size)
		}
	}
}
