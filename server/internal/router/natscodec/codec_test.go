package natscodec

import (
	"fmt"
	"math/rand"
	"regexp"
	"testing"
	"testing/quick"
)

// natsSubjectRe matches a legal NATS subject: only A-Z a-z 0-9 _ - . allowed,
// and individual tokens must not be bare * or >.
var natsSubjectRe = regexp.MustCompile(`^[A-Za-z0-9_\-\.]+$`)

func isNATSLegal(s string) bool {
	if !natsSubjectRe.MatchString(s) {
		return false
	}
	return true
}

var roundTripFixtures = []struct {
	name  string
	topic string
}{
	// All schema variants from CLAUDE.md Topic Schema table
	{"ag", "ag::acme::com.example.chat-agent::v2"},
	{"tu", "tu::ws1::com.example.task::main"},
	{"ta", "ta::ws1::impl::id-123"},
	{"tb", "tb::ws::impl"},
	{"us", "us::user-42::win-1"},
	{"uw", "uw::user-42::ws"},
	{"ga", "ga::ws"},
	{"gu", "gu::ws"},
	{"pg", "pg::ws"},
	{"br", "br::impl::spec"},

	// Reverse-DNS impls (dots inside a single token)
	{"reverse-dns-1", "com.example.foo"},
	{"reverse-dns-2", "org.acme.bar-baz"},

	// Single-token topic (no ::)
	{"single-token", "standalone"},

	// Token containing literal _2E_ (adversarial: looks like escape sequence)
	{"literal-escape-seq", "weird_2Ename::other"},

	// Token with single colon (colon is escaped to _3A_)
	{"single-colon", "a:b::c:d"},

	// UTF-8 multi-byte chars (non-ASCII bytes are escaped per-byte)
	{"utf8", "ag::ws::café::v1"},
	{"utf8-cjk", "ag::ws::日本語::v1"},

	// Wildcard characters that must be escaped
	{"wildcard-gt", "tk::ws::has>wildcard::events"},
	{"wildcard-star", "ag::ws::has*star::v1"},

	// Whitespace in token
	{"space", "a::hello world::b"},
	{"tab", "a::hello\tworld::b"},

	// Control char
	{"control", "a::\x01b::c"},

	// Underscore in token
	{"underscore", "a::under_score::b"},
}

func TestRoundTrip(t *testing.T) {
	for _, tc := range roundTripFixtures {
		t.Run(tc.name, func(t *testing.T) {
			nats := ToNATSSubject(tc.topic)

			// Must be NATS-legal
			if !isNATSLegal(nats) {
				t.Errorf("ToNATSSubject(%q) = %q is not a valid NATS subject", tc.topic, nats)
			}

			// Round-trip must hold
			got := FromNATSSubject(nats)
			if got != tc.topic {
				t.Errorf("round-trip failed: input=%q nats=%q recovered=%q", tc.topic, nats, got)
			}
		})
	}
}

func TestWildcardEscaping(t *testing.T) {
	cases := []string{
		"tk::ws::has>wildcard::events",
		"ag::ws::has*star::v1",
	}
	for _, topic := range cases {
		nats := ToNATSSubject(topic)
		if !isNATSLegal(nats) {
			t.Errorf("wildcard topic %q produced illegal NATS subject %q", topic, nats)
		}
		got := FromNATSSubject(nats)
		if got != topic {
			t.Errorf("round-trip failed for %q: got %q", topic, got)
		}
	}
}

// alphabet used for property testing — includes chars that must be escaped and some safe ones
const propAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.*> \t-:"

func randomTopic(rng *rand.Rand) string {
	// Build 1-4 tokens separated by ::
	nTokens := 1 + rng.Intn(4)
	tokens := make([]string, nTokens)
	for i := range tokens {
		// Each token is 1-12 chars from propAlphabet
		n := 1 + rng.Intn(12)
		buf := make([]byte, n)
		for j := range buf {
			buf[j] = propAlphabet[rng.Intn(len(propAlphabet))]
		}
		tokens[i] = string(buf)
	}
	result := tokens[0]
	for _, t := range tokens[1:] {
		result += "::" + t
	}
	return result
}

func TestPropertyRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 1000; i++ {
		topic := randomTopic(rng)
		nats := ToNATSSubject(topic)
		if !isNATSLegal(nats) {
			t.Errorf("iter %d: topic=%q produced illegal NATS subject %q", i, topic, nats)
		}
		got := FromNATSSubject(nats)
		if got != topic {
			t.Errorf("iter %d: round-trip failed: input=%q nats=%q recovered=%q", i, topic, nats, got)
		}
	}
}

func TestPropertyQuick(t *testing.T) {
	// Use testing/quick with string inputs — just test that round-trip holds for arbitrary strings
	// (arbitrary strings may not be valid aether topics but round-trip should still be bijective)
	f := func(s string) bool {
		nats := ToNATSSubject(s)
		return FromNATSSubject(nats) == s
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 500}); err != nil {
		t.Error(err)
	}
}

func TestLiteralEscapeSequenceNotDoubleEscaped(t *testing.T) {
	// A token that literally contains "_2E_" should round-trip correctly
	// (the _ gets escaped first, so it becomes "_5F_2E_5F_" — not confused with a dot escape)
	topic := "weird_2Ename::other"
	nats := ToNATSSubject(topic)
	got := FromNATSSubject(nats)
	if got != topic {
		t.Errorf("literal escape seq: input=%q nats=%q recovered=%q", topic, nats, got)
	}
	// Also verify the nats form doesn't contain a raw dot from the _2E_ literal
	// The dot separator only comes from ::
	// There should be exactly 1 dot (the :: separator)
	dotCount := 0
	for _, c := range nats {
		if c == '.' {
			dotCount++
		}
	}
	if dotCount != 1 {
		t.Errorf("expected 1 dot separator in NATS subject for 2-token topic, got %d in %q", dotCount, nats)
	}
}

func BenchmarkToNATSSubject(b *testing.B) {
	topic := "ag::ws::com.example.chat-agent::v2"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ToNATSSubject(topic)
	}
	b.ReportMetric(float64(b.N), "iters")
}

func ExampleToNATSSubject() {
	fmt.Println(ToNATSSubject("ag::ws::com.example.chat::v1"))
	// Output: ag.ws.com_2E_example_2E_chat.v1
}

func ExampleFromNATSSubject() {
	fmt.Println(FromNATSSubject("ag.ws.com_2E_example_2E_chat.v1"))
	// Output: ag::ws::com.example.chat::v1
}
