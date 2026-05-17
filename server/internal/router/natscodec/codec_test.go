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

// kvKeyRe matches a legal NATS JetStream KV key (per nats.go validator):
// A-Z a-z 0-9 - _ = . /
var kvKeyRe = regexp.MustCompile(`^[A-Za-z0-9_\-=./]*$`)

// consumerNameRe matches a legal NATS JetStream durable consumer name:
// A-Z a-z 0-9 - _
var consumerNameRe = regexp.MustCompile(`^[A-Za-z0-9_\-]*$`)

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
const propAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.*> \t-:@+&?=/"

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

// ---------------------------------------------------------------------------
// Per-namespace escape variants
// ---------------------------------------------------------------------------

func TestEscapeForSubject_RoundTrip(t *testing.T) {
	cases := []string{
		"plain",
		"with-dash",
		"under_score",
		"colon:in:middle",
		"at@domain",
		"plus+sign",
		"amp&ersand",
		"question?mark",
		"dot.between",
		"star*",
		"gt>",
		"space here",
		"tab\there",
		"café日本語",
		"",
		"_",
		"__",
	}
	for _, s := range cases {
		t.Run(s, func(t *testing.T) {
			esc := EscapeForSubject(s)
			if !natsSubjectRe.MatchString(esc) && esc != "" {
				t.Errorf("EscapeForSubject(%q) = %q is not a legal NATS subject token", s, esc)
			}
			got := Unescape(esc)
			if got != s {
				t.Errorf("round-trip: input=%q esc=%q recovered=%q", s, esc, got)
			}
		})
	}
}

func TestEscapeForKVKey_AllowsKVSafeChars(t *testing.T) {
	// Chars that MUST pass through unescaped for KV keys.
	passthrough := "abcXYZ0189-/=" // alphanumeric, -, /, =
	got := EscapeForKVKey(passthrough)
	if got != passthrough {
		t.Errorf("KV-safe chars should pass through unchanged: got %q want %q", got, passthrough)
	}

	// Chars that MUST be escaped even though they may be ASCII.
	mustEscape := map[byte]string{
		':': "_3A_",
		'@': "_40_",
		'+': "_2B_",
		'.': "_2E_", // even though NATS KV allows '.', we reserve it
		'_': "_5F_", // bijectivity sentinel
	}
	for c, want := range mustEscape {
		in := string([]byte{c})
		got := EscapeForKVKey(in)
		if got != want {
			t.Errorf("EscapeForKVKey(%q) = %q want %q", in, got, want)
		}
	}
}

func TestEscapeForConsumerName_StrictlyAlphanumDashUnderscore(t *testing.T) {
	// Pass-through set: alphanumeric, '-'. Underscore appears in output only as
	// part of escape sequences (e.g. _5F_).
	passthrough := "abcDEF0189-"
	got := EscapeForConsumerName(passthrough)
	if got != passthrough {
		t.Errorf("consumer-name safe chars should pass through: got %q want %q", got, passthrough)
	}

	// '.' '=' '/' '_' ':' '@' all escape.
	for _, c := range []byte{'.', '=', '/', '_', ':', '@'} {
		in := string([]byte{c})
		out := EscapeForConsumerName(in)
		if out == in {
			t.Errorf("EscapeForConsumerName(%q) must escape, got identity %q", in, out)
		}
		if !consumerNameRe.MatchString(out) {
			t.Errorf("EscapeForConsumerName(%q) = %q is not consumer-name legal", in, out)
		}
	}
}

// Property test: any bijective round-trip across all three variants.
func TestEscapeForX_BijectiveProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	const propAlphabet = "abcXYZ012-_:.=/@*+&?> \t\n\x01"
	for i := 0; i < 1000; i++ {
		n := rng.Intn(20)
		buf := make([]byte, n)
		for j := range buf {
			buf[j] = propAlphabet[rng.Intn(len(propAlphabet))]
		}
		in := string(buf)

		if got := Unescape(EscapeForSubject(in)); got != in {
			t.Errorf("subject: in=%q recovered=%q", in, got)
		}
		if got := Unescape(EscapeForKVKey(in)); got != in {
			t.Errorf("kvkey: in=%q recovered=%q", in, got)
		}
		if got := Unescape(EscapeForConsumerName(in)); got != in {
			t.Errorf("consumer: in=%q recovered=%q", in, got)
		}
	}
}

func TestEscapeForKVKey_OutputPassesNATSCharset(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	const alphabet = "abcXYZ012-_:.=/@*+&?> \t\x01日café"
	for i := 0; i < 500; i++ {
		buf := make([]byte, rng.Intn(20))
		for j := range buf {
			buf[j] = alphabet[rng.Intn(len(alphabet))]
		}
		in := string(buf)
		out := EscapeForKVKey(in)
		if !kvKeyRe.MatchString(out) {
			t.Errorf("EscapeForKVKey(%q) = %q violates KV charset", in, out)
		}
	}
}

func TestEscapeForConsumerName_OutputPassesNATSCharset(t *testing.T) {
	rng := rand.New(rand.NewSource(8))
	const alphabet = "abcXYZ012-_:.=/@*+&?> \t\x01日café"
	for i := 0; i < 500; i++ {
		buf := make([]byte, rng.Intn(20))
		for j := range buf {
			buf[j] = alphabet[rng.Intn(len(alphabet))]
		}
		in := string(buf)
		out := EscapeForConsumerName(in)
		if !consumerNameRe.MatchString(out) {
			t.Errorf("EscapeForConsumerName(%q) = %q violates consumer-name charset", in, out)
		}
	}
}

// ---------------------------------------------------------------------------
// LRU cache
// ---------------------------------------------------------------------------

func TestCache_HitOnRepeatedInput(t *testing.T) {
	// Use a small dedicated cache so we don't interfere with other tests.
	SetCacheCapacity(8)
	t.Cleanup(func() { SetCacheCapacity(defaultCacheCapacity) })

	subjectCache.resetStats()
	kvKeyCache.resetStats()
	consumerNameCache.resetStats()

	in := "us::user@example.com::win-1"

	// First call: miss + add.
	_ = EscapeForSubject(in)
	_ = EscapeForKVKey(in)
	_ = EscapeForConsumerName(in)

	// Second call: must hit cache in each shard.
	_ = EscapeForSubject(in)
	_ = EscapeForKVKey(in)
	_ = EscapeForConsumerName(in)

	for name, c := range map[string]*escapeCache{
		"subject":      subjectCache,
		"kvKey":        kvKeyCache,
		"consumerName": consumerNameCache,
	} {
		hits, misses := c.stats()
		if hits < 1 {
			t.Errorf("%s cache: expected >= 1 hit, got hits=%d misses=%d", name, hits, misses)
		}
		if misses < 1 {
			t.Errorf("%s cache: expected >= 1 miss, got hits=%d misses=%d", name, hits, misses)
		}
	}
}

func TestSetCacheCapacity_Resets(t *testing.T) {
	SetCacheCapacity(4)
	// Fill beyond capacity.
	for i := 0; i < 10; i++ {
		_ = EscapeForSubject(fmt.Sprintf("input-%d", i))
	}
	// Reset to default — cache should be empty.
	SetCacheCapacity(defaultCacheCapacity)
	subjectCache.resetStats()
	_ = EscapeForSubject("input-0")
	_, misses := subjectCache.stats()
	if misses == 0 {
		t.Errorf("expected cache miss after SetCacheCapacity reset, got %d misses", misses)
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkToNATSSubject(b *testing.B) {
	topic := "ag::ws::com.example.chat-agent::v2"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ToNATSSubject(topic)
	}
	b.ReportMetric(float64(b.N), "iters")
}

func BenchmarkEscapeForSubject_Cached(b *testing.B) {
	in := "us::user@example.com::win-1"
	// Warm the cache.
	_ = EscapeForSubject(in)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EscapeForSubject(in)
	}
}

func BenchmarkEscapeForSubject_Uncached(b *testing.B) {
	// Use unique inputs each iteration to defeat the cache.
	// Pre-generate strings to keep the benchmark loop free of allocation noise.
	inputs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		inputs[i] = fmt.Sprintf("us::user-%d@example.com::win-%d", i, i)
	}
	// Use a tiny cache so each entry is immediately evicted.
	SetCacheCapacity(1)
	b.Cleanup(func() { SetCacheCapacity(defaultCacheCapacity) })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EscapeForSubject(inputs[i])
	}
}

func BenchmarkEscapeForConsumerName_Cached(b *testing.B) {
	in := "us::user@example.com::win-1"
	_ = EscapeForConsumerName(in)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EscapeForConsumerName(in)
	}
}

func BenchmarkEscapeForConsumerName_Uncached(b *testing.B) {
	inputs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		inputs[i] = fmt.Sprintf("us::user-%d::win-%d", i, i)
	}
	SetCacheCapacity(1)
	b.Cleanup(func() { SetCacheCapacity(defaultCacheCapacity) })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EscapeForConsumerName(inputs[i])
	}
}

func ExampleToNATSSubject() {
	fmt.Println(ToNATSSubject("ag::ws::com.example.chat::v1"))
	// Output: ag.ws.com_2E_example_2E_chat.v1
}

func ExampleFromNATSSubject() {
	fmt.Println(FromNATSSubject("ag.ws.com_2E_example_2E_chat.v1"))
	// Output: ag::ws::com.example.chat::v1
}
