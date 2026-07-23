//go:build race

package integration

import "time"

// scaled widens a positive test deadline under the race detector. The race
// detector's 2-10x slowdown blows the tight timeouts these embedded-NATS
// cluster tests use — especially on CI's 2-core runners, where JetStream Raft
// elections, backups, and tunnel handshakes take far longer than on a dev box.
//
// Use scaled ONLY for positive waits: operation timeouts (context.WithTimeout),
// poll deadlines (time.Now().Add), and completion waits (time.After in a select
// that expects something to arrive). NEVER wrap a negative-assertion window (a
// select that asserts NOTHING arrives within X) — widening that only slows the
// suite. Tick/poll intervals and behavior config (leader TTLs) are also left
// unscaled.
//
// Outside the race detector this is the identity — see timescale_norace_test.go.
func scaled(d time.Duration) time.Duration { return d * 4 }
