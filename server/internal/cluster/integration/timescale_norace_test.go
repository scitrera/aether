//go:build !race

package integration

import "time"

// scaled is the identity outside the race detector: deadlines stay tight for
// fast local feedback. See timescale_race_test.go for the -race widening and
// the rules on where scaled may (and may not) be applied.
func scaled(d time.Duration) time.Duration { return d }
