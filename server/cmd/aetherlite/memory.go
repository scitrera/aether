package main

import (
	"os"
	"runtime/debug"
)

// AetherLite embeds several stores in one process. Give transient garbage a
// finite budget even on hosts where Go sees no memory limit. This is a soft
// runtime limit, not an RSS/container cap; mapped Badger files are excluded.
const defaultMemoryLimit = 1 << 30

func configureMemoryLimit() int64 {
	// The runtime has already parsed an explicit setting at process startup.
	// Preserve every non-empty value, including "off" and zero.
	if os.Getenv("GOMEMLIMIT") == "" {
		debug.SetMemoryLimit(defaultMemoryLimit)
	}
	return debug.SetMemoryLimit(-1)
}
