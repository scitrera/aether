// Package aether: client-version metadata wiring for the
// InitConnection versioning spec. Captured once via sync.Once at first
// use and reused for every connect/reconnect so the gateway-side audit
// log can attribute connections to a specific SDK release without the
// SDK paying repeated runtime/debug.ReadBuildInfo cost.
package aether

import (
	"runtime"
	"runtime/debug"
	"sync"
)

const clientSDKName = "go"

var (
	versionMetaOnce sync.Once
	versionMeta     struct {
		Version string
		Commit  string
		BuiltAt string
		Runtime string
		OS      string
	}
)

// clientVersionMeta returns the Go SDK's version + build metadata. The
// version falls back to the compile-time constant when debug.ReadBuildInfo
// is unavailable (e.g. `go run` without a module) — this keeps audit
// rows attributable even in unusual deployment shapes.
func clientVersionMeta() (version, commit, builtAt, runtimeStr, osStr string) {
	versionMetaOnce.Do(func() {
		versionMeta.Runtime = runtime.Version()
		versionMeta.OS = runtime.GOOS + "/" + runtime.GOARCH
		versionMeta.Version = Version
		if bi, ok := debug.ReadBuildInfo(); ok {
			// Prefer module version when meaningful; otherwise keep the
			// compile-time Version. bi.Main.Version is "(devel)" when
			// the binary was built from a working copy.
			if bi.Main.Version != "" && bi.Main.Version != "(devel)" {
				versionMeta.Version = bi.Main.Version
			}
			for _, s := range bi.Settings {
				switch s.Key {
				case "vcs.revision":
					versionMeta.Commit = s.Value
				case "vcs.time":
					versionMeta.BuiltAt = s.Value
				}
			}
		}
	})
	return versionMeta.Version, versionMeta.Commit, versionMeta.BuiltAt, versionMeta.Runtime, versionMeta.OS
}
