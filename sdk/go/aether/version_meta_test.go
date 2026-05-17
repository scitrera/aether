package aether

import (
	"runtime"
	"strings"
	"testing"
)

func TestClientVersionMeta_PopulatesRuntimeAndOS(t *testing.T) {
	version, _, _, runtimeStr, osStr := clientVersionMeta()
	if version == "" {
		t.Error("expected non-empty version (either ReadBuildInfo or compile-time Version constant)")
	}
	if !strings.HasPrefix(runtimeStr, "go") {
		t.Errorf("runtime = %q, want prefix \"go\"", runtimeStr)
	}
	if !strings.Contains(osStr, "/") {
		t.Errorf("os = %q, want \"GOOS/GOARCH\" shape", osStr)
	}
	if got, want := osStr, runtime.GOOS+"/"+runtime.GOARCH; got != want {
		t.Errorf("os = %q, want %q", got, want)
	}
}

func TestClientVersionMeta_Memoised(t *testing.T) {
	v1, _, _, _, _ := clientVersionMeta()
	v2, _, _, _, _ := clientVersionMeta()
	if v1 != v2 {
		t.Errorf("expected memoised version; got %q then %q", v1, v2)
	}
}
