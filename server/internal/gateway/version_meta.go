// Helpers backing the InitConnection-versioning spec: translate between
// the proto wire types, the state.ConnectMeta carried through the
// session manager, and the gateway-side build info echoed back on
// ConnectionAck. Kept in a dedicated file so the connect handler stays
// focused on lifecycle orchestration.
package gateway

import (
	"runtime"
	"runtime/debug"
	"sync"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/state"
	"github.com/scitrera/aether/internal/version"
)

const (
	unknownClientVersion = "unknown"
	unknownClientSDK     = "unknown"
)

// connectMetaFromInit extracts client-version metadata from an
// InitConnection frame. Returns an empty ConnectMeta when init is nil
// or carries no version fields; callers should propagate the empty
// value through the session manager unchanged.
func connectMetaFromInit(init *pb.InitConnection) state.ConnectMeta {
	if init == nil {
		return state.ConnectMeta{}
	}
	cm := state.ConnectMeta{
		ClientVersion: init.GetClientVersion(),
		ClientSDK:     init.GetClientSdk(),
	}
	if bi := init.GetClientBuildInfo(); bi != nil {
		cm.ClientBuildInfo = &state.BuildInfoMeta{
			Commit:  bi.GetCommit(),
			BuiltAt: bi.GetBuiltAt(),
			Runtime: bi.GetRuntime(),
			OS:      bi.GetOs(),
		}
	}
	return cm
}

// clientVersionOrUnknown maps an empty version string to "unknown" so
// audit consumers can distinguish "old SDK that didn't send a version"
// from "I forgot to populate this field". Non-empty values pass through.
func clientVersionOrUnknown(v string) string {
	if v == "" {
		return unknownClientVersion
	}
	return v
}

func clientSDKOrUnknown(s string) string {
	if s == "" {
		return unknownClientSDK
	}
	return s
}

// connectionEventKind classifies a successful lock acquisition for the
// audit row's `event_kind` field. Mirrors the spec's enumeration:
// "initial" | "reconnect" | "force_takeover".
func connectionEventKind(resumed, forced bool) string {
	switch {
	case resumed:
		return "reconnect"
	case forced:
		return "force_takeover"
	default:
		return "initial"
	}
}

// Server build info is computed once at startup; callers are read-mostly
// hot paths (every connection ack), so memoise rather than reading
// runtime/debug each time.
var (
	serverBuildOnce sync.Once
	serverBuildInfo struct {
		Version string
		Commit  string
		BuiltAt string
		Runtime string
		OS      string
	}
)

func loadServerBuildInfo() {
	serverBuildOnce.Do(func() {
		serverBuildInfo.Version = version.Version
		serverBuildInfo.Runtime = runtime.Version()
		serverBuildInfo.OS = runtime.GOOS + "/" + runtime.GOARCH
		if bi, ok := debug.ReadBuildInfo(); ok {
			for _, s := range bi.Settings {
				switch s.Key {
				case "vcs.revision":
					serverBuildInfo.Commit = s.Value
				case "vcs.time":
					serverBuildInfo.BuiltAt = s.Value
				}
			}
		}
	})
}

func serverVersionString() string {
	loadServerBuildInfo()
	return serverBuildInfo.Version
}

func serverBuildCommit() string {
	loadServerBuildInfo()
	return serverBuildInfo.Commit
}

func serverBuildInfoProto() *pb.BuildInfo {
	loadServerBuildInfo()
	return &pb.BuildInfo{
		Commit:  serverBuildInfo.Commit,
		BuiltAt: serverBuildInfo.BuiltAt,
		Runtime: serverBuildInfo.Runtime,
		Os:      serverBuildInfo.OS,
	}
}
