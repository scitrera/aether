package state

// ConnectMeta carries optional client-version metadata captured from the
// InitConnection frame. All fields are best-effort — older SDKs leave them
// empty and the gateway records "unknown" in audit. Persisted on the
// identity-lock record so session lifetime + reconnection-count are
// preserved across resume_session_id takeovers.
type ConnectMeta struct {
	ClientVersion   string
	ClientSDK       string
	ClientBuildInfo *BuildInfoMeta
}

// BuildInfoMeta mirrors the proto BuildInfo message but avoids dragging
// the generated proto type into the state package. Kept structurally
// identical so the gateway can shuttle values 1:1 between the wire and
// the session registry.
type BuildInfoMeta struct {
	Commit  string `json:"commit,omitempty"`
	BuiltAt string `json:"built_at,omitempty"`
	Runtime string `json:"runtime,omitempty"`
	OS      string `json:"os,omitempty"`
}

// ConnectResult is the canonical lock-acquisition outcome returned by
// AcquireOrResumeLock. Replaces the previous (bool, bool, bool, error)
// return tuple so additive session-lifetime fields can be plumbed back
// to the connect handler for ConnectionAck + audit emission.
type ConnectResult struct {
	Acquired                bool
	Resumed                 bool
	Forced                  bool
	InitialConnectionUnixMs int64 // first-ever connect for this identity (preserved across resume)
	ReconnectionCount       int32 // 0 on fresh / forced takeover, N+1 on resume
}
