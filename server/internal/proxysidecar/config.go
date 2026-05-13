// Package proxysidecar implements the Aether proxy sidecar runtime.
//
// The sidecar is composed of independent surfaces that share a single
// gateway connection (one Aether identity, one distributed lock):
//
//   - terminator: receives ProxyHttpRequest and Tunnel* envelopes from the
//     gateway and forwards them to a configured local backend (HTTP / TCP /
//     UDP / WebSocket). Identity headers are minted via pkg/identityheaders
//     so the wire format matches auth-proxy exactly.
//
//   - initiator: exposes a local HTTP listener that translates each inbound
//     request into a ProxyHttpRequest envelope addressed at a configured
//     target service topic. Used for legacy clients (curl, scripts) that
//     cannot speak the Aether bidirectional protocol directly.
//
//   - relay: a gRPC mitm sidecar that accepts a sandbox process's plain
//     AetherGateway stream over UDS or TCP and forwards filtered envelopes
//     upstream, injecting credentials and overriding the claimed identity.
//
// Each surface is configured under its own YAML section with an explicit
// `enabled: true` flag. Any combination of surfaces can run together in a
// single sidecar process; their downstream envelopes are routed by payload
// type via the runner's downstream router.
package proxysidecar

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Identity-override modes for relay.identity_override.
const (
	// IdentityOverrideEnforce replaces the sandbox's claimed identity with
	// the sidecar's configured Service identity. The claim is logged but
	// discarded.
	IdentityOverrideEnforce = "enforce"
)

// Target-topic clamp modes for relay.target_topic_clamp.mode.
const (
	// TargetClampReject rejects any outbound envelope whose target_topic
	// does not match an entry in allowed_targets. Default mode.
	TargetClampReject = "reject"

	// TargetClampRewriteFirstMatch rewrites the target_topic to the first
	// concrete (no-glob) entry in allowed_targets when the inbound topic
	// does not match. Falls back to reject when no concrete entry exists.
	TargetClampRewriteFirstMatch = "rewrite_first_match"
)

// Operation profile identifiers for relay.allowed_ops.profile.
const (
	// AllowedOpsProfileSandboxDefault is the safest profile: only the
	// lightweight comms/state ops a sandbox typically needs (SendMessage,
	// ProgressReport, KVOperation). No proxy or tunnel envelopes.
	AllowedOpsProfileSandboxDefault = "sandbox-default"

	// AllowedOpsProfileSandboxTunnels extends sandbox-default with proxy and
	// tunnel envelopes. Use only when the sandbox legitimately needs SDK
	// proxy/tunnel access — most sandboxes should rely on the HTTP
	// mitmproxy + initiator path instead.
	AllowedOpsProfileSandboxTunnels = "sandbox-tunnels"

	// AllowedOpsProfileToolStubOnly forbids every upstream op except the
	// init handshake. Useful for sandboxes that act purely as inbound
	// responders (tool stubs that only receive ProxyHttpRequest).
	AllowedOpsProfileToolStubOnly = "tool-stub-only"
)

const (
	// HeaderModeStrict strips all caller X-Auth-*/X-Aether-* headers and mints
	// the canonical trusted set. Use this for backends that should only ever
	// see headers minted by a trusted issuer.
	HeaderModeStrict = "strict"

	// HeaderModePassthrough forwards caller-supplied headers as-is. The
	// terminator mints nothing. Use this for backends that already validate
	// requests independently.
	HeaderModePassthrough = "passthrough"

	// HeaderModeBoth keeps caller-supplied headers and overlays minted
	// identity headers on top. Minted values win on conflict.
	HeaderModeBoth = "both"
)

// Backend kinds recognised by the terminator.
const (
	BackendKindHTTP = "http"
	BackendKindTCP  = "tcp"
	BackendKindWS   = "ws"
	BackendKindUDP  = "udp"
)

// Config is the top-level proxy sidecar configuration loaded from YAML.
//
// Each surface lives under its own section (Terminator / Initiator / Relay)
// with an explicit `enabled` flag; surfaces default to disabled. The
// top-level Gateway, Service, TenantID, and Logging fields apply to every
// enabled surface — all surfaces share one gateway connection and one
// service identity.
type Config struct {
	Gateway  GatewayConfig `yaml:"gateway"`
	Service  ServiceConfig `yaml:"service"`
	TenantID string        `yaml:"tenant_id"`
	Logging  LoggingConfig `yaml:"logging"`

	Terminator TerminatorConfig `yaml:"terminator"`
	Initiator  InitiatorConfig  `yaml:"initiator"`
	Relay      RelayConfig      `yaml:"relay"`
}

// TerminatorConfig configures the terminator surface (gateway → local
// backend). Disabled by default.
type TerminatorConfig struct {
	Enabled  bool            `yaml:"enabled"`
	Backends []BackendConfig `yaml:"backends"`
}

// InitiatorConfig configures the initiator surface (local HTTP → gateway).
// Disabled by default.
type InitiatorConfig struct {
	Enabled bool         `yaml:"enabled"`
	Listen  ListenConfig `yaml:"listen"`
	Target  TargetConfig `yaml:"target"`
}

// RelayConfig configures the gRPC mitm relay sidecar. Disabled by default.
type RelayConfig struct {
	Enabled bool `yaml:"enabled"`

	// Listen is the bind address for the local AetherGateway server the
	// sandbox dials. UDS schemes (`unix://path`) are preferred for
	// security; TCP (`tcp://host:port` or bare `host:port`) is supported
	// as a fallback.
	Listen string `yaml:"listen"`

	// IdentityOverride controls how the sandbox-claimed identity in the
	// inbound InitConnection is handled. Currently only "enforce" is
	// supported (sandbox claim is replaced by sidecar's Service identity).
	IdentityOverride string `yaml:"identity_override"`

	// AllowedOps controls which upstream operations may pass through.
	// Either a named profile or a literal list; empty list means
	// "InitConnection-only".
	AllowedOps AllowedOpsConfig `yaml:"allowed_ops"`

	// TargetTopicClamp constrains the target_topic on outbound proxy/tunnel
	// envelopes the sandbox is permitted to address.
	TargetTopicClamp TargetClampConfig `yaml:"target_topic_clamp"`
}

// AllowedOpsConfig is a YAML union: either a profile name or a literal
// list of operation identifiers. Operators specifying a literal pass a
// YAML sequence; profile mode passes a mapping with `profile`.
//
// Resolved op identifiers are listed in relay_filter.go (Op* constants).
type AllowedOpsConfig struct {
	Profile string   `yaml:"profile"`
	Ops     []string `yaml:"-"`
	Set     bool     `yaml:"-"`
}

// UnmarshalYAML accepts either a sequence of strings or a mapping with a
// `profile` key. An explicit empty sequence is honoured (zero ops allowed).
func (a *AllowedOpsConfig) UnmarshalYAML(node *yaml.Node) error {
	if node == nil {
		return nil
	}
	a.Set = true
	switch node.Kind {
	case yaml.SequenceNode:
		ops := make([]string, 0, len(node.Content))
		for _, child := range node.Content {
			if child.Kind != yaml.ScalarNode {
				return fmt.Errorf("relay.allowed_ops list entries must be strings, got %v", child.Kind)
			}
			ops = append(ops, child.Value)
		}
		a.Ops = ops
		return nil
	case yaml.MappingNode:
		type profileShape struct {
			Profile string `yaml:"profile"`
		}
		var p profileShape
		if err := node.Decode(&p); err != nil {
			return fmt.Errorf("relay.allowed_ops: %w", err)
		}
		a.Profile = p.Profile
		return nil
	case yaml.ScalarNode:
		// Allow `allowed_ops: sandbox-default` shorthand for the profile.
		a.Profile = node.Value
		return nil
	default:
		return fmt.Errorf("relay.allowed_ops: expected sequence or mapping, got %v", node.Kind)
	}
}

// TargetClampConfig describes the relay's target-topic clamp policy.
type TargetClampConfig struct {
	// Mode is "reject" (default) or "rewrite_first_match".
	Mode string `yaml:"mode"`

	// AllowedTargets is a list of glob patterns matched against the
	// target_topic of outbound proxy/tunnel envelopes. An empty list
	// rejects every outbound proxy/tunnel envelope.
	AllowedTargets []string `yaml:"allowed_targets"`
}

// GatewayConfig describes how the sidecar connects to the Aether gateway.
//
// Credential mode is auto-selected by which fields are populated:
//   - APIKey / APIKeyPath  → presented as “api_key“ (long-lived service key)
//   - TaskToken / TaskTokenPath → presented as “token“ (per-task token issued by
//     CreateTask with target_identity; bound to a specific principal for its lifetime)
//
// Setting both kinds is a config error; the loader rejects it.
type GatewayConfig struct {
	Address       string    `yaml:"address"`
	APIKeyPath    string    `yaml:"api_key_path"`
	APIKey        string    `yaml:"api_key"`
	TaskTokenPath string    `yaml:"task_token_path"`
	TaskToken     string    `yaml:"task_token"`
	Insecure      bool      `yaml:"insecure"`
	TLS           TLSConfig `yaml:"tls"`
}

// TLSConfig holds TLS certificate paths for the gateway connection.
type TLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	CAFile   string `yaml:"ca_file"`
}

// ServiceConfig identifies the sidecar to the gateway. Required whenever
// any surface that opens an upstream gateway connection is enabled
// (terminator or relay).
type ServiceConfig struct {
	Implementation string `yaml:"implementation"`
	Specifier      string `yaml:"specifier"`
}

// ListenConfig describes the local HTTP listener used by the initiator.
type ListenConfig struct {
	Bind string `yaml:"bind"`
}

// TargetConfig is the destination service topic for the initiator.
type TargetConfig struct {
	Topic string `yaml:"topic"`
}

// BackendConfig defines a single backend that the terminator can forward
// matching requests to. Most fields apply only to HTTP backends; TCP backends
// use Name/Kind/URL/AllowRemoteHints/MaxBytes/IdleTimeoutMs.
type BackendConfig struct {
	Name          string   `yaml:"name"`
	Kind          string   `yaml:"kind"`
	URL           string   `yaml:"url"`
	AllowPaths    []string `yaml:"allow_paths"`
	AllowMethods  []string `yaml:"allow_methods"`
	MaxBodyBytes  int64    `yaml:"max_body_bytes"`
	IdleTimeoutMs int64    `yaml:"idle_timeout_ms"`
	HeaderMode    string   `yaml:"header_mode"`

	// TCP/UDP fields. AllowRemoteHints is a list of glob patterns matched
	// against TunnelOpen.remote_hint to gate which destinations a caller may
	// reach through this backend; an empty list means the configured URL is
	// the only legal destination.
	AllowRemoteHints []string `yaml:"allow_remote_hints"`
	MaxBytes         int64    `yaml:"max_bytes"`

	// UDP-only: maximum bytes per datagram. Frames exceeding this size are
	// rejected with TunnelClose{ERROR}. Defaults to 1400 bytes.
	MaxDatagramBytes int `yaml:"max_datagram_bytes"`
}

// LoggingConfig configures zerolog output.
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// LoadConfig reads a YAML configuration file from path, applies env-var
// overrides, and returns the parsed config. Validation must be performed
// separately via Config.Validate().
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyEnvOverrides()
	return &cfg, nil
}

// applyEnvOverrides applies environment variables that override YAML values.
// Mirrors the convention used in server/internal/msgbridge/config.go.
func (c *Config) applyEnvOverrides() {
	if v := os.Getenv("AETHER_ADDRESS"); v != "" {
		c.Gateway.Address = v
	}
	if v := os.Getenv("AETHER_API_KEY"); v != "" {
		c.Gateway.APIKey = v
	}
	if v := os.Getenv("AETHER_TASK_TOKEN"); v != "" {
		c.Gateway.TaskToken = v
	}
	if v := os.Getenv("AETHER_TENANT_ID"); v != "" {
		c.TenantID = v
	}
	if v := os.Getenv("PROXY_SIDECAR_LISTEN"); v != "" {
		c.Initiator.Listen.Bind = v
	}
	if v := os.Getenv("PROXY_SIDECAR_TARGET"); v != "" {
		c.Initiator.Target.Topic = v
	}
	if v := os.Getenv("PROXY_SIDECAR_RELAY_LISTEN"); v != "" {
		c.Relay.Listen = v
	}
	if v := os.Getenv("AETHER_LOG_LEVEL"); v != "" {
		c.Logging.Level = v
	}
}

// EnabledSurfaces returns a stable-order list of the surface names that are
// turned on in this config. Used in startup logs and "no surface enabled"
// validation.
func (c *Config) EnabledSurfaces() []string {
	var out []string
	if c.Terminator.Enabled {
		out = append(out, "terminator")
	}
	if c.Initiator.Enabled {
		out = append(out, "initiator")
	}
	if c.Relay.Enabled {
		out = append(out, "relay")
	}
	return out
}

// Validate checks the configuration for correctness. Each enabled surface's
// Validate is invoked; a config with no surfaces enabled is rejected.
func (c *Config) Validate() error {
	var errs []string

	enabled := c.EnabledSurfaces()
	if len(enabled) == 0 {
		errs = append(errs, "at least one surface must be enabled (set terminator.enabled, initiator.enabled, or relay.enabled to true)")
	}

	if c.Gateway.Address == "" {
		errs = append(errs, "gateway.address is required")
	}

	// Service identity is required whenever a surface that authenticates
	// to the gateway is on. The initiator does not (yet) own a connection,
	// so it does not contribute to this requirement.
	needsService := c.Terminator.Enabled || c.Relay.Enabled
	if needsService {
		if c.Service.Implementation == "" {
			errs = append(errs, "service.implementation is required when terminator or relay is enabled")
		}
		if c.Service.Specifier == "" {
			errs = append(errs, "service.specifier is required when terminator or relay is enabled")
		}
	}

	if c.Terminator.Enabled {
		errs = append(errs, c.validateTerminator()...)
	}
	if c.Initiator.Enabled {
		errs = append(errs, c.validateInitiator()...)
	}
	if c.Relay.Enabled {
		errs = append(errs, c.validateRelay()...)
	}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid proxy sidecar config:\n  - %s", strings.Join(errs, "\n  - "))
}

func (c *Config) validateTerminator() []string {
	var errs []string
	if len(c.Terminator.Backends) == 0 {
		errs = append(errs, "terminator.backends: at least one backend is required when terminator is enabled")
	}
	for i := range c.Terminator.Backends {
		b := &c.Terminator.Backends[i]
		errs = append(errs, b.normalizeAndValidate(i)...)
	}
	return errs
}

func (c *Config) validateInitiator() []string {
	var errs []string
	if c.Initiator.Listen.Bind == "" {
		c.Initiator.Listen.Bind = "localhost:8888"
	}
	if c.Initiator.Target.Topic == "" {
		errs = append(errs, "initiator.target.topic is required when initiator is enabled")
	}
	return errs
}

func (c *Config) validateRelay() []string {
	var errs []string
	if c.Relay.Listen == "" {
		errs = append(errs, "relay.listen is required when relay is enabled (use unix:///path or tcp://host:port)")
	}
	if c.Relay.IdentityOverride == "" {
		c.Relay.IdentityOverride = IdentityOverrideEnforce
	}
	if c.Relay.IdentityOverride != IdentityOverrideEnforce {
		errs = append(errs, fmt.Sprintf("relay.identity_override=%q: only %q is supported",
			c.Relay.IdentityOverride, IdentityOverrideEnforce))
	}
	if !c.Relay.AllowedOps.Set {
		// Apply the safe default profile when the operator omits the key.
		c.Relay.AllowedOps.Profile = AllowedOpsProfileSandboxDefault
		c.Relay.AllowedOps.Set = true
	}
	if c.Relay.AllowedOps.Profile != "" {
		switch c.Relay.AllowedOps.Profile {
		case AllowedOpsProfileSandboxDefault,
			AllowedOpsProfileSandboxTunnels,
			AllowedOpsProfileToolStubOnly:
		default:
			errs = append(errs, fmt.Sprintf("relay.allowed_ops.profile=%q: must be one of %q, %q, %q (or pass a literal list)",
				c.Relay.AllowedOps.Profile,
				AllowedOpsProfileSandboxDefault,
				AllowedOpsProfileSandboxTunnels,
				AllowedOpsProfileToolStubOnly))
		}
	}
	if c.Relay.TargetTopicClamp.Mode == "" {
		c.Relay.TargetTopicClamp.Mode = TargetClampReject
	}
	switch c.Relay.TargetTopicClamp.Mode {
	case TargetClampReject, TargetClampRewriteFirstMatch:
	default:
		errs = append(errs, fmt.Sprintf("relay.target_topic_clamp.mode=%q: must be %q or %q",
			c.Relay.TargetTopicClamp.Mode, TargetClampReject, TargetClampRewriteFirstMatch))
	}
	return errs
}

// normalizeAndValidate applies defaults, validates one backend, and returns
// any error messages it produces.
func (b *BackendConfig) normalizeAndValidate(idx int) []string {
	var errs []string
	prefix := fmt.Sprintf("terminator.backends[%d]", idx)
	if b.Name == "" {
		b.Name = "backend-" + strconv.Itoa(idx)
	}
	if b.Kind == "" {
		b.Kind = BackendKindHTTP
	}
	if b.URL == "" {
		errs = append(errs, fmt.Sprintf("%s.url is required", prefix))
	}
	switch b.Kind {
	case BackendKindHTTP:
		errs = append(errs, b.normalizeHTTP(prefix)...)
	case BackendKindTCP:
		errs = append(errs, b.normalizeTCP(prefix)...)
	case BackendKindWS:
		errs = append(errs, b.normalizeWS(prefix)...)
	case BackendKindUDP:
		errs = append(errs, b.normalizeUDP(prefix)...)
	default:
		errs = append(errs, fmt.Sprintf("%s.kind=%q: only %q, %q, %q, and %q are supported in this build",
			prefix, b.Kind, BackendKindHTTP, BackendKindTCP, BackendKindWS, BackendKindUDP))
	}
	return errs
}

// normalizeHTTP applies HTTP-specific defaults and validation.
func (b *BackendConfig) normalizeHTTP(prefix string) []string {
	var errs []string
	if len(b.AllowPaths) == 0 {
		b.AllowPaths = []string{"/*"}
	}
	if len(b.AllowMethods) == 0 {
		b.AllowMethods = []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}
	}
	for i, m := range b.AllowMethods {
		b.AllowMethods[i] = strings.ToUpper(strings.TrimSpace(m))
	}
	if b.MaxBodyBytes <= 0 {
		b.MaxBodyBytes = 10 << 20 // 10 MiB
	}
	if b.IdleTimeoutMs <= 0 {
		b.IdleTimeoutMs = 30_000
	}
	if b.HeaderMode == "" {
		b.HeaderMode = HeaderModeStrict
	}
	switch b.HeaderMode {
	case HeaderModeStrict, HeaderModePassthrough, HeaderModeBoth:
	default:
		errs = append(errs, fmt.Sprintf("%s.header_mode=%q: must be %q, %q, or %q",
			prefix, b.HeaderMode, HeaderModeStrict, HeaderModePassthrough, HeaderModeBoth))
	}
	return errs
}

// normalizeTCP applies TCP-specific defaults. The URL accepts either a bare
// host:port pair or a tcp:// scheme; the dialer strips the scheme.
func (b *BackendConfig) normalizeTCP(_ string) []string {
	if b.MaxBytes <= 0 {
		b.MaxBytes = 100 << 20 // 100 MiB per tunnel
	}
	if b.IdleTimeoutMs <= 0 {
		b.IdleTimeoutMs = 5 * 60 * 1000 // 5 minutes
	}
	return nil
}

// normalizeWS applies WebSocket-specific defaults. The URL is a ws:// or
// wss:// URI naming the default backend; AllowRemoteHints gates redirected
// targets the same way as the TCP backend. The wsBackend dialer rejects
// schemes other than ws/wss.
func (b *BackendConfig) normalizeWS(_ string) []string {
	if b.MaxBytes <= 0 {
		b.MaxBytes = 100 << 20 // 100 MiB per tunnel
	}
	if b.IdleTimeoutMs <= 0 {
		b.IdleTimeoutMs = 5 * 60 * 1000 // 5 minutes
	}
	return nil
}

// normalizeUDP applies UDP-specific defaults. The URL accepts either a bare
// host:port pair or a udp:// scheme; the dialer strips the scheme.
func (b *BackendConfig) normalizeUDP(_ string) []string {
	if b.MaxBytes <= 0 {
		b.MaxBytes = 0 // unlimited by default for UDP
	}
	if b.IdleTimeoutMs <= 0 {
		b.IdleTimeoutMs = 60_000 // 1 minute default for UDP
	}
	if b.MaxDatagramBytes <= 0 {
		b.MaxDatagramBytes = udpDefaultMaxDatagramBytes
	}
	return nil
}
