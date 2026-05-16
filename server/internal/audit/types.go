package audit

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/scitrera/aether/pkg/models"
)

// Event types for comprehensive audit logging
const (
	EventTypeConnection = "connection" // Connection lifecycle events (connect, disconnect)
	EventTypeAuth       = "auth"       // Authentication and identity resolution
	EventTypeMessage    = "message"    // Message routing events
	EventTypeKV         = "kv"         // Key-value operations
	EventTypeTask       = "task"       // Task lifecycle operations
	EventTypeAdmin      = "admin"      // Administrative actions
	EventTypeACL        = "acl"        // ACL decisions (for integration with existing ACL audit)
	// EventTypeAuthorization is the canonical event_type string used by the
	// ACL audit adapter (internal/acl) when forwarding decisions through the
	// shared audit writer. Distinct from EventTypeACL: "authorization" is
	// the value that already lives in comprehensive_audit_log per migrations
	// 008–018 (e.g. the acl_audit_log view filters on it), so we preserve
	// the schema's existing convention rather than rewriting historical
	// rows. The acl.AuditLogger.entryToEvent translation sets this value.
	EventTypeAuthorization = "authorization"
	EventTypeCustom        = "custom" // Custom event type for principal-submitted events
	// EventTypeAuthorityRequest is the canonical event_type string used for
	// authority-request lifecycle events (Phase 2 "sudo" handshake). Distinct
	// from EventTypeAuthorization (the ACL decision row) and from
	// EventTypeACL (typed ACL admin operations): authority requests are a
	// separate audit lane that captures the request/approve/deny flow that
	// later results in a standard authority-grant exchange. Emitted by the
	// gateway-side lifecycle service in Phase 2 Stage B.
	EventTypeAuthorityRequest = "authority_request"
)

// Source values distinguish gateway-emitted audit rows from principal-submitted ones.
// The comprehensive_audit_log.source column carries this value (default "gateway").
const (
	SourceGateway   = "gateway"   // Event emitted by the gateway from its own observation
	SourcePrincipal = "principal" // Event submitted by a connected principal via SubmitAuditEvent
)

// Operation constants for each event type
const (
	// Connection operations
	OpConnectionEstablished = "connection_established"
	OpConnectionClosed      = "connection_closed"
	OpLockAcquired          = "lock_acquired"
	OpLockRejected          = "lock_rejected"
	OpSessionRegistered     = "session_registered"

	// Auth operations
	OpAuthMTLSSuccess       = "auth_mtls_success"
	OpAuthMTLSFailure       = "auth_mtls_failure"
	OpAuthTokenValidation   = "auth_token_validation"
	OpIdentityResolved      = "identity_resolved"
	OpIdentityResolveFailed = "identity_resolve_failed"

	// Message operations
	OpMessageReceived    = "message_received"
	OpMessageRouted      = "message_routed"
	OpMessageRouteFailed = "message_route_failed"
	OpMessageDelivered   = "message_delivered"

	// Proxy + tunnel routing operations.
	OpProxyHttpRouted       = "proxy_http_routed"
	OpProxyHttpFailed       = "proxy_http_failed"
	OpProxyHttpStreamClosed = "proxy_http_stream_closed" // unbounded stream_response_indefinitely path closed (clean fin / idle / max-bytes / caller cancel)
	OpTunnelOpened          = "tunnel_opened"
	OpTunnelOpenFailed      = "tunnel_open_failed"
	OpTunnelClosed          = "tunnel_closed"

	// KV operations
	OpKVGet         = "kv_get"
	OpKVPut         = "kv_put"
	OpKVDelete      = "kv_delete"
	OpKVList        = "kv_list"
	OpKVIncrement   = "kv_increment"
	OpKVDecrement   = "kv_decrement"
	OpKVIncrementIf = "kv_increment_if"
	OpKVDecrementIf = "kv_decrement_if"

	// Task operations
	OpTaskCreate     = "task_create"
	OpTaskTokenIssue = "task_token_issue" // mint of a per-task auth token; checked separately from task_create because it's an authority-elevation primitive (lets the caller spawn a worker that authenticates as a declared identity)

	// Authority-grant lifecycle operations
	OpAuthorityGrantExchange = "authority_grant_exchange"
	OpAuthorityGrantDerive   = "authority_grant_derive"
	OpAuthorityGrantGet      = "authority_grant_get"
	OpAuthorityGrantRenew    = "authority_grant_renew"
	OpAuthorityGrantRevoke   = "authority_grant_revoke"
	OpAuthorityIntermediary  = "authority_intermediary_reroot" // a service principal exercised capability/authority_intermediary to mint a task grant that re-roots from the original subject (preserving principal chain) instead of failing at hop exhaustion

	// Authority-request lifecycle operations (Phase 2 Stage C). Used both as
	// the audit Operation column value and as the operation argument to
	// ACL CheckAccess when the gateway gates a request resolver against
	// the routing capability.
	OpAuthorityRequestResolve = "authority_request_resolve"

	// Admin operations
	OpAdminStateQuery        = "admin_state_query"
	OpAdminSessionDisconnect = "admin_session_disconnect"
	OpAdminConfigChange      = "admin_config_change"
)

// Authority modes for actor/subject audit semantics.
const (
	AuthorityModeDirect     = "direct"
	AuthorityModeOnBehalfOf = "on_behalf_of"
)

// Verbosity levels for message audit logging
const (
	VerbosityLow    = "low"    // Only routing metadata (from, to, timestamp)
	VerbosityMedium = "medium" // Add message type and size
	VerbosityHigh   = "high"   // Include message content; values for known credential-shaped keys are redacted
)

// Resource types for audit events - defined in pkg/models and aliased here for package-local use.
const (
	ResourceTypeSession   = models.ResourceTypeSession
	ResourceTypeTopic     = models.ResourceTypeTopic
	ResourceTypeWorkspace = models.ResourceTypeWorkspace
	ResourceTypeKVKey     = models.ResourceTypeKVKey
	ResourceTypeAgent     = models.ResourceTypeAgent
	ResourceTypeTask      = models.ResourceTypeTask
	ResourceTypeUser      = models.ResourceTypeUser
)

// Default configuration values
const (
	DefaultBatchSize      = 100
	DefaultFlushPeriod    = 5 * time.Second
	DefaultRetentionDays  = 90
	DefaultVerbosityLevel = VerbosityLow
	DefaultChannelBuffer  = 1000
)

// AuditEvent represents a comprehensive audit log entry
// This is the main structure for all security-relevant events in the system
type AuditEvent struct {
	AuditID                int64                  // Generated by database (BIGSERIAL)
	Timestamp              time.Time              // When the event occurred
	EventType              string                 // Type of event (connection, auth, message, kv, task, admin, acl)
	ActorType              string                 // Type of actor (agent, task, user, system)
	ActorID                string                 // Identity of actor
	SubjectType            string                 // Type of subject whose authority was used
	SubjectID              string                 // Identity of subject whose authority was used
	RootSubjectType        string                 // Root subject type across authority grant lineage
	RootSubjectID          string                 // Root subject identity across authority grant lineage
	AuthorityMode          string                 // direct or on_behalf_of
	RootAuthorityGrantID   *string                // Root grant in the authority lineage, if any
	AuthorityGrantID       *string                // Grant used for authority, if any
	ParentAuthorityGrantID *string                // Parent grant, if any
	ResourceType           string                 // Type of resource being accessed
	ResourceID             string                 // ID of resource
	Operation              string                 // What operation was attempted
	Workspace              string                 // Workspace context
	SessionID              uuid.UUID              // Session context
	GatewayID              string                 // Which gateway processed this event
	Success                bool                   // Whether the operation succeeded
	ErrorMessage           string                 // Error message if operation failed
	Metadata               map[string]interface{} // Additional event-specific data
	Source                 string                 // Provenance: "gateway" (default) or "principal"
}

// EventFilter defines filters for querying audit events
type EventFilter struct {
	StartTime        *time.Time // Start of time range
	EndTime          *time.Time // End of time range
	EventType        string     // Filter by event type
	ActorType        string     // Filter by actor type
	ActorID          string     // Filter by specific actor
	SubjectType      string     // Filter by subject type
	SubjectID        string     // Filter by subject identity
	AuthorityMode    string     // Filter by direct/on_behalf_of
	AuthorityGrantID string     // Filter by authority grant
	ResourceType     string     // Filter by resource type
	ResourceID       string     // Filter by specific resource
	Operation        string     // Filter by operation
	Workspace        string     // Filter by workspace
	SessionID        *uuid.UUID // Filter by session
	Success          *bool      // Filter by success/failure
	// Exclusion filters — rows whose actor_type or workspace match any of
	// these values are excluded from the result. Used by UI audit-log views
	// to suppress system-plumbing noise (WorkflowEngine/Orchestrator rows,
	// _system workspace, etc.) without losing inclusion-filter flexibility.
	ExcludeActorTypes []string
	ExcludeWorkspaces []string
	// ExcludeServiceDirect drops rows where actor_type='service' AND
	// authority_mode='direct' — i.e., a Service principal acting as itself
	// rather than on behalf of a user. Those are almost always platform
	// bookkeeping (launch-profile reads, internal config fetches); user
	// audit views typically want them hidden.
	ExcludeServiceDirect bool
	Limit                int // Maximum number of results
	Offset               int // Offset for pagination
}

// Config holds configuration for the audit logger
type Config struct {
	Enabled           bool          // Enable/disable audit logging
	EnabledEventTypes []string      // Which event types to log (empty = all)
	VerbosityLevel    string        // Verbosity level for message logging
	BatchSize         int           // Number of events to batch before writing
	FlushPeriod       time.Duration // How often to flush batched events
	RetentionDays     int           // How long to retain audit logs
	ChannelBuffer     int           // Size of async event channel buffer
}

// DefaultConfig returns a default audit configuration
func DefaultConfig() *Config {
	return &Config{
		Enabled: true,
		EnabledEventTypes: []string{
			EventTypeConnection,
			EventTypeAuth,
			EventTypeMessage,
			EventTypeKV,
			EventTypeTask,
			EventTypeAdmin,
			EventTypeACL,
			EventTypeAuthorization,
			// EventTypeAuthorityRequest is enabled by default so the
			// Phase 2 lifecycle service's audit events flow through the
			// shared writer without the platform having to opt in.
			EventTypeAuthorityRequest,
		},
		VerbosityLevel: DefaultVerbosityLevel,
		BatchSize:      DefaultBatchSize,
		FlushPeriod:    DefaultFlushPeriod,
		RetentionDays:  DefaultRetentionDays,
		ChannelBuffer:  DefaultChannelBuffer,
	}
}

// IsEventTypeEnabled returns true if the given event type should be logged
func (c *Config) IsEventTypeEnabled(eventType string) bool {
	if !c.Enabled {
		return false
	}

	// Empty list means all types are enabled
	if len(c.EnabledEventTypes) == 0 {
		return true
	}

	for _, et := range c.EnabledEventTypes {
		if et == eventType {
			return true
		}
	}

	return false
}

// ValidateEventType returns an error if the event type is invalid.
// Accepts gateway-emitted types and the "custom" type used by principal-
// submitted events via SubmitAuditEvent.
func ValidateEventType(eventType string) error {
	switch eventType {
	case EventTypeConnection, EventTypeAuth, EventTypeMessage,
		EventTypeKV, EventTypeTask, EventTypeAdmin, EventTypeACL,
		EventTypeAuthorization, EventTypeCustom, EventTypeAuthorityRequest:
		return nil
	default:
		return ErrInvalidEventType
	}
}

// ValidateVerbosityLevel returns an error if the verbosity level is invalid
func ValidateVerbosityLevel(level string) error {
	switch level {
	case VerbosityLow, VerbosityMedium, VerbosityHigh:
		return nil
	default:
		return ErrInvalidVerbosityLevel
	}
}

// EventTypeName returns a human-readable name for an event type
func EventTypeName(eventType string) string {
	switch eventType {
	case EventTypeConnection:
		return "Connection"
	case EventTypeAuth:
		return "Authentication"
	case EventTypeMessage:
		return "Message"
	case EventTypeKV:
		return "Key-Value"
	case EventTypeTask:
		return "Task"
	case EventTypeAdmin:
		return "Administrative"
	case EventTypeACL:
		return "Access Control"
	case EventTypeAuthorization:
		return "Authorization"
	default:
		return "Unknown"
	}
}

// VerbosityLevelName returns a human-readable name for a verbosity level
func VerbosityLevelName(level string) string {
	switch level {
	case VerbosityLow:
		return "Low"
	case VerbosityMedium:
		return "Medium"
	case VerbosityHigh:
		return "High"
	default:
		return "Unknown"
	}
}

// ShouldIncludeMessageContent returns true if message content should be logged
// based on the configured verbosity level
func ShouldIncludeMessageContent(verbosity string) bool {
	return verbosity == VerbosityHigh
}

// ShouldIncludeMessageMetadata returns true if message metadata should be logged
// based on the configured verbosity level
func ShouldIncludeMessageMetadata(verbosity string) bool {
	return verbosity == VerbosityMedium || verbosity == VerbosityHigh
}

// Common errors
var (
	ErrInvalidEventType      = fmt.Errorf("invalid event type")
	ErrInvalidVerbosityLevel = fmt.Errorf("invalid verbosity level")
	ErrEventNotEnabled       = fmt.Errorf("event type not enabled")
	ErrAuditLogNotFound      = fmt.Errorf("audit log entry not found")
	ErrInvalidFilter         = fmt.Errorf("invalid audit filter")
)

func applyDirectAuthority(event *AuditEvent) {
	if event.AuthorityMode == "" {
		event.AuthorityMode = AuthorityModeDirect
	}
	if event.SubjectType == "" {
		event.SubjectType = event.ActorType
	}
	if event.SubjectID == "" {
		event.SubjectID = event.ActorID
	}
	if event.RootSubjectType == "" {
		event.RootSubjectType = event.SubjectType
	}
	if event.RootSubjectID == "" {
		event.RootSubjectID = event.SubjectID
	}
	// Normalize principal type casing so downstream queries/filters don't
	// have to guess between "Agent"/"agent" (callers historically passed
	// either ``models.PrincipalType`` capitalized strings or the ACL
	// lowercase variants). Canonical form = lowercase, matching the ACL
	// layer (see acl.PrincipalTypeAgent = "agent").
	event.ActorType = NormalizePrincipalTypeCase(event.ActorType)
	event.SubjectType = NormalizePrincipalTypeCase(event.SubjectType)
	event.RootSubjectType = NormalizePrincipalTypeCase(event.RootSubjectType)
}

// NormalizePrincipalTypeCase returns the canonical lowercase form of a
// principal-type string used in audit rows. Values already lowercase pass
// through; capitalized “models.PrincipalType“ values ("Agent", "Service",
// etc.) are downcased; unknown values are returned unchanged so they remain
// debuggable rather than silently rewritten.
func NormalizePrincipalTypeCase(t string) string {
	switch t {
	case "Agent", "agent":
		return "agent"
	case "Task", "task":
		return "task"
	case "User", "user":
		return "user"
	case "Service", "service":
		return "service"
	case "Bridge", "bridge":
		return "bridge"
	case "Orchestrator", "orchestrator":
		return "orchestrator"
	case "WorkflowEngine", "workflowengine", "workflow_engine":
		return "workflow_engine"
	case "MetricsBridge", "metricsbridge", "metrics_bridge":
		return "metrics_bridge"
	}
	return t
}
