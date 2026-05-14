package audit

// This file re-exports the shared audit types and constants from the legacy
// internal/audit package under the new internal/storage/audit interface
// namespace. The legacy package remains the source of truth during Stage 1
// of the storage-interfaces refactor; Stage 2 will introduce a native
// sqlite sibling and may eventually let us collapse the legacy package
// into this one. For now, downstream callers can
//
//	import "github.com/scitrera/aether/internal/storage/audit"
//
// and find every type, constant, and helper they need to construct and
// query audit events — no double-import of the legacy package required.

import (
	legacy "github.com/scitrera/aether/internal/audit"
)

// Core types — aliased so a single import gets callers everything they need.
type (
	// Event is an audit log entry. See legacy.AuditEvent for field docs.
	Event = legacy.AuditEvent
	// EventFilter is the query-side filter for QueryAuditLog.
	EventFilter = legacy.EventFilter
	// Config holds the audit logger's runtime configuration (enabled flag,
	// event-type allowlist, verbosity, batch size, flush period, retention,
	// channel buffer).
	Config = legacy.Config
)

// Event types — values that land in comprehensive_audit_log.event_type.
// See legacy/types.go and legacy/config.go for usage in DefaultConfig() and
// ValidateEventType().
const (
	EventTypeConnection    = legacy.EventTypeConnection
	EventTypeAuth          = legacy.EventTypeAuth
	EventTypeAuthorization = legacy.EventTypeAuthorization
	EventTypeMessage       = legacy.EventTypeMessage
	EventTypeKV            = legacy.EventTypeKV
	EventTypeTask          = legacy.EventTypeTask
	EventTypeAdmin         = legacy.EventTypeAdmin
	EventTypeACL           = legacy.EventTypeACL
	EventTypeCustom        = legacy.EventTypeCustom
)

// Source values — comprehensive_audit_log.source column. Distinguishes
// gateway-emitted rows from principal-submitted rows.
const (
	SourceGateway   = legacy.SourceGateway
	SourcePrincipal = legacy.SourcePrincipal
)

// Authority modes — comprehensive_audit_log.authority_mode column.
const (
	AuthorityModeDirect     = legacy.AuthorityModeDirect
	AuthorityModeOnBehalfOf = legacy.AuthorityModeOnBehalfOf
)

// Resource types — comprehensive_audit_log.resource_type column. Mirror of
// the models.ResourceType* surface, re-exported here for callers that don't
// otherwise import pkg/models.
const (
	ResourceTypeSession   = legacy.ResourceTypeSession
	ResourceTypeTopic     = legacy.ResourceTypeTopic
	ResourceTypeWorkspace = legacy.ResourceTypeWorkspace
	ResourceTypeKVKey     = legacy.ResourceTypeKVKey
	ResourceTypeAgent     = legacy.ResourceTypeAgent
	ResourceTypeTask      = legacy.ResourceTypeTask
	ResourceTypeUser      = legacy.ResourceTypeUser
)

// Verbosity levels — controls how much message payload detail is captured
// when EventTypeMessage events flow through the logger.
const (
	VerbosityLow    = legacy.VerbosityLow
	VerbosityMedium = legacy.VerbosityMedium
	VerbosityHigh   = legacy.VerbosityHigh
)

// Default configuration knobs — exposed so callers (tests, env-loader
// shims) can reference the same defaults as the impl.
const (
	DefaultBatchSize      = legacy.DefaultBatchSize
	DefaultFlushPeriod    = legacy.DefaultFlushPeriod
	DefaultRetentionDays  = legacy.DefaultRetentionDays
	DefaultVerbosityLevel = legacy.DefaultVerbosityLevel
	DefaultChannelBuffer  = legacy.DefaultChannelBuffer
)

// Sentinel errors surfaced by the Store contract.
var (
	ErrInvalidEventType      = legacy.ErrInvalidEventType
	ErrInvalidVerbosityLevel = legacy.ErrInvalidVerbosityLevel
	ErrEventNotEnabled       = legacy.ErrEventNotEnabled
	ErrAuditLogNotFound      = legacy.ErrAuditLogNotFound
	ErrInvalidFilter         = legacy.ErrInvalidFilter
)

// Config constructors / helpers — re-exported so callers can build a
// Config without reaching into the legacy package.
var (
	// DefaultConfig returns the canonical default audit configuration.
	DefaultConfig = legacy.DefaultConfig
	// LoadConfigFromEnv builds a Config from AETHER_AUDIT_* environment
	// variables, falling back to DefaultConfig() for missing values.
	LoadConfigFromEnv = legacy.LoadConfigFromEnv
	// ValidateEventType returns nil if the given event_type string is
	// recognized. Source of truth for the canonical event-type set.
	ValidateEventType = legacy.ValidateEventType
	// EventTypeName returns a human-readable display name for an event
	// type (used by admin UI).
	EventTypeName = legacy.EventTypeName
	// NormalizePrincipalTypeCase downcases the canonical principal-type
	// strings (Agent → agent, etc.) so audit rows are case-consistent.
	NormalizePrincipalTypeCase = legacy.NormalizePrincipalTypeCase
)

// Event constructors — re-exported helpers for building common AuditEvent
// shapes without depending on the legacy package directly.
var (
	NewConnectionEvent = legacy.NewConnectionEvent
	NewAuthEvent       = legacy.NewAuthEvent
	NewMessageEvent    = legacy.NewMessageEvent
	NewKVEvent         = legacy.NewKVEvent
	NewTaskEvent       = legacy.NewTaskEvent
	NewAdminEvent      = legacy.NewAdminEvent
)
