package models

import (
	"fmt"
	"strings"

	"github.com/scitrera/aether/pkg/sharding"
)

// System workspace and implementation constants for internal identities
const (
	SystemWorkspace             = "_system"
	CleanupLeaderImplementation = "_cleanup"
	CleanupLeaderSpecifier      = "leader"
)

// CleanupLeaderIdentity returns the identity used for cleanup leader election.
// This allows multiple gateway instances to use the existing session lock mechanism
// to elect a single cleanup leader.
func CleanupLeaderIdentity() Identity {
	return Identity{
		Type:           PrincipalTask,
		Workspace:      SystemWorkspace,
		Implementation: CleanupLeaderImplementation,
		Specifier:      CleanupLeaderSpecifier,
	}
}

type PrincipalType string

const (
	PrincipalAgent          PrincipalType = "Agent"
	PrincipalTask           PrincipalType = "Task"
	PrincipalUser           PrincipalType = "User"
	PrincipalService        PrincipalType = "Service"
	PrincipalWorkflowEngine PrincipalType = "WorkflowEngine"
	PrincipalMetricsBridge  PrincipalType = "MetricsBridge"
	PrincipalOrchestrator   PrincipalType = "Orchestrator"
	PrincipalBridge         PrincipalType = "Bridge"
)

type Identity struct {
	Type           PrincipalType
	Workspace      string
	Implementation string
	Specifier      string
	ID             string // Generated for non-unique tasks
}

// ToTopic returns the RabbitMQ topic for this identity to subscribe to.
// Returns empty string for system principals that don't need topic
// subscriptions, and also when any segment contains the reserved "::"
// separator (i.e. the identity is malformed). Callers that need to surface
// validation errors at the API boundary should use ToTopicErr instead.
func (i Identity) ToTopic() string {
	topic, _ := i.ToTopicErr()
	return topic
}

// ToTopicErr returns the RabbitMQ topic for this identity to subscribe to,
// along with a validation error if any segment contains the reserved "::"
// separator. Gateway request handlers should use this variant and translate
// the error into a typed gRPC error at the boundary.
func (i Identity) ToTopicErr() (string, error) {
	switch i.Type {
	case PrincipalAgent:
		return AgentTopic(i.Workspace, i.Implementation, i.Specifier)
	case PrincipalTask:
		if i.Specifier != "" {
			return UniqueTaskTopic(i.Workspace, i.Implementation, i.Specifier)
		}
		return TaskTopic(i.Workspace, i.Implementation, i.ID)
	case PrincipalUser:
		return UserWindowTopic(i.ID, i.Specifier) // ID is UserID, Specifier is WindowID
	case PrincipalService:
		return ServiceTopic(i.Implementation, i.Specifier)
	case PrincipalWorkflowEngine:
		// Single sharded fan-in topic — workflow engines subscribe to the
		// receiver shard, not a per-workspace stream. Today the stub maps
		// every workspace to shard 0. The workspace component on the
		// identity itself is ignored for topic selection (and is typically
		// empty on WFE connections).
		return sharding.ReceiverTopic("event", 0), nil
	case PrincipalMetricsBridge:
		return sharding.ReceiverTopic("metric", 0), nil
	case PrincipalBridge:
		return BridgeTopic(i.Implementation, i.Specifier)
	case PrincipalOrchestrator:
		return "", nil // Orchestrators don't subscribe to RabbitMQ topics
	default:
		return "", nil
	}
}

// String returns a unique identifier for this identity, used for locking and logging.
// Unlike ToTopic(), this always returns a non-empty string for valid identities.
func (i Identity) String() string {
	switch i.Type {
	case PrincipalOrchestrator:
		// Orchestrators use Implementation::Specifier for uniqueness (multiple instances)
		if i.Specifier != "" {
			return "orc" + IdentitySep + i.Implementation + IdentitySep + i.Specifier
		}
		return "orc" + IdentitySep + i.Implementation
	case PrincipalWorkflowEngine:
		// Single-WFE invariant: the identity key is a constant so the
		// existing distributed-lock-on-identity blocks a second WFE from
		// connecting. The Implementation field is ignored for now; future
		// multi-shard support can include the shard index here once
		// sharding.TotalShards() > 1.
		return "wfe" + IdentitySep + "shard0"
	case PrincipalMetricsBridge:
		// Single-MB invariant: matches the WFE sharding model. The
		// Implementation field is ignored for now; future multi-shard support
		// can include the shard index here once sharding.TotalShards() > 1.
		return "metrics" + IdentitySep + "shard0"
	case PrincipalBridge:
		// Bridges use their topic format for uniqueness
		return i.ToTopic()
	default:
		// For agents, tasks, users - use the topic format
		return i.ToTopic()
	}
}

// ParseIdentity parses an identity string back into an Identity struct.
//
// Identity strings use "::" (IdentitySep) as the segment separator, not ".".
// This lets field values legitimately contain "." (Python FQNs like
// "scitrera_ai_runtime.cowork.aether_bridge.CoworkAgent", email-style
// user_ids like "alice@example.com") without breaking the parser.
//
// Expected formats:
//   - ag::{workspace}::{impl}::{spec} - Agent
//   - tu::{workspace}::{impl}::{spec} - Unique task
//   - ta::{workspace}::{impl}::{id}   - Non-unique task
//   - us::{user_id}::{window_id}      - User
//   - sv::{impl}::{spec}              - Service
//   - br::{impl}::{spec}              - Bridge
func ParseIdentity(identityStr string) (Identity, error) {
	var identity Identity

	if len(identityStr) < 3 {
		return identity, fmt.Errorf("invalid identity string: too short")
	}

	parts := strings.Split(identityStr, IdentitySep)
	if len(parts) < 2 {
		return identity, fmt.Errorf("invalid identity string: missing %q separator", IdentitySep)
	}

	prefix := parts[0]

	switch prefix {
	case "ag":
		// ag::workspace::impl::spec
		if len(parts) != 4 {
			return identity, fmt.Errorf("invalid agent identity format: expected 4 parts, got %d", len(parts))
		}
		identity = Identity{
			Type:           PrincipalAgent,
			Workspace:      parts[1],
			Implementation: parts[2],
			Specifier:      parts[3],
		}

	case "tu":
		// tu::workspace::impl::spec
		if len(parts) != 4 {
			return identity, fmt.Errorf("invalid unique task identity format: expected 4 parts, got %d", len(parts))
		}
		identity = Identity{
			Type:           PrincipalTask,
			Workspace:      parts[1],
			Implementation: parts[2],
			Specifier:      parts[3],
		}

	case "ta":
		// ta::workspace::impl::id
		if len(parts) != 4 {
			return identity, fmt.Errorf("invalid non-unique task identity format: expected 4 parts, got %d", len(parts))
		}
		identity = Identity{
			Type:           PrincipalTask,
			Workspace:      parts[1],
			Implementation: parts[2],
			ID:             parts[3],
		}

	case "us":
		// us::user_id::window_id
		if len(parts) != 3 {
			return identity, fmt.Errorf("invalid user identity format: expected 3 parts, got %d", len(parts))
		}
		identity = Identity{
			Type:      PrincipalUser,
			ID:        parts[1],
			Specifier: parts[2],
		}

	case "br":
		// br::impl::spec
		if len(parts) != 3 {
			return identity, fmt.Errorf("invalid bridge identity format: expected 3 parts, got %d", len(parts))
		}
		identity = Identity{
			Type:           PrincipalBridge,
			Implementation: parts[1],
			Specifier:      parts[2],
		}

	case "sv":
		// sv::impl::spec
		if len(parts) != 3 {
			return identity, fmt.Errorf("invalid service identity format: expected 3 parts, got %d", len(parts))
		}
		identity = Identity{
			Type:           PrincipalService,
			Implementation: parts[1],
			Specifier:      parts[2],
		}

	case "orc":
		// orc::impl[::spec] — matches Identity.String() for PrincipalOrchestrator
		if len(parts) < 2 || len(parts) > 3 {
			return identity, fmt.Errorf("invalid orchestrator identity format: expected 2 or 3 parts, got %d", len(parts))
		}
		identity = Identity{
			Type:           PrincipalOrchestrator,
			Implementation: parts[1],
		}
		if len(parts) == 3 {
			identity.Specifier = parts[2]
		}

	case "wfe":
		// wfe::shard0 — canonical singleton form. Any wfe::{anything} is
		// accepted for backward compat (legacy stored grants used
		// wfe::{implementation}) but always collapses to the singleton
		// identity at runtime.
		if len(parts) != 2 {
			return identity, fmt.Errorf("invalid workflow engine identity format: expected 2 parts, got %d", len(parts))
		}
		identity = Identity{Type: PrincipalWorkflowEngine}

	case "metrics":
		// metrics::shard0 — canonical singleton form. Any metrics::{anything}
		// is accepted for backward compat (legacy stored grants used
		// metrics::{implementation}) but always collapses to the singleton
		// identity at runtime.
		if len(parts) != 2 {
			return identity, fmt.Errorf("invalid metrics bridge identity format: expected 2 parts, got %d", len(parts))
		}
		identity = Identity{Type: PrincipalMetricsBridge}

	default:
		return identity, fmt.Errorf("unknown identity prefix: %s", prefix)
	}

	return identity, nil
}
