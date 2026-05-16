package models

import (
	"fmt"
	"strings"
)

// Topic construction helpers for commonly used topic patterns.
//
// Identity strings use "::" as the top-level segment separator — not ".". This
// lets field values (implementation names like Python FQNs, specifiers like
// email addresses) legitimately contain "." without breaking the parser. Any
// field value passed into these builders must NOT contain a raw "::" — that's
// enforced at the API boundary by ValidateSegment.
//
// All builders return (string, error). Callers that have already validated
// their inputs (e.g. internal code paths that construct topics from
// previously-parsed identities) may use the MustXxx wrappers, which panic on
// invalid input. New code reachable from a request boundary should always use
// the error-returning builders and surface the error as a typed gRPC error.

// IdentitySep is the segment separator used in all identity / topic strings.
const IdentitySep = "::"

// ErrInvalidSegment is returned when a field value contains the reserved
// separator sequence. Callers should surface this at the API boundary.
type ErrInvalidSegment struct {
	Field string
	Value string
}

func (e ErrInvalidSegment) Error() string {
	return fmt.Sprintf("identity field %q contains reserved %q sequence: %q", e.Field, IdentitySep, e.Value)
}

// ValidateSegment returns a typed error if the segment contains the reserved
// "::" sequence. Empty segments are allowed (e.g. empty specifier).
func ValidateSegment(field, value string) error {
	if strings.Contains(value, IdentitySep) {
		return ErrInvalidSegment{Field: field, Value: value}
	}
	return nil
}

// validateSegments returns the first non-nil ValidateSegment error encountered,
// or nil if all (field, value) pairs are valid. The variadic arguments must be
// supplied in alternating (field, value) order.
func validateSegments(pairs ...string) error {
	if len(pairs)%2 != 0 {
		return fmt.Errorf("validateSegments: odd number of arguments")
	}
	for i := 0; i < len(pairs); i += 2 {
		if err := ValidateSegment(pairs[i], pairs[i+1]); err != nil {
			return err
		}
	}
	return nil
}

func AgentTopic(workspace, impl, specifier string) (string, error) {
	if err := validateSegments("workspace", workspace, "implementation", impl, "specifier", specifier); err != nil {
		return "", err
	}
	return "ag" + IdentitySep + workspace + IdentitySep + impl + IdentitySep + specifier, nil
}

func UniqueTaskTopic(workspace, impl, specifier string) (string, error) {
	if err := validateSegments("workspace", workspace, "implementation", impl, "specifier", specifier); err != nil {
		return "", err
	}
	return "tu" + IdentitySep + workspace + IdentitySep + impl + IdentitySep + specifier, nil
}

func TaskTopic(workspace, impl, id string) (string, error) {
	if err := validateSegments("workspace", workspace, "implementation", impl, "id", id); err != nil {
		return "", err
	}
	return "ta" + IdentitySep + workspace + IdentitySep + impl + IdentitySep + id, nil
}

func TaskBroadcastTopic(workspace, impl string) (string, error) {
	if err := validateSegments("workspace", workspace, "implementation", impl); err != nil {
		return "", err
	}
	return "tb" + IdentitySep + workspace + IdentitySep + impl, nil
}

func GlobalAgentTopic(workspace string) (string, error) {
	if err := ValidateSegment("workspace", workspace); err != nil {
		return "", err
	}
	return "ga" + IdentitySep + workspace, nil
}

func GlobalUserTopic(workspace string) (string, error) {
	if err := ValidateSegment("workspace", workspace); err != nil {
		return "", err
	}
	return "gu" + IdentitySep + workspace, nil
}

func UserWindowTopic(userID, windowID string) (string, error) {
	if err := validateSegments("user_id", userID, "window_id", windowID); err != nil {
		return "", err
	}
	return "us" + IdentitySep + userID + IdentitySep + windowID, nil
}

func UserWorkspaceTopic(userID, workspace string) (string, error) {
	if err := validateSegments("user_id", userID, "workspace", workspace); err != nil {
		return "", err
	}
	return "uw" + IdentitySep + userID + IdentitySep + workspace, nil
}

func ProgressTopic(workspace string) (string, error) {
	if err := ValidateSegment("workspace", workspace); err != nil {
		return "", err
	}
	return "pg" + IdentitySep + workspace, nil
}

// TaskEventsTopic returns the topic used for a task's per-task event stream.
// Format: tk::{workspace}::{task_id}::events
//
// This topic carries TaskEvent protos (status transitions, progress projection,
// child-task lifecycle, authority-request relay) for a single task_id. Clients
// subscribe via TaskSubscriptionOperation; the gateway publishes from the
// TaskAssignmentService lifecycle hooks and from progress / authority-request
// handlers when the originating message carries a matching task id.
func TaskEventsTopic(workspace, taskID string) (string, error) {
	if err := validateSegments("workspace", workspace, "task_id", taskID); err != nil {
		return "", err
	}
	return "tk" + IdentitySep + workspace + IdentitySep + taskID + IdentitySep + "events", nil
}

// MustTaskEventsTopic builds a task-events topic and panics on invalid input.
// Use only for trusted segments.
func MustTaskEventsTopic(workspace, taskID string) string {
	t, err := TaskEventsTopic(workspace, taskID)
	if err != nil {
		panic(err)
	}
	return t
}

// UserProgressTopic returns a per-user progress stream topic used for
// targeted progress delivery across all of a user's open windows. Agents
// that send chat-kind progress set ProgressReport.recipient to the user's
// identity topic (with or without a window specifier); the gateway always
// publishes to this per-user topic, and the gateway-side filter at delivery
// time decides whether each individual window subscriber should receive the
// update (window-specific recipient = exact match; bare user-level recipient
// = prefix match against `us::{user}::`).
//
// Collapsing the prior per-window topic into a per-user topic cuts Rabbit
// stream count by N (with N concurrent windows per user) and removes the
// stream-orphaning churn of opening/closing tabs. Local gateway fan-out
// dispatches each message to every subscribed window-handler.
func UserProgressTopic(userID string) (string, error) {
	if err := ValidateSegment("user_id", userID); err != nil {
		return "", err
	}
	return "pg" + IdentitySep + "us" + IdentitySep + userID, nil
}

func BridgeTopic(impl, specifier string) (string, error) {
	if err := validateSegments("implementation", impl, "specifier", specifier); err != nil {
		return "", err
	}
	return "br" + IdentitySep + impl + IdentitySep + specifier, nil
}

func ServiceTopic(impl, specifier string) (string, error) {
	if err := validateSegments("implementation", impl, "specifier", specifier); err != nil {
		return "", err
	}
	return "sv" + IdentitySep + impl + IdentitySep + specifier, nil
}

// MustAgentTopic builds an agent topic and panics on invalid input. Use only
// for tests and other code paths where the segments are known-valid (e.g.
// derived from a previously-parsed identity).
func MustAgentTopic(workspace, impl, specifier string) string {
	t, err := AgentTopic(workspace, impl, specifier)
	if err != nil {
		panic(err)
	}
	return t
}

// MustUniqueTaskTopic builds a unique-task topic and panics on invalid input.
// Use only for trusted segments (e.g. derived from a previously-parsed identity).
func MustUniqueTaskTopic(workspace, impl, specifier string) string {
	t, err := UniqueTaskTopic(workspace, impl, specifier)
	if err != nil {
		panic(err)
	}
	return t
}

// MustTaskTopic builds a non-unique task topic and panics on invalid input.
// Use only for trusted segments.
func MustTaskTopic(workspace, impl, id string) string {
	t, err := TaskTopic(workspace, impl, id)
	if err != nil {
		panic(err)
	}
	return t
}

// MustTaskBroadcastTopic builds a task-broadcast topic and panics on invalid input.
// Use only for trusted segments.
func MustTaskBroadcastTopic(workspace, impl string) string {
	t, err := TaskBroadcastTopic(workspace, impl)
	if err != nil {
		panic(err)
	}
	return t
}

// MustGlobalAgentTopic builds a global-agent topic and panics on invalid input.
// Use only for trusted segments.
func MustGlobalAgentTopic(workspace string) string {
	t, err := GlobalAgentTopic(workspace)
	if err != nil {
		panic(err)
	}
	return t
}

// MustGlobalUserTopic builds a global-user topic and panics on invalid input.
// Use only for trusted segments.
func MustGlobalUserTopic(workspace string) string {
	t, err := GlobalUserTopic(workspace)
	if err != nil {
		panic(err)
	}
	return t
}

// MustUserWindowTopic builds a user-window topic and panics on invalid input.
// Use only for trusted segments.
func MustUserWindowTopic(userID, windowID string) string {
	t, err := UserWindowTopic(userID, windowID)
	if err != nil {
		panic(err)
	}
	return t
}

// MustUserWorkspaceTopic builds a user-workspace topic and panics on invalid input.
// Use only for trusted segments.
func MustUserWorkspaceTopic(userID, workspace string) string {
	t, err := UserWorkspaceTopic(userID, workspace)
	if err != nil {
		panic(err)
	}
	return t
}

// MustProgressTopic builds a progress topic and panics on invalid input.
// Use only for trusted segments.
func MustProgressTopic(workspace string) string {
	t, err := ProgressTopic(workspace)
	if err != nil {
		panic(err)
	}
	return t
}

// MustUserProgressTopic builds a per-user progress topic and panics on invalid input.
// Use only for trusted segments.
func MustUserProgressTopic(userID string) string {
	t, err := UserProgressTopic(userID)
	if err != nil {
		panic(err)
	}
	return t
}

// MustBridgeTopic builds a bridge topic and panics on invalid input.
// Use only for trusted segments.
func MustBridgeTopic(impl, specifier string) string {
	t, err := BridgeTopic(impl, specifier)
	if err != nil {
		panic(err)
	}
	return t
}

// MustServiceTopic builds a service topic and panics on invalid input.
// Use only for trusted segments.
func MustServiceTopic(impl, specifier string) string {
	t, err := ServiceTopic(impl, specifier)
	if err != nil {
		panic(err)
	}
	return t
}

// ParseSendTarget parses a service send-target string. It accepts two forms:
//   - sv::{impl}::{specifier}  — canonical 4-segment target; isWildcard=false
//   - sv::{impl}               — bare 3-segment wildcard; isWildcard=true, specifier=""
//
// Any other input (wrong prefix, wrong segment count) returns a non-nil error.
// Field values must not contain "::" — that would split into extra segments
// and is rejected as malformed.
func ParseSendTarget(topic string) (impl, specifier string, isWildcard bool, err error) {
	parts := strings.Split(topic, IdentitySep)
	if len(parts) < 2 || parts[0] != "sv" {
		return "", "", false, fmt.Errorf("invalid service send target: must start with %q, got %q", "sv"+IdentitySep, topic)
	}
	switch len(parts) {
	case 2:
		if parts[1] == "" {
			return "", "", false, fmt.Errorf("invalid service send target: empty implementation in %q", topic)
		}
		return parts[1], "", true, nil
	case 3:
		if parts[1] == "" {
			return "", "", false, fmt.Errorf("invalid service send target: empty implementation in %q", topic)
		}
		return parts[1], parts[2], false, nil
	default:
		return "", "", false, fmt.Errorf("invalid service send target: too many segments in %q", topic)
	}
}
