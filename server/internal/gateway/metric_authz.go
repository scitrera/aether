package gateway

import (
	"context"
	"fmt"
	"math"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/pkg/models"
	"google.golang.org/protobuf/proto"
)

// Metric payload limits. These are hard caps applied at the gateway to bound
// resource usage independently of the per-message payload-size limit.
const (
	maxMetricEntries  = 1024
	maxMetricMetadata = 64
)

// metricValidationError carries a stable client-facing error code alongside
// the human-readable reason. routeMessage uses this to set the appropriate
// ERR_METRIC_* code on the client response and audit metadata.
type metricValidationError struct {
	code   string
	reason string
}

func (e *metricValidationError) Error() string { return e.reason }

func newMetricValidationError(code, reason string) *metricValidationError {
	return &metricValidationError{code: code, reason: reason}
}

// validateMetricShape parses the payload as a Metric proto and enforces the
// structural invariants. It does NOT consult ACL — see checkMetricCredit for
// the negative-delta authorization. Returns the parsed Metric on success or a
// *metricValidationError on failure.
//
// Returns codes:
//
//	ERR_METRIC_INVALID       — payload is not a valid Metric proto, or has
//	                           too many metadata keys
//	ERR_METRIC_EMPTY         — Metric.entries is empty or above the entry cap
//	ERR_METRIC_INVALID_ENTRY — entry has empty name, NaN, or +/-Inf qty
func validateMetricShape(payload []byte) (*pb.Metric, error) {
	m := &pb.Metric{}
	if err := proto.Unmarshal(payload, m); err != nil {
		return nil, newMetricValidationError("ERR_METRIC_INVALID", fmt.Sprintf("metric payload is not a valid Metric proto: %v", err))
	}
	if len(m.Entries) == 0 {
		return nil, newMetricValidationError("ERR_METRIC_EMPTY", "metric must include at least one entry")
	}
	if len(m.Entries) > maxMetricEntries {
		return nil, newMetricValidationError("ERR_METRIC_EMPTY", fmt.Sprintf("metric exceeds maximum of %d entries", maxMetricEntries))
	}
	if len(m.Metadata) > maxMetricMetadata {
		return nil, newMetricValidationError("ERR_METRIC_INVALID", fmt.Sprintf("metric metadata exceeds maximum of %d keys", maxMetricMetadata))
	}
	for i, e := range m.Entries {
		if e == nil {
			return nil, newMetricValidationError("ERR_METRIC_INVALID_ENTRY", fmt.Sprintf("metric entry %d is nil", i))
		}
		if e.Name == "" {
			return nil, newMetricValidationError("ERR_METRIC_INVALID_ENTRY", fmt.Sprintf("metric entry %d has empty name", i))
		}
		if math.IsNaN(e.Qty) || math.IsInf(e.Qty, 0) {
			return nil, newMetricValidationError("ERR_METRIC_INVALID_ENTRY", fmt.Sprintf("metric entry %d (%q) has non-finite qty", i, e.Name))
		}
	}
	return m, nil
}

// metricHasNegative reports whether any entry carries a negative qty.
// Caller is expected to have already validated shape via validateMetricShape.
func metricHasNegative(m *pb.Metric) bool {
	for _, e := range m.Entries {
		if e != nil && e.Qty < 0 {
			return true
		}
	}
	return false
}

// checkMetricCredit verifies the sender (or, in OBO mode, the subject) is
// authorized to publish negative metric deltas. The required permission is
// `capability/metric_credit`, checked at AccessAdmin to match the privilege
// tier of all other admin/capability checks (admin/*, capability/*).
//
// targetTopic is used to derive the workspace ACL scope so that grants can
// be anchored to a specific workspace; for principals without a sender-side
// workspace (bridges, services) this is the only signal available.
//
// When `s.acl == nil` (e.g. ACL disabled in dev mode), this returns a
// distinct ERR_METRIC_NEGATIVE_FORBIDDEN error explaining ACL is required —
// fail-closed because credits are billing-sensitive.
func (s *GatewayServer) checkMetricCredit(ctx context.Context, sender models.Identity, targetTopic string, sessionUUID uuid.UUID, resolvedAuthority *acl.ResolvedAuthority) error {
	if hasMetricCreditPermission(ctx, s, sender, targetTopic, sessionUUID, resolvedAuthority) {
		return nil
	}
	return newMetricValidationError("ERR_METRIC_NEGATIVE_FORBIDDEN", "negative metric deltas require additional authorization")
}

// hasMetricCreditPermission is the production ACL check for negative-delta
// authorization. Exposed as a package-level variable so tests can substitute
// a deterministic stub without standing up a full *acl.Service.
//
// Implementation note: prefers `CheckAccessWithAuthority` when the caller is
// acting on-behalf-of a subject (the subject's grant is what matters for
// credit issuance, not the agent/task that physically delivered the message).
// Workspace scope is taken from the target topic so grants can be anchored
// to the workspace whose counters are being adjusted.
//
// NOTE: tests in this package must not call t.Parallel() while overriding
// this var — the substitution is process-global.
var hasMetricCreditPermission = func(ctx context.Context, s *GatewayServer, sender models.Identity, targetTopic string, sessionUUID uuid.UUID, resolvedAuthority *acl.ResolvedAuthority) bool {
	if s == nil || s.acl == nil {
		return false
	}
	workspace := workspaceFromTopic(targetTopic)
	if workspace == "" {
		workspace = sender.Workspace
	}
	if resolvedAuthority != nil {
		decision, err := s.acl.CheckAccessWithAuthority(
			ctx, sender, resolvedAuthority,
			acl.ResourceTypeCapability, acl.PermissionMetricCredit,
			"metric_write_negative", workspace, sessionUUID, acl.AccessAdmin,
		)
		return err == nil && decision != nil && decision.Allowed
	}
	decision, err := s.acl.CheckAccess(
		ctx, sender,
		acl.ResourceTypeCapability, acl.PermissionMetricCredit,
		"metric_write_negative", workspace, sessionUUID, acl.AccessAdmin,
	)
	return err == nil && decision != nil && decision.Allowed
}

// rejectMetric is the shared rejection path for metric validation failures
// (shape and credit). It logs, increments error metrics, emits an audit
// event (with resolved authority lineage when available), and returns the
// stable ERR_METRIC_* code to the client.
func (s *GatewayServer) rejectMetric(ctx context.Context, client *ClientSession, sender models.Identity, msg *pb.SendMessage, sessionUUID uuid.UUID, resolvedAuthority *acl.ResolvedAuthority, metricErr error) {
	mvErr, ok := metricErr.(*metricValidationError)
	code := "ERR_METRIC_INVALID"
	if ok {
		code = mvErr.code
	}
	logging.Logger.Warn().
		Str("from", sender.ToTopic()).
		Str("to", msg.TargetTopic).
		Str("code", code).
		Err(metricErr).
		Msg("metric payload rejected")
	messageErrors.WithLabelValues(sender.Workspace, "metric_validation").Inc()

	event := audit.NewMessageEvent(
		string(sender.Type), sender.String(), audit.OpMessageRouteFailed,
		msg.TargetTopic, sender.Workspace, sessionUUID, false, metricErr.Error(),
		map[string]interface{}{
			"from":          sender.ToTopic(),
			"to":            msg.TargetTopic,
			"message_type":  msg.MessageType.String(),
			"denied_reason": "metric_validation",
			"error_code":    code,
		},
	)
	applyResolvedAuthorityToAuditEvent(event, resolvedAuthority)
	s.auditLog(ctx, event)

	sendClientError(client, code, metricErr.Error())
}
