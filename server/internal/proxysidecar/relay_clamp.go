package proxysidecar

import (
	"fmt"
	"path"
	"strings"
)

// targetClamp encapsulates the relay's outbound target_topic policy.
type targetClamp struct {
	mode    string
	allowed []string
}

// newTargetClamp converts cfg into a runtime clamp. Validation has already
// rejected unknown modes; this only stores values.
func newTargetClamp(cfg TargetClampConfig) *targetClamp {
	return &targetClamp{
		mode:    cfg.Mode,
		allowed: append([]string(nil), cfg.AllowedTargets...),
	}
}

// clampResult is the outcome of evaluating a target against the clamp.
type clampResult struct {
	// Allowed reports whether the envelope may proceed.
	Allowed bool
	// NewTarget is non-empty when the clamp rewrote the target. The caller
	// stamps it onto the outbound envelope.
	NewTarget string
	// Reason is a short human-readable explanation; populated even on
	// success when the clamp rewrote the target so it can be logged.
	Reason string
}

// evaluate returns a decision for target. An empty target is always rejected:
// proxy/tunnel envelopes must address a topic.
func (c *targetClamp) evaluate(target string) clampResult {
	target = strings.TrimSpace(target)
	if c == nil {
		return clampResult{Allowed: true}
	}
	if target == "" {
		return clampResult{Allowed: false, Reason: "empty target_topic"}
	}
	for _, pattern := range c.allowed {
		if matchTopic(pattern, target) {
			return clampResult{Allowed: true}
		}
	}
	switch c.mode {
	case TargetClampRewriteFirstMatch:
		if rewrite, ok := firstConcreteTarget(c.allowed); ok {
			return clampResult{
				Allowed:   true,
				NewTarget: rewrite,
				Reason:    fmt.Sprintf("rewritten to %q (was %q)", rewrite, target),
			}
		}
		return clampResult{
			Allowed: false,
			Reason:  fmt.Sprintf("no concrete allowed_targets entry to rewrite %q to", target),
		}
	default:
		return clampResult{
			Allowed: false,
			Reason:  fmt.Sprintf("target_topic %q does not match any allowed_targets", target),
		}
	}
}

// matchTopic implements glob matching for relay clamp patterns. A trailing
// `*` is treated as a prefix; otherwise we delegate to path.Match. Topic
// segments use `.` separators so patterns like `ag.ws.impl.*` work the
// natural way without leaking shell semantics.
func matchTopic(pattern, target string) bool {
	if pattern == target {
		return true
	}
	if strings.HasSuffix(pattern, ".*") {
		prefix := strings.TrimSuffix(pattern, ".*")
		if strings.HasPrefix(target, prefix+".") {
			return true
		}
	}
	if pattern == "*" {
		return true
	}
	if matched, err := path.Match(pattern, target); err == nil && matched {
		return true
	}
	return false
}

// firstConcreteTarget finds the first allowed_targets entry that contains
// no glob metacharacters; that is the rewrite destination.
func firstConcreteTarget(patterns []string) (string, bool) {
	for _, p := range patterns {
		if !strings.ContainsAny(p, "*?[") {
			return p, true
		}
	}
	return "", false
}

// hybridFloor returns max(claim, observed+1). Sandboxes may bump the
// chain depth higher (legitimate when they relay on behalf of a deeper
// upstream chain) but never below the inbound floor. observed is the
// largest depth the relay has seen on inbound proxy/tunnel envelopes.
//
// observed+1 overflow is not a practical concern (uint32 max is 4B; the
// gateway rejects depth > 8 well before then), but we still guard the
// arithmetic so a malicious sandbox can't trick us into wrapping.
func hybridFloor(claim, observed uint32) uint32 {
	floor := observed
	if observed < ^uint32(0) {
		floor = observed + 1
	}
	if claim > floor {
		return claim
	}
	return floor
}
