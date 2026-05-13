package authproxy

import (
	"fmt"
	"strings"
)

// AuthRule is one declarative check applied to a verified token's claims.
//
// Examples:
//
//	{Claim: "tid", AllowEq: []string{"<azure-tenant-uuid>"}, Required: true}
//	{Claim: "email_verified", AllowEq: []string{"true"}, Required: true}
//	{Claim: "hd", AllowEq: []string{"scitrera.com"}, Required: false}
//
// Required=true causes a missing claim to be rejected. AllowEq is a string
// equality whitelist; an empty list means "claim presence is enough".
type AuthRule struct {
	Claim    string
	AllowEq  []string
	Required bool
}

// EvaluateAuthRules walks the rule set against claims. It returns (true, "")
// when every rule passes, or (false, reason) on the first failure.
//
// Bool claims (e.g. email_verified) are compared as the string "true"/"false".
// Numeric and other non-string claims are not currently supported.
func EvaluateAuthRules(claims map[string]any, rules []AuthRule) (bool, string) {
	for _, rule := range rules {
		raw, present := claims[rule.Claim]
		if !present {
			if rule.Required {
				return false, fmt.Sprintf("required claim %q missing", rule.Claim)
			}
			continue
		}
		if len(rule.AllowEq) == 0 {
			continue
		}
		val := claimAsString(raw)
		if val == "" {
			return false, fmt.Sprintf("claim %q has unsupported type %T", rule.Claim, raw)
		}
		if !contains(rule.AllowEq, val) {
			return false, fmt.Sprintf("claim %q value not in allowed set", rule.Claim)
		}
	}
	return true, ""
}

// ParseRule parses a compact env-style rule spec into an AuthRule.
//
// Format: [name=]claim:val1[,val2,...][,REQUIRED]
//
// Examples:
//
//	"tid:beeaef61-22c3-4e35-a140-05d2b6615d37,REQUIRED"
//	"azure_tid=tid:beeaef61-22c3-4e35-a140-05d2b6615d37,REQUIRED"
//	"hd:scitrera.com"
//	"email_verified:true,REQUIRED"
//
// The leading "name=" prefix is optional metadata; it is dropped during
// parsing. Trailing ",REQUIRED" (case-insensitive) sets Required=true.
func ParseRule(spec string) (AuthRule, error) {
	if i := strings.IndexByte(spec, '='); i >= 0 {
		spec = spec[i+1:]
	}
	parts := strings.SplitN(spec, ":", 2)
	if len(parts) != 2 {
		return AuthRule{}, fmt.Errorf("rule %q: expected claim:values format", spec)
	}
	rule := AuthRule{Claim: strings.TrimSpace(parts[0])}
	for _, v := range strings.Split(parts[1], ",") {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if strings.EqualFold(v, "REQUIRED") {
			rule.Required = true
			continue
		}
		rule.AllowEq = append(rule.AllowEq, v)
	}
	if rule.Claim == "" {
		return AuthRule{}, fmt.Errorf("rule %q: empty claim", spec)
	}
	return rule, nil
}

func claimAsString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
