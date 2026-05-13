package models

// PrincipalRef is the stable principal reference shape used by ACL, audit,
// and authority-grant plumbing.
type PrincipalRef struct {
	Type PrincipalType
	ID   string
}

// CanonicalPrincipalID returns the stable principal identifier used by ACL,
// audit, and authority grants.
//
// Users intentionally use the user ID so ACL grants apply across windows.
// Structured principals use their canonical identity string when available,
// falling back to ID for pre-materialized references reconstructed from storage.
func (i Identity) CanonicalPrincipalID() string {
	switch i.Type {
	case PrincipalUser:
		if i.ID != "" {
			return i.ID
		}
	case PrincipalAgent:
		if i.Workspace != "" && i.Implementation != "" && i.Specifier != "" {
			return i.String()
		}
	case PrincipalTask:
		if i.Workspace != "" && i.Implementation != "" && (i.Specifier != "" || i.ID != "") {
			return i.String()
		}
	case PrincipalService, PrincipalBridge:
		if i.Implementation != "" && i.Specifier != "" {
			return i.String()
		}
	case PrincipalOrchestrator:
		if i.Implementation != "" {
			return i.String()
		}
	case PrincipalWorkflowEngine, PrincipalMetricsBridge:
		if id := i.String(); id != "" {
			return id
		}
	}

	return i.ID
}

// PrincipalRef returns the stable principal reference for the identity.
func (i Identity) PrincipalRef() PrincipalRef {
	return PrincipalRef{
		Type: i.Type,
		ID:   i.CanonicalPrincipalID(),
	}
}

// IsZero reports whether the principal reference is incomplete.
func (r PrincipalRef) IsZero() bool {
	return r.Type == "" || r.ID == ""
}
