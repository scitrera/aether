package gateway

import (
	"context"
	"crypto/x509"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/scitrera/aether/pkg/models"
)

// AnonymousCertCN is the CN used for anonymous mTLS certificates that provide
// transport security without carrying auth identity.
const AnonymousCertCN = "_anonymous"

// MTLSMode defines how strictly to interpret client certificate identities.
// In strict mode, the certificate CN fully specifies the identity.
// In relaxed mode, the certificate only confirms the principal type,
// and the specific identity details come from InitConnection.
type MTLSMode string

const (
	// MTLSModeStrict requires certificate CN to fully specify identity
	MTLSModeStrict MTLSMode = "strict"
	// MTLSModeSemiStrict uses workspace + implementation from the certificate CN,
	// but allows the specifier to be provided or overridden via InitConnection.
	// This enables a single certificate to be shared across multiple instances
	// (e.g., horizontally-scaled platform servers) while still cryptographically
	// binding the identity to a specific workspace and implementation.
	MTLSModeSemiStrict MTLSMode = "semi-strict"
	// MTLSModeRelaxed allows certificate to only confirm principal type
	MTLSModeRelaxed MTLSMode = "relaxed"
)

// IsValid checks if the MTLSMode is a recognized value
func (m MTLSMode) IsValid() bool {
	switch m {
	case MTLSModeStrict, MTLSModeSemiStrict, MTLSModeRelaxed:
		return true
	default:
		return false
	}
}

// String returns the string representation of the MTLSMode
func (m MTLSMode) String() string {
	return string(m)
}

// ExtractIdentityFromCertificate extracts identity from client certificate CN
// Expected CN format: {type}.{workspace}.{impl}.{spec}
// Examples:
//   - ag.production.python-worker.instance-1
//   - ta.default.data-processor.job-123
//   - us.alice.window-1
//   - sv.frontend-api.pod-1
func ExtractIdentityFromCertificate(ctx context.Context) (models.Identity, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return models.Identity{}, fmt.Errorf("no peer info in context")
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return models.Identity{}, fmt.Errorf("peer auth info is not TLS")
	}

	if len(tlsInfo.State.PeerCertificates) == 0 {
		return models.Identity{}, fmt.Errorf("no client certificate")
	}

	cert := tlsInfo.State.PeerCertificates[0]

	// Validate certificate before extracting identity
	if err := ValidateCertificate(cert); err != nil {
		return models.Identity{}, fmt.Errorf("certificate validation failed: %w", err)
	}

	return ParseIdentityFromCN(cert.Subject.CommonName)
}

// ParseIdentityFromCN parses a certificate CN into an Identity
// CN format: {type}.{workspace}.{impl}.{spec}
// Examples:
//   - ag::production::python-worker::instance-1
//   - ta::default::data-processor::job-123
//   - us::alice::window-1
//   - sv::frontend-api::pod-1
//   - ga::default
//   - tb::default::worker
//
// Uses the identity-string separator ("::") so certificate CNs align with
// Aether topic names and the underlying Identity representation. Field values
// can now contain "." safely (Python FQNs, email-style user_ids) because "::"
// is the boundary.
func ParseIdentityFromCN(cn string) (models.Identity, error) {
	identity := models.Identity{}

	// Single-token CNs (wf, mb) don't use the separator at all.
	if cn == "wf" {
		identity.Type = models.PrincipalWorkflowEngine
		return identity, nil
	}
	if cn == "mb" {
		identity.Type = models.PrincipalMetricsBridge
		return identity, nil
	}

	parts := strings.Split(cn, models.IdentitySep)
	if len(parts) < 2 {
		return models.Identity{}, fmt.Errorf("invalid CN format: %s", cn)
	}

	switch parts[0] {
	case "ag": // Agent
		if len(parts) != 4 {
			return models.Identity{}, fmt.Errorf("invalid agent CN format: %s (expected: ag::workspace::impl::spec)", cn)
		}
		identity.Type = models.PrincipalAgent
		identity.Workspace = parts[1]
		identity.Implementation = parts[2]
		identity.Specifier = parts[3]

	case "ta": // Task (non-unique, server-assigned ID)
		if len(parts) < 3 || len(parts) > 4 {
			return models.Identity{}, fmt.Errorf("invalid task CN format: %s", cn)
		}
		identity.Type = models.PrincipalTask
		identity.Workspace = parts[1]
		identity.Implementation = parts[2]
		if len(parts) == 4 {
			identity.Specifier = parts[3]
		}

	case "tu": // Task (unique)
		if len(parts) != 4 {
			return models.Identity{}, fmt.Errorf("invalid unique task CN format: %s (expected: tu::workspace::impl::spec)", cn)
		}
		identity.Type = models.PrincipalTask
		identity.Workspace = parts[1]
		identity.Implementation = parts[2]
		identity.Specifier = parts[3]

	case "us": // User
		if len(parts) != 3 {
			return models.Identity{}, fmt.Errorf("invalid user CN format: %s (expected: us::user_id::window_id)", cn)
		}
		identity.Type = models.PrincipalUser
		identity.ID = parts[1]
		identity.Specifier = parts[2]

	case "ga": // Global Agent broadcast
		if len(parts) != 2 {
			return models.Identity{}, fmt.Errorf("invalid global agent CN format: %s", cn)
		}
		identity.Type = models.PrincipalAgent
		identity.Workspace = parts[1]
		identity.Implementation = "*"
		identity.Specifier = "*"

	case "gu": // Global User broadcast
		if len(parts) != 2 {
			return models.Identity{}, fmt.Errorf("invalid global user CN format: %s", cn)
		}
		identity.Type = models.PrincipalUser
		identity.Workspace = parts[1]
		identity.ID = "*"

	case "tb": // Task broadcast (for pool-based workers)
		if len(parts) != 3 {
			return models.Identity{}, fmt.Errorf("invalid task broadcast CN format: %s", cn)
		}
		identity.Type = models.PrincipalTask
		identity.Workspace = parts[1]
		identity.Implementation = parts[2]
		identity.Specifier = "*"

	case "or": // Orchestrator
		if len(parts) < 2 || len(parts) > 3 {
			return models.Identity{}, fmt.Errorf("invalid orchestrator CN format: %s", cn)
		}
		identity.Type = models.PrincipalOrchestrator
		identity.Implementation = parts[1]
		if len(parts) == 3 {
			identity.Specifier = parts[2]
		}

	case "br": // Bridge
		if len(parts) != 3 {
			return models.Identity{}, fmt.Errorf("invalid bridge CN format: %s (expected: br::impl::spec)", cn)
		}
		identity.Type = models.PrincipalBridge
		identity.Implementation = parts[1]
		identity.Specifier = parts[2]

	case "sv": // Service
		if len(parts) != 3 {
			return models.Identity{}, fmt.Errorf("invalid service CN format: %s (expected: sv::impl::spec)", cn)
		}
		identity.Type = models.PrincipalService
		identity.Implementation = parts[1]
		identity.Specifier = parts[2]

	default:
		return models.Identity{}, fmt.Errorf("unknown principal type: %s (expected: ag, ta, tu, us, ga, gu, tb, wf, mb, or, br, sv)", parts[0])
	}

	return identity, nil
}

// ValidateCertificate performs additional certificate validation
func ValidateCertificate(cert *x509.Certificate) error {
	now := time.Now()

	// Check expiration
	if now.Before(cert.NotBefore) {
		return fmt.Errorf("certificate not yet valid (valid from %s)", cert.NotBefore)
	}
	if now.After(cert.NotAfter) {
		return fmt.Errorf("certificate expired (expired on %s)", cert.NotAfter)
	}

	// Check for basic constraints (should be a CA cert or end-entity cert)
	if cert.BasicConstraintsValid && cert.IsCA {
		// This is a CA certificate, may not be suitable for client auth
		return fmt.Errorf("certificate is a CA certificate, not suitable for client authentication")
	}

	return nil
}

// GetPeerCertificate returns the peer certificate from the context
func GetPeerCertificate(ctx context.Context) (*x509.Certificate, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no peer info in context")
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, fmt.Errorf("peer auth info is not TLS")
	}

	if len(tlsInfo.State.PeerCertificates) == 0 {
		return nil, fmt.Errorf("no client certificate")
	}

	return tlsInfo.State.PeerCertificates[0], nil
}

// IsMTLSConnection checks if the connection has TLS with client certificate
func IsMTLSConnection(ctx context.Context) bool {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return false
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return false
	}

	return len(tlsInfo.State.PeerCertificates) > 0
}

// IsAnonymousCert checks if the peer connection has an anonymous mTLS certificate.
// Anonymous certs have CN="_anonymous" and provide transport security without auth identity.
func IsAnonymousCert(ctx context.Context) bool {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return false
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return false
	}
	if len(tlsInfo.State.PeerCertificates) == 0 {
		return false
	}
	return tlsInfo.State.PeerCertificates[0].Subject.CommonName == AnonymousCertCN
}

// ExtractPrincipalTypeFromCertificate extracts only the principal type from a client certificate CN.
// This is used in relaxed mTLS mode where the certificate only confirms the client is an agent, task, user, etc.
// The specific identity details (workspace, impl, specifier, etc.) are provided in InitConnection.
//
// Expected CN formats:
//   - ag.*, ta.*, tu.*, tb.*, ga.* -> PrincipalTask or PrincipalAgent
//   - us.*, gu.* -> PrincipalUser
//   - wf -> PrincipalWorkflowEngine
//   - mb -> PrincipalMetricsBridge
//   - or.* -> PrincipalOrchestrator
//   - sv.* -> PrincipalService
//
// The CN may be partial (e.g., "ag" or "ta::development") - only the principal type prefix is validated.
func ExtractPrincipalTypeFromCertificate(ctx context.Context) (models.PrincipalType, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "no peer info in context")
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "peer auth info is not TLS")
	}

	if len(tlsInfo.State.PeerCertificates) == 0 {
		return "", status.Error(codes.Unauthenticated, "no client certificate")
	}

	cert := tlsInfo.State.PeerCertificates[0]

	// Validate certificate before extracting principal type
	if err := ValidateCertificate(cert); err != nil {
		return "", status.Error(codes.Unauthenticated, fmt.Sprintf("certificate validation failed: %v", err))
	}

	return parsePrincipalTypeFromCN(cert.Subject.CommonName)
}

// parsePrincipalTypeFromCN extracts only the principal type from a CN prefix.
// It handles partial CNs that only specify the type prefix (e.g., "ag", "ta::development").
func parsePrincipalTypeFromCN(cn string) (models.PrincipalType, error) {
	parts := strings.Split(cn, ".")

	if len(parts) == 0 {
		return "", fmt.Errorf("empty CN")
	}

	switch parts[0] {
	case "ag", "ga": // Agent or Global Agent broadcast
		return models.PrincipalAgent, nil
	case "ta", "tu", "tb": // Task (non-unique, unique, or broadcast)
		return models.PrincipalTask, nil
	case "us", "gu": // User or Global User broadcast
		return models.PrincipalUser, nil
	case "wf": // Workflow engine
		return models.PrincipalWorkflowEngine, nil
	case "mb": // Metrics bridge
		return models.PrincipalMetricsBridge, nil
	case "or": // Orchestrator
		return models.PrincipalOrchestrator, nil
	case "br": // Bridge
		return models.PrincipalBridge, nil
	case "sv": // Service
		return models.PrincipalService, nil
	default:
		return "", fmt.Errorf("unknown principal type prefix: %s (expected: ag, ta, tu, tb, us, wf, mb, or, br, sv)", parts[0])
	}
}

// ValidateIdentityAgainstCertificate validates that the identity from InitConnection
// is consistent with the principal type confirmed by the certificate.
// This is used in relaxed mTLS mode.
func ValidateIdentityAgainstCertificate(identity models.Identity, certPrincipalType models.PrincipalType) error {
	if identity.Type != certPrincipalType {
		return fmt.Errorf("identity type mismatch: certificate confirms %s but InitConnection provides %s",
			certPrincipalType, identity.Type)
	}
	return nil
}
