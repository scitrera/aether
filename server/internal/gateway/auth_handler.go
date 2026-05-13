package gateway

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/auth"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/state"
	"github.com/scitrera/aether/internal/tracing"
	"github.com/scitrera/aether/pkg/models"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AuthHandler encapsulates authentication and identity resolution concerns for the gateway.
// It holds the mTLS configuration, ACL service, composite authenticator, and audit logger
// needed to authenticate connections and validate credentials.
type AuthHandler struct {
	authenticator *auth.CompositeAuthenticator
	mtlsRequired  bool
	mtlsMode      MTLSMode
	acl           *acl.Service
	auditLogger   *audit.AuditLogger
	// tokenStore is used to validate orchestration task tokens for agents.
	// It is set when orchestration is configured and may be nil.
	tokenStore state.TokenStore
}

// newAuthHandler creates an AuthHandler from gateway server configuration.
func newAuthHandler(authenticator *auth.CompositeAuthenticator, mtlsRequired bool, mtlsMode MTLSMode, aclService *acl.Service, auditLogger *audit.AuditLogger) *AuthHandler {
	return &AuthHandler{
		authenticator: authenticator,
		mtlsRequired:  mtlsRequired,
		mtlsMode:      mtlsMode,
		acl:           aclService,
		auditLogger:   auditLogger,
	}
}

// auditLog logs an audit event if the audit logger is configured.
func (h *AuthHandler) auditLog(ctx context.Context, event *audit.AuditEvent) {
	if h.auditLogger != nil {
		h.auditLogger.LogEvent(ctx, event)
	}
}

// authenticateMTLS handles mTLS certificate validation based on configuration mode.
// Returns the resolved identity (strict mode only), the certificate principal type
// (relaxed mode only), whether a certificate was present, whether it is anonymous, and any error.
// Anonymous certificates (CN="_anonymous") provide transport security without auth identity.
func (h *AuthHandler) authenticateMTLS(ctx context.Context) (identity models.Identity, certPrincipalType models.PrincipalType, hasCertificate bool, isAnonymous bool, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "gateway.AuthenticateMTLS")
	defer span.End()

	if !IsMTLSConnection(ctx) {
		return identity, certPrincipalType, false, false, nil
	}
	hasCertificate = true

	// Check for anonymous certificate — provides transport security without identity.
	if IsAnonymousCert(ctx) {
		logging.Logger.Info().Msg("anonymous mTLS certificate detected (transport-only)")
		h.auditLog(ctx, audit.NewAuthEvent("anonymous", AnonymousCertCN, audit.OpAuthMTLSSuccess, "", uuid.New(), true, "", map[string]interface{}{
			"anonymous_cert": true,
		}))
		// Return hasCertificate=true but empty identity and principal type.
		// The caller will fall through to InitConnection-based identity resolution.
		return identity, certPrincipalType, true, true, nil
	}

	if h.mtlsMode == MTLSModeStrict {
		// Strict mode: Extract full identity from certificate
		certIdentity, extractErr := ExtractIdentityFromCertificate(ctx)
		if extractErr != nil {
			logging.Logger.Error().Err(extractErr).Msg("mTLS certificate identity extraction failed")
			h.auditLog(ctx, audit.NewAuthEvent("unknown", "unknown", audit.OpAuthMTLSFailure, "", uuid.New(), false, extractErr.Error(), map[string]interface{}{
				"mtls_mode": "strict",
			}))
			return identity, certPrincipalType, true, false, status.Error(codes.Unauthenticated, "invalid client certificate")
		}
		identity = certIdentity
		logging.Logger.Info().Str("identity", identity.String()).Msg("mTLS authenticated identity (strict mode)")
		h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identity.String(), audit.OpAuthMTLSSuccess, identity.Workspace, uuid.New(), true, "", map[string]interface{}{
			"mtls_mode": "strict",
		}))
	} else {
		// Relaxed mode: Extract only principal type from certificate
		principalType, extractErr := ExtractPrincipalTypeFromCertificate(ctx)
		if extractErr != nil {
			logging.Logger.Error().Err(extractErr).Msg("mTLS principal type extraction failed")
			h.auditLog(ctx, audit.NewAuthEvent("unknown", "unknown", audit.OpAuthMTLSFailure, "", uuid.New(), false, extractErr.Error(), map[string]interface{}{
				"mtls_mode": "relaxed",
			}))
			return identity, certPrincipalType, true, false, status.Error(codes.Unauthenticated, "invalid client certificate")
		}
		certPrincipalType = principalType
		logging.Logger.Info().Str("principal_type", string(certPrincipalType)).Msg("mTLS authenticated principal type (relaxed mode)")
		h.auditLog(ctx, audit.NewAuthEvent(string(certPrincipalType), string(certPrincipalType), audit.OpAuthMTLSSuccess, "", uuid.New(), true, "", map[string]interface{}{
			"mtls_mode": "relaxed",
		}))
	}

	return identity, certPrincipalType, hasCertificate, false, nil
}

// resolveConnectionIdentity resolves the client identity from the InitConnection message,
// taking into account mTLS mode, certificate presence, certificate principal type, and whether
// the certificate is anonymous (transport-only, no auth identity).
func (h *AuthHandler) resolveConnectionIdentity(ctx context.Context, init *pb.InitConnection, certIdentity models.Identity, certPrincipalType models.PrincipalType, hasCertificate bool, isAnonymous bool) (models.Identity, error) {
	ctx, span := tracing.Tracer.Start(ctx, "gateway.ResolveIdentity")
	defer span.End()
	span.SetAttributes(
		attribute.String("mtls_mode", string(h.mtlsMode)),
		attribute.Bool("has_certificate", hasCertificate),
		attribute.Bool("is_anonymous_cert", isAnonymous),
	)

	// Anonymous certificate: always use InitConnection identity regardless of mTLS mode.
	// The cert provides transport security but carries no auth identity.
	if isAnonymous {
		identity, err := h.resolveIdentity(init)
		if err != nil {
			h.auditLog(ctx, audit.NewAuthEvent("unknown", "unknown", audit.OpIdentityResolveFailed, "", uuid.New(), false, err.Error(), map[string]interface{}{
				"anonymous_cert": true,
			}))
			return identity, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
		}
		logging.Logger.Info().Str("identity", identity.String()).Msg("using InitConnection identity (anonymous cert)")
		h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identity.String(), audit.OpIdentityResolved, identity.Workspace, uuid.New(), true, "", map[string]interface{}{
			"anonymous_cert": true,
		}))
		return identity, nil
	}

	if h.mtlsMode == MTLSModeStrict {
		// Strict mode with certificate: certificate identity is authoritative
		if hasCertificate {
			logging.Logger.Info().Str("identity", certIdentity.String()).Msg("using certificate identity (strict mode)")
			return certIdentity, nil
		} else if !h.mtlsRequired {
			// No certificate, mTLS not required: use InitConnection identity
			identity, err := h.resolveIdentity(init)
			if err != nil {
				h.auditLog(ctx, audit.NewAuthEvent("unknown", "unknown", audit.OpIdentityResolveFailed, "", uuid.New(), false, err.Error(), map[string]interface{}{
					"mtls_mode": "strict",
					"reason":    "resolve_identity_failed",
				}))
				return identity, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
			}
			logging.Logger.Info().Str("identity", identity.String()).Msg("using InitConnection identity (mTLS not required)")
			h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identity.String(), audit.OpIdentityResolved, identity.Workspace, uuid.New(), true, "", map[string]interface{}{
				"mtls_mode": "strict",
				"mtls_used": false,
			}))
			return identity, nil
		} else {
			// No certificate but mTLS required
			return models.Identity{}, status.Error(codes.Unauthenticated, "mTLS is required")
		}
	}

	// Semi-strict mode: certificate provides workspace + implementation, specifier from InitConnection
	if h.mtlsMode == MTLSModeSemiStrict {
		if hasCertificate {
			// Use certificate identity as the base, but allow InitConnection to provide/override specifier
			initIdentity, err := h.resolveIdentity(init)
			if err != nil {
				h.auditLog(ctx, audit.NewAuthEvent("unknown", "unknown", audit.OpIdentityResolveFailed, "", uuid.New(), false, err.Error(), map[string]interface{}{
					"mtls_mode": "semi-strict",
					"reason":    "resolve_identity_failed",
				}))
				return initIdentity, status.Errorf(codes.InvalidArgument, "invalid identity in InitConnection: %v", err)
			}

			// Validate principal type and workspace + implementation match the certificate
			if initIdentity.Type != certIdentity.Type {
				h.auditLog(ctx, audit.NewAuthEvent(string(initIdentity.Type), initIdentity.String(), audit.OpIdentityResolveFailed, initIdentity.Workspace, uuid.New(), false, "principal type mismatch", map[string]interface{}{
					"mtls_mode":    "semi-strict",
					"reason":       "type_mismatch",
					"cert_type":    string(certIdentity.Type),
					"claimed_type": string(initIdentity.Type),
				}))
				return initIdentity, status.Errorf(codes.PermissionDenied, "principal type mismatch: cert=%s, claimed=%s", certIdentity.Type, initIdentity.Type)
			}
			if initIdentity.Workspace != certIdentity.Workspace {
				h.auditLog(ctx, audit.NewAuthEvent(string(initIdentity.Type), initIdentity.String(), audit.OpIdentityResolveFailed, initIdentity.Workspace, uuid.New(), false, "workspace mismatch", map[string]interface{}{
					"mtls_mode":         "semi-strict",
					"reason":            "workspace_mismatch",
					"cert_workspace":    certIdentity.Workspace,
					"claimed_workspace": initIdentity.Workspace,
				}))
				return initIdentity, status.Errorf(codes.PermissionDenied, "workspace mismatch: cert=%s, claimed=%s", certIdentity.Workspace, initIdentity.Workspace)
			}
			if initIdentity.Implementation != certIdentity.Implementation {
				h.auditLog(ctx, audit.NewAuthEvent(string(initIdentity.Type), initIdentity.String(), audit.OpIdentityResolveFailed, initIdentity.Workspace, uuid.New(), false, "implementation mismatch", map[string]interface{}{
					"mtls_mode":    "semi-strict",
					"reason":       "impl_mismatch",
					"cert_impl":    certIdentity.Implementation,
					"claimed_impl": initIdentity.Implementation,
				}))
				return initIdentity, status.Errorf(codes.PermissionDenied, "implementation mismatch: cert=%s, claimed=%s", certIdentity.Implementation, initIdentity.Implementation)
			}

			// Specifier may differ — that's the whole point of semi-strict mode.
			// Use the InitConnection identity (which has the caller's chosen specifier).
			logging.Logger.Info().
				Str("identity", initIdentity.String()).
				Str("cert_specifier", certIdentity.Specifier).
				Msg("using InitConnection identity with cert-validated workspace+impl (semi-strict mode)")
			h.auditLog(ctx, audit.NewAuthEvent(string(initIdentity.Type), initIdentity.String(), audit.OpIdentityResolved, initIdentity.Workspace, uuid.New(), true, "", map[string]interface{}{
				"mtls_mode":      "semi-strict",
				"cert_specifier": certIdentity.Specifier,
				"init_specifier": initIdentity.Specifier,
			}))
			return initIdentity, nil
		} else if !h.mtlsRequired {
			// No certificate, mTLS not required: use InitConnection identity
			identity, err := h.resolveIdentity(init)
			if err != nil {
				return identity, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
			}
			return identity, nil
		} else {
			return models.Identity{}, status.Error(codes.Unauthenticated, "mTLS is required")
		}
	}

	// Relaxed mode: certificate only confirms principal type
	if hasCertificate {
		// Certificate provided: validate InitConnection against certificate principal type
		initIdentity, err := h.resolveIdentity(init)
		if err != nil {
			h.auditLog(ctx, audit.NewAuthEvent("unknown", "unknown", audit.OpIdentityResolveFailed, "", uuid.New(), false, err.Error(), map[string]interface{}{
				"mtls_mode": "relaxed",
				"reason":    "resolve_identity_failed",
			}))
			return initIdentity, status.Errorf(codes.InvalidArgument, "invalid identity in InitConnection: %v", err)
		}

		// Validate that InitConnection principal type matches certificate
		if err := ValidateIdentityAgainstCertificate(initIdentity, certPrincipalType); err != nil {
			logging.Logger.Warn().Err(err).Msg("identity validation failed against certificate")
			h.auditLog(ctx, audit.NewAuthEvent(string(initIdentity.Type), initIdentity.String(), audit.OpIdentityResolveFailed, initIdentity.Workspace, uuid.New(), false, err.Error(), map[string]interface{}{
				"mtls_mode":        "relaxed",
				"reason":           "identity_mismatch",
				"cert_principal":   string(certPrincipalType),
				"claimed_identity": initIdentity.String(),
			}))
			return initIdentity, status.Errorf(codes.PermissionDenied, "identity mismatch: %v", err)
		}

		logging.Logger.Info().Str("identity", initIdentity.String()).Msg("InitConnection validated against certificate (relaxed mode)")
		h.auditLog(ctx, audit.NewAuthEvent(string(initIdentity.Type), initIdentity.String(), audit.OpIdentityResolved, initIdentity.Workspace, uuid.New(), true, "", map[string]interface{}{
			"mtls_mode":      "relaxed",
			"cert_principal": string(certPrincipalType),
		}))
		return initIdentity, nil
	}

	// No certificate provided
	if h.mtlsRequired {
		return models.Identity{}, status.Error(codes.Unauthenticated, "mTLS is required but no client certificate provided")
	}
	// mTLS not required: use InitConnection identity directly
	identity, err := h.resolveIdentity(init)
	if err != nil {
		h.auditLog(ctx, audit.NewAuthEvent("unknown", "unknown", audit.OpIdentityResolveFailed, "", uuid.New(), false, err.Error(), map[string]interface{}{
			"mtls_mode": "relaxed",
			"reason":    "resolve_identity_failed",
		}))
		return identity, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}
	logging.Logger.Info().Str("identity", identity.String()).Msg("using InitConnection identity (mTLS not required)")
	h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identity.String(), audit.OpIdentityResolved, identity.Workspace, uuid.New(), true, "", map[string]interface{}{
		"mtls_mode": "relaxed",
		"mtls_used": false,
	}))
	return identity, nil
}

// authenticateCredentials validates task tokens and API key/OAuth credentials.
// Returns the associated task ID (if any) and potentially updated identity.
func (h *AuthHandler) authenticateCredentials(ctx context.Context, init *pb.InitConnection, identity models.Identity, hasCertificate bool) (string, models.Identity, error) {
	ctx, span := tracing.Tracer.Start(ctx, "gateway.AuthenticateCredentials")
	defer span.End()
	span.SetAttributes(attribute.Bool("has_certificate", hasCertificate))

	// DIAG: surface exactly what the gateway sees for credentials so we can
	// trace where auth takes each path.
	if init != nil {
		credKeys := make([]string, 0, len(init.Credentials))
		for k := range init.Credentials {
			credKeys = append(credKeys, k)
		}
		tokenLen := 0
		if t, ok := init.Credentials["token"]; ok {
			tokenLen = len(t)
		}
		logging.Logger.Debug().
			Str("identity", identity.String()).
			Str("identity_type", string(identity.Type)).
			Interface("cred_keys", credKeys).
			Int("token_len", tokenLen).
			Bool("has_certificate", hasCertificate).
			Bool("token_store_configured", h.tokenStore != nil).
			Msg("[DIAG] authenticateCredentials entry")
	}

	// 2.5 Token validation for orchestrated workers
	//
	// Originally gated on PrincipalAgent because that was the only worker
	// shape that connected with a task token. The lift of sandbox-sidecar
	// from Agent to Service made Services equally likely to present one;
	// gating by principal type meant Service-shaped lease tasks
	// authenticated cleanly but never had their associatedTaskID set, so
	// connect.go's "transition assigned → running" path (line 187) never
	// fired. The token's own TargetIdentity == identity.String() check
	// below is the actual security gate — the principal-type filter was
	// belt-and-suspenders that excluded a legitimate worker shape.
	var associatedTaskID string
	if init != nil {
		if token, ok := init.Credentials["token"]; ok && token != "" {
			if h.tokenStore != nil {
				taskToken, err := h.tokenStore.ValidateToken(ctx, token)
				if err != nil {
					logging.Logger.Warn().Err(err).Str("identity", identity.String()).Msg("token validation failed")
					h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identity.String(), audit.OpAuthTokenValidation, identity.Workspace, uuid.New(), false, err.Error(), map[string]interface{}{
						"reason": "token_validation_failed",
					}))
					return "", identity, status.Errorf(codes.Unauthenticated, "invalid or revoked token: %v", err)
				}

				// Verify the connecting identity matches the token's target
				if taskToken.TargetIdentity != identity.String() {
					logging.Logger.Warn().Str("token_target", taskToken.TargetIdentity).Str("connecting_as", identity.String()).Msg("token identity mismatch")
					h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identity.String(), audit.OpAuthTokenValidation, identity.Workspace, uuid.New(), false, "token identity mismatch", map[string]interface{}{
						"reason":          "token_identity_mismatch",
						"token_target":    taskToken.TargetIdentity,
						"connecting_as":   identity.String(),
						"associated_task": taskToken.TaskID,
					}))
					return "", identity, status.Errorf(codes.PermissionDenied,
						"token issued for %s, not %s",
						taskToken.TargetIdentity,
						identity.String())
				}

				logging.Logger.Info().Str("identity", identity.String()).Str("task_id", taskToken.TaskID).Str("principal_type", string(identity.Type)).Msg("task token validated")
				h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identity.String(), audit.OpAuthTokenValidation, identity.Workspace, uuid.New(), true, "", map[string]interface{}{
					"associated_task": taskToken.TaskID,
				}))
				associatedTaskID = taskToken.TaskID
			}
		}
	}

	// 2.6 API key / OAuth authentication via composite authenticator
	if h.authenticator != nil && init != nil && associatedTaskID == "" {
		authResult, authErr := h.authenticator.Authenticate(ctx, init.Credentials)
		if authErr != nil {
			logging.Logger.Warn().Err(authErr).Str("identity", identity.String()).Msg("credential authentication failed")
			h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identity.String(), audit.OpAuthTokenValidation, identity.Workspace, uuid.New(), false, authErr.Error(), map[string]interface{}{
				"reason": "credential_auth_failed",
				"method": "composite",
			}))
			return "", identity, status.Errorf(codes.Unauthenticated, "credential authentication failed: %v", authErr)
		}
		if authResult != nil && authResult.Authenticated {
			if !hasCertificate {
				if authResult.Method == "oauth" {
					identity = authResult.Identity
				}
				if authResult.Method == "api_key" {
					if identity.Type != authResult.Identity.Type && authResult.Identity.Type != "" {
						logging.Logger.Warn().Str("api_key_type", string(authResult.Identity.Type)).Str("init_type", string(identity.Type)).Msg("API key principal type doesn't match InitConnection type")
					}
					if wsPatterns, ok := authResult.Metadata["workspace_patterns"].([]string); ok {
						if identity.Workspace != "" {
							matched := false
							for _, pattern := range wsPatterns {
								if pattern == "*" {
									matched = true
									break
								}
								if m, _ := filepath.Match(pattern, identity.Workspace); m {
									matched = true
									break
								}
							}
							if !matched && len(wsPatterns) > 0 {
								return "", identity, status.Errorf(codes.PermissionDenied,
									"API key not authorized for workspace %s", identity.Workspace)
							}
						}
					}
				}
			}
			logging.Logger.Info().Str("method", authResult.Method).Str("identity", identity.String()).Msg("credential auth succeeded")
			h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identity.String(), audit.OpAuthTokenValidation, identity.Workspace, uuid.New(), true, "", map[string]interface{}{
				"auth_method": authResult.Method,
				"metadata":    authResult.Metadata,
			}))
		}
	}

	return associatedTaskID, identity, nil
}

// resolveIdentity resolves the client identity from the InitConnection message.
func (h *AuthHandler) resolveIdentity(init *pb.InitConnection) (models.Identity, error) {
	var ident models.Identity
	switch t := init.ClientType.(type) {
	case *pb.InitConnection_Agent:
		ident = models.Identity{
			Type:           models.PrincipalAgent,
			Workspace:      t.Agent.Workspace,
			Implementation: t.Agent.Implementation,
			Specifier:      t.Agent.Specifier,
		}
	case *pb.InitConnection_Task:
		ident = models.Identity{
			Type:           models.PrincipalTask,
			Workspace:      t.Task.Workspace,
			Implementation: t.Task.Implementation,
			Specifier:      t.Task.UniqueSpecifier,
		}
		if ident.Specifier == "" {
			ident.ID = uuid.New().String() // Non-unique task gets a generated ID
		}
	case *pb.InitConnection_User:
		ident = models.Identity{
			Type:      models.PrincipalUser,
			ID:        t.User.UserId,
			Specifier: t.User.WindowId,
		}
	case *pb.InitConnection_Orchestrator:
		specifier := t.Orchestrator.Specifier
		if specifier == "" {
			specifier = uuid.New().String()[:8]
		}
		ident = models.Identity{
			Type:           models.PrincipalOrchestrator,
			Implementation: t.Orchestrator.Implementation,
			Specifier:      specifier,
		}
	case *pb.InitConnection_WorkflowEngine:
		if t.WorkflowEngine != nil {
			ident = models.Identity{Type: models.PrincipalWorkflowEngine}
		}
	case *pb.InitConnection_MetricsBridge:
		if t.MetricsBridge != nil {
			ident = models.Identity{Type: models.PrincipalMetricsBridge}
		}
	case *pb.InitConnection_Bridge:
		ident = models.Identity{
			Type:           models.PrincipalBridge,
			Implementation: t.Bridge.Implementation,
			Specifier:      t.Bridge.Specifier,
		}
	case *pb.InitConnection_Service:
		ident = models.Identity{
			Type:           models.PrincipalService,
			Implementation: t.Service.Implementation,
			Specifier:      t.Service.Specifier,
		}
	}

	if ident.Type == "" {
		return ident, fmt.Errorf("unknown principal type")
	}

	// Validate that no segment contains the reserved "::" separator. This is the
	// boundary: downstream topic builders (AgentTopic, UserWindowTopic, etc.) and
	// the MustXxx wrappers assume identities have already passed this check, so
	// rejecting bad input here keeps panics out of the hot path.
	if _, terr := ident.ToTopicErr(); terr != nil {
		return models.Identity{}, fmt.Errorf("invalid identity segment: %w", terr)
	}

	return ident, nil
}
