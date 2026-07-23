package gateway

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/internal/audit"
	"github.com/scitrera/aether/internal/auth"
	"github.com/scitrera/aether/internal/identval"
	"github.com/scitrera/aether/internal/logging"
	"github.com/scitrera/aether/internal/state"
	aclstore "github.com/scitrera/aether/internal/storage/acl"
	auditstore "github.com/scitrera/aether/internal/storage/audit"
	"github.com/scitrera/aether/internal/tracing"
	"github.com/scitrera/aether/pkg/models"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Per-identity auth-failure throttle tunables. These gate repeated credential
// validation for a single identity to relieve log/audit spam and validation
// load when a stale/zombie client reconnects in a tight loop with a token the
// gateway no longer recognizes. They do not alter the actual token-validation
// or identity-match security logic.
const (
	// authFailThreshold is the number of failures within authFailWindow that
	// engages the cooldown.
	authFailThreshold = 5
	// authFailWindow is the sliding window over which failures are counted.
	authFailWindow = 30 * time.Second
	// authFailCooldown is how long an identity is throttled (fast-rejected
	// without re-validation) once the threshold is reached.
	authFailCooldown = 30 * time.Second
	// authFailMapMaxEntries caps the throttle map size to bound memory across
	// many transient identities; on overflow, stale records are swept.
	authFailMapMaxEntries = 1024
)

// authFailRecord tracks recent auth failures for a single identity.
type authFailRecord struct {
	failures      int
	windowStart   time.Time
	cooldownUntil time.Time
}

// AuthHandler encapsulates authentication and identity resolution concerns for the gateway.
// It holds the mTLS configuration, ACL service, composite authenticator, and audit logger
// needed to authenticate connections and validate credentials.
type AuthHandler struct {
	authenticator *auth.CompositeAuthenticator
	mtlsRequired  bool
	mtlsMode      MTLSMode
	// acl is the ACL domain Store (internal/storage/acl).
	acl aclstore.Store
	// auditLogger is the audit domain Store (internal/storage/audit).
	auditLogger auditstore.Store
	// tokenStore is used to validate orchestration task tokens for agents.
	// It is set when orchestration is configured and may be nil.
	tokenStore state.TokenStore

	// authFailMu guards authFailRecords. The gateway may authenticate
	// concurrent connects, so the per-identity throttle state must be locked.
	authFailMu sync.Mutex
	// authFailRecords holds per-identity (keyed by identity.String()) failure
	// bookkeeping for the auth-failure throttle.
	authFailRecords map[string]*authFailRecord

	// devMode relaxes the anonymous/no-cert credential requirement for
	// impersonable principals (Agent/Service/Task/User/Bridge): when true, such
	// a connection is admitted WITHOUT a credential, with a warning. Set from
	// AETHER_DEV_MODE (the -dev flag) and NEVER true in production. This lets
	// local/e2e sidecars connect to a -dev gateway that has no authenticator
	// configured; production (dev mode off) still enforces credentials.
	devMode bool
}

// newAuthHandler creates an AuthHandler from gateway server configuration.
func newAuthHandler(authenticator *auth.CompositeAuthenticator, mtlsRequired bool, mtlsMode MTLSMode, aclService aclstore.Store, auditLogger auditstore.Store) *AuthHandler {
	return &AuthHandler{
		authenticator:   authenticator,
		mtlsRequired:    mtlsRequired,
		mtlsMode:        mtlsMode,
		acl:             aclService,
		auditLogger:     auditLogger,
		authFailRecords: make(map[string]*authFailRecord),
		devMode:         os.Getenv("AETHER_DEV_MODE") == "true",
	}
}

// throttled reports whether the given identity is currently within its
// auth-failure cooldown. When true, callers should fast-reject without
// re-validating credentials and without emitting per-attempt WARNING/audit.
func (h *AuthHandler) throttled(identityKey string, now time.Time) bool {
	h.authFailMu.Lock()
	defer h.authFailMu.Unlock()
	rec, ok := h.authFailRecords[identityKey]
	if !ok {
		return false
	}
	return now.Before(rec.cooldownUntil)
}

// recordAuthFailure records a credential-validation failure for the identity
// and returns true if this failure engaged (newly crossed) the cooldown
// threshold. The window is sliding: failures older than authFailWindow reset
// the count. The caller logs the single throttle-engaged WARNING/audit when
// this returns true.
func (h *AuthHandler) recordAuthFailure(identityKey string, now time.Time) (engaged bool) {
	h.authFailMu.Lock()
	defer h.authFailMu.Unlock()

	h.evictStaleLocked(now)

	rec, ok := h.authFailRecords[identityKey]
	if !ok {
		rec = &authFailRecord{}
		h.authFailRecords[identityKey] = rec
	}

	if rec.windowStart.IsZero() || now.Sub(rec.windowStart) > authFailWindow {
		rec.failures = 1
		rec.windowStart = now
	} else {
		rec.failures++
	}

	if rec.failures >= authFailThreshold && now.After(rec.cooldownUntil) {
		rec.cooldownUntil = now.Add(authFailCooldown)
		return true
	}
	return false
}

// clearAuthFailure removes any throttle bookkeeping for the identity so a
// recovered client is not penalized after a successful authentication.
func (h *AuthHandler) clearAuthFailure(identityKey string) {
	h.authFailMu.Lock()
	defer h.authFailMu.Unlock()
	delete(h.authFailRecords, identityKey)
}

// evictStaleLocked opportunistically removes records whose window and cooldown
// are both well in the past, bounding memory across many transient identities.
// Callers must hold authFailMu. The sweep only runs when the map exceeds the
// size cap, so the common case adds no work.
func (h *AuthHandler) evictStaleLocked(now time.Time) {
	if len(h.authFailRecords) < authFailMapMaxEntries {
		return
	}
	// A record is stale once it is no longer in cooldown and its window has
	// fully elapsed; such records carry no live throttle state.
	staleBefore := now.Add(-authFailWindow)
	for k, rec := range h.authFailRecords {
		if now.After(rec.cooldownUntil) && rec.windowStart.Before(staleBefore) {
			delete(h.authFailRecords, k)
		}
	}
}

// auditLog logs an audit event if the audit logger is configured.
func (h *AuthHandler) auditLog(ctx context.Context, event *audit.AuditEvent) {
	// audit.AuditEvent is aliased as auditstore.Event by the new interface package,
	// so the same legacy-type pointer satisfies both signatures.
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

	// In-process bufconn connection (AetherLite embedded workflow engine).
	// Mirrors the anonymous-mTLS path below: returns hasCertificate=true,
	// isAnonymous=true so resolveConnectionIdentity falls through to
	// InitConnection-based identity resolution and the mtlsRequired check
	// in connect.go is satisfied without requiring a transport cert. The
	// bytes never leave the process so transport identity adds nothing.
	if IsInProcessConn(ctx) {
		logging.Logger.Info().Msg("in-process gRPC connection detected (bufconn, transport-trust-only)")
		h.auditLog(ctx, audit.NewAuthEvent("in_process", "in_process", audit.OpAuthMTLSSuccess, "", uuid.New(), true, "", map[string]interface{}{
			"in_process_conn": true,
		}))
		return identity, certPrincipalType, true, true, nil
	}

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

	if h.mtlsMode == MTLSModeStrict || h.mtlsMode == MTLSModeSemiStrict {
		// Strict and semi-strict modes: extract the FULL identity from the
		// certificate. resolveConnectionIdentity consumes certIdentity wholesale
		// in strict mode, and uses the cert's workspace+implementation+type (with
		// the InitConnection-supplied specifier) in semi-strict mode. The cert
		// identity MUST be populated for the semi-strict validation block to have
		// real values to compare against.
		certIdentity, extractErr := ExtractIdentityFromCertificate(ctx)
		if extractErr != nil {
			logging.Logger.Error().Err(extractErr).Msg("mTLS certificate identity extraction failed")
			h.auditLog(ctx, audit.NewAuthEvent("unknown", "unknown", audit.OpAuthMTLSFailure, "", uuid.New(), false, extractErr.Error(), map[string]interface{}{
				"mtls_mode": string(h.mtlsMode),
			}))
			return identity, certPrincipalType, true, false, status.Error(codes.Unauthenticated, "invalid client certificate")
		}
		identity = certIdentity
		if h.mtlsMode == MTLSModeSemiStrict {
			logging.Logger.Info().Str("identity", identity.String()).Msg("mTLS authenticated identity (semi-strict mode)")
			h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identity.String(), audit.OpAuthMTLSSuccess, identity.Workspace, uuid.New(), true, "", map[string]interface{}{
				"mtls_mode": "semi-strict",
			}))
		} else {
			logging.Logger.Info().Str("identity", identity.String()).Msg("mTLS authenticated identity (strict mode)")
			h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identity.String(), audit.OpAuthMTLSSuccess, identity.Workspace, uuid.New(), true, "", map[string]interface{}{
				"mtls_mode": "strict",
			}))
		}
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

// isImpersonablePrincipal reports whether a principal type requires an explicit
// credential on the anonymous-cert / no-cert path. System principals
// (WorkflowEngine, Orchestrator, MetricsBridge) connect without per-user
// credentials by design; they present named mTLS certs in production and are
// granted in-process trust in lite. Every other type (User, Agent, Service,
// Task, Bridge) is "impersonable" — a caller on an unauthenticated transport
// MUST prove they hold the claimed identity via a matching credential.
func isImpersonablePrincipal(t models.PrincipalType) bool {
	switch t {
	case models.PrincipalWorkflowEngine, models.PrincipalOrchestrator, models.PrincipalMetricsBridge:
		return false
	default:
		// User, Agent, Service, Task, Bridge — all require a credential on
		// the anonymous/no-cert path.
		return t != ""
	}
}

// authenticateCredentials validates task tokens and API key/OAuth credentials.
// Returns the associated task ID (if any) and potentially updated identity.
//
// isAnonymous indicates the transport presented an ANONYMOUS mTLS certificate
// (CN="_anonymous", or an in-process bufconn). The anonymous cert provides
// transport security but carries NO auth identity, so — exactly like the
// no-certificate path — the api_key/oauth principal block must run to bind the
// authenticated principal. The strict/semi-strict cert paths (non-anonymous
// cert: isAnonymous=false, hasCertificate=true) deliberately skip that block:
// their identity is already authoritatively bound by the certificate.
//
// SECURITY: on the external anonymous-cert path, impersonable principal types
// MUST present a matching credential. Returning success with an unauthenticated
// identity would leave authorization entirely to the downstream ACL fallback,
// which may be permissive in development/lite deployments.
func (h *AuthHandler) authenticateCredentials(ctx context.Context, init *pb.InitConnection, identity models.Identity, hasCertificate bool, isAnonymous bool) (string, models.Identity, error) {
	ctx, span := tracing.Tracer.Start(ctx, "gateway.AuthenticateCredentials")
	defer span.End()
	span.SetAttributes(
		attribute.Bool("has_certificate", hasCertificate),
		attribute.Bool("is_anonymous_cert", isAnonymous),
	)

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
				identityKey := identity.String()

				// Defense-in-depth throttle: if this identity has recently
				// failed token validation repeatedly, fast-reject without
				// re-validating and without per-attempt WARNING/audit spam.
				// This relieves load from stale/zombie clients reconnecting in
				// a tight loop with a token the gateway no longer knows.
				if h.throttled(identityKey, time.Now()) {
					logging.Logger.Debug().Str("identity", identityKey).Msg("auth throttled: skipping token validation during cooldown")
					return "", identity, status.Errorf(codes.Unauthenticated, "auth temporarily throttled after repeated failures")
				}

				taskToken, err := h.tokenStore.ValidateToken(ctx, token)
				if err != nil {
					engaged := h.recordAuthFailure(identityKey, time.Now())
					if engaged {
						logging.Logger.Warn().Err(err).Str("identity", identityKey).Int("failures", authFailThreshold).Dur("cooldown", authFailCooldown).Msg("auth throttled for identity after repeated failures, cooling down")
						h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identityKey, audit.OpAuthTokenValidation, identity.Workspace, uuid.New(), false, err.Error(), map[string]interface{}{
							"reason":        "auth_throttle_engaged",
							"failures":      authFailThreshold,
							"cooldown_secs": int(authFailCooldown.Seconds()),
						}))
					} else {
						logging.Logger.Warn().Err(err).Str("identity", identityKey).Msg("token validation failed")
						h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identityKey, audit.OpAuthTokenValidation, identity.Workspace, uuid.New(), false, err.Error(), map[string]interface{}{
							"reason": "token_validation_failed",
						}))
					}
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

				// Token validated OK: a recovered client must not stay
				// penalized — clear any throttle bookkeeping for this identity.
				h.clearAuthFailure(identityKey)

				logging.Logger.Info().Str("identity", identity.String()).Str("task_id", taskToken.TaskID).Str("principal_type", string(identity.Type)).Msg("task token validated")
				h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identity.String(), audit.OpAuthTokenValidation, identity.Workspace, uuid.New(), true, "", map[string]interface{}{
					"associated_task": taskToken.TaskID,
				}))
				associatedTaskID = taskToken.TaskID
			}
		}
	}

	// 2.6 API key / OAuth authentication via composite authenticator
	//
	// The credential-requirement gate (Fix #1) and the composite call share
	// the same per-identity throttle as the task-token path so that a flood
	// of failed bind attempts from attacker-controlled anonymous connections
	// does not drive unbounded log/audit writes.
	if h.authenticator != nil && init != nil && associatedTaskID == "" {
		identityKey := identity.String()

		// Check throttle before calling the authenticator, just as the
		// task-token path does. A connection presenting a bad api_key in a
		// tight loop must not drive unbounded authenticator calls.
		if h.throttled(identityKey, time.Now()) {
			logging.Logger.Debug().Str("identity", identityKey).Msg("auth throttled: skipping composite auth during cooldown")
			return "", identity, status.Errorf(codes.Unauthenticated, "auth temporarily throttled after repeated failures")
		}

		authResult, authErr := h.authenticator.Authenticate(ctx, init.Credentials)
		if authErr != nil {
			engaged := h.recordAuthFailure(identityKey, time.Now())
			logging.Logger.Warn().Err(authErr).Str("identity", identityKey).Msg("credential authentication failed")
			h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identityKey, audit.OpAuthTokenValidation, identity.Workspace, uuid.New(), false, authErr.Error(), map[string]interface{}{
				"reason":       "credential_auth_failed",
				"method":       "composite",
				"throttle_new": engaged,
			}))
			return "", identity, status.Errorf(codes.Unauthenticated, "credential authentication failed: %v", authErr)
		}
		if authResult != nil && authResult.Authenticated {
			// The principal-binding block runs when the transport carries no
			// auth identity: either no certificate at all, or an ANONYMOUS
			// certificate (transport-only). On the strict/semi-strict cert
			// path (non-anonymous cert) the identity is already authoritatively
			// bound by the certificate, so we deliberately skip rebinding here.
			// The no-cert and anonymous-cert paths share the SAME enforcement
			// below — there is no security divergence between them.
			if !hasCertificate || isAnonymous {
				if authResult.Method == "oauth" {
					identity = authResult.Identity
				}
				if authResult.Method == "api_key" {
					// SECURITY: the API key authenticates a concrete principal
					// (Type, ID) = (token.PrincipalType, token.CreatedBy). The
					// InitConnection carries only a CLAIM. If the claim names a
					// principal Type/ID, it MUST equal the key's — otherwise a
					// caller could present user:alice's key while claiming to be
					// user:drew. A mismatch is a HARD FAIL (PermissionDenied).
					//
					// The binding is ALWAYS from the key — never from the claim.
					// Even when keyID is empty (a key with no created_by), we set
					// identity.ID = keyID so the claim can never silently supply
					// an ID the key does not authenticate. Such keys should be
					// rejected at mint time (see token_handler.go), but the
					// binding enforces it defensively here too.
					keyType := authResult.Identity.Type
					keyID := authResult.Identity.ID
					tokenID, _ := authResult.Metadata["token_id"].(string)

					if identity.Type != "" && keyType != "" && identity.Type != keyType {
						engaged := h.recordAuthFailure(identityKey, time.Now())
						logging.Logger.Warn().
							Str("key_type", string(keyType)).
							Str("key_id", keyID).
							Str("token_id", tokenID).
							Str("claimed_type", string(identity.Type)).
							Str("claimed_identity", identity.String()).
							Bool("throttle_new", engaged).
							Msg("API key principal type does not match InitConnection claim")
						h.auditLog(ctx, audit.NewAuthEvent(string(keyType), keyID, audit.OpAuthTokenValidation, identity.Workspace, uuid.New(), false, "api key principal type mismatch", map[string]interface{}{
							"reason":           "api_key_principal_type_mismatch",
							"key_type":         string(keyType),
							"key_id":           keyID,
							"token_id":         tokenID,
							"claimed_type":     string(identity.Type),
							"claimed_identity": identity.String(),
						}))
						return "", identity, status.Errorf(codes.PermissionDenied,
							"API key principal type mismatch: key=%s, claimed=%s", keyType, identity.Type)
					}
					// keyID != claim check: no guard on keyID == "" so a key
					// with empty created_by can never be bypassed by an empty claim.
					if identity.ID != "" && identity.ID != keyID {
						engaged := h.recordAuthFailure(identityKey, time.Now())
						logging.Logger.Warn().
							Str("key_type", string(keyType)).
							Str("key_id", keyID).
							Str("token_id", tokenID).
							Str("claimed_id", identity.ID).
							Str("claimed_identity", identity.String()).
							Bool("throttle_new", engaged).
							Msg("API key principal id does not match InitConnection claim")
						h.auditLog(ctx, audit.NewAuthEvent(string(keyType), keyID, audit.OpAuthTokenValidation, identity.Workspace, uuid.New(), false, "api key principal id mismatch", map[string]interface{}{
							"reason":           "api_key_principal_id_mismatch",
							"key_type":         string(keyType),
							"key_id":           keyID,
							"token_id":         tokenID,
							"claimed_id":       identity.ID,
							"claimed_identity": identity.String(),
						}))
						return "", identity, status.Errorf(codes.PermissionDenied,
							"API key principal id mismatch: key=%s, claimed=%s", keyID, identity.ID)
					}

					// Claim accepted: bind the AUTHENTICATED principal from the
					// key. Type + ID always come from the key (authenticated
					// facts); Specifier/window + Workspace are preserved from
					// the claim (the key does not carry those).
					if keyType != "" {
						identity.Type = keyType
					}
					// Always adopt keyID — even when empty — so the claim can
					// never supply an ID that the key doesn't authenticate.
					identity.ID = keyID

					// On a successful bind, clear any prior throttle state so a
					// legitimate reconnect is not penalized.
					h.clearAuthFailure(identityKey)

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
						// NOTE(security): user identities carry an empty
						// Workspace at connect time (the workspace is selected
						// later via SwitchWorkspace), so this workspace_patterns
						// check is a no-op for user connections. The key's
						// workspace_patterns are NOT yet enforced at the point a
						// user connection binds a workspace. See the TODO at
						// handleSwitchWorkspace / checkConnection for the
						// deferred enforcement point.
					}
				}
			}
			logging.Logger.Info().Str("method", authResult.Method).Str("identity", identity.String()).Msg("credential auth succeeded")
			h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identity.String(), audit.OpAuthTokenValidation, identity.Workspace, uuid.New(), true, "", map[string]interface{}{
				"auth_method": authResult.Method,
				"metadata":    authResult.Metadata,
			}))
		} else if authResult == nil {
			// Composite returned (nil, nil): no authenticator recognised the
			// credentials. On the external anonymous-cert / no-cert path, an
			// impersonable principal that presented NO recognised credential is
			// unauthenticated. Fail closed.
			//
			// Exemptions (must NOT fail here):
			//   (a) associatedTaskID != "" — already handled by task-token path above.
			//   (b) IsInProcessConn — in-process bufconn connections are trusted
			//       at the transport layer; they do not carry api-key credentials.
			//   (c) Non-anonymous cert path — cert is the authority, no api-key needed.
			//   (d) Non-impersonable principals (WorkflowEngine, Orchestrator,
			//       MetricsBridge) — system principals connect without per-user keys.
			needsCredential := (!hasCertificate || isAnonymous) &&
				!IsInProcessConn(ctx) &&
				isImpersonablePrincipal(identity.Type)
			if needsCredential && h.devMode {
				logging.Logger.Warn().
					Str("identity", identityKey).
					Str("claimed_type", string(identity.Type)).
					Msg("dev mode (AETHER_DEV_MODE): admitting anonymous impersonable principal without a credential — NOT FOR PRODUCTION")
				needsCredential = false
			}
			if needsCredential {
				engaged := h.recordAuthFailure(identityKey, time.Now())
				logging.Logger.Warn().
					Str("identity", identityKey).
					Bool("throttle_new", engaged).
					Msg("anonymous/no-cert connection: impersonable principal presented no valid credential")
				h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identityKey, audit.OpAuthTokenValidation, identity.Workspace, uuid.New(), false, "no credential presented for impersonable principal", map[string]interface{}{
					"reason":       "no_credential_for_impersonable",
					"claimed_type": string(identity.Type),
					"is_anonymous": isAnonymous,
					"has_cert":     hasCertificate,
					"throttle_new": engaged,
				}))
				return "", identity, status.Errorf(codes.Unauthenticated,
					"anonymous connection as %s requires a matching API key credential", identity.Type)
			}
		}
	}

	// SECURITY: credential-required gate for the case where there is NO
	// composite authenticator configured at all (h.authenticator == nil).
	// Without this, an external anonymous-cert connection claiming an
	// impersonable principal would succeed with no auth check at all.
	// In-process (bufconn) connections and task-token-authenticated workers
	// are exempt — see the analogous exemptions in the authResult==nil block
	// above.
	if h.authenticator == nil && associatedTaskID == "" &&
		(!hasCertificate || isAnonymous) &&
		!IsInProcessConn(ctx) &&
		isImpersonablePrincipal(identity.Type) {
		if h.devMode {
			logging.Logger.Warn().
				Str("identity", identity.String()).
				Str("claimed_type", string(identity.Type)).
				Msg("dev mode (AETHER_DEV_MODE): admitting anonymous impersonable principal with no authenticator configured — NOT FOR PRODUCTION")
		} else {
			logging.Logger.Warn().
				Str("identity", identity.String()).
				Msg("anonymous/no-cert connection: impersonable principal with no authenticator configured")
			h.auditLog(ctx, audit.NewAuthEvent(string(identity.Type), identity.String(), audit.OpAuthTokenValidation, identity.Workspace, uuid.New(), false, "no authenticator configured for impersonable principal on anonymous path", map[string]interface{}{
				"reason":       "no_authenticator_for_impersonable",
				"claimed_type": string(identity.Type),
				"is_anonymous": isAnonymous,
				"has_cert":     hasCertificate,
			}))
			return "", identity, status.Errorf(codes.Unauthenticated,
				"anonymous connection as %s requires API key auth but no authenticator is configured", identity.Type)
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

	// Validate identifier charset at the ingestion boundary. This catches '*',
	// '>', whitespace, control characters, '::' substrings, and oversized tokens
	// before any persistent write or topic construction occurs.
	// Workspace and specifier use ValidateToken; implementation uses ValidateImpl
	// which permits '.' for reverse-DNS names (e.g. "com.example.chat-agent").
	if ident.Workspace != "" {
		if err := identval.ValidateToken(ident.Workspace, "workspace"); err != nil {
			return models.Identity{}, err
		}
	}
	if ident.Implementation != "" {
		if err := identval.ValidateImpl(ident.Implementation); err != nil {
			return models.Identity{}, err
		}
	}
	if ident.Specifier != "" {
		if err := identval.ValidateToken(ident.Specifier, "specifier"); err != nil {
			return models.Identity{}, err
		}
	}
	if ident.ID != "" {
		if err := identval.ValidateToken(ident.ID, "id"); err != nil {
			return models.Identity{}, err
		}
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
