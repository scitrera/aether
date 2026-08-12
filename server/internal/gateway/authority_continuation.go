package gateway

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/server/internal/acl"
	"github.com/scitrera/aether/server/internal/audit"
	"github.com/scitrera/aether/server/pkg/models"
)

const (
	messageAuthorityContinuationTTL = 5 * time.Minute
	continuationMetadataKindKey     = "authority_continuation"
	continuationMetadataTargetKey   = "delivery_target"
)

// deriveMessageAuthorityContinuation creates (or reuses) a short-lived leaf
// grant for the concrete service that will receive a message. The caller's
// authority has already been resolved against its authenticated connection;
// CreateAuthorityGrant enforces parent scope, expiry, and hop attenuation.
func (s *GatewayServer) deriveMessageAuthorityContinuation(
	ctx context.Context,
	authority *acl.ResolvedAuthority,
	deliveryTarget string,
	sessionID uuid.UUID,
) (*pb.ForwardedAuthorization, error) {
	if s.acl == nil {
		return nil, fmt.Errorf("authority continuation requires ACL service")
	}
	if authority == nil || authority.Grant == nil {
		return nil, fmt.Errorf("authority continuation requires resolved on-behalf-of authority")
	}
	if !authority.Grant.CanDelegate() {
		return nil, acl.ErrAuthorityGrantDelegationDenied
	}

	target, err := models.ParseIdentity(deliveryTarget)
	if err != nil || target.Type != models.PrincipalService || target.Specifier == "" {
		return nil, fmt.Errorf("authority continuation target must be a concrete service identity")
	}
	audienceID := target.CanonicalPrincipalID()
	now := time.Now().UTC()
	expiresAt := now.Add(messageAuthorityContinuationTTL)
	if authority.Grant.ExpiresAt.Before(expiresAt) {
		expiresAt = authority.Grant.ExpiresAt
	}
	if !expiresAt.After(now) {
		return nil, acl.ErrAuthorityGrantExpired
	}

	grant, findErr := s.acl.FindVisibleDerivedGrant(
		ctx,
		authority.Grant.GrantID,
		target,
		acl.AuthorityAudienceService,
		audienceID,
	)
	reused := findErr == nil && messageAuthorityContinuationReusable(grant, authority.Grant, deliveryTarget, now)
	if !reused {
		rootSubjectType := authority.Grant.RootSubjectType
		rootSubjectID := authority.Grant.RootSubjectID
		if rootSubjectType == "" || rootSubjectID == "" {
			rootSubjectType = authority.Grant.SubjectType
			rootSubjectID = authority.Grant.SubjectID
		}
		rootSubject, rootErr := identityFromAuthorityPrincipal(rootSubjectType, rootSubjectID)
		if rootErr != nil {
			return nil, fmt.Errorf("invalid continuation root subject: %w", rootErr)
		}

		parentGrantID := authority.Grant.GrantID
		grant, err = s.acl.CreateAuthorityGrant(ctx, acl.CreateAuthorityGrantRequest{
			Subject:                  authority.Subject,
			Delegate:                 target,
			IssuedBy:                 authority.Actor,
			RootSubject:              &rootSubject,
			ParentGrantID:            &parentGrantID,
			MayDelegate:              false,
			RemainingHops:            0,
			WorkspaceScope:           cloneStringSlice(authority.Grant.WorkspaceScope),
			ResourceScope:            cloneResourceScope(authority.Grant.ResourceScope),
			OperationScope:           cloneStringSlice(authority.Grant.OperationScope),
			MaxAccessLevel:           authority.Grant.MaxAccessLevel,
			AudienceType:             acl.AuthorityAudienceService,
			AudienceID:               audienceID,
			ValidWhileAudienceActive: false,
			ExpiresAt:                expiresAt,
			RenewableUntil:           expiresAt,
			Reason:                   "message-authority-continuation",
			Metadata: map[string]interface{}{
				continuationMetadataKindKey:   true,
				continuationMetadataTargetKey: deliveryTarget,
				"derived_from_grant_id":       parentGrantID,
			},
		})
		if err != nil {
			s.logAuthorityGrantLifecycle(ctx, authority.Actor, sessionID, audit.OpAuthorityGrantDerive, nil, false, err.Error(), map[string]interface{}{
				"authority_continuation": true,
				"delivery_target":        deliveryTarget,
			})
			return nil, err
		}
	}

	operation := audit.OpAuthorityGrantDerive
	if reused {
		operation = audit.OpAuthorityGrantGet
	}
	s.logAuthorityGrantLifecycle(ctx, authority.Actor, sessionID, operation, grant, true, "", map[string]interface{}{
		"authority_continuation": true,
		"delivery_target":        deliveryTarget,
		"reused_existing":        reused,
	})

	rootGrantID := grant.RootGrantID
	if rootGrantID == "" {
		rootGrantID = grant.GrantID
	}
	return &pb.ForwardedAuthorization{
		Authorization: &pb.AuthorizationContext{
			AuthorityMode: audit.AuthorityModeOnBehalfOf,
			Subject:       identityToProtoPrincipalRef(authority.Subject),
			GrantId:       grant.GrantID,
		},
		RootGrantId:    rootGrantID,
		ExpiresAtMs:    grant.ExpiresAt.UnixMilli(),
		DeliveryTarget: deliveryTarget,
	}, nil
}

func messageAuthorityContinuationReusable(grant, parent *acl.AuthorityGrant, deliveryTarget string, now time.Time) bool {
	if grant == nil || parent == nil || grant.ParentGrantID == nil || *grant.ParentGrantID != parent.GrantID {
		return false
	}
	if err := grant.ValidateActiveAt(now); err != nil {
		return false
	}
	if kind, ok := grant.Metadata[continuationMetadataKindKey].(bool); !ok || !kind {
		return false
	}
	if target, ok := grant.Metadata[continuationMetadataTargetKey].(string); !ok || target != deliveryTarget {
		return false
	}
	if grant.MayDelegate || grant.RemainingHops != 0 ||
		grant.MaxAccessLevel != parent.MaxAccessLevel ||
		grant.SubjectType != parent.SubjectType || grant.SubjectID != parent.SubjectID ||
		grant.RootSubjectType != parent.RootSubjectType || grant.RootSubjectID != parent.RootSubjectID ||
		!slices.Equal(grant.WorkspaceScope, parent.WorkspaceScope) ||
		!slices.Equal(grant.OperationScope, parent.OperationScope) ||
		!resourceScopesEqual(grant.ResourceScope, parent.ResourceScope) ||
		grant.ExpiresAt.After(parent.ExpiresAt) ||
		!grant.RenewableUntil.Equal(grant.ExpiresAt) {
		return false
	}
	return true
}

func resourceScopesEqual(left, right map[string][]string) bool {
	if len(left) != len(right) {
		return false
	}
	for resourceType, patterns := range left {
		if !slices.Equal(patterns, right[resourceType]) {
			return false
		}
	}
	return true
}
