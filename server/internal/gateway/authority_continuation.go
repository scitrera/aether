package gateway

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
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
	continuationMetadataBindingKey  = "binding_id"
	continuationMetadataModeKey     = "scope_mode"
	maxContinuationBindingIDLength  = 256
)

type messageAuthorityContinuationConfig struct {
	target         models.Identity
	audienceType   string
	audienceID     string
	bindingID      string
	workspaceScope []string
	resourceScope  map[string][]string
	operationScope []string
	maxAccessLevel int
	remainingHops  int
	ttl            time.Duration
	reusable       bool
}

// deriveMessageAuthorityContinuation creates a short-lived leaf grant for the
// concrete service or agent that will receive a message. Reusable service
// continuations may inherit the parent ceiling. Agent continuations are always
// explicitly attenuated and minted per checked invocation.
func (s *GatewayServer) deriveMessageAuthorityContinuation(
	ctx context.Context,
	authority *acl.ResolvedAuthority,
	deliveryTarget string,
	request *pb.AuthorityContinuationRequest,
	accessReceipt *pb.AccessDecisionReceipt,
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

	config, err := resolveMessageAuthorityContinuation(authority.Grant, deliveryTarget, request, accessReceipt)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	expiresAt := now.Add(config.ttl)
	if authority.Grant.ExpiresAt.Before(expiresAt) {
		expiresAt = authority.Grant.ExpiresAt
	}
	if !expiresAt.After(now) {
		return nil, acl.ErrAuthorityGrantExpired
	}

	var grant *acl.AuthorityGrant
	reused := false
	if config.reusable {
		candidate, findErr := s.acl.FindVisibleDerivedGrant(
			ctx, authority.Grant.GrantID, config.target, config.audienceType, config.audienceID,
		)
		if findErr == nil && messageAuthorityContinuationReusable(candidate, authority.Grant, deliveryTarget, now) {
			grant, reused = candidate, true
		}
	}
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
		metadata := map[string]interface{}{
			continuationMetadataKindKey:   true,
			continuationMetadataTargetKey: deliveryTarget,
			continuationMetadataModeKey:   request.GetScopeMode().String(),
			"derived_from_grant_id":       parentGrantID,
		}
		if config.bindingID != "" {
			metadata[continuationMetadataBindingKey] = config.bindingID
			metadata["access_decision_id"] = accessReceipt.GetDecisionId()
			metadata["checked_resource_type"] = accessReceipt.GetRequest().GetResourceType()
			metadata["checked_resource_id"] = accessReceipt.GetRequest().GetResourceId()
		}
		grant, err = s.acl.CreateAuthorityGrant(ctx, acl.CreateAuthorityGrantRequest{
			Subject:                  authority.Subject,
			Delegate:                 config.target,
			IssuedBy:                 authority.Actor,
			RootSubject:              &rootSubject,
			ParentGrantID:            &parentGrantID,
			MayDelegate:              config.remainingHops > 0,
			RemainingHops:            config.remainingHops,
			WorkspaceScope:           cloneStringSlice(config.workspaceScope),
			ResourceScope:            cloneResourceScope(config.resourceScope),
			OperationScope:           cloneStringSlice(config.operationScope),
			MaxAccessLevel:           config.maxAccessLevel,
			AudienceType:             config.audienceType,
			AudienceID:               config.audienceID,
			ValidWhileAudienceActive: false,
			ExpiresAt:                expiresAt,
			RenewableUntil:           expiresAt,
			Reason:                   "message-authority-continuation",
			Metadata:                 metadata,
		})
		if err != nil {
			s.logAuthorityGrantLifecycle(ctx, authority.Actor, sessionID, audit.OpAuthorityGrantDerive, nil, false, err.Error(), map[string]interface{}{
				"authority_continuation": true,
				"delivery_target":        deliveryTarget,
				"binding_id":             config.bindingID,
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
		"binding_id":             config.bindingID,
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
		BindingId:      config.bindingID,
		Scope:          authorityContinuationScope(grant),
	}, nil
}

func resolveMessageAuthorityContinuation(
	parent *acl.AuthorityGrant,
	deliveryTarget string,
	request *pb.AuthorityContinuationRequest,
	accessReceipt *pb.AccessDecisionReceipt,
) (messageAuthorityContinuationConfig, error) {
	var config messageAuthorityContinuationConfig
	if parent == nil || request == nil {
		return config, fmt.Errorf("authority continuation request is required")
	}
	target, err := models.ParseIdentity(deliveryTarget)
	if err != nil || !isConcreteContinuationTarget(target, deliveryTarget) {
		return config, fmt.Errorf("authority continuation target must be an exact service or agent identity")
	}
	config.target = target
	config.audienceID = target.CanonicalPrincipalID()
	switch target.Type {
	case models.PrincipalService:
		config.audienceType = acl.AuthorityAudienceService
	case models.PrincipalAgent:
		config.audienceType = acl.AuthorityAudienceAgent
	default:
		return config, fmt.Errorf("authority continuation target must be an exact service or agent identity")
	}

	config.remainingHops = int(request.GetRemainingHops())
	if config.remainingHops > 8 || config.remainingHops > parent.RemainingHops-1 {
		return config, acl.ErrAuthorityGrantDelegationDenied
	}
	if config.remainingHops > 0 && target.Type != models.PrincipalService {
		return config, fmt.Errorf("only service continuations may delegate")
	}
	config.ttl = messageAuthorityContinuationTTL
	if seconds := request.GetExpiresInSeconds(); seconds > 0 {
		if seconds > 900 {
			return config, fmt.Errorf("continuation duration cannot exceed 900 seconds")
		}
		config.ttl = time.Duration(seconds) * time.Second
	}
	switch request.GetScopeMode() {
	case pb.AuthorityContinuationRequest_SCOPE_MODE_INHERIT_PARENT:
		if target.Type != models.PrincipalService {
			return config, fmt.Errorf("agent authority continuation requires explicit attenuation")
		}
		if request.GetBindingId() != "" || request.GetScope() != nil {
			return config, fmt.Errorf("inherited service continuation cannot include a binding or scope")
		}
		config.workspaceScope = cloneStringSlice(parent.WorkspaceScope)
		config.resourceScope = cloneResourceScope(parent.ResourceScope)
		config.operationScope = cloneStringSlice(parent.OperationScope)
		config.maxAccessLevel = parent.MaxAccessLevel
		config.reusable = config.remainingHops == 0 && request.GetExpiresInSeconds() == 0
		return config, nil

	case pb.AuthorityContinuationRequest_SCOPE_MODE_ATTENUATE:
		bindingID := request.GetBindingId()
		if err := validateContinuationBindingID(bindingID); err != nil {
			return config, err
		}
		if accessReceipt == nil || !accessReceipt.GetAllowed() || accessReceipt.GetRequest() == nil {
			return config, fmt.Errorf("attenuated continuation requires an allowed checked-access receipt")
		}
		checked := accessReceipt.GetRequest()
		if checked.GetCorrelationId() != bindingID {
			return config, fmt.Errorf("continuation binding must match checked-access correlation")
		}
		scope := request.GetScope()
		if scope == nil {
			return config, fmt.Errorf("attenuated continuation scope is required")
		}
		workspaces, err := validateContinuationStringScope("workspace", scope.GetWorkspaceScope())
		if err != nil {
			return config, err
		}
		operations, err := validateContinuationStringScope("operation", scope.GetOperationScope())
		if err != nil {
			return config, err
		}
		resources, err := continuationResourceScope(scope.GetResourceScope())
		if err != nil {
			return config, err
		}
		maxAccess := int(scope.GetMaxAccessLevel())
		if err := acl.ValidateAccessLevel(maxAccess); err != nil || maxAccess <= 0 {
			return config, fmt.Errorf("attenuated continuation max access level is invalid")
		}
		if checked.GetWorkspace() == "" || len(workspaces) != 1 || workspaces[0] != checked.GetWorkspace() {
			return config, fmt.Errorf("attenuated continuation must be confined to the checked workspace")
		}
		if err := acl.ValidateAuthorityGrantScopeAttenuation(parent, acl.CreateAuthorityGrantRequest{
			WorkspaceScope: workspaces, ResourceScope: resources, OperationScope: operations,
			MaxAccessLevel: maxAccess, RemainingHops: 0,
		}); err != nil {
			return config, err
		}
		config.bindingID = bindingID
		config.workspaceScope = workspaces
		config.resourceScope = resources
		config.operationScope = operations
		config.maxAccessLevel = maxAccess
		return config, nil

	default:
		return config, fmt.Errorf("authority continuation scope mode is required")
	}
}

func isConcreteContinuationTarget(target models.Identity, raw string) bool {
	if target.CanonicalPrincipalID() == "" || target.CanonicalPrincipalID() != raw || strings.ContainsAny(raw, "*?[]") {
		return false
	}
	switch target.Type {
	case models.PrincipalService:
		return target.Implementation != "" && target.Specifier != ""
	case models.PrincipalAgent:
		return target.Workspace != "" && target.Implementation != "" && target.Specifier != ""
	default:
		return false
	}
}

func validateContinuationBindingID(value string) error {
	if value == "" || value != strings.TrimSpace(value) || len(value) > maxContinuationBindingIDLength || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("attenuated continuation binding id is invalid")
	}
	return nil
}

func validateContinuationStringScope(label string, values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("attenuated continuation %s scope is required", label)
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" || value != strings.TrimSpace(value) || strings.ContainsRune(value, '\x00') {
			return nil, fmt.Errorf("attenuated continuation %s scope contains an invalid value", label)
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("attenuated continuation %s scope contains a duplicate value", label)
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}

func continuationResourceScope(entries []*pb.ACLAuthorityGrantResourceScopeEntry) (map[string][]string, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("attenuated continuation resource scope is required")
	}
	out := make(map[string][]string, len(entries))
	for _, entry := range entries {
		if entry == nil || entry.GetResourceType() == "" || entry.GetResourceType() != strings.TrimSpace(entry.GetResourceType()) || strings.ContainsRune(entry.GetResourceType(), '\x00') {
			return nil, fmt.Errorf("attenuated continuation resource scope contains an invalid resource type")
		}
		if _, exists := out[entry.GetResourceType()]; exists {
			return nil, fmt.Errorf("attenuated continuation resource scope contains a duplicate resource type")
		}
		patterns, err := validateContinuationStringScope("resource pattern", entry.GetPatterns())
		if err != nil {
			return nil, err
		}
		out[entry.GetResourceType()] = patterns
	}
	return out, nil
}

func authorityContinuationScope(grant *acl.AuthorityGrant) *pb.AuthorityContinuationScope {
	if grant == nil {
		return nil
	}
	resourceTypes := make([]string, 0, len(grant.ResourceScope))
	for resourceType := range grant.ResourceScope {
		resourceTypes = append(resourceTypes, resourceType)
	}
	sort.Strings(resourceTypes)
	resources := make([]*pb.ACLAuthorityGrantResourceScopeEntry, 0, len(resourceTypes))
	for _, resourceType := range resourceTypes {
		resources = append(resources, &pb.ACLAuthorityGrantResourceScopeEntry{
			ResourceType: resourceType,
			Patterns:     cloneStringSlice(grant.ResourceScope[resourceType]),
		})
	}
	return &pb.AuthorityContinuationScope{
		WorkspaceScope: cloneStringSlice(grant.WorkspaceScope),
		ResourceScope:  resources,
		OperationScope: cloneStringSlice(grant.OperationScope),
		MaxAccessLevel: int32(grant.MaxAccessLevel),
	}
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
	if mode, ok := grant.Metadata[continuationMetadataModeKey].(string); !ok || mode != pb.AuthorityContinuationRequest_SCOPE_MODE_INHERIT_PARENT.String() {
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
