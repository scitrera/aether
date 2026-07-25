package admin

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	aclstore "github.com/scitrera/aether/server/internal/storage/acl"
)

// =============================================================================
// ACL Handlers
// =============================================================================

func (s *Server) handleListACLRules(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters for filtering
	query := r.URL.Query()
	filter := &ACLRuleFilter{
		PrincipalType: query.Get("principal_type"),
		PrincipalID:   query.Get("principal_id"),
		ResourceType:  query.Get("resource_type"),
		ResourceID:    query.Get("resource_id"),
	}

	rules, err := s.provider.ListACLRules(r.Context(), filter)
	if err != nil {
		s.respondInternalError(w, "failed to list ACL rules", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"rules": rules,
		"count": len(rules),
	})
}

func (s *Server) handleGetACLRule(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters for rule lookup
	query := r.URL.Query()
	principalType := query.Get("principal_type")
	principalID := query.Get("principal_id")
	resourceType := query.Get("resource_type")
	resourceID := query.Get("resource_id")

	if principalType == "" || principalID == "" || resourceType == "" || resourceID == "" {
		respondError(w, http.StatusBadRequest, "principal_type, principal_id, resource_type, and resource_id are required")
		return
	}

	rule, err := s.provider.GetACLRule(r.Context(), principalType, principalID, resourceType, resourceID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, rule)
}

func (s *Server) handleGrantACLAccess(w http.ResponseWriter, r *http.Request) {
	req := decodeJSON[GrantACLAccessRequest](w, r)
	if req == nil {
		return
	}

	// Validate required fields
	if req.PrincipalType == "" {
		respondError(w, http.StatusBadRequest, "principal_type is required")
		return
	}
	if req.PrincipalID == "" {
		respondError(w, http.StatusBadRequest, "principal_id is required")
		return
	}
	if req.ResourceType == "" {
		respondError(w, http.StatusBadRequest, "resource_type is required")
		return
	}
	if req.ResourceID == "" {
		respondError(w, http.StatusBadRequest, "resource_id is required")
		return
	}
	if req.GrantedBy == "" {
		respondError(w, http.StatusBadRequest, "granted_by is required")
		return
	}

	rule, err := s.provider.GrantACLAccess(r.Context(), req)
	if err != nil {
		s.respondInternalError(w, "failed to grant ACL access", err)
		return
	}

	respondJSON(w, http.StatusCreated, map[string]interface{}{
		"message": fmt.Sprintf("ACL access granted for %s:%s on %s:%s", req.PrincipalType, req.PrincipalID, req.ResourceType, req.ResourceID),
		"rule":    rule,
	})
}

func (s *Server) handleRevokeACLAccess(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters for revocation
	query := r.URL.Query()
	principalType := query.Get("principal_type")
	principalID := query.Get("principal_id")
	resourceType := query.Get("resource_type")
	resourceID := query.Get("resource_id")

	if principalType == "" || principalID == "" || resourceType == "" || resourceID == "" {
		respondError(w, http.StatusBadRequest, "principal_type, principal_id, resource_type, and resource_id are required")
		return
	}

	if err := s.provider.RevokeACLAccess(r.Context(), principalType, principalID, resourceType, resourceID); err != nil {
		s.respondInternalError(w, "failed to revoke ACL access", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("ACL access revoked for %s:%s on %s:%s", principalType, principalID, resourceType, resourceID),
	})
}

func (s *Server) handleQueryACLAuditLog(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters for filtering
	query := r.URL.Query()
	filter := &ACLAuditLogFilter{
		PrincipalType: query.Get("principal_type"),
		PrincipalID:   query.Get("principal_id"),
		ResourceType:  query.Get("resource_type"),
		ResourceID:    query.Get("resource_id"),
		Decision:      query.Get("decision"),
		Workspace:     query.Get("workspace"),
	}

	// Parse limit if provided
	if limitStr := query.Get("limit"); limitStr != "" {
		var limit int
		if _, err := fmt.Sscanf(limitStr, "%d", &limit); err == nil {
			filter.Limit = limit
		}
	}

	entries, err := s.provider.QueryACLAuditLog(r.Context(), filter)
	if err != nil {
		s.respondInternalError(w, "failed to query ACL audit log", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"entries": entries,
		"count":   len(entries),
	})
}

func (s *Server) handleListACLAuthorityGrants(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	filter := &ACLAuthorityGrantFilter{
		RootGrantID:    query.Get("root_grant_id"),
		SubjectType:    query.Get("subject_type"),
		SubjectID:      query.Get("subject_id"),
		DelegateType:   query.Get("delegate_type"),
		DelegateID:     query.Get("delegate_id"),
		AudienceType:   query.Get("audience_type"),
		AudienceID:     query.Get("audience_id"),
		IncludeRevoked: query.Get("include_revoked") == "true",
		ActiveOnly:     query.Get("active_only") == "true",
	}
	if limitStr := query.Get("limit"); limitStr != "" {
		if _, err := fmt.Sscanf(limitStr, "%d", &filter.Limit); err != nil {
			respondError(w, http.StatusBadRequest, "invalid limit parameter")
			return
		}
	}
	if offsetStr := query.Get("offset"); offsetStr != "" {
		if _, err := fmt.Sscanf(offsetStr, "%d", &filter.Offset); err != nil {
			respondError(w, http.StatusBadRequest, "invalid offset parameter")
			return
		}
	}

	grants, err := s.provider.ListACLAuthorityGrants(r.Context(), filter)
	if err != nil {
		s.respondInternalError(w, "failed to list authority grants", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"grants": grants,
		"count":  len(grants),
	})
}

func (s *Server) handleGetACLAuthorityGrant(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	grantID := vars["grant_id"]
	if grantID == "" {
		respondError(w, http.StatusBadRequest, "grant_id is required")
		return
	}

	grant, err := s.provider.GetACLAuthorityGrant(r.Context(), grantID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, grant)
}

func (s *Server) handleCreateACLAuthorityGrant(w http.ResponseWriter, r *http.Request) {
	req := decodeJSON[CreateACLAuthorityGrantRequest](w, r)
	if req == nil {
		return
	}

	if req.Subject == nil || req.Subject.PrincipalType == "" || req.Subject.PrincipalID == "" {
		respondError(w, http.StatusBadRequest, "subject principal_type and principal_id are required")
		return
	}
	if req.Delegate == nil || req.Delegate.PrincipalType == "" || req.Delegate.PrincipalID == "" {
		respondError(w, http.StatusBadRequest, "delegate principal_type and principal_id are required")
		return
	}
	if req.IssuedBy == nil || req.IssuedBy.PrincipalType == "" || req.IssuedBy.PrincipalID == "" {
		respondError(w, http.StatusBadRequest, "issued_by principal_type and principal_id are required")
		return
	}
	if req.ExpiresAt.IsZero() {
		respondError(w, http.StatusBadRequest, "expires_at is required")
		return
	}
	if req.RenewableUntil.IsZero() {
		respondError(w, http.StatusBadRequest, "renewable_until is required")
		return
	}
	if req.AudienceType == "" || req.AudienceID == "" {
		respondError(w, http.StatusBadRequest, "audience_type and audience_id are required")
		return
	}

	grant, err := s.provider.CreateACLAuthorityGrant(r.Context(), req)
	if err != nil {
		s.respondInternalError(w, "failed to create authority grant", err)
		return
	}

	respondJSON(w, http.StatusCreated, map[string]interface{}{
		"message": "authority grant created",
		"grant":   grant,
	})
}

func (s *Server) handleRenewACLAuthorityGrant(w http.ResponseWriter, r *http.Request) {
	req := decodeJSON[RenewACLAuthorityGrantRequest](w, r)
	if req == nil {
		return
	}

	vars := mux.Vars(r)
	grantID := vars["grant_id"]
	if req.GrantID == "" {
		req.GrantID = grantID
	}
	if req.GrantID == "" {
		respondError(w, http.StatusBadRequest, "grant_id is required")
		return
	}
	if req.ExpiresAt.IsZero() {
		respondError(w, http.StatusBadRequest, "expires_at is required")
		return
	}

	grant, err := s.provider.RenewACLAuthorityGrant(r.Context(), req)
	if err != nil {
		s.respondInternalError(w, "failed to renew authority grant", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"message": "authority grant renewed",
		"grant":   grant,
	})
}

func (s *Server) handleRevokeACLAuthorityGrant(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	grantID := vars["grant_id"]
	if grantID == "" {
		respondError(w, http.StatusBadRequest, "grant_id is required")
		return
	}

	if err := s.provider.RevokeACLAuthorityGrant(r.Context(), grantID); err != nil {
		s.respondInternalError(w, "failed to revoke authority grant", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("authority grant %s revoked successfully", grantID),
	})
}

func (s *Server) handleGetACLFallbackPolicy(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	ruleCategory := query.Get("rule_category")

	if ruleCategory == "" {
		respondError(w, http.StatusBadRequest, "rule_category is required")
		return
	}

	policy, err := s.provider.GetACLFallbackPolicy(r.Context(), ruleCategory)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, policy)
}

func (s *Server) handleSetACLFallbackPolicy(w http.ResponseWriter, r *http.Request) {
	req := decodeJSON[SetFallbackPolicyRequest](w, r)
	if req == nil {
		return
	}

	// Validate required fields
	if req.RuleCategory == "" {
		respondError(w, http.StatusBadRequest, "rule_category is required")
		return
	}
	if req.UpdatedBy == "" {
		respondError(w, http.StatusBadRequest, "updated_by is required")
		return
	}

	if err := s.provider.SetACLFallbackPolicy(r.Context(), req); err != nil {
		s.respondInternalError(w, "failed to set ACL fallback policy", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("fallback policy for %s updated successfully", req.RuleCategory),
	})
}

func (s *Server) handleCleanupExpiredACLRules(w http.ResponseWriter, r *http.Request) {
	count, err := s.provider.CleanupExpiredACLRules(r.Context())
	if err != nil {
		s.respondInternalError(w, "failed to cleanup expired ACL rules", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"message": "expired ACL rules cleaned up",
		"count":   count,
	})
}

func (s *Server) handleCleanupOldACLAuditLogs(w http.ResponseWriter, r *http.Request) {
	// Parse retention days from query parameter, default to 90
	retentionDays := 90
	if retentionStr := r.URL.Query().Get("retention_days"); retentionStr != "" {
		if _, err := fmt.Sscanf(retentionStr, "%d", &retentionDays); err != nil {
			respondError(w, http.StatusBadRequest, "invalid retention_days parameter")
			return
		}
	}

	count, err := s.provider.CleanupOldACLAuditLogs(r.Context(), retentionDays)
	if err != nil {
		s.respondInternalError(w, "failed to cleanup old ACL audit logs", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"message":        fmt.Sprintf("audit logs older than %d days cleaned up", retentionDays),
		"count":          count,
		"retention_days": retentionDays,
	})
}

// =============================================================================
// ACL Group Handlers
// =============================================================================

func (s *Server) handleListACLGroups(w http.ResponseWriter, r *http.Request) {
	groups, err := s.provider.ListACLGroups(r.Context())
	if err != nil {
		s.respondInternalError(w, "failed to list ACL groups", err)
		return
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"groups": groups,
		"count":  len(groups),
	})
}

func (s *Server) handleCreateACLGroup(w http.ResponseWriter, r *http.Request) {
	req := decodeJSON[CreateACLGroupRequest](w, r)
	if req == nil {
		return
	}
	if req.Name == "" {
		respondError(w, http.StatusBadRequest, "name is required")
		return
	}

	group, err := s.provider.CreateACLGroup(r.Context(), req)
	if err != nil {
		if errors.Is(err, aclstore.ErrGroupExists) {
			respondError(w, http.StatusConflict, fmt.Sprintf("group %q already exists", req.Name))
			return
		}
		s.respondInternalError(w, "failed to create ACL group", err)
		return
	}

	respondJSON(w, http.StatusCreated, map[string]interface{}{
		"message": fmt.Sprintf("group %q created", req.Name),
		"group":   group,
	})
}

func (s *Server) handleGetACLGroup(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	group, err := s.provider.GetACLGroup(r.Context(), name)
	if err != nil {
		if errors.Is(err, aclstore.ErrGroupNotFound) {
			respondError(w, http.StatusNotFound, fmt.Sprintf("group %q not found", name))
			return
		}
		s.respondInternalError(w, "failed to get ACL group", err)
		return
	}

	respondJSON(w, http.StatusOK, group)
}

func (s *Server) handleDeleteACLGroup(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	if err := s.provider.DeleteACLGroup(r.Context(), name); err != nil {
		if errors.Is(err, aclstore.ErrGroupNotFound) {
			respondError(w, http.StatusNotFound, fmt.Sprintf("group %q not found", name))
			return
		}
		s.respondInternalError(w, "failed to delete ACL group", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("group %q deleted", name),
	})
}

func (s *Server) handleListACLGroupMembers(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	members, err := s.provider.ListACLGroupMembers(r.Context(), name)
	if err != nil {
		if errors.Is(err, aclstore.ErrGroupNotFound) {
			respondError(w, http.StatusNotFound, fmt.Sprintf("group %q not found", name))
			return
		}
		s.respondInternalError(w, "failed to list ACL group members", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"group_name": name,
		"members":    members,
		"count":      len(members),
	})
}

func (s *Server) handleAddACLGroupMember(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	req := decodeJSON[AddACLGroupMemberRequest](w, r)
	if req == nil {
		return
	}
	if req.MemberType == "" {
		respondError(w, http.StatusBadRequest, "member_type is required")
		return
	}
	if req.MemberID == "" {
		respondError(w, http.StatusBadRequest, "member_id is required")
		return
	}

	member, err := s.provider.AddACLGroupMember(r.Context(), name, req)
	if err != nil {
		if errors.Is(err, aclstore.ErrGroupNotFound) {
			respondError(w, http.StatusNotFound, fmt.Sprintf("group %q not found", name))
			return
		}
		if errors.Is(err, aclstore.ErrMembershipCycle) {
			respondError(w, http.StatusBadRequest, "membership would create a cycle")
			return
		}
		s.respondInternalError(w, "failed to add ACL group member", err)
		return
	}

	respondJSON(w, http.StatusCreated, map[string]interface{}{
		"message": fmt.Sprintf("member %s:%s added to group %q", req.MemberType, req.MemberID, name),
		"member":  member,
	})
}

func (s *Server) handleRemoveACLGroupMember(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	query := r.URL.Query()
	memberType := query.Get("member_type")
	memberID := query.Get("member_id")

	if memberType == "" || memberID == "" {
		respondError(w, http.StatusBadRequest, "member_type and member_id are required")
		return
	}

	if err := s.provider.RemoveACLGroupMember(r.Context(), name, memberType, memberID); err != nil {
		if errors.Is(err, aclstore.ErrGroupNotFound) {
			respondError(w, http.StatusNotFound, fmt.Sprintf("group %q not found", name))
			return
		}
		if errors.Is(err, aclstore.ErrMembershipNotFound) {
			respondError(w, http.StatusNotFound, fmt.Sprintf("member %s:%s not found in group %q", memberType, memberID, name))
			return
		}
		s.respondInternalError(w, "failed to remove ACL group member", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("member %s:%s removed from group %q", memberType, memberID, name),
	})
}

// =============================================================================
// ACL Role Handlers
// =============================================================================

func (s *Server) handleListACLRoles(w http.ResponseWriter, r *http.Request) {
	roles, err := s.provider.ListACLRoles(r.Context())
	if err != nil {
		s.respondInternalError(w, "failed to list ACL roles", err)
		return
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"roles": roles,
		"count": len(roles),
	})
}

func (s *Server) handleCreateACLRole(w http.ResponseWriter, r *http.Request) {
	req := decodeJSON[CreateACLRoleRequest](w, r)
	if req == nil {
		return
	}
	if req.Name == "" {
		respondError(w, http.StatusBadRequest, "name is required")
		return
	}

	role, err := s.provider.CreateACLRole(r.Context(), req)
	if err != nil {
		if errors.Is(err, aclstore.ErrRoleExists) {
			respondError(w, http.StatusConflict, fmt.Sprintf("role %q already exists", req.Name))
			return
		}
		s.respondInternalError(w, "failed to create ACL role", err)
		return
	}

	respondJSON(w, http.StatusCreated, map[string]interface{}{
		"message": fmt.Sprintf("role %q created", req.Name),
		"role":    role,
	})
}

func (s *Server) handleGetACLRole(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	role, err := s.provider.GetACLRole(r.Context(), name)
	if err != nil {
		if errors.Is(err, aclstore.ErrRoleNotFound) {
			respondError(w, http.StatusNotFound, fmt.Sprintf("role %q not found", name))
			return
		}
		s.respondInternalError(w, "failed to get ACL role", err)
		return
	}

	respondJSON(w, http.StatusOK, role)
}

func (s *Server) handleDeleteACLRole(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	if err := s.provider.DeleteACLRole(r.Context(), name); err != nil {
		if errors.Is(err, aclstore.ErrRoleNotFound) {
			respondError(w, http.StatusNotFound, fmt.Sprintf("role %q not found", name))
			return
		}
		s.respondInternalError(w, "failed to delete ACL role", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("role %q deleted", name),
	})
}

func (s *Server) handleListACLRoleAssignments(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	assignments, err := s.provider.ListACLRoleAssignments(r.Context(), name)
	if err != nil {
		if errors.Is(err, aclstore.ErrRoleNotFound) {
			respondError(w, http.StatusNotFound, fmt.Sprintf("role %q not found", name))
			return
		}
		s.respondInternalError(w, "failed to list ACL role assignments", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"role_name":   name,
		"assignments": assignments,
		"count":       len(assignments),
	})
}

func (s *Server) handleAssignACLRole(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	req := decodeJSON[AssignACLRoleRequest](w, r)
	if req == nil {
		return
	}
	if req.AssigneeType == "" {
		respondError(w, http.StatusBadRequest, "assignee_type is required")
		return
	}
	if req.AssigneeID == "" {
		respondError(w, http.StatusBadRequest, "assignee_id is required")
		return
	}

	assignment, err := s.provider.AssignACLRole(r.Context(), name, req)
	if err != nil {
		if errors.Is(err, aclstore.ErrRoleNotFound) {
			respondError(w, http.StatusNotFound, fmt.Sprintf("role %q not found", name))
			return
		}
		if errors.Is(err, aclstore.ErrMembershipCycle) {
			respondError(w, http.StatusBadRequest, "assignment would create a cycle")
			return
		}
		s.respondInternalError(w, "failed to assign ACL role", err)
		return
	}

	respondJSON(w, http.StatusCreated, map[string]interface{}{
		"message":    fmt.Sprintf("role %q assigned to %s:%s", name, req.AssigneeType, req.AssigneeID),
		"assignment": assignment,
	})
}

func (s *Server) handleUnassignACLRole(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	query := r.URL.Query()
	assigneeType := query.Get("assignee_type")
	assigneeID := query.Get("assignee_id")

	if assigneeType == "" || assigneeID == "" {
		respondError(w, http.StatusBadRequest, "assignee_type and assignee_id are required")
		return
	}

	if err := s.provider.UnassignACLRole(r.Context(), name, assigneeType, assigneeID); err != nil {
		if errors.Is(err, aclstore.ErrRoleNotFound) {
			respondError(w, http.StatusNotFound, fmt.Sprintf("role %q not found", name))
			return
		}
		if errors.Is(err, aclstore.ErrAssignmentNotFound) {
			respondError(w, http.StatusNotFound, fmt.Sprintf("assignment of %s:%s to role %q not found", assigneeType, assigneeID, name))
			return
		}
		s.respondInternalError(w, "failed to unassign ACL role", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("role %q unassigned from %s:%s", name, assigneeType, assigneeID),
	})
}

// =============================================================================
// ACL Principal Handlers
// =============================================================================

func (s *Server) handleListACLPrincipalGroups(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	memberType := vars["type"]
	memberID := vars["id"]

	members, err := s.provider.ListACLPrincipalGroups(r.Context(), memberType, memberID)
	if err != nil {
		s.respondInternalError(w, "failed to list principal groups", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"member_type": memberType,
		"member_id":   memberID,
		"groups":      members,
		"count":       len(members),
	})
}

func (s *Server) handleListACLPrincipalRoles(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	assigneeType := vars["type"]
	assigneeID := vars["id"]

	assignments, err := s.provider.ListACLPrincipalRoles(r.Context(), assigneeType, assigneeID)
	if err != nil {
		s.respondInternalError(w, "failed to list principal roles", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"assignee_type": assigneeType,
		"assignee_id":   assigneeID,
		"roles":         assignments,
		"count":         len(assignments),
	})
}

// handleExplainACLAccess explains how a principal's effective access to a
// resource is decided: the resolved subject set (self + groups/roles), the
// rules that matched, and the resulting decision. Emits an "explain_access"
// audit event recording the caller (admin_api + remote addr) and subject.
// GET /acl/principals/{type}/{id}/effective?resource_type=&resource_id=&required_level=
func (s *Server) handleExplainACLAccess(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	principalType := vars["type"]
	principalID := vars["id"]

	q := r.URL.Query()
	resourceType := q.Get("resource_type")
	resourceID := q.Get("resource_id")
	if resourceType == "" || resourceID == "" {
		respondError(w, http.StatusBadRequest, "resource_type and resource_id query parameters are required")
		return
	}
	requiredLevel := 0
	if rl := q.Get("required_level"); rl != "" {
		v, err := strconv.Atoi(rl)
		if err != nil {
			respondError(w, http.StatusBadRequest, "required_level must be an integer")
			return
		}
		requiredLevel = v
	}

	// The admin REST API is gated by admin middleware, not a principal
	// session, so record the source + remote address as the caller for the
	// audit trail rather than a principal identity.
	exp, err := s.provider.ExplainACLAccess(r.Context(), principalType, principalID, resourceType, resourceID, requiredLevel, "admin_api", r.RemoteAddr)
	if err != nil {
		s.respondInternalError(w, "failed to explain access", err)
		return
	}
	respondJSON(w, http.StatusOK, exp)
}
