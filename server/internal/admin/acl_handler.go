package admin

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
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
