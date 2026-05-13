package admin

import (
	"net/http"

	"github.com/gorilla/mux"
)

// =============================================================================
// Token Handlers — delegate to StateProvider
// =============================================================================

func (s *Server) handleListTokens(w http.ResponseWriter, r *http.Request) {
	includeRevoked := r.URL.Query().Get("include_revoked") == "true"
	tokens, err := s.provider.ListTokens(r.Context(), 100, 0, includeRevoked)
	if err != nil {
		s.respondInternalError(w, "failed to list tokens", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"tokens": tokens,
		"count":  len(tokens),
	})
}

func (s *Server) handleCreateToken(w http.ResponseWriter, r *http.Request) {
	body := decodeJSON[struct {
		Name              string   `json:"name"`
		PrincipalType     string   `json:"principal_type"`
		WorkspacePatterns []string `json:"workspace_patterns"`
		Scopes            []string `json:"scopes"`
		ExpiresInHours    int      `json:"expires_in_hours"`
		CreatedBy         string   `json:"created_by"`
	}](w, r)
	if body == nil {
		return
	}

	if body.Name == "" {
		respondError(w, http.StatusBadRequest, "name is required")
		return
	}
	if body.PrincipalType == "" {
		respondError(w, http.StatusBadRequest, "principal_type is required")
		return
	}

	result, err := s.provider.CreateToken(r.Context(), &CreateTokenRequest{
		Name:              body.Name,
		PrincipalType:     body.PrincipalType,
		WorkspacePatterns: body.WorkspacePatterns,
		Scopes:            body.Scopes,
		ExpiresInHours:    body.ExpiresInHours,
		CreatedBy:         body.CreatedBy,
	})
	if err != nil {
		s.respondInternalError(w, "failed to create token", err)
		return
	}

	respondJSON(w, http.StatusCreated, map[string]interface{}{
		"token":     result.PlaintextToken,
		"api_token": result.Token,
	})
}

func (s *Server) handleGetToken(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tokenID := vars["token_id"]

	token, err := s.provider.GetToken(r.Context(), tokenID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, token)
}

func (s *Server) handleRevokeToken(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tokenID := vars["token_id"]

	if err := s.provider.RevokeToken(r.Context(), tokenID); err != nil {
		s.respondInternalError(w, "failed to revoke token", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{"status": "revoked"})
}

func (s *Server) handleDeleteToken(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tokenID := vars["token_id"]

	if err := s.provider.DeleteToken(r.Context(), tokenID); err != nil {
		s.respondInternalError(w, "failed to delete token", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}
