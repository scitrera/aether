package admin

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/scitrera/aether/internal/identval"
)

// =============================================================================
// Workspace Handlers
// =============================================================================

func (s *Server) handleListWorkspaces(w http.ResponseWriter, r *http.Request) {
	workspaces, err := s.provider.GetWorkspaces(r.Context())
	if err != nil {
		s.respondInternalError(w, "failed to list workspaces", err)
		return
	}

	limit, offset := parsePagination(r)
	page, total := applyPagination(workspaces, limit, offset)
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"workspaces": page,
		"count":      len(page),
		"total":      total,
		"limit":      limit,
		"offset":     offset,
	})
}

func (s *Server) handleGetWorkspace(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workspaceID := vars["workspace_id"]

	workspace, err := s.provider.GetWorkspaceByID(r.Context(), workspaceID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, workspace)
}

func (s *Server) handleCreateWorkspace(w http.ResponseWriter, r *http.Request) {
	body := decodeJSON[struct {
		WorkspaceID string                 `json:"workspace_id"`
		DisplayName string                 `json:"display_name"`
		Description string                 `json:"description"`
		TenantID    string                 `json:"tenant_id"`
		Metadata    map[string]interface{} `json:"metadata"`
	}](w, r)
	if body == nil {
		return
	}

	if body.WorkspaceID == "" {
		respondError(w, http.StatusBadRequest, "workspace_id is required")
		return
	}

	if err := identval.ValidateToken(body.WorkspaceID, "workspace"); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	workspace := &WorkspaceInfo{
		WorkspaceID: body.WorkspaceID,
		DisplayName: body.DisplayName,
		Description: body.Description,
		TenantID:    body.TenantID,
		Metadata:    body.Metadata,
	}

	if err := s.provider.CreateWorkspace(r.Context(), workspace); err != nil {
		s.respondInternalError(w, "failed to create workspace", err)
		return
	}

	respondJSON(w, http.StatusCreated, map[string]string{
		"message": fmt.Sprintf("workspace %s created successfully", body.WorkspaceID),
	})
}

func (s *Server) handleUpdateWorkspace(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workspaceID := vars["workspace_id"]

	body := decodeJSON[struct {
		DisplayName string                 `json:"display_name"`
		Description string                 `json:"description"`
		TenantID    string                 `json:"tenant_id"`
		Metadata    map[string]interface{} `json:"metadata"`
	}](w, r)
	if body == nil {
		return
	}

	workspace := &WorkspaceInfo{
		WorkspaceID: workspaceID,
		DisplayName: body.DisplayName,
		Description: body.Description,
		TenantID:    body.TenantID,
		Metadata:    body.Metadata,
	}

	if err := s.provider.UpdateWorkspace(r.Context(), workspaceID, workspace); err != nil {
		s.respondInternalError(w, "failed to update workspace", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("workspace %s updated successfully", workspaceID),
	})
}

func (s *Server) handleDeleteWorkspace(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workspaceID := vars["workspace_id"]

	if err := s.provider.DeleteWorkspace(r.Context(), workspaceID); err != nil {
		s.respondInternalError(w, "failed to delete workspace", err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{
		"message": fmt.Sprintf("workspace %s deleted successfully", workspaceID),
	})
}

func (s *Server) handleGetMessageFlow(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workspaceID := vars["workspace_id"]

	messageFlow, err := s.provider.GetMessageFlow(r.Context(), workspaceID)
	if err != nil {
		s.respondInternalError(w, "failed to get message flow", err)
		return
	}

	respondJSON(w, http.StatusOK, messageFlow)
}
