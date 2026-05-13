package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/scitrera/aether/internal/admin"
	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/pkg/models"
)

// =============================================================================
// Workspace metadata helpers
// =============================================================================

// loadWorkspaceMetadata loads workspace metadata from the KV store.
// Returns nil map (not an error) when no metadata exists yet.
func (p *GatewayStateProvider) loadWorkspaceMetadata(ctx context.Context, workspace string) (map[string]interface{}, error) {
	kvKey := fmt.Sprintf("workspace:%s:metadata", workspace)
	result, err := p.kvStore.Get(ctx, adminIdentity, kv.ScopeGlobal, kvKey, "", "")
	if err != nil || result == "" {
		return nil, nil
	}
	var metadata map[string]interface{}
	if err := json.Unmarshal([]byte(result), &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal workspace metadata: %w", err)
	}
	return metadata, nil
}

// saveWorkspaceMetadata marshals and stores workspace metadata in the KV store.
func (p *GatewayStateProvider) saveWorkspaceMetadata(ctx context.Context, workspace string, metadata map[string]interface{}) error {
	kvKey := fmt.Sprintf("workspace:%s:metadata", workspace)
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal workspace metadata: %w", err)
	}
	return p.kvStore.Set(ctx, adminIdentity, kv.ScopeGlobal, kvKey, string(metadataJSON), "", "", 0)
}

// =============================================================================
// Workspaces
// =============================================================================

func (p *GatewayStateProvider) GetWorkspaces(ctx context.Context) ([]*admin.WorkspaceInfo, error) {
	// Query distinct workspaces from sessions and tasks
	workspaceMap := make(map[string]*admin.WorkspaceInfo)

	// Get workspaces from active sessions
	if p.gateway != nil {
		p.gateway.activeStreams.Range(func(_, value interface{}) bool {
			if session, ok := value.(*ClientSession); ok && session.Identity.Workspace != "" {
				ws := session.Identity.Workspace
				if _, exists := workspaceMap[ws]; !exists {
					workspaceMap[ws] = &admin.WorkspaceInfo{
						WorkspaceID: ws,
						DisplayName: ws,
						CreatedAt:   time.Now(),
					}
				}
				// Count active connections by type
				switch session.Identity.Type {
				case models.PrincipalAgent:
					workspaceMap[ws].ActiveAgents++
				case models.PrincipalTask:
					workspaceMap[ws].ActiveTasks++
				case models.PrincipalUser:
					workspaceMap[ws].ActiveUsers++
				}
			}
			return true
		})
	}

	// Get workspaces from database if available
	if p.db != nil {
		rows, err := p.db.QueryContext(ctx, `
			SELECT DISTINCT workspace, MIN(created_at) as created_at
			FROM tasks
			WHERE workspace IS NOT NULL AND workspace != ''
			GROUP BY workspace
		`)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var ws string
				var createdAt time.Time
				if err := rows.Scan(&ws, &createdAt); err == nil {
					if _, exists := workspaceMap[ws]; !exists {
						workspaceMap[ws] = &admin.WorkspaceInfo{
							WorkspaceID: ws,
							DisplayName: ws,
							CreatedAt:   createdAt,
						}
					}
				}
			}
		}

		// Get task counts per workspace
		rows, err = p.db.QueryContext(ctx, `
			SELECT workspace, COUNT(*) as task_count
			FROM tasks
			WHERE workspace IS NOT NULL AND workspace != ''
			GROUP BY workspace
		`)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var ws string
				var count int64
				if err := rows.Scan(&ws, &count); err == nil {
					if info, exists := workspaceMap[ws]; exists {
						info.TotalMessages = count
					}
				}
			}
		}
	}

	// Load workspace metadata from KV store if available
	if p.kvStore != nil {
		for wsID, wsInfo := range workspaceMap {
			metadata, err := p.loadWorkspaceMetadata(ctx, wsID)
			if err == nil && metadata != nil {
				if displayName, ok := metadata["display_name"].(string); ok {
					wsInfo.DisplayName = displayName
				}
				if description, ok := metadata["description"].(string); ok {
					wsInfo.Description = description
				}
				if tenantID, ok := metadata["tenant_id"].(string); ok {
					wsInfo.TenantID = tenantID
				}
				wsInfo.Metadata = metadata
			}
		}
	}

	// Convert map to slice
	var workspaces []*admin.WorkspaceInfo
	for _, ws := range workspaceMap {
		ws.UpdatedAt = time.Now()
		workspaces = append(workspaces, ws)
	}

	return workspaces, nil
}

func (p *GatewayStateProvider) GetWorkspaceByID(ctx context.Context, workspaceID string) (*admin.WorkspaceInfo, error) {
	wsInfo := &admin.WorkspaceInfo{
		WorkspaceID: workspaceID,
		DisplayName: workspaceID,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	// Count active connections for this workspace
	if p.gateway != nil {
		p.gateway.activeStreams.Range(func(_, value interface{}) bool {
			if session, ok := value.(*ClientSession); ok && session.Identity.Workspace == workspaceID {
				switch session.Identity.Type {
				case models.PrincipalAgent:
					wsInfo.ActiveAgents++
				case models.PrincipalTask:
					wsInfo.ActiveTasks++
				case models.PrincipalUser:
					wsInfo.ActiveUsers++
				}
			}
			return true
		})
	}

	// Get task statistics from database
	if p.db != nil {
		var count int64
		var createdAt time.Time
		err := p.db.QueryRowContext(ctx, `
			SELECT COUNT(*), MIN(created_at)
			FROM tasks
			WHERE workspace = $1
		`, workspaceID).Scan(&count, &createdAt)
		if err == nil {
			wsInfo.TotalMessages = count
			if !createdAt.IsZero() {
				wsInfo.CreatedAt = createdAt
			}
		}
	}

	// Load workspace metadata from KV store
	if p.kvStore != nil {
		metadata, err := p.loadWorkspaceMetadata(ctx, workspaceID)
		if err == nil && metadata != nil {
			if displayName, ok := metadata["display_name"].(string); ok {
				wsInfo.DisplayName = displayName
			}
			if description, ok := metadata["description"].(string); ok {
				wsInfo.Description = description
			}
			if tenantID, ok := metadata["tenant_id"].(string); ok {
				wsInfo.TenantID = tenantID
			}
			if createdAt, ok := metadata["created_at"].(string); ok {
				if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
					wsInfo.CreatedAt = t
				}
			}
			wsInfo.Metadata = metadata
		}
	}

	return wsInfo, nil
}

func (p *GatewayStateProvider) CreateWorkspace(ctx context.Context, workspace *admin.WorkspaceInfo) error {
	if p.kvStore == nil {
		return fmt.Errorf("kv store not available")
	}

	// Store workspace metadata in KV store
	metadata := map[string]interface{}{
		"workspace_id": workspace.WorkspaceID,
		"display_name": workspace.DisplayName,
		"description":  workspace.Description,
		"tenant_id":    workspace.TenantID,
		"created_at":   time.Now().Format(time.RFC3339),
	}

	// Merge any additional metadata
	if workspace.Metadata != nil {
		for k, v := range workspace.Metadata {
			if k != "workspace_id" && k != "created_at" {
				metadata[k] = v
			}
		}
	}

	if err := p.saveWorkspaceMetadata(ctx, workspace.WorkspaceID, metadata); err != nil {
		return fmt.Errorf("failed to store workspace metadata: %w", err)
	}

	// Publish event
	p.PublishEvent(&admin.Event{
		Type:      admin.EventTypeConnection,
		Action:    admin.EventActionCreated,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"workspace_id": workspace.WorkspaceID,
			"display_name": workspace.DisplayName,
		},
	})

	return nil
}

func (p *GatewayStateProvider) UpdateWorkspace(ctx context.Context, workspaceID string, workspace *admin.WorkspaceInfo) error {
	if p.kvStore == nil {
		return fmt.Errorf("kv store not available")
	}

	// Load existing metadata (ignore error — start fresh if none exists)
	metadata, _ := p.loadWorkspaceMetadata(ctx, workspaceID)
	if metadata == nil {
		metadata = make(map[string]interface{})
	}

	// Update fields
	metadata["workspace_id"] = workspaceID
	if workspace.DisplayName != "" {
		metadata["display_name"] = workspace.DisplayName
	}
	if workspace.Description != "" {
		metadata["description"] = workspace.Description
	}
	if workspace.TenantID != "" {
		metadata["tenant_id"] = workspace.TenantID
	}
	metadata["updated_at"] = time.Now().Format(time.RFC3339)

	// Merge any additional metadata
	if workspace.Metadata != nil {
		for k, v := range workspace.Metadata {
			if k != "workspace_id" && k != "created_at" {
				metadata[k] = v
			}
		}
	}

	if err := p.saveWorkspaceMetadata(ctx, workspaceID, metadata); err != nil {
		return fmt.Errorf("failed to update workspace metadata: %w", err)
	}

	// Publish event
	p.PublishEvent(&admin.Event{
		Type:      admin.EventTypeConnection,
		Action:    admin.EventActionUpdated,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"workspace_id": workspaceID,
			"display_name": workspace.DisplayName,
		},
	})

	return nil
}

func (p *GatewayStateProvider) DeleteWorkspace(ctx context.Context, workspaceID string) error {
	if p.kvStore == nil {
		return fmt.Errorf("kv store not available")
	}

	// Check if workspace has active connections
	activeConns := 0
	if p.gateway != nil {
		p.gateway.activeStreams.Range(func(_, value interface{}) bool {
			if session, ok := value.(*ClientSession); ok && session.Identity.Workspace == workspaceID {
				activeConns++
			}
			return true
		})
	}

	if activeConns > 0 {
		return fmt.Errorf("cannot delete workspace %s: has %d active connections", workspaceID, activeConns)
	}

	// Delete workspace metadata from KV store
	kvKey := fmt.Sprintf("workspace:%s:metadata", workspaceID)
	if err := p.kvStore.Delete(ctx, adminIdentity, kv.ScopeGlobal, kvKey, "", ""); err != nil {
		return fmt.Errorf("failed to delete workspace metadata: %w", err)
	}

	// Publish event
	p.PublishEvent(&admin.Event{
		Type:      admin.EventTypeConnection,
		Action:    admin.EventActionDeleted,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"workspace_id": workspaceID,
		},
	})

	return nil
}

func (p *GatewayStateProvider) GetMessageFlow(ctx context.Context, workspaceID string) (*admin.MessageFlowInfo, error) {
	nodes := make([]*admin.FlowNode, 0)
	nodeMap := make(map[string]*admin.FlowNode)

	// Build nodes from active connections in the workspace
	if p.gateway != nil {
		p.gateway.activeStreams.Range(func(_, value interface{}) bool {
			if session, ok := value.(*ClientSession); ok && session.Identity.Workspace == workspaceID {
				nodeID := session.Identity.ToTopic()
				if _, exists := nodeMap[nodeID]; !exists {
					node := &admin.FlowNode{
						ID:             nodeID,
						Label:          session.Identity.ID,
						Type:           string(session.Identity.Type),
						Status:         "online",
						Implementation: session.Identity.Implementation,
						Specifier:      session.Identity.Specifier,
						Topic:          nodeID,
					}
					nodeMap[nodeID] = node
					nodes = append(nodes, node)
				}
			}
			return true
		})
	}

	// For now, edges are empty - in future versions, we could track message flows
	// by instrumenting the router or using metrics
	edges := make([]*admin.FlowEdge, 0)

	return &admin.MessageFlowInfo{
		WorkspaceID: workspaceID,
		Nodes:       nodes,
		Edges:       edges,
		UpdatedAt:   time.Now(),
	}, nil
}

// =============================================================================
// Workspace Rate Limits
// =============================================================================

func (p *GatewayStateProvider) SetWorkspaceRateLimit(workspace string, rate float64) error {
	if p.workspaceRateLimiter == nil {
		return fmt.Errorf("workspace rate limiter not configured")
	}
	p.workspaceRateLimiter.SetWorkspaceRate(workspace, rate)
	return nil
}

func (p *GatewayStateProvider) GetWorkspaceRateLimit(workspace string) (float64, error) {
	if p.workspaceRateLimiter == nil {
		return 0, fmt.Errorf("workspace rate limiter not configured")
	}
	return p.workspaceRateLimiter.GetWorkspaceRate(workspace), nil
}

func (p *GatewayStateProvider) RemoveWorkspaceRateLimit(workspace string) error {
	if p.workspaceRateLimiter == nil {
		return fmt.Errorf("workspace rate limiter not configured")
	}
	p.workspaceRateLimiter.RemoveWorkspaceRate(workspace)
	return nil
}

func (p *GatewayStateProvider) ListWorkspaceRateLimits() (map[string]float64, error) {
	if p.workspaceRateLimiter == nil {
		return map[string]float64{}, nil
	}
	return p.workspaceRateLimiter.ListWorkspaceRates(), nil
}
