package workflow

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

// joinFakeStore is a minimal WorkflowStore used to exercise the join
// observability handlers (LIST_JOINS / GET_JOIN / CANCEL_JOIN). It embeds
// the WorkflowStore interface so the unused methods compile as nil; only the
// join-related methods are implemented against an in-memory slice.
type joinFakeStore struct {
	WorkflowStore
	joins []Join
}

func (f *joinFakeStore) ListJoins(_ context.Context, workspace string) ([]Join, error) {
	if workspace == "" || workspace == "*" {
		return f.joins, nil
	}
	var out []Join
	for _, j := range f.joins {
		if j.Workspace == workspace {
			out = append(out, j)
		}
	}
	return out, nil
}

func (f *joinFakeStore) GetJoin(_ context.Context, joinName, workspace, correlationKey string) (*Join, error) {
	for i := range f.joins {
		j := &f.joins[i]
		if j.JoinName == joinName && j.Workspace == workspace && j.CorrelationKey == correlationKey {
			return j, nil
		}
	}
	return nil, nil
}

func (f *joinFakeStore) MarkJoinTerminal(_ context.Context, id int64, status string, lingerUntil time.Time) error {
	for i := range f.joins {
		if f.joins[i].ID == id {
			f.joins[i].Status = status
			f.joins[i].LingerUntil = &lingerUntil
			return nil
		}
	}
	return nil
}

func seededJoinServer() (*Server, *joinFakeStore) {
	store := &joinFakeStore{
		joins: []Join{
			{ID: 1, JoinName: "j1", Workspace: "ws", CorrelationKey: "c1", Mode: JoinModeCount, ArrivedCount: 1, Status: JoinStatusOpen},
			{ID: 2, JoinName: "j2", Workspace: "ws", CorrelationKey: "c2", Mode: JoinModeSet, ArrivedCount: 3, Status: JoinStatusFired},
		},
	}
	return &Server{store: store}, store
}

func TestHandleListJoins_returnsSeededJoins(t *testing.T) {
	srv, _ := seededJoinServer()
	resp, err := srv.handleWorkflowOperation(context.Background(), &pb.WorkflowOperation{
		Op:        pb.WorkflowOperation_LIST_JOINS,
		Workspace: "ws",
	})
	if err != nil {
		t.Fatalf("handleListJoins err: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success, got error: %s", resp.Error)
	}
	var views []joinView
	if err := json.Unmarshal(resp.Data, &views); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(views) != 2 {
		t.Fatalf("expected 2 joins, got %d", len(views))
	}
}

func TestHandleGetJoin_found(t *testing.T) {
	srv, _ := seededJoinServer()
	resp, err := srv.handleWorkflowOperation(context.Background(), &pb.WorkflowOperation{
		Op:          pb.WorkflowOperation_GET_JOIN,
		Id:          "j1",
		Workspace:   "ws",
		SecondaryId: "c1",
	})
	if err != nil {
		t.Fatalf("handleGetJoin err: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success, got error: %s", resp.Error)
	}
	var view joinView
	if err := json.Unmarshal(resp.Data, &view); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if view.JoinName != "j1" || view.CorrelationKey != "c1" {
		t.Fatalf("unexpected join view: %+v", view)
	}
}

func TestHandleGetJoin_missingReturnsNotFound(t *testing.T) {
	srv, _ := seededJoinServer()
	resp, err := srv.handleWorkflowOperation(context.Background(), &pb.WorkflowOperation{
		Op:          pb.WorkflowOperation_GET_JOIN,
		Id:          "nope",
		Workspace:   "ws",
		SecondaryId: "x",
	})
	if err != nil {
		t.Fatalf("handleGetJoin err: %v", err)
	}
	if resp.Success {
		t.Fatal("expected failure for missing join")
	}
	if resp.Error != "join not found" {
		t.Fatalf("expected 'join not found', got %q", resp.Error)
	}
}

func TestHandleCancelJoin_flipsToCancelled(t *testing.T) {
	srv, store := seededJoinServer()
	resp, err := srv.handleWorkflowOperation(context.Background(), &pb.WorkflowOperation{
		Op:          pb.WorkflowOperation_CANCEL_JOIN,
		Id:          "j1",
		Workspace:   "ws",
		SecondaryId: "c1",
	})
	if err != nil {
		t.Fatalf("handleCancelJoin err: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success, got error: %s", resp.Error)
	}
	if store.joins[0].Status != JoinStatusCancelled {
		t.Fatalf("expected join status %q, got %q", JoinStatusCancelled, store.joins[0].Status)
	}
}

func TestHandleCancelJoin_idempotentWhenTerminal(t *testing.T) {
	srv, store := seededJoinServer()
	// j2 is already Fired (terminal).
	resp, err := srv.handleWorkflowOperation(context.Background(), &pb.WorkflowOperation{
		Op:          pb.WorkflowOperation_CANCEL_JOIN,
		Id:          "j2",
		Workspace:   "ws",
		SecondaryId: "c2",
	})
	if err != nil {
		t.Fatalf("handleCancelJoin err: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected idempotent success, got error: %s", resp.Error)
	}
	if store.joins[1].Status != JoinStatusFired {
		t.Fatalf("terminal join must not change status, got %q", store.joins[1].Status)
	}
}
