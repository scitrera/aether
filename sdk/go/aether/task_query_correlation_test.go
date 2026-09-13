// SPDX-License-Identifier: Apache-2.0
package aether

import (
	"context"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
)

func TestTaskQueriesCorrelateOutOfOrderResponses(t *testing.T) {
	client, err := NewBaseClient(BaseClientConfig{ServerAddr: TestServerAddr})
	if err != nil {
		t.Fatal(err)
	}
	client.running.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type result struct {
		want     string
		response *TaskQueryResponse
		err      error
	}
	results := make(chan result, 3)
	for _, id := range []string{"task-a", "task-b", ""} {
		go func(id string) {
			var response *TaskQueryResponse
			var err error
			if id == "" {
				response, err = client.QueryTasks(ctx, &pb.TaskFilter{Workspace: "default"}, time.Second)
			} else {
				response, err = client.GetTask(ctx, id, time.Second)
			}
			results <- result{id, response, err}
		}(id)
	}
	queries := make([]*pb.TaskQuery, 0, 3)
	ids := map[string]bool{}
	for len(queries) < 3 {
		select {
		case message := <-client.RequestQueue():
			query := message.GetTaskQuery()
			if query == nil || query.RequestId == "" || ids[query.RequestId] {
				t.Fatal("task query missing unique correlation ID")
			}
			ids[query.RequestId] = true
			queries = append(queries, query)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	for i := len(queries) - 1; i >= 0; i-- {
		query := queries[i]
		response := &pb.TaskQueryResponse{Success: true, RequestId: query.RequestId}
		if query.Op == pb.TaskQuery_GET {
			response.Task = &pb.TaskInfo{TaskId: query.TaskId}
		} else {
			response.Tasks = []*pb.TaskInfo{{TaskId: "listed-task"}}
			response.TotalCount = 1
		}
		if err := client.handleTaskQueryResponse(ctx, response); err != nil {
			t.Fatal(err)
		}
	}
	for range queries {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.want == "" {
			if got.response.Task != nil || len(got.response.Tasks) != 1 || got.response.Tasks[0].TaskID != "listed-task" {
				t.Fatalf("list received another query response: %+v", got.response)
			}
		} else if got.response.Task == nil || got.response.Task.TaskID != got.want {
			t.Fatalf("%s received another task", got.want)
		}
	}
}
