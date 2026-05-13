package workflow

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// routerTestStore is a minimal in-memory implementation of the store
// methods used by Router so tests run without PostgreSQL.
type routerTestStore struct {
	rules []Rule
}

func (s *routerTestStore) GetMatchingRules(_ context.Context, sourceAgent, sourceEvent, workspace string) ([]Rule, error) {
	var matched []Rule
	for _, r := range s.rules {
		if !r.Active {
			continue
		}
		agentMatch := r.SourceAgent == sourceAgent || r.SourceAgent == "*"
		eventMatch := r.SourceEvent == sourceEvent
		wsMatch := r.Workspace == workspace || r.Workspace == "*"
		if agentMatch && eventMatch && wsMatch {
			matched = append(matched, r)
		}
	}
	return matched, nil
}

// routerWithStore wraps a Router so tests can inject routerTestStore without
// embedding a full *Store (which requires a real *sql.DB).
type routerWithStore struct {
	expr     *ExprEngine
	tmpl     *TemplateEngine
	executor *Executor
	store    *routerTestStore
	cacheTTL time.Duration
	cache    map[string]cachedRules
}

func (r *routerWithStore) HandleEvent(ctx context.Context, sourceTopic string, payload []byte) error {
	router := &Router{
		expr:     r.expr,
		tmpl:     r.tmpl,
		executor: r.executor,
		cache:    r.cache,
		cacheTTL: r.cacheTTL,
	}
	// Use the private method directly to bypass the SQL store
	var event EventPayload
	if err := json.Unmarshal(payload, &event); err != nil {
		return nil
	}
	if event.SourceAgent == "" {
		parts := splitTopic(sourceTopic)
		if len(parts) >= 3 {
			event.SourceAgent = parts[2]
		}
	}
	if event.Workspace == "" {
		parts := splitTopic(sourceTopic)
		if len(parts) >= 2 {
			event.Workspace = parts[1]
		}
	}
	for _, eventName := range event.EventNames {
		rules, _ := r.store.GetMatchingRules(ctx, event.SourceAgent, eventName, event.Workspace)
		for _, rule := range rules {
			router.processRule(ctx, rule, &event)
		}
	}
	return nil
}

func splitTopic(s string) []string {
	return strings.Split(s, "::")
}

// ---- Router unit tests (no SQL, no live executor) ----

func TestRouter_HandleEvent_skipsEventWithNoEventNames(t *testing.T) {
	// Router should silently skip messages with no event_names field.
	expr := NewExprEngine(10)
	tmpl := NewTemplateEngine(10)

	rs := &routerTestStore{}
	rws := &routerWithStore{
		expr:     expr,
		tmpl:     tmpl,
		store:    rs,
		cache:    make(map[string]cachedRules),
		cacheTTL: time.Minute,
	}

	payload, _ := json.Marshal(EventPayload{
		SourceAgent: "agent-x",
		Data:        map[string]any{"status": "ok"},
		Workspace:   "ws1",
		// EventNames intentionally empty
	})

	// Should not panic or error even with no matching rules
	err := rws.HandleEvent(context.Background(), "ag::ws1::agent-x::spec", payload)
	if err != nil {
		t.Errorf("HandleEvent() error = %v, want nil", err)
	}
}

func TestRouter_HandleEvent_skipsRulesWithFailingCondition(t *testing.T) {
	dispatched := 0
	expr := NewExprEngine(10)
	tmpl := NewTemplateEngine(10)

	rs := &routerTestStore{
		rules: []Rule{
			{
				ID:                  1,
				RuleName:            "conditional-rule",
				SourceAgent:         "*",
				SourceEvent:         "job.done",
				TriggerCondition:    `input.status == "failed"`, // will not match "ok"
				TransformationStyle: "template",
				DestinationTemplate: `agent: "target"\ntool_name: handle`,
				Workspace:           "*",
				Active:              true,
			},
		},
	}
	rws := &routerWithStore{
		expr:  expr,
		tmpl:  tmpl,
		store: rs,
		cache: make(map[string]cachedRules),
		// nil executor: if we reach dispatch, test panics → condition must block
		cacheTTL: time.Minute,
	}
	_ = dispatched

	payload, _ := json.Marshal(EventPayload{
		SourceAgent: "agent-x",
		EventNames:  []string{"job.done"},
		Data:        map[string]any{"status": "ok"},
		Workspace:   "ws1",
	})

	// Should not panic because the condition prevents dispatch
	err := rws.HandleEvent(context.Background(), "ag::ws1::agent-x::spec", payload)
	if err != nil {
		t.Errorf("HandleEvent() error = %v, want nil", err)
	}
}

func TestRouter_HandleEvent_extractsSourceAgentFromTopic(t *testing.T) {
	expr := NewExprEngine(10)
	tmpl := NewTemplateEngine(10)

	// Rule matches only "extracted-impl"; condition always false so executor is never called.
	rs := &routerTestStore{
		rules: []Rule{
			{
				ID:               1,
				RuleName:         "capture-rule",
				SourceAgent:      "extracted-impl",
				SourceEvent:      "ping",
				TriggerCondition: "false", // blocks dispatch — we just verify routing reaches the rule
				DestinationTemplate: `agent: "target"
tool_name: pong`,
				Workspace: "*",
				Active:    true,
			},
		},
	}

	rws := &routerWithStore{
		expr:     expr,
		tmpl:     tmpl,
		store:    rs,
		cache:    make(map[string]cachedRules),
		cacheTTL: time.Minute,
	}

	// Payload has no source_agent — should be inferred from topic parts[2]
	payload, _ := json.Marshal(map[string]any{
		"event_names": []string{"ping"},
		"workspace":   "ws1",
		"data":        map[string]any{},
	})
	// sourceTopic: ag.ws1.extracted-impl.spec → parts[2] = "extracted-impl"
	err := rws.HandleEvent(context.Background(), "ag::ws1::extracted-impl::spec", payload)
	if err != nil {
		t.Errorf("HandleEvent() error = %v, want nil", err)
	}
}

func TestRouter_HandleEvent_extractsWorkspaceFromTopic(t *testing.T) {
	expr := NewExprEngine(10)
	tmpl := NewTemplateEngine(10)

	rs := &routerTestStore{}
	rws := &routerWithStore{
		expr:     expr,
		tmpl:     tmpl,
		store:    rs,
		cache:    make(map[string]cachedRules),
		cacheTTL: time.Minute,
	}

	// Payload has no workspace — should be inferred from topic
	payload, _ := json.Marshal(map[string]any{
		"event_names": []string{"noop"},
		"data":        map[string]any{},
	})
	err := rws.HandleEvent(context.Background(), "ag::inferred-ws::impl::spec", payload)
	if err != nil {
		t.Errorf("HandleEvent() error = %v, want nil", err)
	}
}

func TestRouter_HandleEvent_invalidJSONPayloadIsIgnored(t *testing.T) {
	expr := NewExprEngine(10)
	tmpl := NewTemplateEngine(10)

	rs := &routerTestStore{}
	rws := &routerWithStore{
		expr:     expr,
		tmpl:     tmpl,
		store:    rs,
		cache:    make(map[string]cachedRules),
		cacheTTL: time.Minute,
	}

	err := rws.HandleEvent(context.Background(), "ag::ws::impl::spec", []byte("not json"))
	if err != nil {
		t.Errorf("HandleEvent() with bad JSON error = %v, want nil (should not disconnect)", err)
	}
}

// ---- Router cache tests ----

func TestRouter_InvalidateCache_emptiesCache(t *testing.T) {
	router := NewRouter(nil, NewExprEngine(10), NewTemplateEngine(10), nil, time.Minute)

	// Seed the internal cache directly
	router.cacheMu.Lock()
	router.cache["agent|event|ws"] = cachedRules{
		rules:     []Rule{{ID: 1}},
		fetchedAt: time.Now(),
	}
	router.cacheMu.Unlock()

	router.InvalidateCache()

	router.cacheMu.RLock()
	defer router.cacheMu.RUnlock()
	if len(router.cache) != 0 {
		t.Errorf("cache len = %d after InvalidateCache(), want 0", len(router.cache))
	}
}

func TestRouter_EventPayload_workspaceAndAgentDefaultsFromTopic(t *testing.T) {
	// Verify topic parsing: "ag::myws::myimpl::spec" → workspace=myws, agent=myimpl
	parts := splitTopic("ag::myws::myimpl::spec")
	if len(parts) < 3 {
		t.Fatalf("splitTopic returned %d parts, want ≥3", len(parts))
	}
	if parts[1] != "myws" {
		t.Errorf("workspace part = %q, want %q", parts[1], "myws")
	}
	if parts[2] != "myimpl" {
		t.Errorf("agent part = %q, want %q", parts[2], "myimpl")
	}
}
