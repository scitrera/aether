package workflow

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// Router matches incoming events against rules, evaluates conditions,
// transforms payloads, and dispatches actions.
type Router struct {
	store    *Store
	expr     *ExprEngine
	tmpl     *TemplateEngine
	executor *Executor

	// Rule cache
	cacheMu  sync.RWMutex
	cache    map[string]cachedRules
	cacheTTL time.Duration
}

type cachedRules struct {
	rules     []Rule
	fetchedAt time.Time
}

func NewRouter(store *Store, expr *ExprEngine, tmpl *TemplateEngine, executor *Executor, cacheTTL time.Duration) *Router {
	return &Router{
		store:    store,
		expr:     expr,
		tmpl:     tmpl,
		executor: executor,
		cache:    make(map[string]cachedRules),
		cacheTTL: cacheTTL,
	}
}

// EventPayload is the expected structure of an incoming event message.
type EventPayload struct {
	SourceAgent string   `json:"source_agent"`
	EventNames  []string `json:"event_names"`
	Data        any      `json:"data"`
	Workspace   string   `json:"workspace"`
}

// HandleEvent processes an incoming event message, matching it against rules.
func (r *Router) HandleEvent(ctx context.Context, sourceTopic string, payload []byte) error {
	var event EventPayload
	if err := json.Unmarshal(payload, &event); err != nil {
		log.Warn().Err(err).Str("source", sourceTopic).Msg("failed to parse event payload")
		return nil // Don't return error to avoid disconnection
	}

	if event.SourceAgent == "" {
		// Try to extract agent impl from source topic (ag.workspace.impl.spec)
		parts := strings.Split(sourceTopic, ".")
		if len(parts) >= 3 {
			event.SourceAgent = parts[2]
		}
	}

	if event.Workspace == "" {
		// Extract workspace from source topic
		parts := strings.Split(sourceTopic, ".")
		if len(parts) >= 2 {
			event.Workspace = parts[1]
		}
	}

	if len(event.EventNames) == 0 {
		log.Debug().Str("source", sourceTopic).Msg("event has no event_names, skipping")
		return nil
	}

	matchCount := 0
	for _, eventName := range event.EventNames {
		rules, err := r.getMatchingRules(ctx, event.SourceAgent, eventName, event.Workspace)
		if err != nil {
			log.Error().Err(err).Str("event", eventName).Msg("failed to get matching rules")
			continue
		}

		for _, rule := range rules {
			if err := r.processRule(ctx, rule, &event); err != nil {
				log.Error().Err(err).
					Int("rule_id", rule.ID).
					Str("rule_name", rule.RuleName).
					Msg("failed to process rule")
				continue
			}
			matchCount++
		}
	}

	if matchCount > 0 {
		log.Debug().
			Int("matches", matchCount).
			Str("source_agent", event.SourceAgent).
			Strs("events", event.EventNames).
			Msg("event processed")
	}

	return nil
}

func (r *Router) processRule(ctx context.Context, rule Rule, event *EventPayload) error {
	// Build environment for expression evaluation
	env := map[string]any{
		"input": event.Data,
		"source": map[string]any{
			"agent":     event.SourceAgent,
			"workspace": event.Workspace,
		},
	}

	// Evaluate trigger condition
	if rule.TriggerCondition != "" {
		matched, err := r.expr.Evaluate(rule.TriggerCondition, env)
		if err != nil {
			return err
		}
		if !matched {
			return nil
		}
	}

	// Transform the output
	result, err := r.tmpl.Transform(rule.DestinationTemplate, env)
	if err != nil {
		return err
	}

	// Dispatch the action
	return r.executor.DispatchTransformResult(result)
}

func (r *Router) getMatchingRules(ctx context.Context, sourceAgent, sourceEvent, workspace string) ([]Rule, error) {
	cacheKey := sourceAgent + "|" + sourceEvent + "|" + workspace

	r.cacheMu.RLock()
	if cached, ok := r.cache[cacheKey]; ok && time.Since(cached.fetchedAt) < r.cacheTTL {
		r.cacheMu.RUnlock()
		return cached.rules, nil
	}
	r.cacheMu.RUnlock()

	rules, err := r.store.GetMatchingRules(ctx, sourceAgent, sourceEvent, workspace)
	if err != nil {
		return nil, err
	}

	r.cacheMu.Lock()
	r.cache[cacheKey] = cachedRules{rules: rules, fetchedAt: time.Now()}
	r.cacheMu.Unlock()

	return rules, nil
}

// InvalidateCache clears the rule cache.
func (r *Router) InvalidateCache() {
	r.cacheMu.Lock()
	r.cache = make(map[string]cachedRules)
	r.cacheMu.Unlock()
}
