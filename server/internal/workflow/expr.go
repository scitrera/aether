package workflow

import (
	"fmt"
	"sync"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// ExprEngine evaluates expr-lang expressions with compiled program caching.
type ExprEngine struct {
	mu    sync.RWMutex
	cache map[string]*vm.Program
	max   int
}

func NewExprEngine(maxCacheSize int) *ExprEngine {
	if maxCacheSize <= 0 {
		maxCacheSize = 2048
	}
	return &ExprEngine{
		cache: make(map[string]*vm.Program, maxCacheSize),
		max:   maxCacheSize,
	}
}

// Evaluate compiles (with caching) and runs an expr-lang expression.
// The env map provides variables accessible in the expression.
// Returns the boolean result of the expression.
func (e *ExprEngine) Evaluate(expression string, env map[string]any) (bool, error) {
	if expression == "" {
		return true, nil
	}

	program, err := e.getOrCompile(expression)
	if err != nil {
		return false, fmt.Errorf("expr compile error: %w", err)
	}

	result, err := expr.Run(program, env)
	if err != nil {
		return false, fmt.Errorf("expr eval error: %w", err)
	}

	b, ok := result.(bool)
	if !ok {
		return false, fmt.Errorf("expr result is %T, expected bool", result)
	}
	return b, nil
}

// EvaluateAny compiles and runs an expression, returning the raw result.
func (e *ExprEngine) EvaluateAny(expression string, env map[string]any) (any, error) {
	if expression == "" {
		return nil, nil
	}

	program, err := e.getOrCompile(expression)
	if err != nil {
		return nil, fmt.Errorf("expr compile error: %w", err)
	}

	result, err := expr.Run(program, env)
	if err != nil {
		return nil, fmt.Errorf("expr eval error: %w", err)
	}
	return result, nil
}

func (e *ExprEngine) getOrCompile(expression string) (*vm.Program, error) {
	e.mu.RLock()
	if p, ok := e.cache[expression]; ok {
		e.mu.RUnlock()
		return p, nil
	}
	e.mu.RUnlock()

	program, err := expr.Compile(expression, expr.AllowUndefinedVariables())
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	if len(e.cache) >= e.max {
		// Evict ~25% of entries when cache is full
		count := 0
		for k := range e.cache {
			delete(e.cache, k)
			count++
			if count >= e.max/4 {
				break
			}
		}
	}
	e.cache[expression] = program
	e.mu.Unlock()

	return program, nil
}
