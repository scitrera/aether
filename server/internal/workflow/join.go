package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/scitrera/aether/internal/kv"
	"github.com/scitrera/aether/sdk/go/coord"
)

// JoinSpec is the declarative description of a fan-in / barrier / coalesce
// destination, parsed from the `join:` block of a rendered rule destination.
//
// CorrelationKey / ExpectedCount / DedupKey are expr-lang expressions evaluated
// against the arrival env ({input, source}) — the same dialect as a rule's
// TriggerCondition. OnComplete / OnTimeout are ordinary ActionDefs whose Go
// text/template fields were already rendered when the enclosing destination was
// transformed, so by the time the engine sees them they are concrete.
type JoinSpec struct {
	Name           string `yaml:"name" json:"name"`
	CorrelationKey string `yaml:"correlation_key" json:"correlation_key"`
	Mode           string `yaml:"mode" json:"mode"` // count | coalesce | set

	// ExpectedCount (count mode): expr-lang yielding an integer. Empty until an
	// arm_on_event arrival supplies it (dynamic-N).
	ExpectedCount string `yaml:"expected_count" json:"expected_count"`
	// ArmOnEvent: the event_name whose arrival carries ExpectedCount. Empty means
	// ExpectedCount (if set) is evaluable on every arrival.
	ArmOnEvent string `yaml:"arm_on_event" json:"arm_on_event"`

	// DedupKey: expr-lang yielding this arrival's dedup id. Empty disables dedup.
	DedupKey string `yaml:"dedup_key" json:"dedup_key"`

	// ExpectedSet (set mode): expr-lang yielding a list of member ids; the join
	// completes when every distinct member has reported (cardinality == len).
	ExpectedSet string `yaml:"expected_set" json:"expected_set"`
	// MemberKey (set mode): expr-lang yielding THIS arrival's member id. The set
	// dedups members inherently, so set mode needs no separate dedup_key.
	MemberKey string `yaml:"member_key" json:"member_key"`

	// Window (coalesce mode): debounce/cooldown duration, e.g. "5s". Within the
	// window after a firing, further arrivals coalesce (set dirty) instead of
	// firing. Empty/0 ⇒ no debounce (each arrival fires).
	Window string `yaml:"window" json:"window"`
	// Timeout: barrier deadline, e.g. "15m". Drives the deadline sweep.
	Timeout string `yaml:"timeout" json:"timeout"`
	// Linger: late-arrival retention after terminal, e.g. "1m". Defaults applied
	// by the engine when empty.
	Linger string `yaml:"linger" json:"linger"`

	OnComplete       *ActionDef `yaml:"on_complete" json:"on_complete"`
	OnTimeout        *ActionDef `yaml:"on_timeout" json:"on_timeout"`
	OnPartialFailure string     `yaml:"on_partial_failure" json:"on_partial_failure"`
}

// joinCounterCeiling is a sentinel ceiling that the per-correlation arrival
// counter will never reach, so IncrementIf(+1) always applies and returns this
// arrival's monotonic sequence number. Completeness is judged against the
// (separately tracked) expected count, and exactly-once firing is gated by a
// SetNX fire-marker — not by the counter hitting a real ceiling.
const joinCounterCeiling int64 = 1 << 40

// joinDefaultLinger is the retention window after a join goes terminal, during
// which late arrivals are dropped rather than resurrecting a new instance.
const joinDefaultLinger = time.Minute

// joinStore is the narrow slice of WorkflowStore the join engine needs. Keeping
// it small (interface segregation) makes the engine unit-testable with a tiny
// in-memory fake; the full *Store / WorkflowStore satisfies it structurally.
type joinStore interface {
	EnsureJoin(ctx context.Context, j *Join) (*Join, error)
	GetJoin(ctx context.Context, joinName, workspace, correlationKey string) (*Join, error)
	UpdateJoinArrived(ctx context.Context, id int64, arrivedCount int64) error
	SetJoinExpected(ctx context.Context, id int64, expected int64) error
	SetJoinDirty(ctx context.Context, id int64, dirty bool) error
	MarkJoinTerminal(ctx context.Context, id int64, status string, lingerUntil time.Time) error
}

// actionDispatcher is the slice of Executor the join engine uses to run a
// fired action. *Executor satisfies it; tests inject a recording fake.
type actionDispatcher interface {
	DispatchAction(action *ActionDef) error
	EmitEvent(action *ActionDef) error
}

// joinSet is the atomic set primitive backing set-mode membership: SetAdd
// returns whether the member was newly added and the cardinality after the add.
// Production adapts *aether.KV (SetAddSync) to it; tests inject a fake. nil when
// set mode is unavailable (e.g. a backend without the gRPC set ops wired).
type joinSet interface {
	SetAdd(ctx context.Context, key, member string, ttl time.Duration) (added bool, cardinality int64, err error)
}

// JoinEngine drives fan-in joins. It depends on abstract coord primitives
// (Counter for the atomic arrival counter, Locker for SetNX-style dedup and
// fire-marker gates) so it is unit-testable without a live gateway; production
// wires these from the WorkflowEngine client's KV (client.KV().Counter/.Locker
// on the global scope, keys under the reserved _sys/coord/ infra fast-path).
type JoinEngine struct {
	store      joinStore
	expr       *ExprEngine
	dispatcher actionDispatcher
	counter    coord.Counter
	locker     coord.Locker
	set        joinSet

	defaultWorkspace string
}

// NewJoinEngine constructs a JoinEngine. counter/locker/set must be bound to a
// shared scope (typically global) so all replicas rendezvous on the same keys.
// set may be nil if the deployment has not wired the gRPC set ops (set-mode
// joins then return an error rather than silently mis-firing).
func NewJoinEngine(store joinStore, expr *ExprEngine, dispatcher actionDispatcher, counter coord.Counter, locker coord.Locker, set joinSet, defaultWorkspace string) *JoinEngine {
	return &JoinEngine{
		store:            store,
		expr:             expr,
		dispatcher:       dispatcher,
		counter:          counter,
		locker:           locker,
		set:              set,
		defaultWorkspace: defaultWorkspace,
	}
}

// HandleArrival processes one event arrival against a join destination. It
// evaluates the correlation key, applies dedup, mirrors arrival state to the
// store, and fires OnComplete exactly once when the join completes.
func (je *JoinEngine) HandleArrival(ctx context.Context, spec *JoinSpec, env map[string]any, eventName, workspace string) error {
	if spec == nil || spec.Name == "" {
		return fmt.Errorf("join: spec.name is required")
	}
	if workspace == "" {
		workspace = je.defaultWorkspace
	}

	corr, err := je.evalString(spec.CorrelationKey, env)
	if err != nil {
		return fmt.Errorf("join %q: correlation_key eval: %w", spec.Name, err)
	}
	if corr == "" {
		return fmt.Errorf("join %q: correlation_key evaluated empty (degenerate key) — producer must stamp a shared correlation id", spec.Name)
	}

	mode := spec.Mode
	if mode == "" {
		mode = JoinModeCount
	}
	base := joinBaseKey(spec.Name, workspace, corr)
	ttl := je.retentionTTL(spec)

	// Dedup (N3): the first arrival with a given dedup id wins the slot; repeats
	// are dropped. Uses TryAcquire (SetNX) keyed per dedup id under the join base.
	if spec.DedupKey != "" {
		dedupID, derr := je.evalString(spec.DedupKey, env)
		if derr != nil {
			return fmt.Errorf("join %q: dedup_key eval: %w", spec.Name, derr)
		}
		if dedupID != "" {
			fresh, aerr := je.locker.TryAcquire(ctx, base+"/d/"+hashSeg(dedupID), "1", ttl)
			if aerr != nil {
				return fmt.Errorf("join %q: dedup check: %w", spec.Name, aerr)
			}
			if !fresh {
				log.Debug().Str("join", spec.Name).Str("corr", corr).Str("dedup", dedupID).Msg("join: dropping duplicate arrival")
				return nil
			}
		}
	}

	// Ensure the durable mirror row exists (idempotent). expected_count is set
	// now only when known on this arrival (literal/expr not gated by arm_on_event).
	expected, expectedKnown, eerr := je.evalExpected(spec, env, eventName)
	if eerr != nil {
		return fmt.Errorf("join %q: expected_count eval: %w", spec.Name, eerr)
	}
	row, err := je.ensureRow(ctx, spec, mode, workspace, corr, expected, expectedKnown)
	if err != nil {
		return fmt.Errorf("join %q: ensure row: %w", spec.Name, err)
	}

	switch mode {
	case JoinModeCoalesce:
		return je.handleCoalesceArrival(ctx, spec, row, base, ttl)
	case JoinModeCount:
		return je.handleCountArrival(ctx, spec, row, base, eventName, env, ttl)
	case JoinModeSet:
		return je.handleSetArrival(ctx, spec, row, base, env, ttl)
	default:
		return fmt.Errorf("join %q: unknown mode %q", spec.Name, mode)
	}
}

// handleCountArrival implements the count barrier. A *member* arrival bumps the
// atomic counter (its returned value is the arrival's sequence number) and fires
// when the count reaches the known expected value. An *arming* arrival — one
// whose event name is arm_on_event — supplies expected_count but is NOT itself a
// member (arming typically rides a different event, e.g. a manifest), so it does
// not increment the counter; it just records expected and re-checks completeness
// against the current count. Exactly-once firing is gated by the fire-marker.
func (je *JoinEngine) handleCountArrival(ctx context.Context, spec *JoinSpec, row *Join, base, eventName string, env map[string]any, ttl time.Duration) error {
	if spec.ArmOnEvent != "" && eventName == spec.ArmOnEvent {
		armVal, ok, err := je.evalInt(spec.ExpectedCount, env)
		if err != nil {
			return fmt.Errorf("join %q: arm expected_count eval: %w", spec.Name, err)
		}
		if !ok {
			return nil // arm event without a usable count — nothing to arm
		}
		if err := je.store.SetJoinExpected(ctx, row.ID, armVal); err != nil {
			return fmt.Errorf("join %q: set expected: %w", spec.Name, err)
		}
		// Read the current member count (delta 0) so an arming that lands after
		// all members still completes the barrier.
		cur, _, rerr := je.counter.IncrementIf(ctx, base, 0, joinCounterCeiling)
		if rerr != nil {
			return fmt.Errorf("join %q: read counter: %w", spec.Name, rerr)
		}
		if cur >= armVal {
			return je.fire(ctx, spec, row, base, ttl, spec.OnComplete, false)
		}
		return nil
	}

	// Member arrival: increment and mirror.
	value, _, err := je.counter.IncrementIf(ctx, base, 1, joinCounterCeiling)
	if err != nil {
		return fmt.Errorf("join %q: increment: %w", spec.Name, err)
	}
	if err := je.store.UpdateJoinArrived(ctx, row.ID, value); err != nil {
		log.Warn().Err(err).Str("join", spec.Name).Msg("join: mirror arrived_count failed (non-fatal)")
	}
	if row.ExpectedCount != nil && value >= *row.ExpectedCount {
		return je.fire(ctx, spec, row, base, ttl, spec.OnComplete, false)
	}
	return nil
}

// handleCoalesceArrival collapses a burst into a single firing. The first
// arrival acquires the active/cooldown slot and fires; arrivals while the slot
// is held set the dirty flag (a trailing run is driven later by the deadline
// sweep or the next post-cooldown arrival).
func (je *JoinEngine) handleCoalesceArrival(ctx context.Context, spec *JoinSpec, row *Join, base string, ttl time.Duration) error {
	cooldown := je.parseDuration(spec.Window, 0)
	gateTTL := cooldown
	if gateTTL <= 0 {
		gateTTL = ttl // no debounce window: still bound the gate so a crashed firer self-heals
	}
	won, err := je.locker.TryAcquire(ctx, base+"/active", "1", gateTTL)
	if err != nil {
		return fmt.Errorf("join %q: coalesce gate: %w", spec.Name, err)
	}
	if !won {
		// A firing is in flight / cooling down — record that more work arrived.
		if derr := je.store.SetJoinDirty(ctx, row.ID, true); derr != nil {
			log.Warn().Err(derr).Str("join", spec.Name).Msg("join: set dirty failed (non-fatal)")
		}
		return nil
	}
	if derr := je.store.SetJoinDirty(ctx, row.ID, false); derr != nil {
		log.Warn().Err(derr).Str("join", spec.Name).Msg("join: clear dirty failed (non-fatal)")
	}
	return je.runAction(ctx, spec, spec.OnComplete)
}

// fire elects a single firer via the fire-marker gate and runs the given action
// exactly once, then marks the row terminal. degraded indicates a timeout-path
// firing (recorded as fired here; the deadline sweep sets timed_out separately).
func (je *JoinEngine) fire(ctx context.Context, spec *JoinSpec, row *Join, base string, ttl time.Duration, action *ActionDef, degraded bool) error {
	won, err := je.locker.TryAcquire(ctx, base+"/fired", "1", ttl)
	if err != nil {
		return fmt.Errorf("join %q: fire gate: %w", spec.Name, err)
	}
	if !won {
		return nil // another arrival already fired this instance
	}
	log.Info().Str("join", spec.Name).Str("corr", row.CorrelationKey).Int64("arrived", row.ArrivedCount).Bool("degraded", degraded).Msg("join: firing")
	if aerr := je.runAction(ctx, spec, action); aerr != nil {
		return aerr
	}
	return je.store.MarkJoinTerminal(ctx, row.ID, JoinStatusFired, time.Now().Add(je.retentionLinger(spec)))
}

// HandleDeadline is invoked by the scheduler sweep for an open join past its
// deadline. Policy on_partial_failure: "abort" marks timed_out without firing;
// "proceed"/"proceed_degraded"/"" fire the persisted on_timeout action (falling
// back to on_complete) exactly once via the fire-marker gate, then mark timed_out.
func (je *JoinEngine) HandleDeadline(ctx context.Context, j *Join) error {
	if j.Status != JoinStatusOpen {
		return nil
	}

	base := joinBaseKey(j.JoinName, j.Workspace, j.CorrelationKey)
	linger := joinDefaultLinger
	term := time.Now().Add(linger)

	// Abort policy: the barrier failed — go terminal without firing any action.
	if j.OnPartialFailure == "abort" {
		if err := je.store.MarkJoinTerminal(ctx, j.ID, JoinStatusTimedOut, term); err != nil {
			return fmt.Errorf("join %q: mark timed_out: %w", j.JoinName, err)
		}
		log.Info().Str("join", j.JoinName).Str("corr", j.CorrelationKey).Msg("join: deadline reached, abort policy — timed out without firing")
		return nil
	}

	// Proceed (possibly degraded): fire the timeout action, falling back to the
	// completion action when no dedicated timeout action was persisted.
	actionJSON := j.OnTimeout
	if actionJSON == "" {
		actionJSON = j.OnComplete
	}
	if actionJSON == "" {
		// Nothing to fire — just go terminal.
		if err := je.store.MarkJoinTerminal(ctx, j.ID, JoinStatusTimedOut, term); err != nil {
			return fmt.Errorf("join %q: mark timed_out: %w", j.JoinName, err)
		}
		log.Info().Str("join", j.JoinName).Str("corr", j.CorrelationKey).Msg("join: deadline reached, no timeout action — timed out")
		return nil
	}

	var action ActionDef
	if err := json.Unmarshal([]byte(actionJSON), &action); err != nil {
		return fmt.Errorf("join %q: unmarshal timeout action: %w", j.JoinName, err)
	}

	// Fire-marker gate: bounded TTL prevents a double-fire against a late
	// completion arrival that might also reach the barrier.
	won, err := je.locker.TryAcquire(ctx, base+"/fired", "1", joinDefaultLinger+time.Hour)
	if err != nil {
		return fmt.Errorf("join %q: deadline fire gate: %w", j.JoinName, err)
	}
	if won {
		log.Info().Str("join", j.JoinName).Str("corr", j.CorrelationKey).Msg("join: deadline reached, firing timeout action")
		if aerr := je.runAction(ctx, &JoinSpec{Name: j.JoinName}, &action); aerr != nil {
			return fmt.Errorf("join %q: run timeout action: %w", j.JoinName, aerr)
		}
	}

	// The instance is terminal regardless of who won the fire-marker.
	if err := je.store.MarkJoinTerminal(ctx, j.ID, JoinStatusTimedOut, term); err != nil {
		return fmt.Errorf("join %q: mark timed_out: %w", j.JoinName, err)
	}
	return nil
}

// marshalAction serializes an ActionDef to JSON text for durable persistence on
// the join row. nil yields "". A marshal error logs a warning and yields "".
func marshalAction(action *ActionDef) string {
	if action == nil {
		return ""
	}
	b, err := json.Marshal(action)
	if err != nil {
		log.Warn().Err(err).Msg("join: marshal action failed (non-fatal); action will not be persisted")
		return ""
	}
	return string(b)
}

// handleSetArrival implements the set barrier: each arrival adds its member id
// to a KV set (which inherently dedups), and the join fires when the set's
// cardinality reaches the expected_set size. The member whose add completes the
// set is the unique firer (added && cardinality>=size), gated by the fire-marker.
func (je *JoinEngine) handleSetArrival(ctx context.Context, spec *JoinSpec, row *Join, base string, env map[string]any, ttl time.Duration) error {
	if je.set == nil {
		return fmt.Errorf("join %q: set mode unavailable (set backend not wired)", spec.Name)
	}
	member, err := je.evalString(spec.MemberKey, env)
	if err != nil {
		return fmt.Errorf("join %q: member_key eval: %w", spec.Name, err)
	}
	if member == "" {
		return fmt.Errorf("join %q: set mode requires a non-empty member_key", spec.Name)
	}

	added, card, err := je.set.SetAdd(ctx, base+"/members", member, ttl)
	if err != nil {
		return fmt.Errorf("join %q: set add: %w", spec.Name, err)
	}
	if err := je.store.UpdateJoinArrived(ctx, row.ID, card); err != nil {
		log.Warn().Err(err).Str("join", spec.Name).Msg("join: mirror set cardinality failed (non-fatal)")
	}

	size, known, err := je.evalSetSize(spec, env)
	if err != nil {
		return fmt.Errorf("join %q: expected_set eval: %w", spec.Name, err)
	}
	if known && (row.ExpectedCount == nil || *row.ExpectedCount != size) {
		if perr := je.store.SetJoinExpected(ctx, row.ID, size); perr != nil {
			log.Warn().Err(perr).Str("join", spec.Name).Msg("join: mirror expected size failed (non-fatal)")
		}
	}

	if added && known && card >= size {
		return je.fire(ctx, spec, row, base, ttl, spec.OnComplete, false)
	}
	return nil
}

// evalSetSize evaluates expected_set to a list and returns its length. ok=false
// when the expression is empty or yields nil (size not yet known).
func (je *JoinEngine) evalSetSize(spec *JoinSpec, env map[string]any) (int64, bool, error) {
	if spec.ExpectedSet == "" {
		return 0, false, nil
	}
	v, err := je.expr.EvaluateAny(spec.ExpectedSet, env)
	if err != nil {
		return 0, false, err
	}
	switch list := v.(type) {
	case nil:
		return 0, false, nil
	case []any:
		return int64(len(list)), true, nil
	case []string:
		return int64(len(list)), true, nil
	default:
		return 0, false, fmt.Errorf("expected_set evaluated to %T, want a list", v)
	}
}

// runAction dispatches an OnComplete/OnTimeout action: a create_task or
// emit_event. nil is a no-op.
func (je *JoinEngine) runAction(ctx context.Context, spec *JoinSpec, action *ActionDef) error {
	if action == nil {
		return nil
	}
	if action.Type == "emit_event" {
		return je.dispatcher.EmitEvent(action)
	}
	return je.dispatcher.DispatchAction(action)
}

// ensureRow inserts the durable mirror row if absent.
func (je *JoinEngine) ensureRow(ctx context.Context, spec *JoinSpec, mode, workspace, corr string, expected int64, expectedKnown bool) (*Join, error) {
	j := &Join{
		JoinName:         spec.Name,
		Workspace:        workspace,
		CorrelationKey:   corr,
		Mode:             mode,
		Status:           JoinStatusOpen,
		OnComplete:       marshalAction(spec.OnComplete),
		OnTimeout:        marshalAction(spec.OnTimeout),
		OnPartialFailure: spec.OnPartialFailure,
	}
	if expectedKnown {
		j.ExpectedCount = &expected
	}
	if d := je.parseDuration(spec.Timeout, 0); d > 0 {
		deadline := time.Now().Add(d)
		j.DeadlineAt = &deadline
	}
	return je.store.EnsureJoin(ctx, j)
}

// evalExpected returns the expected count when it is knowable on this arrival:
// either there is no arm gate, or this arrival IS the arm event. Otherwise the
// count stays unknown (returns ok=false) until the arm event lands.
func (je *JoinEngine) evalExpected(spec *JoinSpec, env map[string]any, eventName string) (int64, bool, error) {
	if spec.ExpectedCount == "" {
		return 0, false, nil
	}
	if spec.ArmOnEvent != "" && eventName != spec.ArmOnEvent {
		return 0, false, nil
	}
	return je.evalInt(spec.ExpectedCount, env)
}

func (je *JoinEngine) evalString(expression string, env map[string]any) (string, error) {
	if expression == "" {
		return "", nil
	}
	v, err := je.expr.EvaluateAny(expression, env)
	if err != nil {
		return "", err
	}
	if v == nil {
		return "", nil
	}
	return fmt.Sprint(v), nil
}

func (je *JoinEngine) evalInt(expression string, env map[string]any) (int64, bool, error) {
	if expression == "" {
		return 0, false, nil
	}
	v, err := je.expr.EvaluateAny(expression, env)
	if err != nil {
		return 0, false, err
	}
	switch n := v.(type) {
	case nil:
		return 0, false, nil
	case int:
		return int64(n), true, nil
	case int8:
		return int64(n), true, nil
	case int16:
		return int64(n), true, nil
	case int32:
		return int64(n), true, nil
	case int64:
		return n, true, nil
	case uint:
		return int64(n), true, nil
	case uint32:
		return int64(n), true, nil
	case uint64:
		return int64(n), true, nil
	case float32:
		return int64(n), true, nil
	case float64:
		return int64(n), true, nil
	default:
		return 0, false, fmt.Errorf("expected_count evaluated to %T, want integer", v)
	}
}

func (je *JoinEngine) parseDuration(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		log.Warn().Str("value", s).Msg("join: invalid duration, using default")
		return def
	}
	return d
}

func (je *JoinEngine) retentionLinger(spec *JoinSpec) time.Duration {
	return je.parseDuration(spec.Linger, joinDefaultLinger)
}

// retentionTTL bounds the lifetime of a join's KV keys (counter, dedup ledger,
// fire-marker): the barrier deadline plus the post-terminal linger, so abandoned
// instances self-GC even if the deadline sweep never runs.
func (je *JoinEngine) retentionTTL(spec *JoinSpec) time.Duration {
	return je.parseDuration(spec.Timeout, time.Hour) + je.retentionLinger(spec)
}

// joinBaseKey derives the reserved-namespace KV key prefix for a join instance.
// The correlation key is hashed so arbitrary event-derived values always yield a
// valid, collision-free KV key; the human-readable form lives in the SQL mirror.
func joinBaseKey(name, workspace, corr string) string {
	return kv.ReservedCoordKeyPrefix + "joins/" + hashSeg(name+"\x00"+workspace+"\x00"+corr)
}

func hashSeg(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
