package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

// AdminServer provides a REST API for managing workflow rules, definitions,
// schedules, executions, and state machines.
type AdminServer struct {
	store      *Store
	router     *Router
	dagEng     *DAGEngine
	scheduler  *Scheduler
	stateMach  *StateMachineEngine
	httpServer *http.Server
	apiKey     string
}

func NewAdminServer(port int, apiKey string, store *Store, router *Router, dagEng *DAGEngine, scheduler *Scheduler, stateMach *StateMachineEngine) *AdminServer {
	s := &AdminServer{
		store:     store,
		router:    router,
		dagEng:    dagEng,
		scheduler: scheduler,
		stateMach: stateMach,
		apiKey:    apiKey,
	}

	r := mux.NewRouter()
	api := r.PathPrefix("/api/v1").Subrouter()

	if apiKey != "" {
		api.Use(s.authMiddleware)
	}

	// Rules
	api.HandleFunc("/rules", s.listRules).Methods("GET")
	api.HandleFunc("/rules", s.createRule).Methods("POST")
	api.HandleFunc("/rules/{id}", s.getRule).Methods("GET")
	api.HandleFunc("/rules/{id}", s.updateRule).Methods("PUT")
	api.HandleFunc("/rules/{id}", s.deleteRule).Methods("DELETE")

	// Workflow Definitions
	api.HandleFunc("/workflows", s.listWorkflows).Methods("GET")
	api.HandleFunc("/workflows", s.createWorkflow).Methods("POST")
	api.HandleFunc("/workflows/{id}", s.getWorkflow).Methods("GET")
	api.HandleFunc("/workflows/{id}", s.deleteWorkflow).Methods("DELETE")

	// Schedules
	api.HandleFunc("/schedules", s.listSchedules).Methods("GET")
	api.HandleFunc("/schedules", s.createSchedule).Methods("POST")
	api.HandleFunc("/schedules/{id}", s.deleteSchedule).Methods("DELETE")

	// Executions
	api.HandleFunc("/executions", s.listExecutions).Methods("GET")
	api.HandleFunc("/executions/{id}", s.getExecution).Methods("GET")
	api.HandleFunc("/executions/{id}/cancel", s.cancelExecution).Methods("POST")

	// State Machines
	api.HandleFunc("/statemachines", s.listStateMachines).Methods("GET")
	api.HandleFunc("/statemachines", s.createStateMachine).Methods("POST")
	api.HandleFunc("/statemachines/{id}", s.getStateMachine).Methods("GET")
	api.HandleFunc("/statemachines/{id}", s.deleteStateMachine).Methods("DELETE")
	api.HandleFunc("/statemachines/{id}/instances", s.listStateMachineInstances).Methods("GET")
	api.HandleFunc("/statemachines/{id}/instances", s.createStateMachineInstance).Methods("POST")
	api.HandleFunc("/statemachines/{mid}/instances/{iid}", s.getStateMachineInstance).Methods("GET")
	api.HandleFunc("/statemachines/{mid}/instances/{iid}/event", s.sendStateMachineEvent).Methods("POST")

	// Health
	r.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}).Methods("GET")

	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return s
}

func (s *AdminServer) Start() error {
	log.Info().Str("addr", s.httpServer.Addr).Msg("workflow admin API listening")
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *AdminServer) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *AdminServer) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Authorization")
		if key == "" {
			key = r.Header.Get("X-API-Key")
		}
		if key == "Bearer "+s.apiKey || key == s.apiKey {
			next.ServeHTTP(w, r)
			return
		}
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
	})
}

// =============================================================================
// Rules endpoints
// =============================================================================

func (s *AdminServer) listRules(w http.ResponseWriter, r *http.Request) {
	workspace := r.URL.Query().Get("workspace")
	if workspace == "" {
		workspace = "*"
	}
	rules, err := s.store.ListRules(r.Context(), workspace)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if rules == nil {
		rules = []Rule{}
	}
	writeJSON(w, http.StatusOK, rules)
}

func (s *AdminServer) createRule(w http.ResponseWriter, r *http.Request) {
	var rule Rule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if rule.RuleName == "" || rule.SourceEvent == "" || rule.DestinationTemplate == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "rule_name, source_event, and destination_template are required"})
		return
	}
	if rule.SourceAgent == "" {
		rule.SourceAgent = "*"
	}
	if rule.Workspace == "" {
		rule.Workspace = "*"
	}
	if rule.TransformationStyle == "" {
		rule.TransformationStyle = "template-yaml"
	}
	rule.Active = true

	if err := s.store.CreateRule(r.Context(), &rule); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.router.InvalidateCache()
	writeJSON(w, http.StatusCreated, rule)
}

func (s *AdminServer) getRule(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid rule ID"})
		return
	}
	rule, err := s.store.GetRule(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if rule == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "rule not found"})
		return
	}
	writeJSON(w, http.StatusOK, rule)
}

func (s *AdminServer) updateRule(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid rule ID"})
		return
	}
	var rule Rule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	rule.ID = id
	if err := s.store.UpdateRule(r.Context(), &rule); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.router.InvalidateCache()
	writeJSON(w, http.StatusOK, rule)
}

func (s *AdminServer) deleteRule(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid rule ID"})
		return
	}
	if err := s.store.DeleteRule(r.Context(), id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.router.InvalidateCache()
	writeJSON(w, http.StatusNoContent, nil)
}

// =============================================================================
// Workflow Definition endpoints
// =============================================================================

func (s *AdminServer) listWorkflows(w http.ResponseWriter, r *http.Request) {
	workspace := r.URL.Query().Get("workspace")
	if workspace == "" {
		workspace = "*"
	}
	defs, err := s.store.GetWorkflowDefinitionsForTrigger(r.Context(), workspace)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if defs == nil {
		defs = []WorkflowDefinition{}
	}
	writeJSON(w, http.StatusOK, defs)
}

func (s *AdminServer) createWorkflow(w http.ResponseWriter, r *http.Request) {
	var def WorkflowDefinition
	if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if def.ID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id is required"})
		return
	}
	if len(def.Definition) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "definition is required"})
		return
	}
	// Validate the definition parses as a DAG
	var dagDef DAGDefinition
	if err := json.Unmarshal(def.Definition, &dagDef); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid DAG definition: " + err.Error()})
		return
	}
	if def.Workspace == "" {
		def.Workspace = "*"
	}
	if def.Version == 0 {
		def.Version = 1
	}
	def.Active = true

	if err := s.store.CreateWorkflowDefinition(r.Context(), &def); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, def)
}

func (s *AdminServer) getWorkflow(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	def, err := s.store.GetWorkflowDefinition(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if def == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "workflow not found"})
		return
	}
	writeJSON(w, http.StatusOK, def)
}

func (s *AdminServer) deleteWorkflow(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if err := s.store.DeactivateWorkflowDefinition(r.Context(), id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusNoContent, nil)
}

// =============================================================================
// Schedule endpoints
// =============================================================================

func (s *AdminServer) listSchedules(w http.ResponseWriter, r *http.Request) {
	workspace := r.URL.Query().Get("workspace")
	if workspace == "" {
		workspace = "*"
	}
	schedules, err := s.store.ListSchedules(r.Context(), workspace)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if schedules == nil {
		schedules = []Schedule{}
	}
	writeJSON(w, http.StatusOK, schedules)
}

func (s *AdminServer) createSchedule(w http.ResponseWriter, r *http.Request) {
	var sc Schedule
	if err := json.NewDecoder(r.Body).Decode(&sc); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if sc.ID == "" || sc.Name == "" || sc.ScheduleType == "" || sc.ScheduleExpr == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id, name, schedule_type, and schedule_expr are required"})
		return
	}
	if len(sc.Action) == 0 && sc.WorkflowID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "action or workflow_id is required"})
		return
	}
	if sc.Workspace == "" {
		sc.Workspace = "*"
	}
	if sc.MissPolicy == "" {
		sc.MissPolicy = "skip"
	}
	sc.Enabled = true

	// Compute initial next fire time
	nextFire, err := s.scheduler.ComputeInitialNextFire(sc.ScheduleType, sc.ScheduleExpr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid schedule expression: " + err.Error()})
		return
	}
	sc.NextFireAt = nextFire

	if err := s.store.CreateSchedule(r.Context(), &sc); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, sc)
}

func (s *AdminServer) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if err := s.store.DeleteSchedule(r.Context(), id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusNoContent, nil)
}

// =============================================================================
// Execution endpoints
// =============================================================================

func (s *AdminServer) listExecutions(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	var execs []WorkflowExecution
	var err error
	if status == "running" || status == "" {
		execs, err = s.store.GetRunningExecutions(r.Context())
	} else {
		execs, err = s.store.GetExecutionsByStatus(r.Context(), status)
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if execs == nil {
		execs = []WorkflowExecution{}
	}
	writeJSON(w, http.StatusOK, execs)
}

func (s *AdminServer) getExecution(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	exec, err := s.store.GetExecution(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if exec == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "execution not found"})
		return
	}
	steps, err := s.store.GetStepStates(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"execution": exec,
		"steps":     steps,
	})
}

func (s *AdminServer) cancelExecution(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	exec, err := s.store.GetExecution(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if exec == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "execution not found"})
		return
	}
	if exec.Status != ExecStatusRunning {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "execution is not running"})
		return
	}
	if err := s.store.UpdateExecutionStatus(r.Context(), id, ExecStatusCancelled, "cancelled via admin API"); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

// =============================================================================
// State Machine endpoints
// =============================================================================

func (s *AdminServer) listStateMachines(w http.ResponseWriter, r *http.Request) {
	workspace := r.URL.Query().Get("workspace")
	if workspace == "" {
		workspace = "*"
	}
	machines, err := s.store.ListStateMachines(r.Context(), workspace)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if machines == nil {
		machines = []StateMachineDef{}
	}
	writeJSON(w, http.StatusOK, machines)
}

func (s *AdminServer) createStateMachine(w http.ResponseWriter, r *http.Request) {
	var sm StateMachineDef
	if err := json.NewDecoder(r.Body).Decode(&sm); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if sm.ID == "" || len(sm.Definition) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id and definition are required"})
		return
	}
	// Validate the definition parses
	var def StateMachineDefinition
	if err := json.Unmarshal(sm.Definition, &def); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid state machine definition: " + err.Error()})
		return
	}
	if def.InitialState == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "initial_state is required in definition"})
		return
	}
	if sm.Workspace == "" {
		sm.Workspace = "*"
	}
	sm.Active = true

	if err := s.store.CreateStateMachine(r.Context(), &sm); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, sm)
}

func (s *AdminServer) getStateMachine(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	sm, err := s.store.GetStateMachine(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if sm == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "state machine not found"})
		return
	}
	writeJSON(w, http.StatusOK, sm)
}

func (s *AdminServer) deleteStateMachine(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if err := s.store.DeactivateStateMachine(r.Context(), id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusNoContent, nil)
}

func (s *AdminServer) listStateMachineInstances(w http.ResponseWriter, r *http.Request) {
	machineID := mux.Vars(r)["id"]
	instances, err := s.store.ListStateMachineInstances(r.Context(), machineID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if instances == nil {
		instances = []StateMachineInstance{}
	}
	writeJSON(w, http.StatusOK, instances)
}

func (s *AdminServer) createStateMachineInstance(w http.ResponseWriter, r *http.Request) {
	machineID := mux.Vars(r)["id"]

	var body struct {
		InstanceID string          `json:"instance_id"`
		Workspace  string          `json:"workspace"`
		Data       json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if body.InstanceID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "instance_id is required"})
		return
	}
	if body.Workspace == "" {
		body.Workspace = "*"
	}

	instance, err := s.stateMach.CreateInstance(r.Context(), machineID, body.InstanceID, body.Workspace, body.Data)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, instance)
}

func (s *AdminServer) getStateMachineInstance(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	instance, err := s.store.GetStateMachineInstance(r.Context(), vars["iid"])
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if instance == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "instance not found"})
		return
	}
	writeJSON(w, http.StatusOK, instance)
}

func (s *AdminServer) sendStateMachineEvent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	var body struct {
		Event string          `json:"event"`
		Data  json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if body.Event == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "event is required"})
		return
	}

	instance, err := s.stateMach.SendEvent(r.Context(), vars["iid"], body.Event, body.Data)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, instance)
}

// =============================================================================
// Helpers
// =============================================================================

func writeJSON(w http.ResponseWriter, status int, v any) {
	if v == nil {
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
