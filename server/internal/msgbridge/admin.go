package msgbridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"github.com/scitrera/aether/internal/msgbridge/platforms"
)

// AdminServer provides a REST API for managing msgbridge channel mappings,
// user mappings, and querying message logs.
type AdminServer struct {
	store    *Store
	adapters map[string]platforms.PlatformAdapter
	server   *http.Server
	apiKey   string
}

func NewAdminServer(store *Store, port int, apiKey string, adapters map[string]platforms.PlatformAdapter) *AdminServer {
	a := &AdminServer{
		store:    store,
		adapters: adapters,
		apiKey:   apiKey,
	}

	r := mux.NewRouter()
	api := r.PathPrefix("/api/v1").Subrouter()

	if apiKey != "" {
		api.Use(a.authMiddleware)
	}

	// Channel Mappings
	api.HandleFunc("/mappings", a.listMappings).Methods("GET")
	api.HandleFunc("/mappings", a.createMapping).Methods("POST")
	api.HandleFunc("/mappings/{id}", a.getMapping).Methods("GET")
	api.HandleFunc("/mappings/{id}", a.updateMapping).Methods("PUT")
	api.HandleFunc("/mappings/{id}", a.deleteMapping).Methods("DELETE")

	// User Mappings
	api.HandleFunc("/users", a.listUsers).Methods("GET")
	api.HandleFunc("/users", a.createUser).Methods("POST")
	api.HandleFunc("/users/{id}", a.deleteUser).Methods("DELETE")

	// Message Logs
	api.HandleFunc("/logs", a.queryLogs).Methods("GET")

	// Health (no auth) — returns per-platform status
	r.HandleFunc("/health", a.healthHandler).Methods("GET")

	// Metrics (no auth)
	r.Handle("/metrics", promhttp.Handler())

	a.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return a
}

// healthHandler returns overall status and per-platform health.
func (a *AdminServer) healthHandler(w http.ResponseWriter, _ *http.Request) {
	type platformStatus struct {
		Enabled bool `json:"enabled"`
		Healthy bool `json:"healthy"`
	}

	platforms := make(map[string]platformStatus, len(a.adapters))
	for name, adapter := range a.adapters {
		platforms[name] = platformStatus{
			Enabled: true,
			Healthy: adapter.IsHealthy(),
		}
	}

	writeMsgbridgeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"platforms": platforms,
	})
}

// Start begins listening in a goroutine. Non-blocking.
func (a *AdminServer) Start() error {
	log.Info().Str("addr", a.server.Addr).Msg("msgbridge admin API listening")
	go func() {
		if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("msgbridge admin server error")
		}
	}()
	return nil
}

// Stop gracefully shuts down the admin HTTP server.
func (a *AdminServer) Stop(ctx context.Context) error {
	return a.server.Shutdown(ctx)
}

func (a *AdminServer) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Authorization")
		if key == "" {
			key = r.Header.Get("X-API-Key")
		}
		if key == "Bearer "+a.apiKey || key == a.apiKey {
			next.ServeHTTP(w, r)
			return
		}
		writeMsgbridgeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
	})
}

// =============================================================================
// Channel Mapping endpoints
// =============================================================================

func (a *AdminServer) listMappings(w http.ResponseWriter, r *http.Request) {
	mappings, err := a.store.ListChannelMappings(r.Context())
	if err != nil {
		writeMsgbridgeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if mappings == nil {
		mappings = []*ChannelMapping{}
	}
	writeMsgbridgeJSON(w, http.StatusOK, map[string]any{"data": mappings})
}

func (a *AdminServer) createMapping(w http.ResponseWriter, r *http.Request) {
	var m ChannelMapping
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		writeMsgbridgeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if m.Name == "" || m.Platform == "" || m.ChannelID == "" || m.TargetType == "" {
		writeMsgbridgeJSON(w, http.StatusBadRequest, map[string]string{"error": "name, platform, channel_id, and target_type are required"})
		return
	}
	if m.Direction == "" {
		m.Direction = "both"
	}
	m.Enabled = true

	if err := a.store.CreateChannelMapping(r.Context(), &m); err != nil {
		writeMsgbridgeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeMsgbridgeJSON(w, http.StatusCreated, map[string]any{"data": m})
}

func (a *AdminServer) getMapping(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(mux.Vars(r)["id"], 10, 64)
	if err != nil {
		writeMsgbridgeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid mapping ID"})
		return
	}
	m, err := a.store.GetChannelMapping(r.Context(), id)
	if err != nil {
		writeMsgbridgeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if m == nil {
		writeMsgbridgeJSON(w, http.StatusNotFound, map[string]string{"error": "mapping not found"})
		return
	}
	writeMsgbridgeJSON(w, http.StatusOK, map[string]any{"data": m})
}

func (a *AdminServer) updateMapping(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(mux.Vars(r)["id"], 10, 64)
	if err != nil {
		writeMsgbridgeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid mapping ID"})
		return
	}
	var m ChannelMapping
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		writeMsgbridgeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	m.ID = id
	if err := a.store.UpdateChannelMapping(r.Context(), &m); err != nil {
		writeMsgbridgeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeMsgbridgeJSON(w, http.StatusOK, map[string]any{"data": m})
}

func (a *AdminServer) deleteMapping(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(mux.Vars(r)["id"], 10, 64)
	if err != nil {
		writeMsgbridgeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid mapping ID"})
		return
	}
	if err := a.store.DeleteChannelMapping(r.Context(), id); err != nil {
		writeMsgbridgeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// =============================================================================
// User Mapping endpoints
// =============================================================================

func (a *AdminServer) listUsers(w http.ResponseWriter, r *http.Request) {
	mappings, err := a.store.ListUserMappings(r.Context())
	if err != nil {
		writeMsgbridgeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if mappings == nil {
		mappings = []*UserMapping{}
	}
	writeMsgbridgeJSON(w, http.StatusOK, map[string]any{"data": mappings})
}

func (a *AdminServer) createUser(w http.ResponseWriter, r *http.Request) {
	var m UserMapping
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		writeMsgbridgeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if m.Platform == "" || m.PlatformUserID == "" || m.AetherUserID == "" {
		writeMsgbridgeJSON(w, http.StatusBadRequest, map[string]string{"error": "platform, platform_user_id, and aether_user_id are required"})
		return
	}
	if err := a.store.CreateUserMapping(r.Context(), &m); err != nil {
		writeMsgbridgeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeMsgbridgeJSON(w, http.StatusCreated, map[string]any{"data": m})
}

func (a *AdminServer) deleteUser(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(mux.Vars(r)["id"], 10, 64)
	if err != nil {
		writeMsgbridgeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid user mapping ID"})
		return
	}
	if err := a.store.DeleteUserMapping(r.Context(), id); err != nil {
		writeMsgbridgeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// =============================================================================
// Message Log endpoints
// =============================================================================

func (a *AdminServer) queryLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	platform := q.Get("platform")
	channelID := q.Get("channel_id")
	limit := 100
	if ls := q.Get("limit"); ls != "" {
		if n, err := strconv.Atoi(ls); err == nil && n > 0 {
			limit = n
		}
	}

	entries, err := a.store.QueryMessageLog(r.Context(), platform, channelID, limit)
	if err != nil {
		writeMsgbridgeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if entries == nil {
		entries = []*MessageLogEntry{}
	}
	writeMsgbridgeJSON(w, http.StatusOK, map[string]any{"data": entries})
}

// =============================================================================
// Helpers
// =============================================================================

func writeMsgbridgeJSON(w http.ResponseWriter, status int, v any) {
	if v == nil {
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// Best-effort response body write; the HTTP transport already surfaces
	// client disconnects through its own logging, so an encode error here
	// has nowhere actionable to land.
	_ = json.NewEncoder(w).Encode(v)
}
