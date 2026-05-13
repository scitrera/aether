package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scitrera/aether/internal/logging"
)

// OpsServer serves health probes and Prometheus metrics on a dedicated port,
// isolated from the admin API for security and operational independence.
type OpsServer struct {
	port     int
	provider StateProvider
	server   *http.Server

	mu    sync.Mutex
	ready bool
}

// NewOpsServer creates a new ops server on the given port.
func NewOpsServer(port int, provider StateProvider) *OpsServer {
	return &OpsServer{
		port:     port,
		provider: provider,
	}
}

// SetReady marks the ops server as ready (startup probe will pass).
func (o *OpsServer) SetReady(ready bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ready = ready
}

// isReady returns the current readiness state.
func (o *OpsServer) isReady() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.ready
}

// Start begins serving health and metrics endpoints.
func (o *OpsServer) Start() error {
	mux := http.NewServeMux()

	// Prometheus metrics
	mux.Handle("/metrics", promhttp.Handler())

	// Kubernetes health probes
	mux.HandleFunc("/health/live", o.handleLive)
	mux.HandleFunc("/health/ready", o.handleReady)
	mux.HandleFunc("/health/startup", o.handleStartup)

	o.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", o.port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	logging.Logger.Info().Int("port", o.port).Msg("ops server starting (health + metrics)")
	return o.server.ListenAndServe()
}

// Stop gracefully shuts down the ops server.
func (o *OpsServer) Stop(ctx context.Context) error {
	if o.server == nil {
		return nil
	}
	return o.server.Shutdown(ctx)
}

func (o *OpsServer) handleLive(w http.ResponseWriter, r *http.Request) {
	o.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (o *OpsServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if o.provider == nil {
		o.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	health, err := o.provider.GetHealthStatus(r.Context())
	if err != nil {
		o.writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"status": "unavailable",
			"error":  err.Error(),
		})
		return
	}

	if health.Status == "unhealthy" {
		o.writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"status": health.Status,
			"checks": health.Checks,
		})
		return
	}

	o.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": health.Status,
		"checks": health.Checks,
	})
}

func (o *OpsServer) handleStartup(w http.ResponseWriter, r *http.Request) {
	if !o.isReady() {
		o.writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "starting"})
		return
	}
	o.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (o *OpsServer) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		logging.Logger.Error().Err(err).Msg("ops server: error encoding JSON response")
	}
}
