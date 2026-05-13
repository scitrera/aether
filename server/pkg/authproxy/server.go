package authproxy

import (
	"context"
	"fmt"
	"net/http"

	"github.com/scitrera/aether/internal/logging"
	"net/http/httputil"
	"net/url"
	"time"
)

// Server is the auth-proxy HTTP server. It operates in one of two modes:
//   - proxy mode: authenticates, injects headers, reverse-proxies to backend
//   - verify mode: authenticates and returns 200 with headers (for nginx auth_request)
type Server struct {
	cfg        *Config
	middleware *AuthMiddleware
	httpServer *http.Server
	proxy      *httputil.ReverseProxy
	mux        *http.ServeMux
}

// Mux exposes the server's underlying mux so optional subsystems (notably
// the browser-OAuth login module) can register additional routes after
// construction.
func (s *Server) Mux() *http.ServeMux { return s.mux }

// NewServer creates a new auth-proxy server. In proxy mode it also
// initialises a reverse proxy to the configured backend URL.
func NewServer(cfg *Config, middleware *AuthMiddleware) (*Server, error) {
	s := &Server{
		cfg:        cfg,
		middleware: middleware,
	}

	// Set up reverse proxy for proxy mode
	if cfg.Mode == ModeProxy {
		backendURL, err := url.Parse(cfg.BackendURL)
		if err != nil {
			return nil, fmt.Errorf("invalid backend URL %q: %w", cfg.BackendURL, err)
		}
		s.proxy = httputil.NewSingleHostReverseProxy(backendURL)

		// Customise the proxy director to preserve the original host
		// and inject trusted headers.
		originalDirector := s.proxy.Director
		s.proxy.Director = func(req *http.Request) {
			originalDirector(req)
			// The auth middleware has already injected headers and
			// stripped Authorization before we get here.
		}

		// Strip CORS headers from backend responses to avoid duplicates
		// (the auth-proxy's CORS middleware handles CORS at the edge).
		s.proxy.ModifyResponse = func(resp *http.Response) error {
			resp.Header.Del("Access-Control-Allow-Origin")
			resp.Header.Del("Access-Control-Allow-Methods")
			resp.Header.Del("Access-Control-Allow-Headers")
			resp.Header.Del("Access-Control-Allow-Credentials")
			resp.Header.Del("Access-Control-Max-Age")
			return nil
		}

		s.proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			logging.Logger.Error().Err(err).Str("method", r.Method).Str("path", r.URL.Path).Msg("proxy error")
			writeJSONError(w, http.StatusBadGateway, "bad gateway", "backend unavailable")
		}
	}

	mux := http.NewServeMux()
	s.mux = mux

	// Health check (unauthenticated)
	mux.HandleFunc("/healthz", s.handleHealthz)

	// Auth verify endpoint (available in both modes, primary in verify mode)
	mux.HandleFunc("/auth/verify", s.handleAuthVerify)

	if cfg.Mode == ModeProxy {
		// In proxy mode, all other requests go through auth + reverse proxy
		mux.HandleFunc("/", s.handleProxy)
	} else {
		// In verify mode, only /auth/verify and /healthz are served
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		})
	}

	var handler http.Handler = mux
	if cfg.CORSOrigin != "" {
		handler = corsMiddleware(mux, cfg.CORSOrigin)
		logging.Logger.Info().Str("origin", cfg.CORSOrigin).Msg("CORS enabled")
	}

	s.httpServer = &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	return s, nil
}

// Start begins listening for HTTP requests. It blocks until the server
// is shut down or encounters a fatal error.
func (s *Server) Start() error {
	logging.Logger.Info().Str("addr", s.cfg.ListenAddr).Str("mode", string(s.cfg.Mode)).Msg("auth proxy listening")
	if s.cfg.Mode == ModeProxy {
		logging.Logger.Info().Str("backend", s.cfg.BackendURL).Msg("proxy backend configured")
	}
	if s.cfg.TLSCertFile != "" && s.cfg.TLSKeyFile != "" {
		logging.Logger.Info().Str("cert", s.cfg.TLSCertFile).Msg("TLS enabled for auth proxy")
		return s.httpServer.ListenAndServeTLS(s.cfg.TLSCertFile, s.cfg.TLSKeyFile)
	}
	logging.Logger.Warn().Msg("auth proxy running without TLS — credentials transmitted in plaintext")
	return s.httpServer.ListenAndServe()
}

// Stop performs a graceful shutdown of the HTTP server, waiting up to
// the given context deadline for in-flight requests to complete.
func (s *Server) Stop(ctx context.Context) error {
	logging.Logger.Info().Msg("auth proxy shutting down")
	return s.httpServer.Shutdown(ctx)
}

// handleHealthz responds with 200 OK for liveness/readiness probes.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"status":"ok"}`)
}

// handleAuthVerify validates credentials and returns 200 with identity
// headers on success, or 401/403 on failure. This is the endpoint
// consumed by nginx auth_request or Envoy ext_authz.
func (s *Server) handleAuthVerify(w http.ResponseWriter, r *http.Request) {
	authed, err := s.middleware.Authenticate(w, r)
	if err != nil {
		// Authenticate already wrote the error response
		return
	}

	// Set identity headers on the response for nginx to consume
	s.middleware.SetResponseHeaders(w, authed)
	w.WriteHeader(http.StatusOK)
}

// corsMiddleware adds CORS headers and handles preflight OPTIONS requests.
func corsMiddleware(next http.Handler, origin string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Accept, X-Workspace-ID, X-Session-ID, X-API-Key")
		w.Header().Set("Access-Control-Max-Age", "86400")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// handleProxy validates credentials, injects trusted headers, strips
// the Authorization header, and forwards the request to the backend.
func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	authed, err := s.middleware.Authenticate(w, r)
	if err != nil {
		// Authenticate already wrote the error response
		return
	}

	// Inject trusted headers and strip Authorization
	s.middleware.InjectHeaders(r, authed)

	// Forward to backend
	s.proxy.ServeHTTP(w, r)
}
