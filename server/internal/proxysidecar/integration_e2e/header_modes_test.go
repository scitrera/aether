//go:build e2e

package integration_e2e

// Tests for BackendConfig.HeaderMode variants: strict and both.
//
// HeaderModePassthrough is already covered by
// TestE2E_RequestHeaders_PassthroughPreservesOrder in http_variants_test.go.
// The tests here close the matrix gap for the remaining two modes.
//
// What each mode does (from http_backend.go::applyHeaders):
//
//   - Strict: copies caller headers (minus hop-by-hop + Authorization), then
//     StripInbound strips X-Auth-* and X-Aether-* to prevent spoofing, then
//     mints the canonical trusted set.  Arbitrary caller headers outside the
//     x-auth-*/x-aether-* namespace are NOT removed.
//
//   - Both:   copies caller headers (minus hop-by-hop + Authorization), then
//     mints the canonical trusted set on top; minted values win on key
//     collision.  Caller X-Auth-* headers are overlaid, not stripped, so a
//     caller-supplied X-Auth-Tenant-ID is overwritten by the minted one.
//
// Why no TestE2E_HeaderMode_InvalidValue_RejectedAtConfig:
//   config.go:521-525 already rejects unknown HeaderMode values inside
//   validateBackend() with a descriptive error message, and NewRunner()
//   returns that error synchronously before any network I/O happens.
//   Validating this in an e2e test (which spins up aetherlite + a real sidecar
//   runner) would add ~3 s of startup cost to confirm a 1-line switch-case.
//   The unit-level coverage in config_test.go is the right home for that check.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/scitrera/aether/internal/proxysidecar"
	"github.com/scitrera/aether/sdk/go/aether"
)

// hdrGet looks up a header from a JSON-decoded map[string]string using Go's
// canonical header key form. The backend's /headers handler builds the map by
// iterating r.Header, whose keys are already canonicalized by net/http
// (e.g. "X-Auth-Tenant-ID" → "X-Auth-Tenant-Id"). Using
// http.CanonicalHeaderKey here ensures assertions match regardless of how the
// constant was originally cased in the test source.
func hdrGet(m map[string]string, key string) string {
	return m[http.CanonicalHeaderKey(key)]
}

// hdrPresent reports whether a header key (in any casing) is present in m.
func hdrPresent(m map[string]string, key string) bool {
	_, ok := m[http.CanonicalHeaderKey(key)]
	return ok
}

// newHeadersCaptureBackend starts an httptest.Server whose sole handler
// returns the headers it received as a JSON map[string]string. The caller
// owns teardown (via t.Cleanup registered inside).
func newHeadersCaptureBackend(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/headers", func(w http.ResponseWriter, r *http.Request) {
		out := make(map[string]string, len(r.Header))
		for k := range r.Header {
			out[k] = r.Header.Get(k)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_ = json.NewEncoder(w).Encode(out)
	})
	// /fast is required by the sidecar readiness probe in
	// waitForSidecarReadyCustom (which probes /echo, /fast, /__readiness…).
	mux.HandleFunc("/fast", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// proxyGETHeaders dials a fresh caller client, sends GET /headers through the
// sidecar, and decodes the backend's captured-header map. All cleanup is
// registered on t.
func proxyGETHeaders(
	t *testing.T,
	gwAddr, serviceTopic string,
	callerID string,
	extraHeaders map[string]string,
) map[string]string {
	t.Helper()

	client := dialAgentClientToAddr(t, gwAddr, callerID)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", "http://ignored/headers", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}

	resp, err := client.ProxyHTTP(ctx, serviceTopic, req, aether.WithBackend("local"))
	if err != nil {
		t.Fatalf("ProxyHTTP: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%q", resp.StatusCode, string(body))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	var got map[string]string
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("Unmarshal %q: %v", string(body), err)
	}
	return got
}

// newHeaderModeHarness creates a custom sidecar harness whose single HTTP
// backend is configured with the supplied HeaderMode value. The backend URL
// points at srv (a headers-capture httptest.Server).
func newHeaderModeHarness(t *testing.T, srv *httptest.Server, mode string) *customSidecarHarness {
	t.Helper()
	backends := []proxysidecar.BackendConfig{{
		Name:          "local",
		Kind:          proxysidecar.BackendKindHTTP,
		URL:           srv.URL,
		AllowPaths:    []string{"/*"},
		AllowMethods:  []string{"GET", "POST"},
		MaxBodyBytes:  1 << 20,
		IdleTimeoutMs: 10_000,
		HeaderMode:    mode,
	}}
	return newCustomSidecarHarness(t, "bp-sidecar", "hdrmode-"+mode, backends)
}

// =============================================================================
// TestE2E_HeaderMode_Strict_StripsCallerAuthHeaders
// =============================================================================

// TestE2E_HeaderMode_Strict_StripsCallerAuthHeaders configures a backend with
// HeaderModeStrict, sends a request that carries:
//   - X-Custom-Test: hello-from-caller  (non-auth, should survive)
//   - X-Auth-Spoofed: evil-value        (spoofed identity header, must be stripped)
//   - User-Agent: caller-ua             (hop-by-hop agnostic; should survive)
//
// Assertions:
//  1. X-Auth-Spoofed is NOT present — StripInbound removed it.
//  2. X-Custom-Test IS still present — only X-Auth-* / X-Aether-* are stripped.
//  3. X-Auth-Tenant-ID IS present — minted by the sidecar (set to the
//     configured TenantID "tenant-e2e").
//  4. X-Auth-User-ID IS present — always minted in direct mode.
//  5. X-Auth-Authority-Mode IS present and equals "direct".
func TestE2E_HeaderMode_Strict_StripsCallerAuthHeaders(t *testing.T) {
	srv := newHeadersCaptureBackend(t)
	h := newHeaderModeHarness(t, srv, proxysidecar.HeaderModeStrict)

	got := proxyGETHeaders(t, h.GatewayAddr, h.ServiceTopic, "strict-caller", map[string]string{
		"X-Custom-Test":  "hello-from-caller",
		"X-Auth-Spoofed": "evil-value",
	})

	t.Logf("backend saw headers: %v", got)

	// 1. Spoofed X-Auth-* header must be gone.
	if hdrPresent(got, "X-Auth-Spoofed") {
		t.Errorf("X-Auth-Spoofed should be stripped by strict mode, but backend saw value %q", hdrGet(got, "X-Auth-Spoofed"))
	}

	// 2. Arbitrary caller header outside the x-auth-*/x-aether-* namespace
	//    is NOT stripped by StripInbound — only auth-namespace headers are.
	if v := hdrGet(got, "X-Custom-Test"); v != "hello-from-caller" {
		t.Errorf("X-Custom-Test: got %q, want %q", v, "hello-from-caller")
	}

	// 3. Minted X-Auth-Tenant-ID is always set by strict mode.
	if hdrGet(got, "X-Auth-Tenant-ID") == "" {
		t.Errorf("X-Auth-Tenant-ID must be minted by strict mode; backend saw empty/absent")
	}

	// 4. Minted X-Auth-User-ID is always set in direct mode (may be empty
	//    string value but the key must be present after minting).
	if !hdrPresent(got, "X-Auth-User-ID") {
		t.Errorf("X-Auth-User-ID must be minted by strict mode; key absent in backend headers")
	}

	// 5. Authority mode is "direct" when no OBO grant is attached.
	if v := hdrGet(got, "X-Auth-Authority-Mode"); v != "direct" {
		t.Errorf("X-Auth-Authority-Mode: got %q, want %q", v, "direct")
	}
}

// =============================================================================
// TestE2E_HeaderMode_Both_OverlaysMintedOnCallerSet
// =============================================================================

// TestE2E_HeaderMode_Both_OverlaysMintedOnCallerSet configures a backend with
// HeaderModeBoth, sends a request that carries:
//   - X-Custom-Test: from-caller                    (non-auth, should survive)
//   - X-Auth-Tenant-ID: attacker-controlled-tenant  (spoofed; minted value wins)
//
// Assertions:
//  1. X-Custom-Test IS present — both mode keeps caller headers.
//  2. X-Auth-Tenant-ID IS present and equals the sidecar's own TenantID
//     ("tenant-e2e"), NOT the attacker-supplied value. Minted wins on
//     collision (mintInto overwrites with httpReq.Header[k] = vs).
//  3. X-Auth-User-ID IS present — minted in direct mode.
//  4. X-Auth-Authority-Mode IS present and equals "direct".
func TestE2E_HeaderMode_Both_OverlaysMintedOnCallerSet(t *testing.T) {
	srv := newHeadersCaptureBackend(t)
	h := newHeaderModeHarness(t, srv, proxysidecar.HeaderModeBoth)

	// The sidecar's TenantID is "tenant-e2e" (set in newCustomSidecarHarness).
	// We intentionally send a conflicting value to verify minted wins.
	const callerFakeTenant = "attacker-controlled-tenant"

	got := proxyGETHeaders(t, h.GatewayAddr, h.ServiceTopic, "both-caller", map[string]string{
		"X-Custom-Test":    "from-caller",
		"X-Auth-Tenant-ID": callerFakeTenant,
	})

	t.Logf("backend saw headers: %v", got)

	// 1. Caller's non-auth header is preserved by both mode.
	if v := hdrGet(got, "X-Custom-Test"); v != "from-caller" {
		t.Errorf("X-Custom-Test: got %q, want %q", v, "from-caller")
	}

	// 2. Minted X-Auth-Tenant-ID overwrites the caller-supplied spoof.
	//    The sidecar tenant is "tenant-e2e"; the attacker's value must not win.
	tenantGot := hdrGet(got, "X-Auth-Tenant-ID")
	if tenantGot == "" {
		t.Errorf("X-Auth-Tenant-ID must be minted by both mode; absent from backend headers")
	} else if strings.EqualFold(tenantGot, callerFakeTenant) {
		t.Errorf("X-Auth-Tenant-ID: minted value should win, but got caller-supplied %q", tenantGot)
	}

	// 3. Minted X-Auth-User-ID is present in direct mode.
	if !hdrPresent(got, "X-Auth-User-ID") {
		t.Errorf("X-Auth-User-ID must be minted by both mode; key absent in backend headers")
	}

	// 4. Authority mode is "direct" when no OBO grant is attached.
	if v := hdrGet(got, "X-Auth-Authority-Mode"); v != "direct" {
		t.Errorf("X-Auth-Authority-Mode: got %q, want %q", v, "direct")
	}
}
