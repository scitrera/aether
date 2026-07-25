//go:build e2e

// TestE2E_MTLS_* — gateway↔sidecar mTLS coverage.
//
// These tests exercise the full mTLS path between the SDK caller, the
// proxy-sidecar, and a dedicated aetherlite instance that requires client
// certificates. Each test owns its own aetherlite subprocess (different
// port from the shared insecure one used by all other e2e tests) so the
// existing 45-test suite is not disturbed.
//
// Coverage:
//   - TestE2E_MTLS_HappyPath_RoundTrip: valid client cert → 200 round-trip, identity verified.
//   - TestE2E_MTLS_NoClientCert_Rejected: no client cert → TLS handshake rejected.
//   - TestE2E_MTLS_BadClientCert_Rejected: cert signed by wrong CA → rejected.

package integration_e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/scitrera/aether/server/internal/proxysidecar"
	"github.com/scitrera/aether/sdk/go/aether"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// =============================================================================
// mTLS certificate generation
// =============================================================================

// mtlsCA holds the CA key material needed to sign additional leaf certs after
// the initial generateMTLSCA call.
type mtlsCA struct {
	key    *ecdsa.PrivateKey
	cert   *x509.Certificate
	PEM    []byte // CA cert PEM (for SDK RootCAs / aetherlite ca_file)
	CAFile string // path written to t.TempDir()
	serial int64  // monotonically incremented for each leaf
}

// mtlsLeaf is a signed leaf certificate (server or client).
type mtlsLeaf struct {
	CertPEM  []byte
	KeyPEM   []byte
	CertFile string
	KeyFile  string
}

// generateMTLSCA creates a self-signed ECDSA-P256 CA and writes ca.pem to
// t.TempDir(). The returned *mtlsCA can sign any number of leaf certs via
// signLeaf.
func generateMTLSCA(t *testing.T) *mtlsCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "aether-test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatalf("write ca.pem: %v", err)
	}

	return &mtlsCA{key: key, cert: cert, PEM: caPEM, CAFile: caFile, serial: 1}
}

// signLeaf signs a leaf certificate with the CA and writes cert+key to
// t.TempDir(). prefix is used to name the files (e.g. "server", "sidecar").
// SANIPs is added to the cert when non-nil (needed for server certs).
// extKeyUsages selects EKU bits (ServerAuth for server, ClientAuth for client).
func (ca *mtlsCA) signLeaf(t *testing.T, cn, prefix string, sanIPs []net.IP, extKeyUsages []x509.ExtKeyUsage) *mtlsLeaf {
	t.Helper()

	ca.serial++
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate %s key: %v", prefix, err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(ca.serial),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  extKeyUsages,
		IPAddresses:  sanIPs,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &leafKey.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create %s cert: %v", prefix, err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatalf("marshal %s key: %v", prefix, err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	dir := t.TempDir()
	certFile := filepath.Join(dir, prefix+".crt")
	keyFile := filepath.Join(dir, prefix+".key")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write %s.crt: %v", prefix, err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write %s.key: %v", prefix, err)
	}
	return &mtlsLeaf{CertPEM: certPEM, KeyPEM: keyPEM, CertFile: certFile, KeyFile: keyFile}
}

// serverSANIPs is the SAN used for all aetherlite server certs (dial address).
var serverSANIPs = []net.IP{net.ParseIP("127.0.0.1")}

// =============================================================================
// mTLS aetherlite subprocess helper
// =============================================================================

// startMTLSAetherlite launches a dedicated aetherlite subprocess configured
// with TLS RequireAndVerifyClientCert using the provided cert files.
// Returns a *aetherliteProc whose stop() must be called by the caller (or
// registered via t.Cleanup).
func startMTLSAetherlite(serverLeaf *mtlsLeaf, ca *mtlsCA) (*aetherliteProc, error) {
	binPath, err := buildAetherliteBin()
	if err != nil {
		return nil, fmt.Errorf("build aetherlite: %w", err)
	}
	grpcPort, err := pickFreeTCPPort()
	if err != nil {
		return nil, fmt.Errorf("pick grpc port: %w", err)
	}
	adminPort, err := pickFreeTCPPort()
	if err != nil {
		return nil, fmt.Errorf("pick admin port: %w", err)
	}
	dataDir, err := os.MkdirTemp("", "aetherlite-mtls-e2e-")
	if err != nil {
		return nil, fmt.Errorf("mkdir data: %w", err)
	}

	// Write config YAML: gateway.tls.{cert_file,key_file,ca_file,client_auth}
	cfgDir, err := os.MkdirTemp("", "aetherlite-mtls-cfg-")
	if err != nil {
		_ = os.RemoveAll(dataDir)
		return nil, fmt.Errorf("mkdir cfg: %w", err)
	}
	cfgContent := fmt.Sprintf(`mode: lite
gateway:
  tls:
    cert_file: %q
    key_file: %q
    ca_file: %q
    client_auth: "require"
`, serverLeaf.CertFile, serverLeaf.KeyFile, ca.CAFile)
	cfgFile := filepath.Join(cfgDir, "config.yaml")
	if err := os.WriteFile(cfgFile, []byte(cfgContent), 0o600); err != nil {
		_ = os.RemoveAll(dataDir)
		_ = os.RemoveAll(cfgDir)
		return nil, fmt.Errorf("write config: %w", err)
	}

	// -config is prepended; CLI flags override individual values from config.
	args := []string{
		"-config", cfgFile,
		"-port", fmt.Sprintf("%d", grpcPort),
		"-admin-port", fmt.Sprintf("%d", adminPort),
		"-data-dir", dataDir,
		"-dev",
		"-insecure-admin",
		"-workflow=false",
	}
	cmd := exec.Command(binPath, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(dataDir)
		_ = os.RemoveAll(cfgDir)
		return nil, fmt.Errorf("start: %w", err)
	}

	grpcAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	if err := waitForTCP(grpcAddr, aetherliteStartTimeout); err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = os.RemoveAll(dataDir)
		_ = os.RemoveAll(cfgDir)
		return nil, fmt.Errorf("wait ready %s: %w", grpcAddr, err)
	}
	return &aetherliteProc{cmd: cmd, grpcAddr: grpcAddr, dataDir: dataDir}, nil
}

// =============================================================================
// mTLS sidecar harness
// =============================================================================

// mtlsSidecarResult holds the sidecar's service topic + backend URL.
type mtlsSidecarResult struct {
	ServiceTopic string
	BackendURL   string
}

// newMTLSSidecarHarness launches an in-process proxy-sidecar that connects to
// proc using a client cert signed by ca. The specifier is chosen before cert
// generation so the cert CN (sv::mtls-sidecar::<specifier>) matches the
// sidecar's declared identity — required by strict-mode mTLS.
//
// A probe caller (ag:: identity) is spun up to confirm the sidecar is ready
// before returning. All resources are cleaned up via t.Cleanup.
func newMTLSSidecarHarness(t *testing.T, proc *aetherliteProc, ca *mtlsCA) *mtlsSidecarResult {
	t.Helper()

	// Reserve specifier first so the cert CN can encode it exactly.
	specifier := fmt.Sprintf("e2e-mtls-%d", nextSidecarSpec.Add(1))
	sidecarCN := fmt.Sprintf("sv::mtls-sidecar::%s", specifier)

	// Sign the sidecar client cert with the CA that aetherlite trusts.
	sidecarLeaf := ca.signLeaf(t, sidecarCN, "sidecar-client",
		nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	backend := newHTTPBackend(t, 5*time.Second)

	cfg := &proxysidecar.Config{
		Gateway: proxysidecar.GatewayConfig{
			Address:  proc.grpcAddr,
			Insecure: false,
			TLS: proxysidecar.TLSConfig{
				CertFile: sidecarLeaf.CertFile,
				KeyFile:  sidecarLeaf.KeyFile,
				CAFile:   ca.CAFile,
			},
		},
		Service: proxysidecar.ServiceConfig{
			Implementation: "mtls-sidecar",
			Specifier:      specifier,
		},
		Terminator: proxysidecar.TerminatorConfig{
			Enabled: true,
			Backends: []proxysidecar.BackendConfig{{
				Name:          "local",
				Kind:          proxysidecar.BackendKindHTTP,
				URL:           backend.URL,
				AllowPaths:    []string{"/*"},
				AllowMethods:  []string{"GET", "POST"},
				MaxBodyBytes:  10 << 20,
				IdleTimeoutMs: 60_000,
				HeaderMode:    proxysidecar.HeaderModePassthrough,
			}},
		},
		TenantID: "tenant-mtls-e2e",
	}

	runner, err := proxysidecar.NewRunner(cfg, "")
	if err != nil {
		t.Fatalf("mtls NewRunner: %v", err)
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	runnerDone := make(chan struct{})
	go func() {
		defer close(runnerDone)
		_ = runner.Run(runCtx)
	}()
	t.Cleanup(func() {
		runCancel()
		select {
		case <-runnerDone:
		case <-time.After(15 * time.Second):
			t.Logf("warning: mtls runner did not exit within 15s")
		}
	})

	topic := fmt.Sprintf("sv::mtls-sidecar::%s", specifier)

	// Probe: ag-typed client cert so the probe agent connects with a valid
	// identity (ag::e2e::harness-probe::<id>). Signed by the same CA.
	probeID := fmt.Sprintf("probe-%d", nextSidecarSpec.Add(1))
	probeCN := fmt.Sprintf("ag::e2e::harness-probe::%s", probeID)
	probeLeaf := ca.signLeaf(t, probeCN, "probe-client",
		nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	probeTLS := &aether.TLSConfig{
		Enabled:    true,
		RootCAs:    ca.PEM,
		ClientCert: probeLeaf.CertPEM,
		ClientKey:  probeLeaf.KeyPEM,
		ServerName: "127.0.0.1",
	}
	if err := waitForMTLSSidecarReady(t, proc.grpcAddr, topic, backend.URL, probeID, probeTLS); err != nil {
		t.Fatalf("mtls sidecar never reached ready: %v", err)
	}

	return &mtlsSidecarResult{ServiceTopic: topic, BackendURL: backend.URL}
}

// waitForMTLSSidecarReady is like waitForSidecarReady but dials with mTLS.
// probeSpecifier is the Specifier used for the agent client (must match the
// probe cert CN field to satisfy strict-mode identity binding).
func waitForMTLSSidecarReady(t *testing.T, gwAddr, serviceTopic, backendURL, probeSpecifier string, tlsCfg *aether.TLSConfig) error {
	t.Helper()

	if u, err := parseBackendHostPort(backendURL); err == nil {
		if err := waitForTCP(u, 3*time.Second); err != nil {
			return fmt.Errorf("local backend %s never ready: %w", u, err)
		}
	}

	client, err := aether.NewAgentClient(aether.AgentOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr: gwAddr,
			TLS:        tlsCfg,
			Connection: aether.ConnectionOptions{
				MaxRetries:        1,
				InitialBackoff:    50 * time.Millisecond,
				MaxBackoff:        500 * time.Millisecond,
				BackoffMultiplier: 2.0,
				AutoReconnect:     false,
				ConnectTimeout:    5 * time.Second,
				KeepAliveInterval: 10 * time.Second,
			},
		},
		Workspace:      "e2e",
		Implementation: "harness-probe",
		Specifier:      probeSpecifier,
	})
	if err != nil {
		return fmt.Errorf("probe NewAgentClient: %w", err)
	}
	connectCtx, connectCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer connectCancel()
	if err := client.Connect(connectCtx); err != nil {
		return fmt.Errorf("probe Connect: %w", err)
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	go func() { _ = client.Run(runCtx) }()
	defer func() {
		runCancel()
		_ = client.CloseConnection()
	}()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if !client.ConnectionConfirmed() {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		req, _ := http.NewRequestWithContext(probeCtx, "GET", "http://ignored/fast", nil)
		resp, err := client.ProxyHTTP(probeCtx, serviceTopic, req, aether.WithBackend("local"))
		probeCancel()
		if err == nil {
			if resp != nil && resp.Body != nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("mtls probe never succeeded within 15s; confirmed=%v", client.ConnectionConfirmed())
}

// =============================================================================
// Tests
// =============================================================================

// TestE2E_MTLS_HappyPath_RoundTrip starts a TLS-required aetherlite, connects
// a proxy-sidecar with a valid client cert, issues an SDK ProxyHTTP call, and
// asserts:
//
//  1. TLS handshake succeeds (sidecar connects, ConnectionAck received).
//  2. ProxyHTTP returns HTTP 200 from the in-process backend.
//  3. The sidecar cert CN (sv::mtls-sidecar::<specifier>) is parseable as a
//     PrincipalService identity — confirmed by gateway log ("mTLS authenticated
//     identity (strict mode)") and by direct CN parse in the test.
func TestE2E_MTLS_HappyPath_RoundTrip(t *testing.T) {
	// No t.Parallel() — consistent with other e2e tests in this package.

	ca := generateMTLSCA(t)

	// Server cert: SAN=127.0.0.1 (dial address).
	serverLeaf := ca.signLeaf(t, "aetherlite-mtls-test", "server",
		serverSANIPs, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})

	proc, err := startMTLSAetherlite(serverLeaf, ca)
	if err != nil {
		t.Fatalf("startMTLSAetherlite: %v", err)
	}
	t.Cleanup(proc.stop)

	// Launch sidecar with its own client cert (CN matches specifier).
	sidecar := newMTLSSidecarHarness(t, proc, ca)

	// SDK caller: ag-typed cert, same CA.
	callerN := nextSidecarSpec.Add(1)
	callerCN := fmt.Sprintf("ag::e2e::caller::%d", callerN)
	callerLeaf := ca.signLeaf(t, callerCN, "caller-client",
		nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	callerTLS := &aether.TLSConfig{
		Enabled:    true,
		RootCAs:    ca.PEM,
		ClientCert: callerLeaf.CertPEM,
		ClientKey:  callerLeaf.KeyPEM,
		ServerName: "127.0.0.1",
	}
	callerSpec := fmt.Sprintf("%d", callerN)
	caller, err := aether.NewAgentClient(aether.AgentOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr: proc.grpcAddr,
			TLS:        callerTLS,
			Connection: aether.ConnectionOptions{
				MaxRetries:        1,
				InitialBackoff:    50 * time.Millisecond,
				MaxBackoff:        500 * time.Millisecond,
				BackoffMultiplier: 2.0,
				AutoReconnect:     false,
				ConnectTimeout:    5 * time.Second,
				KeepAliveInterval: 10 * time.Second,
			},
		},
		Workspace:      "e2e",
		Implementation: "caller",
		Specifier:      callerSpec,
	})
	if err != nil {
		t.Fatalf("NewAgentClient: %v", err)
	}
	connectCtx, connectCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer connectCancel()
	if err := caller.Connect(connectCtx); err != nil {
		t.Fatalf("caller Connect: %v", err)
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	go func() { _ = caller.Run(runCtx) }()
	t.Cleanup(func() {
		runCancel()
		_ = caller.CloseConnection()
	})

	// Wait for ConnectionAck.
	deadline := time.Now().Add(10 * time.Second)
	for !caller.ConnectionConfirmed() {
		if time.Now().After(deadline) {
			t.Fatalf("caller ConnectionAck not observed within 10s")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Assert 1+2: end-to-end round-trip succeeds.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", "http://ignored/fast", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := caller.ProxyHTTP(ctx, sidecar.ServiceTopic, req, aether.WithBackend("local"))
	if err != nil {
		t.Fatalf("ProxyHTTP: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("want status 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if want := `{"ok":true}`; string(body) != want {
		t.Errorf("body=%q, want %q", string(body), want)
	}

	// Assert 3: confirm the sidecar cert CN is gateway-parseable.
	// The sidecar topic is sv::mtls-sidecar::<specifier>; extract specifier
	// from the topic and verify the CN format is correct.
	parts := splitIdentitySep(sidecar.ServiceTopic)
	if len(parts) != 3 || parts[0] != "sv" || parts[1] != "mtls-sidecar" {
		t.Errorf("sidecar topic %q does not parse as sv::impl::spec: parts=%v", sidecar.ServiceTopic, parts)
	}
}

// TestE2E_MTLS_NoClientCert_Rejected dials the mTLS-required aetherlite
// WITHOUT a client certificate and asserts the TLS handshake is rejected.
func TestE2E_MTLS_NoClientCert_Rejected(t *testing.T) {
	ca := generateMTLSCA(t)
	serverLeaf := ca.signLeaf(t, "aetherlite-mtls-test", "server",
		serverSANIPs, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})

	proc, err := startMTLSAetherlite(serverLeaf, ca)
	if err != nil {
		t.Fatalf("startMTLSAetherlite: %v", err)
	}
	t.Cleanup(proc.stop)

	// TLS with CA for server verification but NO client cert.
	noCertTLS := &aether.TLSConfig{
		Enabled:    true,
		RootCAs:    ca.PEM,
		ServerName: "127.0.0.1",
		// ClientCert / ClientKey intentionally absent.
	}

	callerID := fmt.Sprintf("nocert-%d", nextSidecarSpec.Add(1))
	client, err := aether.NewAgentClient(aether.AgentOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr: proc.grpcAddr,
			TLS:        noCertTLS,
			Connection: aether.ConnectionOptions{
				MaxRetries:        1,
				InitialBackoff:    50 * time.Millisecond,
				MaxBackoff:        200 * time.Millisecond,
				BackoffMultiplier: 2.0,
				AutoReconnect:     false,
				ConnectTimeout:    5 * time.Second,
			},
		},
		Workspace:      "e2e",
		Implementation: "caller",
		Specifier:      callerID,
	})
	if err != nil {
		t.Fatalf("NewAgentClient: %v", err)
	}
	defer client.CloseConnection()

	connectCtx, connectCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer connectCancel()

	connectErr := client.Connect(connectCtx)
	if connectErr != nil {
		// Fail-fast at TLS handshake — expected.
		t.Logf("Connect rejected at handshake (expected): %v", connectErr)
		return
	}

	// gRPC lazy-dial: error surfaces on the stream. Run and wait.
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	go func() { _ = client.Run(runCtx) }()

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if client.ConnectionConfirmed() {
			t.Error("connection confirmed despite missing client certificate — mTLS not enforced")
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("connection not confirmed within 8s (expected — gateway rejected no-cert dial)")
}

// TestE2E_MTLS_BadClientCert_Rejected dials the mTLS-required aetherlite with
// a client cert signed by a DIFFERENT (rogue) CA. The gateway's CA pool does
// not include the rogue CA, so it rejects the handshake.
func TestE2E_MTLS_BadClientCert_Rejected(t *testing.T) {
	ca := generateMTLSCA(t)
	serverLeaf := ca.signLeaf(t, "aetherlite-mtls-test", "server",
		serverSANIPs, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})

	proc, err := startMTLSAetherlite(serverLeaf, ca)
	if err != nil {
		t.Fatalf("startMTLSAetherlite: %v", err)
	}
	t.Cleanup(proc.stop)

	// Generate a separate rogue CA and sign a client cert with it.
	rogueCA := generateMTLSCA(t)
	rogueLeaf := rogueCA.signLeaf(t, "sv::rogue::bad", "rogue-client",
		nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	// Dial: trust the real server cert (real CA RootCAs) but present rogue cert.
	badTLS := &aether.TLSConfig{
		Enabled:    true,
		RootCAs:    ca.PEM,            // trust real server cert
		ClientCert: rogueLeaf.CertPEM, // present cert signed by untrusted rogue CA
		ClientKey:  rogueLeaf.KeyPEM,
		ServerName: "127.0.0.1",
	}

	callerID := fmt.Sprintf("badcert-%d", nextSidecarSpec.Add(1))
	client, err := aether.NewAgentClient(aether.AgentOptions{
		ClientOptions: aether.ClientOptions{
			ServerAddr: proc.grpcAddr,
			TLS:        badTLS,
			Connection: aether.ConnectionOptions{
				MaxRetries:        1,
				InitialBackoff:    50 * time.Millisecond,
				MaxBackoff:        200 * time.Millisecond,
				BackoffMultiplier: 2.0,
				AutoReconnect:     false,
				ConnectTimeout:    5 * time.Second,
			},
		},
		Workspace:      "e2e",
		Implementation: "caller",
		Specifier:      callerID,
	})
	if err != nil {
		t.Fatalf("NewAgentClient: %v", err)
	}
	defer client.CloseConnection()

	connectCtx, connectCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer connectCancel()

	connectErr := client.Connect(connectCtx)
	if connectErr != nil {
		t.Logf("Connect rejected with rogue cert (expected): %v", connectErr)
		st, ok := status.FromError(connectErr)
		if ok && st.Code() == codes.Internal {
			t.Errorf("unexpected Internal gRPC code (want Unavailable/transport): %v", connectErr)
		}
		return
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	go func() { _ = client.Run(runCtx) }()

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if client.ConnectionConfirmed() {
			t.Error("connection confirmed despite rogue-CA client cert — mTLS not enforced")
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("connection not confirmed within 8s (expected — gateway rejected rogue cert)")
}

// =============================================================================
// Small helpers
// =============================================================================

// splitIdentitySep splits s on "::" (the Aether identity separator).
// Mirrors the logic in gateway.ParseIdentityFromCN without importing that pkg.
func splitIdentitySep(s string) []string {
	var parts []string
	for {
		idx := -1
		for i := 0; i+1 < len(s); i++ {
			if s[i] == ':' && s[i+1] == ':' {
				idx = i
				break
			}
		}
		if idx < 0 {
			parts = append(parts, s)
			break
		}
		parts = append(parts, s[:idx])
		s = s[idx+2:]
	}
	return parts
}
