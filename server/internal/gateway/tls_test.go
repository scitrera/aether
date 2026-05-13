package gateway

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
)

// TestTLSServerConfig tests loading TLS configuration
func TestLoadTLSConfig(t *testing.T) {
	// Create temp directory for test certificates
	tmpDir, err := os.MkdirTemp("", "aether-tls-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Generate test CA
	caKey, caCert := generateTestCA(t, tmpDir)

	// Generate server certificate
	generateTestServerCert(t, tmpDir, caKey, caCert)

	// Test loading valid TLS config
	config, err := LoadTLSConfig(
		filepath.Join(tmpDir, "server-cert.pem"),
		filepath.Join(tmpDir, "server-key.pem"),
		filepath.Join(tmpDir, "ca-cert.pem"),
		tls.RequireAndVerifyClientCert,
	)
	if err != nil {
		t.Fatalf("LoadTLSConfig() error = %v", err)
	}

	if config == nil {
		t.Error("LoadTLSConfig() returned nil config")
	}

	if config.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("ClientAuth = %v, want RequireAndVerifyClientCert", config.ClientAuth)
	}

	if config.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %v, want TLS13", config.MinVersion)
	}

	// Test loading with non-existent files
	_, err = LoadTLSConfig(
		"non-existent-cert.pem",
		"non-existent-key.pem",
		"non-existent-ca.pem",
		tls.RequireAndVerifyClientCert,
	)
	if err == nil {
		t.Error("LoadTLSConfig() with non-existent files should fail")
	}
}

// TestParseClientAuth tests all valid ParseClientAuth values
func TestParseClientAuth(t *testing.T) {
	tests := []struct {
		input string
		want  tls.ClientAuthType
	}{
		{"require", tls.RequireAndVerifyClientCert},
		{"REQUIRE", tls.RequireAndVerifyClientCert},
		{"", tls.RequireAndVerifyClientCert},
		{"unknown", tls.RequireAndVerifyClientCert},
		{"request", tls.VerifyClientCertIfGiven},
		{"REQUEST", tls.VerifyClientCertIfGiven},
		{"none", tls.NoClientCert},
		{"NONE", tls.NoClientCert},
		{"  none  ", tls.NoClientCert},
	}

	for _, tt := range tests {
		got := ParseClientAuth(tt.input)
		if got != tt.want {
			t.Errorf("ParseClientAuth(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

// TestLoadTLSConfig_NoClientCert verifies CA pool is nil when clientAuth is NoClientCert
func TestLoadTLSConfig_NoClientCert(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aether-tls-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	caKey, caCert := generateTestCA(t, tmpDir)
	generateTestServerCert(t, tmpDir, caKey, caCert)

	config, err := LoadTLSConfig(
		filepath.Join(tmpDir, "server-cert.pem"),
		filepath.Join(tmpDir, "server-key.pem"),
		filepath.Join(tmpDir, "ca-cert.pem"),
		tls.NoClientCert,
	)
	if err != nil {
		t.Fatalf("LoadTLSConfig() error = %v", err)
	}

	if config.ClientAuth != tls.NoClientCert {
		t.Errorf("ClientAuth = %v, want NoClientCert", config.ClientAuth)
	}
	if config.ClientCAs != nil {
		t.Error("ClientCAs should be nil when clientAuth is NoClientCert")
	}
}

// TestLoadTLSConfig_VerifyClientCertIfGiven verifies CA pool is set when clientAuth is VerifyClientCertIfGiven
func TestLoadTLSConfig_VerifyClientCertIfGiven(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aether-tls-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	caKey, caCert := generateTestCA(t, tmpDir)
	generateTestServerCert(t, tmpDir, caKey, caCert)

	config, err := LoadTLSConfig(
		filepath.Join(tmpDir, "server-cert.pem"),
		filepath.Join(tmpDir, "server-key.pem"),
		filepath.Join(tmpDir, "ca-cert.pem"),
		tls.VerifyClientCertIfGiven,
	)
	if err != nil {
		t.Fatalf("LoadTLSConfig() error = %v", err)
	}

	if config.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Errorf("ClientAuth = %v, want VerifyClientCertIfGiven", config.ClientAuth)
	}
	if config.ClientCAs == nil {
		t.Error("ClientCAs should be set when clientAuth is VerifyClientCertIfGiven")
	}
}

// TestNewGRPCServerWithTLS tests creating a gRPC server with TLS
func TestNewGRPCServerWithTLS(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aether-tls-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Generate test certificates
	generateTestCA(t, tmpDir)
	generateTestServerCert(t, tmpDir, nil, nil)

	tlsConfig := TLSConfig{
		CertFile: filepath.Join(tmpDir, "server-cert.pem"),
		KeyFile:  filepath.Join(tmpDir, "server-key.pem"),
		CAFile:   filepath.Join(tmpDir, "ca-cert.pem"),
	}

	server, err := NewGRPCServerWithTLS(tlsConfig)
	if err != nil {
		t.Fatalf("NewGRPCServerWithTLS() error = %v", err)
	}

	if server == nil {
		t.Error("NewGRPCServerWithTLS() returned nil server")
	}

	server.Stop()
}

// TestLoadClientCertificate tests loading client certificate
func TestLoadClientCertificate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aether-tls-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Generate test certificates - they are written to files by generateTestClientCert
	generateTestCA(t, tmpDir)
	generateTestClientCert(t, tmpDir, nil, nil)

	cert, err := LoadClientCertificate(
		filepath.Join(tmpDir, "client-cert.pem"),
		filepath.Join(tmpDir, "client-key.pem"),
	)
	if err != nil {
		t.Fatalf("LoadClientCertificate() error = %v", err)
	}

	// tls.Certificate is a struct, check that we got valid data
	if len(cert.Certificate) == 0 {
		t.Error("LoadClientCertificate() returned certificate with no public key data")
	}
}

// TestNewClientTLSConfig tests creating client TLS configuration
func TestNewClientTLSConfig(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aether-tls-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Generate test CA
	generateTestCA(t, tmpDir)

	// Generate client certificate
	clientKey, clientCertPEM := generateTestClientCert(t, tmpDir, nil, nil)
	_ = clientKey

	// Write client cert for loading
	certFile := filepath.Join(tmpDir, "client-cert.pem")
	if err := os.WriteFile(certFile, clientCertPEM, 0644); err != nil {
		t.Fatalf("failed to write client cert: %v", err)
	}

	clientCert, err := tls.LoadX509KeyPair(certFile, filepath.Join(tmpDir, "client-key.pem"))
	if err != nil {
		t.Fatalf("failed to load client cert: %v", err)
	}

	caCertPEM, err := os.ReadFile(filepath.Join(tmpDir, "ca-cert.pem"))
	if err != nil {
		t.Fatalf("failed to read CA cert: %v", err)
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCertPEM) {
		t.Fatal("failed to append CA cert")
	}

	config := NewClientTLSConfig("test-server", clientCert, caPool)
	if config == nil {
		t.Error("NewClientTLSConfig() returned nil config")
	}

	if config.ServerName != "test-server" {
		t.Errorf("ServerName = %s, want test-server", config.ServerName)
	}
}

// Helper functions for testing

// generateTestCAWithPEM generates a CA and returns both key and cert as byte slices
func generateTestCAWithPEM(t *testing.T, tmpDir string) (*rsa.PrivateKey, []byte, error) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:   "Aether Test CA",
			Organization: []string{"Aether"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	if err := os.WriteFile(filepath.Join(tmpDir, "ca-cert.pem"), certPEM, 0644); err != nil {
		return nil, nil, err
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "ca-key.pem"), keyPEM, 0644); err != nil {
		return nil, nil, err
	}

	return privateKey, certPEM, nil
}

func generateTestCA(t *testing.T, tmpDir string) (*rsa.PrivateKey, []byte) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate CA private key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:   "Aether Test CA",
			Organization: []string{"Aether"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("failed to create CA certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	if err := os.WriteFile(filepath.Join(tmpDir, "ca-cert.pem"), certPEM, 0644); err != nil {
		t.Fatalf("failed to write CA cert: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "ca-key.pem"), keyPEM, 0644); err != nil {
		t.Fatalf("failed to write CA key: %v", err)
	}

	return privateKey, certPEM
}

func generateTestServerCert(t *testing.T, tmpDir string, caKey *rsa.PrivateKey, caCertPEM []byte) (*rsa.PrivateKey, []byte) {
	t.Helper()

	if caKey == nil {
		var err error
		caKey, caCertPEM, err = generateTestCAWithPEM(t, tmpDir)
		if err != nil {
			t.Fatalf("failed to generate CA: %v", err)
		}
	}

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate server private key: %v", err)
	}

	caCert, _ := parsePEMCert(caCertPEM)

	template := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName:   "aether-gateway",
			Organization: []string{"Aether"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, caCert, &privateKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create server certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	if err := os.WriteFile(filepath.Join(tmpDir, "server-cert.pem"), certPEM, 0644); err != nil {
		t.Fatalf("failed to write server cert: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "server-key.pem"), keyPEM, 0644); err != nil {
		t.Fatalf("failed to write server key: %v", err)
	}

	return privateKey, certPEM
}

func generateTestClientCert(t *testing.T, tmpDir string, caKey *rsa.PrivateKey, caCertPEM []byte) (*rsa.PrivateKey, []byte) {
	t.Helper()

	if caKey == nil {
		var err error
		caKey, caCertPEM, err = generateTestCAWithPEM(t, tmpDir)
		if err != nil {
			t.Fatalf("failed to generate CA: %v", err)
		}
	}

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate client private key: %v", err)
	}

	caCert, _ := parsePEMCert(caCertPEM)

	template := x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			CommonName:   "ag::test::workspace::agent-1",
			Organization: []string{"Aether"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, caCert, &privateKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create client certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	if err := os.WriteFile(filepath.Join(tmpDir, "client-cert.pem"), certPEM, 0644); err != nil {
		t.Fatalf("failed to write client cert: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "client-key.pem"), keyPEM, 0644); err != nil {
		t.Fatalf("failed to write client key: %v", err)
	}

	return privateKey, certPEM
}

func parsePEMCert(pemData []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, nil
	}
	return x509.ParseCertificate(block.Bytes)
}

func getTLSInfoFromCert(cert *x509.Certificate) credentials.TLSInfo {
	return credentials.TLSInfo{
		State: tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		},
	}
}
