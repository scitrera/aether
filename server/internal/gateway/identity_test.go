package gateway

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/scitrera/aether/pkg/models"
)

func TestParseIdentityFromCN(t *testing.T) {
	tests := []struct {
		name     string
		cn       string
		expected models.Identity
		wantErr  bool
	}{
		{
			name: "valid agent CN",
			cn:   "ag::production::python-worker::instance-1",
			expected: models.Identity{
				Type:           models.PrincipalAgent,
				Workspace:      "production",
				Implementation: "python-worker",
				Specifier:      "instance-1",
			},
			wantErr: false,
		},
		{
			name: "valid staging agent CN",
			cn:   "ag::staging::data-processor::v1",
			expected: models.Identity{
				Type:           models.PrincipalAgent,
				Workspace:      "staging",
				Implementation: "data-processor",
				Specifier:      "v1",
			},
			wantErr: false,
		},
		{
			name: "valid unique task CN",
			cn:   "tu::default::batch-job::job-123",
			expected: models.Identity{
				Type:           models.PrincipalTask,
				Workspace:      "default",
				Implementation: "batch-job",
				Specifier:      "job-123",
			},
			wantErr: false,
		},
		{
			name: "valid non-unique task CN",
			cn:   "ta::production::stream-processor",
			expected: models.Identity{
				Type:           models.PrincipalTask,
				Workspace:      "production",
				Implementation: "stream-processor",
			},
			wantErr: false,
		},
		{
			name: "valid user CN",
			cn:   "us::alice::window-1",
			expected: models.Identity{
				Type:      models.PrincipalUser,
				ID:        "alice",
				Specifier: "window-1",
			},
			wantErr: false,
		},
		{
			name: "valid global agent broadcast CN",
			cn:   "ga::default",
			expected: models.Identity{
				Type:           models.PrincipalAgent,
				Workspace:      "default",
				Implementation: "*",
				Specifier:      "*",
			},
			wantErr: false,
		},
		{
			name: "valid task broadcast CN",
			cn:   "tb::default::worker",
			expected: models.Identity{
				Type:           models.PrincipalTask,
				Workspace:      "default",
				Implementation: "worker",
				Specifier:      "*",
			},
			wantErr: false,
		},
		{
			name: "valid workflow engine CN",
			cn:   "wfe::shard0",
			expected: models.Identity{
				Type: models.PrincipalWorkflowEngine,
			},
			wantErr: false,
		},
		{
			name: "valid metrics bridge CN",
			cn:   "metrics::shard0",
			expected: models.Identity{
				Type: models.PrincipalMetricsBridge,
			},
			wantErr: false,
		},
		{
			name: "valid orchestrator CN",
			cn:   "orc::agent-orchestrator",
			expected: models.Identity{
				Type:           models.PrincipalOrchestrator,
				Implementation: "agent-orchestrator",
			},
			wantErr: false,
		},
		{
			name: "valid orchestrator CN with specifier",
			cn:   "orc::kubernetes::cluster-1",
			expected: models.Identity{
				Type:           models.PrincipalOrchestrator,
				Implementation: "kubernetes",
				Specifier:      "cluster-1",
			},
			wantErr: false,
		},
		{
			name: "valid service CN",
			cn:   "sv::frontend-api::pod-1",
			expected: models.Identity{
				Type:           models.PrincipalService,
				Implementation: "frontend-api",
				Specifier:      "pod-1",
			},
			wantErr: false,
		},
		{
			name:    "invalid - too few parts",
			cn:      "invalid",
			wantErr: true,
		},
		{
			name:    "invalid - unknown type",
			cn:      "xx.default.test",
			wantErr: true,
		},
		{
			name: "agent with dotted implementation name",
			cn:   "ag::production::claude.code::instance-1",
			expected: models.Identity{
				Type:           models.PrincipalAgent,
				Workspace:      "production",
				Implementation: "claude.code",
				Specifier:      "instance-1",
			},
			wantErr: false,
		},
		{
			name: "unique task with dotted implementation name",
			cn:   "tu::default::my.dotted.impl::task-1",
			expected: models.Identity{
				Type:           models.PrincipalTask,
				Workspace:      "default",
				Implementation: "my.dotted.impl",
				Specifier:      "task-1",
			},
			wantErr: false,
		},
		{
			name:    "invalid - agent with wrong parts count",
			cn:      "ag::production::python-worker",
			wantErr: true,
		},
		{
			name:    "invalid - user with wrong parts count",
			cn:      "us::alice",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseIdentityFromCN(tt.cn)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseIdentityFromCN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !compareIdentity(got, tt.expected) {
				t.Errorf("ParseIdentityFromCN() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestValidateCertificate(t *testing.T) {
	// Create a valid certificate
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "test-cert",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  false,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("failed to parse certificate: %v", err)
	}

	// Test valid certificate
	if err := ValidateCertificate(cert); err != nil {
		t.Errorf("ValidateCertificate() valid cert error = %v, want nil", err)
	}

	// Test CA certificate (should fail)
	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: "test-ca",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("failed to create CA certificate: %v", err)
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		t.Fatalf("failed to parse CA certificate: %v", err)
	}

	if err := ValidateCertificate(caCert); err == nil {
		t.Error("ValidateCertificate() CA cert should fail")
	}
}

func TestGenerateTestCertificate(t *testing.T) {
	// Create a test certificate for testing
	cert, key, err := generateTestCertCN("ag::test::workspace::agent-1")
	if err != nil {
		t.Fatalf("failed to generate test certificate: %v", err)
	}

	if cert.Subject.CommonName != "ag::test::workspace::agent-1" {
		t.Errorf("certificate CN = %s, want ag::test::workspace::agent-1", cert.Subject.CommonName)
	}

	if key == nil {
		t.Error("private key should not be nil")
	}
}

// Helper functions for testing

func compareIdentity(a, b models.Identity) bool {
	return a.Type == b.Type &&
		a.Workspace == b.Workspace &&
		a.Implementation == b.Implementation &&
		a.Specifier == b.Specifier &&
		a.ID == b.ID
}

func generateTestCertCN(cn string) (*x509.Certificate, *rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName: cn,
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  false,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, err
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, nil, err
	}

	return cert, privateKey, nil
}
