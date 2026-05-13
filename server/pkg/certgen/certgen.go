package certgen

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

const AnonymousCertCN = "_anonymous"

// CA holds a Certificate Authority certificate and key.
type CA struct {
	Cert    *x509.Certificate
	Key     crypto.PrivateKey
	CertPEM []byte
	KeyPEM  []byte
}

// CertBundle holds a signed certificate and key along with the CA cert PEM.
type CertBundle struct {
	Cert    *x509.Certificate
	Key     crypto.PrivateKey
	CertPEM []byte
	KeyPEM  []byte
	CAPEM   []byte
}

// CAOptions configures CA generation.
type CAOptions struct {
	CommonName string
	Org        string
	Validity   time.Duration
}

// ServerCertOptions configures server TLS cert generation.
type ServerCertOptions struct {
	CommonName string
	DNSNames   []string
	IPs        []net.IP
	Validity   time.Duration
}

// ClientCertOptions configures client mTLS cert generation.
type ClientCertOptions struct {
	CommonName string
	Org        string
	OrgUnit    string
	Validity   time.Duration
}

func applyCADefaults(opts *CAOptions) {
	if opts.CommonName == "" {
		opts.CommonName = "Aether CA"
	}
	if opts.Org == "" {
		opts.Org = "Aether"
	}
	if opts.Validity == 0 {
		opts.Validity = 10 * 365 * 24 * time.Hour
	}
}

func applyServerDefaults(opts *ServerCertOptions) {
	if opts.CommonName == "" {
		opts.CommonName = "aether-gateway"
	}
	if len(opts.DNSNames) == 0 {
		opts.DNSNames = []string{"localhost", "aether-gateway"}
	}
	if len(opts.IPs) == 0 {
		opts.IPs = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	}
	if opts.Validity == 0 {
		opts.Validity = 365 * 24 * time.Hour
	}
}

func applyClientDefaults(opts *ClientCertOptions) {
	if opts.Org == "" {
		opts.Org = "Aether"
	}
	if opts.Validity == 0 {
		opts.Validity = 365 * 24 * time.Hour
	}
}

func generateSerialNumber() (*big.Int, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	return serial, nil
}

func generateECDSAKey() (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
}

func encodeCertPEM(derBytes []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
}

func encodeKeyPEM(key *ecdsa.PrivateKey) ([]byte, error) {
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), nil
}

func parseECKeyPEM(data []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, &pemDecodeError{"failed to decode PEM block for EC private key"}
	}
	return x509.ParseECPrivateKey(block.Bytes)
}

type pemDecodeError struct{ msg string }

func (e *pemDecodeError) Error() string { return e.msg }

// GenerateCA generates a new ECDSA P-256 CA key and self-signed CA certificate.
func GenerateCA(opts CAOptions) (*CA, error) {
	applyCADefaults(&opts)

	key, err := generateECDSAKey()
	if err != nil {
		return nil, err
	}

	serial, err := generateSerialNumber()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   opts.CommonName,
			Organization: []string{opts.Org},
		},
		NotBefore:             now,
		NotAfter:              now.Add(opts.Validity),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, err
	}

	certPEM := encodeCertPEM(certDER)
	keyPEM, err := encodeKeyPEM(key)
	if err != nil {
		return nil, err
	}

	return &CA{
		Cert:    cert,
		Key:     key,
		CertPEM: certPEM,
		KeyPEM:  keyPEM,
	}, nil
}

// LoadCA loads an existing CA from PEM files on disk.
func LoadCA(certPath, keyPath string) (*CA, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, err
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, &pemDecodeError{"failed to decode CA certificate PEM"}
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}

	key, err := parseECKeyPEM(keyPEM)
	if err != nil {
		return nil, err
	}

	return &CA{
		Cert:    cert,
		Key:     key,
		CertPEM: certPEM,
		KeyPEM:  keyPEM,
	}, nil
}

// EnsureCA loads an existing CA if both files exist, otherwise generates a new one and saves it.
func EnsureCA(certPath, keyPath string, opts CAOptions) (*CA, error) {
	_, certErr := os.Stat(certPath)
	_, keyErr := os.Stat(keyPath)
	if certErr == nil && keyErr == nil {
		return LoadCA(certPath, keyPath)
	}

	ca, err := GenerateCA(opts)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(certPath), 0750); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0750); err != nil {
		return nil, err
	}
	if err := os.WriteFile(certPath, ca.CertPEM, 0600); err != nil {
		return nil, err
	}
	if err := os.WriteFile(keyPath, ca.KeyPEM, 0600); err != nil {
		return nil, err
	}

	return ca, nil
}

// GenerateServerCert generates a server TLS certificate signed by the CA.
func (ca *CA) GenerateServerCert(opts ServerCertOptions) (*CertBundle, error) {
	applyServerDefaults(&opts)

	key, err := generateECDSAKey()
	if err != nil {
		return nil, err
	}

	serial, err := generateSerialNumber()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: opts.CommonName,
		},
		DNSNames:    opts.DNSNames,
		IPAddresses: opts.IPs,
		NotBefore:   now,
		NotAfter:    now.Add(opts.Validity),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.Key)
	if err != nil {
		return nil, err
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, err
	}

	certPEM := encodeCertPEM(certDER)
	keyPEM, err := encodeKeyPEM(key)
	if err != nil {
		return nil, err
	}

	return &CertBundle{
		Cert:    cert,
		Key:     key,
		CertPEM: certPEM,
		KeyPEM:  keyPEM,
		CAPEM:   ca.CertPEM,
	}, nil
}

// GenerateClientCert generates an identity-bearing mTLS client certificate signed by the CA.
func (ca *CA) GenerateClientCert(opts ClientCertOptions) (*CertBundle, error) {
	applyClientDefaults(&opts)

	key, err := generateECDSAKey()
	if err != nil {
		return nil, err
	}

	serial, err := generateSerialNumber()
	if err != nil {
		return nil, err
	}

	subject := pkix.Name{
		CommonName:   opts.CommonName,
		Organization: []string{opts.Org},
	}
	if opts.OrgUnit != "" {
		subject.OrganizationalUnit = []string{opts.OrgUnit}
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      subject,
		NotBefore:    now,
		NotAfter:     now.Add(opts.Validity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.Key)
	if err != nil {
		return nil, err
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, err
	}

	certPEM := encodeCertPEM(certDER)
	keyPEM, err := encodeKeyPEM(key)
	if err != nil {
		return nil, err
	}

	return &CertBundle{
		Cert:    cert,
		Key:     key,
		CertPEM: certPEM,
		KeyPEM:  keyPEM,
		CAPEM:   ca.CertPEM,
	}, nil
}

// GenerateAnonymousClientCert generates an anonymous mTLS client certificate.
func (ca *CA) GenerateAnonymousClientCert() (*CertBundle, error) {
	return ca.GenerateClientCert(ClientCertOptions{
		CommonName: AnonymousCertCN,
		Org:        "Aether",
		OrgUnit:    "Anonymous",
		Validity:   365 * 24 * time.Hour,
	})
}

// SaveToFiles writes the certificate and key PEM files to disk.
func (b *CertBundle) SaveToFiles(certPath, keyPath string) error {
	if err := os.MkdirAll(filepath.Dir(certPath), 0750); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0750); err != nil {
		return err
	}
	if err := os.WriteFile(certPath, b.CertPEM, 0600); err != nil {
		return err
	}
	return os.WriteFile(keyPath, b.KeyPEM, 0600)
}

// TLSCertificate returns an in-memory tls.Certificate from the bundle's PEM bytes.
func (b *CertBundle) TLSCertificate() (tls.Certificate, error) {
	return tls.X509KeyPair(b.CertPEM, b.KeyPEM)
}
