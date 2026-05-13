package certgen

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGenerateCA(t *testing.T) {
	ca, err := GenerateCA(CAOptions{})
	if err != nil {
		t.Fatalf("GenerateCA failed: %v", err)
	}

	if ca.Cert == nil {
		t.Fatal("CA cert is nil")
	}
	if ca.Key == nil {
		t.Fatal("CA key is nil")
	}
	if len(ca.CertPEM) == 0 {
		t.Fatal("CA CertPEM is empty")
	}
	if len(ca.KeyPEM) == 0 {
		t.Fatal("CA KeyPEM is empty")
	}

	// Verify it's a CA
	if !ca.Cert.IsCA {
		t.Error("expected IsCA=true")
	}
	if !ca.Cert.BasicConstraintsValid {
		t.Error("expected BasicConstraintsValid=true")
	}

	// Verify default CN and Org
	if ca.Cert.Subject.CommonName != "Aether CA" {
		t.Errorf("expected CN='Aether CA', got %q", ca.Cert.Subject.CommonName)
	}
	if len(ca.Cert.Subject.Organization) == 0 || ca.Cert.Subject.Organization[0] != "Aether" {
		t.Errorf("expected Org='Aether', got %v", ca.Cert.Subject.Organization)
	}

	// Verify self-signed (Issuer == Subject)
	if ca.Cert.Issuer.CommonName != ca.Cert.Subject.CommonName {
		t.Errorf("expected self-signed, issuer CN=%q != subject CN=%q", ca.Cert.Issuer.CommonName, ca.Cert.Subject.CommonName)
	}

	// Verify validity window
	now := time.Now()
	if ca.Cert.NotBefore.After(now) {
		t.Error("CA NotBefore is in the future")
	}
	if ca.Cert.NotAfter.Before(now.Add(9 * 365 * 24 * time.Hour)) {
		t.Error("CA NotAfter is less than 9 years from now")
	}
}

func TestGenerateServerCert(t *testing.T) {
	ca, err := GenerateCA(CAOptions{})
	if err != nil {
		t.Fatalf("GenerateCA failed: %v", err)
	}

	bundle, err := ca.GenerateServerCert(ServerCertOptions{})
	if err != nil {
		t.Fatalf("GenerateServerCert failed: %v", err)
	}

	if bundle.Cert == nil {
		t.Fatal("server cert is nil")
	}

	// Verify signed by CA
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	_, err = bundle.Cert.Verify(x509.VerifyOptions{Roots: pool})
	if err != nil {
		t.Errorf("server cert not verified by CA: %v", err)
	}

	// Verify ExtKeyUsage
	found := false
	for _, eku := range bundle.Cert.ExtKeyUsage {
		if eku == x509.ExtKeyUsageServerAuth {
			found = true
			break
		}
	}
	if !found {
		t.Error("server cert missing ExtKeyUsageServerAuth")
	}

	// Verify default SANs
	hasLocalhost := false
	for _, dns := range bundle.Cert.DNSNames {
		if dns == "localhost" {
			hasLocalhost = true
			break
		}
	}
	if !hasLocalhost {
		t.Errorf("expected 'localhost' in DNSNames, got %v", bundle.Cert.DNSNames)
	}

	has127 := false
	for _, ip := range bundle.Cert.IPAddresses {
		if ip.Equal(net.ParseIP("127.0.0.1")) {
			has127 = true
			break
		}
	}
	if !has127 {
		t.Errorf("expected 127.0.0.1 in IPAddresses, got %v", bundle.Cert.IPAddresses)
	}

	// Verify CAPEM is set
	if !bytes.Equal(bundle.CAPEM, ca.CertPEM) {
		t.Error("bundle CAPEM does not match CA CertPEM")
	}
}

func TestGenerateClientCert(t *testing.T) {
	ca, err := GenerateCA(CAOptions{})
	if err != nil {
		t.Fatalf("GenerateCA failed: %v", err)
	}

	opts := ClientCertOptions{
		CommonName: "ag::production::worker::v1",
		OrgUnit:    "Agent",
	}
	bundle, err := ca.GenerateClientCert(opts)
	if err != nil {
		t.Fatalf("GenerateClientCert failed: %v", err)
	}

	// Verify signed by CA
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	_, err = bundle.Cert.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	if err != nil {
		t.Errorf("client cert not verified by CA: %v", err)
	}

	// Verify CN and OU
	if bundle.Cert.Subject.CommonName != "ag::production::worker::v1" {
		t.Errorf("expected CN='ag::production::worker::v1', got %q", bundle.Cert.Subject.CommonName)
	}
	if len(bundle.Cert.Subject.OrganizationalUnit) == 0 || bundle.Cert.Subject.OrganizationalUnit[0] != "Agent" {
		t.Errorf("expected OU='Agent', got %v", bundle.Cert.Subject.OrganizationalUnit)
	}

	// Verify ExtKeyUsage
	found := false
	for _, eku := range bundle.Cert.ExtKeyUsage {
		if eku == x509.ExtKeyUsageClientAuth {
			found = true
			break
		}
	}
	if !found {
		t.Error("client cert missing ExtKeyUsageClientAuth")
	}
}

func TestGenerateAnonymousClientCert(t *testing.T) {
	ca, err := GenerateCA(CAOptions{})
	if err != nil {
		t.Fatalf("GenerateCA failed: %v", err)
	}

	bundle, err := ca.GenerateAnonymousClientCert()
	if err != nil {
		t.Fatalf("GenerateAnonymousClientCert failed: %v", err)
	}

	if bundle.Cert.Subject.CommonName != AnonymousCertCN {
		t.Errorf("expected CN=%q, got %q", AnonymousCertCN, bundle.Cert.Subject.CommonName)
	}
	if len(bundle.Cert.Subject.OrganizationalUnit) == 0 || bundle.Cert.Subject.OrganizationalUnit[0] != "Anonymous" {
		t.Errorf("expected OU='Anonymous', got %v", bundle.Cert.Subject.OrganizationalUnit)
	}

	// Verify ExtKeyUsage
	found := false
	for _, eku := range bundle.Cert.ExtKeyUsage {
		if eku == x509.ExtKeyUsageClientAuth {
			found = true
			break
		}
	}
	if !found {
		t.Error("anonymous cert missing ExtKeyUsageClientAuth")
	}
}

func TestSaveAndLoadCA(t *testing.T) {
	ca, err := GenerateCA(CAOptions{CommonName: "Test CA"})
	if err != nil {
		t.Fatalf("GenerateCA failed: %v", err)
	}

	dir := t.TempDir()
	certPath := filepath.Join(dir, "ca.crt")
	keyPath := filepath.Join(dir, "ca.key")

	if err := os.WriteFile(certPath, ca.CertPEM, 0600); err != nil {
		t.Fatalf("write cert failed: %v", err)
	}
	if err := os.WriteFile(keyPath, ca.KeyPEM, 0600); err != nil {
		t.Fatalf("write key failed: %v", err)
	}

	loaded, err := LoadCA(certPath, keyPath)
	if err != nil {
		t.Fatalf("LoadCA failed: %v", err)
	}

	if !bytes.Equal(loaded.CertPEM, ca.CertPEM) {
		t.Error("loaded CertPEM does not match original")
	}
	if !bytes.Equal(loaded.KeyPEM, ca.KeyPEM) {
		t.Error("loaded KeyPEM does not match original")
	}
	if loaded.Cert.Subject.CommonName != "Test CA" {
		t.Errorf("expected CN='Test CA', got %q", loaded.Cert.Subject.CommonName)
	}
}

func TestEnsureCA_GeneratesNew(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "subdir", "ca.crt")
	keyPath := filepath.Join(dir, "subdir", "ca.key")

	ca, err := EnsureCA(certPath, keyPath, CAOptions{})
	if err != nil {
		t.Fatalf("EnsureCA failed: %v", err)
	}
	if ca == nil {
		t.Fatal("EnsureCA returned nil CA")
	}

	// Files should exist
	if _, err := os.Stat(certPath); err != nil {
		t.Errorf("cert file not created: %v", err)
	}
	if _, err := os.Stat(keyPath); err != nil {
		t.Errorf("key file not created: %v", err)
	}
}

func TestEnsureCA_LoadsExisting(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "ca.crt")
	keyPath := filepath.Join(dir, "ca.key")

	// Generate and save first
	ca1, err := EnsureCA(certPath, keyPath, CAOptions{})
	if err != nil {
		t.Fatalf("first EnsureCA failed: %v", err)
	}

	// Second call should load existing
	ca2, err := EnsureCA(certPath, keyPath, CAOptions{})
	if err != nil {
		t.Fatalf("second EnsureCA failed: %v", err)
	}

	if !bytes.Equal(ca1.CertPEM, ca2.CertPEM) {
		t.Error("second EnsureCA did not load the same CA")
	}
}

func TestCertBundle_SaveToFiles(t *testing.T) {
	ca, err := GenerateCA(CAOptions{})
	if err != nil {
		t.Fatalf("GenerateCA failed: %v", err)
	}

	bundle, err := ca.GenerateServerCert(ServerCertOptions{})
	if err != nil {
		t.Fatalf("GenerateServerCert failed: %v", err)
	}

	dir := t.TempDir()
	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")

	if err := bundle.SaveToFiles(certPath, keyPath); err != nil {
		t.Fatalf("SaveToFiles failed: %v", err)
	}

	certData, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatalf("read cert failed: %v", err)
	}
	keyData, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read key failed: %v", err)
	}

	if !bytes.Equal(certData, bundle.CertPEM) {
		t.Error("saved cert does not match bundle CertPEM")
	}
	if !bytes.Equal(keyData, bundle.KeyPEM) {
		t.Error("saved key does not match bundle KeyPEM")
	}
}

func TestCertBundle_TLSCertificate(t *testing.T) {
	ca, err := GenerateCA(CAOptions{})
	if err != nil {
		t.Fatalf("GenerateCA failed: %v", err)
	}

	bundle, err := ca.GenerateServerCert(ServerCertOptions{})
	if err != nil {
		t.Fatalf("GenerateServerCert failed: %v", err)
	}

	tlsCert, err := bundle.TLSCertificate()
	if err != nil {
		t.Fatalf("TLSCertificate failed: %v", err)
	}

	if len(tlsCert.Certificate) == 0 {
		t.Error("tls.Certificate has no certificate data")
	}
	if tlsCert.PrivateKey == nil {
		t.Error("tls.Certificate has no private key")
	}
}

func TestServerCertDefaults(t *testing.T) {
	ca, err := GenerateCA(CAOptions{})
	if err != nil {
		t.Fatalf("GenerateCA failed: %v", err)
	}

	bundle, err := ca.GenerateServerCert(ServerCertOptions{})
	if err != nil {
		t.Fatalf("GenerateServerCert failed: %v", err)
	}

	// Check default DNSNames
	expectedDNS := []string{"localhost", "aether-gateway"}
	for _, expected := range expectedDNS {
		found := false
		for _, dns := range bundle.Cert.DNSNames {
			if dns == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected DNS name %q not found in %v", expected, bundle.Cert.DNSNames)
		}
	}

	// Check default IPs
	expectedIPs := []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	for _, expectedIP := range expectedIPs {
		found := false
		for _, ip := range bundle.Cert.IPAddresses {
			if ip.Equal(expectedIP) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected IP %v not found in %v", expectedIP, bundle.Cert.IPAddresses)
		}
	}

	// Check default CN
	if bundle.Cert.Subject.CommonName != "aether-gateway" {
		t.Errorf("expected CN='aether-gateway', got %q", bundle.Cert.Subject.CommonName)
	}
}

func TestClientCertTLSHandshake(t *testing.T) {
	ca, err := GenerateCA(CAOptions{})
	if err != nil {
		t.Fatalf("GenerateCA failed: %v", err)
	}

	serverBundle, err := ca.GenerateServerCert(ServerCertOptions{
		DNSNames: []string{"localhost"},
		IPs:      []net.IP{net.ParseIP("127.0.0.1")},
	})
	if err != nil {
		t.Fatalf("GenerateServerCert failed: %v", err)
	}

	clientBundle, err := ca.GenerateClientCert(ClientCertOptions{
		CommonName: "test-client",
		OrgUnit:    "Agent",
	})
	if err != nil {
		t.Fatalf("GenerateClientCert failed: %v", err)
	}

	serverTLSCert, err := serverBundle.TLSCertificate()
	if err != nil {
		t.Fatalf("server TLSCertificate failed: %v", err)
	}
	clientTLSCert, err := clientBundle.TLSCertificate()
	if err != nil {
		t.Fatalf("client TLSCertificate failed: %v", err)
	}

	// Build CA pool
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(ca.CertPEM)

	// Start a TLS server
	serverConfig := &tls.Config{
		Certificates: []tls.Certificate{serverTLSCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverConfig)
	if err != nil {
		t.Fatalf("tls.Listen failed: %v", err)
	}
	defer ln.Close()

	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		// Force handshake
		if err := conn.(*tls.Conn).Handshake(); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	clientConfig := &tls.Config{
		Certificates: []tls.Certificate{clientTLSCert},
		RootCAs:      caPool,
		ServerName:   "localhost",
	}
	conn, err := tls.Dial("tcp", ln.Addr().String(), clientConfig)
	if err != nil {
		t.Fatalf("tls.Dial failed: %v", err)
	}
	defer conn.Close()

	if err := <-errCh; err != nil {
		t.Errorf("server handshake error: %v", err)
	}

	// Verify client cert presented to server
	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		// Client doesn't see server's client certs; check server side via accept
		t.Log("connection established successfully")
	}
}
