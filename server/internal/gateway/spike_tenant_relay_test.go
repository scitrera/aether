package gateway

// SPIKE (feasibility): sandbox-provider tenant-relay redesign ("Direction A").
//
// Goal of this file: prove the gateway-side foundation of the design empirically,
// over a REAL mTLS gRPC handshake, against the production identity-resolution path
// (authenticateMTLS -> resolveConnectionIdentity), NOT mocks.
//
// Design claim under test:
//   A tenant-namespace relay holds the tenant-CA client cert locally and dials the
//   LOCAL tenant gateway as the sandbox-provider service. The remote provider's
//   InitConnection is forwarded opaquely and supplies only the specifier. Under
//   semi-strict mTLS the gateway must:
//     (1) authorize type+implementation from the tenant cert CN, and
//     (2) take the specifier from the forwarded init,
//   so the provider never needs to hold the tenant cert. An init claiming a
//   different implementation than the cert must be rejected.
//
// This is the empirically-unproven hop (existing tests inject the cert identity
// below authenticateMTLS; none drive a real peer cert through a live handshake).

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// spikeGenCA returns a self-signed CA cert + key.
func spikeGenCA(t *testing.T) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "spike-tenant-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	return cert, key
}

// spikeGenLeaf returns a CA-signed leaf cert (client or server) with the given CN.
func spikeGenLeaf(t *testing.T, serial int64, cn string, ca *x509.Certificate, caKey *rsa.PrivateKey, server bool) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	eku := x509.ExtKeyUsageClientAuth
	if server {
		eku = x509.ExtKeyUsageServerAuth
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{eku},
		BasicConstraintsValid: true,
	}
	if server {
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		tmpl.DNSNames = []string{"localhost"}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func serviceInit(impl, specifier string) *pb.UpstreamMessage {
	return &pb.UpstreamMessage{
		Payload: &pb.UpstreamMessage_Init{
			Init: &pb.InitConnection{
				ClientType: &pb.InitConnection_Service{
					Service: &pb.ServiceIdentity{
						Implementation: impl,
						Specifier:      specifier,
					},
				},
			},
		},
	}
}

// spikeGwServer is a minimal AetherGateway server that runs the REAL
// identity-resolution path and reports the resolved identity (or the denial
// reason) back in ConnectionAck.assigned_id.
type spikeGwServer struct {
	pb.UnimplementedAetherGatewayServer
	h *AuthHandler
}

func (s *spikeGwServer) Connect(stream pb.AetherGateway_ConnectServer) error {
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	init := msg.GetInit()
	if init == nil {
		return stream.Send(spikeAck("ERR: expected init first"))
	}
	ctx := stream.Context()
	certID, certPT, hasCert, isAnon, err := s.h.authenticateMTLS(ctx)
	if err != nil {
		return stream.Send(spikeAck("ERR: authenticateMTLS: " + err.Error()))
	}
	resolved, err := s.h.resolveConnectionIdentity(ctx, init, certID, certPT, hasCert, isAnon)
	if err != nil {
		return stream.Send(spikeAck("ERR: " + err.Error()))
	}
	return stream.Send(spikeAck(resolved.String()))
}

func spikeAck(assignedID string) *pb.DownstreamMessage {
	return &pb.DownstreamMessage{
		Payload: &pb.DownstreamMessage_ConnectionAck{
			ConnectionAck: &pb.ConnectionAck{SessionId: "spike", AssignedId: assignedID},
		},
	}
}

// spikeDialResolve performs a real mTLS Connect with clientCert, sends init, and
// returns the assigned_id the server reported (resolved identity, or "ERR: ...").
func spikeDialResolve(t *testing.T, addr string, clientCert tls.Certificate, init *pb.UpstreamMessage) string {
	t.Helper()
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{clientCert},
		// Server identity is not under test here; we only exercise CLIENT-cert
		// verification at the server. Skip server verification to avoid SAN setup.
		InsecureSkipVerify: true,
	})
	conn, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := pb.NewAetherGatewayClient(conn).Connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := stream.Send(init); err != nil {
		t.Fatalf("send init: %v", err)
	}
	down, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv ack: %v", err)
	}
	return down.GetConnectionAck().GetAssignedId()
}

func TestSpike_SemiStrictTenantCertServiceIdentity(t *testing.T) {
	caCert, caKey := spikeGenCA(t)
	serverCert := spikeGenLeaf(t, 2, "spike-gateway", caCert, caKey, true)
	// The tenant-CA-signed client cert the tenant-relay presents to the LOCAL gateway.
	tenantCert := spikeGenLeaf(t, 3, "sv::sandbox-provider::tenant-alice", caCert, caKey, false)

	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	// Real production semi-strict auth handler (no authenticator/acl/audit deps).
	h := newAuthHandler(nil, false, MTLSModeSemiStrict, nil, nil)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
	})))
	pb.RegisterAetherGatewayServer(srv, &spikeGwServer{h: h})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	addr := lis.Addr().String()

	// (1) Positive: cert authorizes type=Service + impl=sandbox-provider; the
	// forwarded init's specifier (pod-7) wins. The provider never holds the cert.
	got := spikeDialResolve(t, addr, tenantCert, serviceInit("sandbox-provider", "pod-7"))
	if got != "sv::sandbox-provider::pod-7" {
		t.Fatalf("resolved identity = %q, want %q", got, "sv::sandbox-provider::pod-7")
	}

	// (2) Negative: a forwarded init claiming a different implementation than the
	// tenant cert must be rejected by semi-strict validation.
	got = spikeDialResolve(t, addr, tenantCert, serviceInit("evil-service", "pod-7"))
	if !strings.Contains(got, "implementation mismatch") {
		t.Fatalf("expected impl-mismatch denial, got %q", got)
	}
}
