package gateway

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Conservative defaults for gRPC server resource limits. These cap the size
// of individual frames a client can send/receive, mitigating a class of
// memory-exhaustion DoS attacks. Override via GRPCServerLimits when callers
// need a different policy.
const (
	defaultGRPCMaxRecvMsgSize    = 10 * 1024 * 1024 // 10 MiB
	defaultGRPCMaxSendMsgSize    = 10 * 1024 * 1024 // 10 MiB
	defaultGRPCMaxHeaderListSize = 16 * 1024        // 16 KiB
)

// GRPCServerLimits configures per-connection resource caps applied as default
// gRPC server options. Zero values fall back to the defaults defined above.
type GRPCServerLimits struct {
	MaxRecvMsgSize    int    // bytes; 0 → defaultGRPCMaxRecvMsgSize
	MaxSendMsgSize    int    // bytes; 0 → defaultGRPCMaxSendMsgSize
	MaxHeaderListSize uint32 // bytes; 0 → defaultGRPCMaxHeaderListSize
}

// serverOptions returns the gRPC ServerOption slice expressed by these limits,
// substituting defaults for any zero field.
func (l GRPCServerLimits) serverOptions() []grpc.ServerOption {
	recv := l.MaxRecvMsgSize
	if recv == 0 {
		recv = defaultGRPCMaxRecvMsgSize
	}
	send := l.MaxSendMsgSize
	if send == 0 {
		send = defaultGRPCMaxSendMsgSize
	}
	hdr := l.MaxHeaderListSize
	if hdr == 0 {
		hdr = defaultGRPCMaxHeaderListSize
	}
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(recv),
		grpc.MaxSendMsgSize(send),
		grpc.MaxHeaderListSize(hdr),
	}
}

// TLSConfig holds TLS configuration for the gRPC server.
type TLSConfig struct {
	CertFile   string
	KeyFile    string
	CAFile     string
	ClientAuth tls.ClientAuthType // defaults to RequireAndVerifyClientCert if zero
	Limits     GRPCServerLimits   // optional per-connection resource caps; zero values use safe defaults
}

// LoadTLSConfig loads server TLS configuration with configurable client auth.
func LoadTLSConfig(certFile, keyFile, caFile string, clientAuth tls.ClientAuthType) (*tls.Config, error) {
	// Load server certificate
	serverCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server cert: %w", err)
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   clientAuth,
		MinVersion:   tls.VersionTLS13,
	}

	// Load CA certificate for client verification if CA file is provided and client auth needs it
	if caFile != "" && clientAuth > tls.NoClientCert {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load CA cert: %w", err)
		}

		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA cert")
		}
		config.ClientCAs = caPool
	}

	return config, nil
}

// NewGRPCServerWithTLS creates a gRPC server with TLS and any additional server options.
// The TLS credentials and resource limit defaults are prepended to opts so that
// caller-supplied opts can still override them by appearing later in the list.
func NewGRPCServerWithTLS(tlsConfig TLSConfig, opts ...grpc.ServerOption) (*grpc.Server, error) {
	clientAuth := tlsConfig.ClientAuth
	if clientAuth == 0 {
		clientAuth = tls.RequireAndVerifyClientCert // backwards compatible default
	}
	config, err := LoadTLSConfig(tlsConfig.CertFile, tlsConfig.KeyFile, tlsConfig.CAFile, clientAuth)
	if err != nil {
		return nil, err
	}
	creds := credentials.NewTLS(config)
	defaults := append([]grpc.ServerOption{grpc.Creds(creds)}, tlsConfig.Limits.serverOptions()...)
	allOpts := append(defaults, opts...)
	return grpc.NewServer(allOpts...), nil
}

// NewGRPCServerWithDynamicTLS creates a gRPC server whose certificate is served via
// a GetCertificate callback. This enables zero-downtime certificate rotation: callers
// update the certificate behind the callback (e.g., via ReloadableConfig) and new TLS
// handshakes pick up the updated certificate without a server restart.
//
// caFile and clientAuth are applied once at startup and are NOT hot-reloadable (a
// restart is required to change the CA pool or client-auth policy).
func NewGRPCServerWithDynamicTLS(
	getCert func(*tls.ClientHelloInfo) (*tls.Certificate, error),
	caFile string,
	clientAuth tls.ClientAuthType,
	opts ...grpc.ServerOption,
) (*grpc.Server, error) {
	return NewGRPCServerWithDynamicTLSAndLimits(getCert, caFile, clientAuth, GRPCServerLimits{}, opts...)
}

// NewGRPCServerWithDynamicTLSAndLimits is the variant of NewGRPCServerWithDynamicTLS
// that accepts an explicit GRPCServerLimits. Zero fields fall back to the package
// defaults (10 MiB recv/send, 16 KiB header list).
func NewGRPCServerWithDynamicTLSAndLimits(
	getCert func(*tls.ClientHelloInfo) (*tls.Certificate, error),
	caFile string,
	clientAuth tls.ClientAuthType,
	limits GRPCServerLimits,
	opts ...grpc.ServerOption,
) (*grpc.Server, error) {
	if clientAuth == 0 {
		clientAuth = tls.RequireAndVerifyClientCert
	}

	config := &tls.Config{
		GetCertificate: getCert,
		ClientAuth:     clientAuth,
		MinVersion:     tls.VersionTLS13,
	}

	if caFile != "" && clientAuth > tls.NoClientCert {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load CA cert: %w", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA cert")
		}
		config.ClientCAs = caPool
	}

	creds := credentials.NewTLS(config)
	defaults := append([]grpc.ServerOption{grpc.Creds(creds)}, limits.serverOptions()...)
	allOpts := append(defaults, opts...)
	return grpc.NewServer(allOpts...), nil
}

// ParseClientAuth converts a config string to tls.ClientAuthType.
// Valid values: "require" (default), "request", "none".
func ParseClientAuth(s string) tls.ClientAuthType {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "request":
		return tls.VerifyClientCertIfGiven
	case "none":
		return tls.NoClientCert
	default: // "require" or empty
		return tls.RequireAndVerifyClientCert
	}
}

// LoadClientCertificate loads a client certificate from files
func LoadClientCertificate(certFile, keyFile string) (tls.Certificate, error) {
	return tls.LoadX509KeyPair(certFile, keyFile)
}

// LoadClientCAPool loads a CA certificate pool for verifying servers
func LoadClientCAPool(caFile string) (*x509.CertPool, error) {
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load CA cert: %w", err)
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA cert")
	}

	return caPool, nil
}

// NewClientTLSConfig creates TLS configuration for a gRPC client with mTLS
func NewClientTLSConfig(serverName string, clientCert tls.Certificate, caPool *x509.CertPool) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caPool,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS13,
	}
}
