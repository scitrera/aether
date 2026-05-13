package proxysidecar

import (
	"fmt"
	"os"
	"strings"

	"github.com/scitrera/aether/sdk/go/aether"
)

// CredentialKind tags which authenticator the gateway will route the
// presented credential through. Picked by “loadGatewayCredential“ based on
// which config fields are populated.
type CredentialKind int

const (
	// CredentialKindNone means no credential is configured (insecure mode).
	CredentialKindNone CredentialKind = iota
	// CredentialKindAPIKey routes via the long-lived API key authenticator.
	CredentialKindAPIKey
	// CredentialKindTaskToken routes via the per-task token authenticator
	// (orchestration_integration.go::maybeIssueTaskToken mints these on
	// CreateTask with target_identity). Bound to a specific principal for
	// the token's lifetime.
	CredentialKindTaskToken
)

// loadGatewayCredential returns the gateway credential and the kind so the
// caller can route it through the right SDK setter. Inline values win over
// file paths so per-deployment overrides take precedence. Setting both
// API-key fields and task-token fields is a config error — they route
// through different authenticator paths and would mask which one is
// actually in use.
func loadGatewayCredential(g GatewayConfig) (string, CredentialKind, error) {
	apiKeyConfigured := g.APIKey != "" || g.APIKeyPath != ""
	taskTokenConfigured := g.TaskToken != "" || g.TaskTokenPath != ""

	if apiKeyConfigured && taskTokenConfigured {
		return "", CredentialKindNone, fmt.Errorf(
			"gateway: configure either api_key/api_key_path OR task_token/task_token_path, not both",
		)
	}

	if taskTokenConfigured {
		if g.TaskToken != "" {
			return g.TaskToken, CredentialKindTaskToken, nil
		}
		data, err := os.ReadFile(g.TaskTokenPath)
		if err != nil {
			return "", CredentialKindNone, fmt.Errorf("read task_token_path %q: %w", g.TaskTokenPath, err)
		}
		return strings.TrimSpace(string(data)), CredentialKindTaskToken, nil
	}

	if g.APIKey != "" {
		return g.APIKey, CredentialKindAPIKey, nil
	}
	if g.APIKeyPath == "" {
		return "", CredentialKindNone, nil
	}
	data, err := os.ReadFile(g.APIKeyPath)
	if err != nil {
		return "", CredentialKindNone, fmt.Errorf("read api_key_path %q: %w", g.APIKeyPath, err)
	}
	return strings.TrimSpace(string(data)), CredentialKindAPIKey, nil
}

// loadAPIKey is kept for backward compatibility with code that doesn't care
// about the credential kind. New callers should use “loadGatewayCredential“.
func loadAPIKey(g GatewayConfig) (string, error) {
	if g.APIKey != "" {
		return g.APIKey, nil
	}
	if g.APIKeyPath == "" {
		return "", nil
	}
	data, err := os.ReadFile(g.APIKeyPath)
	if err != nil {
		return "", fmt.Errorf("read api_key_path %q: %w", g.APIKeyPath, err)
	}
	return strings.TrimSpace(string(data)), nil
}

// buildTLSConfig converts the YAML TLS settings into the SDK TLS configuration.
// Returns nil when TLS is disabled (gateway.insecure=true or no certs).
func buildTLSConfig(g GatewayConfig) (*aether.TLSConfig, error) {
	if g.Insecure {
		return nil, nil
	}
	if g.TLS.CertFile == "" && g.TLS.KeyFile == "" && g.TLS.CAFile == "" {
		return nil, nil
	}
	tlsCfg := &aether.TLSConfig{Enabled: true}
	if g.TLS.CAFile != "" {
		ca, err := os.ReadFile(g.TLS.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read tls.ca_file %q: %w", g.TLS.CAFile, err)
		}
		tlsCfg.RootCAs = ca
	}
	if g.TLS.CertFile != "" {
		cert, err := os.ReadFile(g.TLS.CertFile)
		if err != nil {
			return nil, fmt.Errorf("read tls.cert_file %q: %w", g.TLS.CertFile, err)
		}
		tlsCfg.ClientCert = cert
	}
	if g.TLS.KeyFile != "" {
		key, err := os.ReadFile(g.TLS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("read tls.key_file %q: %w", g.TLS.KeyFile, err)
		}
		tlsCfg.ClientKey = key
	}
	return tlsCfg, nil
}
