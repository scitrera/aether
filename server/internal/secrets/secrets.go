package secrets

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/scitrera/aether/internal/acl"
	"github.com/scitrera/aether/internal/auth"
	"github.com/scitrera/aether/internal/config"
	"github.com/scitrera/aether/pkg/crypto"
	"github.com/scitrera/aether/pkg/models"
	"gopkg.in/yaml.v3"
)

// GeneratedSecrets represents the structure of the generated secrets file.
type GeneratedSecrets struct {
	Auth  AuthSecrets  `yaml:"auth"`
	Admin AdminSecrets `yaml:"admin"`
	TLS   TLSSecrets   `yaml:"tls,omitempty"`
}

// TLSSecrets holds paths to generated TLS certificate artifacts.
type TLSSecrets struct {
	CACertFile string `yaml:"ca_cert_file,omitempty"`
	CertFile   string `yaml:"cert_file,omitempty"`
	KeyFile    string `yaml:"key_file,omitempty"`
	Dir        string `yaml:"dir,omitempty"`
}

// AuthSecrets holds auth-related generated secrets.
type AuthSecrets struct {
	TokenHMACKey string `yaml:"token_hmac_key"`
}

// AdminSecrets holds admin-related generated secrets.
type AdminSecrets struct {
	APIKey string `yaml:"api_key"`
}

// EnsureSecrets checks whether secrets are already configured (via env, config,
// or a previously generated secrets file) and generates any that are missing.
// It writes the results to outputPath and merges them into cfg.
func EnsureSecrets(cfg *config.Config, outputPath string, force bool) (*GeneratedSecrets, error) {
	// If not forcing, try to load existing generated secrets
	if !force {
		existing, err := LoadGeneratedSecrets(outputPath)
		if err == nil {
			ApplyToConfig(cfg, existing)
			return existing, nil
		}
		// File doesn't exist or is unreadable — continue to generate
	}

	// Determine what needs generating
	needHMAC := cfg.Auth.TokenHMACKey == ""
	needAdminKey := cfg.Admin.APIKey == ""

	if !needHMAC && !needAdminKey && !force {
		return &GeneratedSecrets{
			Auth:  AuthSecrets{TokenHMACKey: cfg.Auth.TokenHMACKey},
			Admin: AdminSecrets{APIKey: cfg.Admin.APIKey},
		}, nil
	}

	gs := &GeneratedSecrets{}

	if needHMAC || force {
		key, err := generateRandomBase64(32)
		if err != nil {
			return nil, fmt.Errorf("generating HMAC key: %w", err)
		}
		gs.Auth.TokenHMACKey = key
	} else {
		gs.Auth.TokenHMACKey = cfg.Auth.TokenHMACKey
	}

	if needAdminKey || force {
		key, err := generateRandomURLSafeBase64(32)
		if err != nil {
			return nil, fmt.Errorf("generating admin API key: %w", err)
		}
		gs.Admin.APIKey = key
	} else {
		gs.Admin.APIKey = cfg.Admin.APIKey
	}

	if err := SaveGeneratedSecrets(outputPath, gs); err != nil {
		return nil, err
	}

	ApplyToConfig(cfg, gs)
	return gs, nil
}

// LoadGeneratedSecrets reads a previously written secrets file.
func LoadGeneratedSecrets(path string) (*GeneratedSecrets, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading secrets file: %w", err)
	}

	var gs GeneratedSecrets
	if err := yaml.Unmarshal(data, &gs); err != nil {
		return nil, fmt.Errorf("parsing secrets file: %w", err)
	}
	return &gs, nil
}

// ApplyToConfig merges generated secrets into the config, only filling in
// fields that are currently empty (env/config take precedence).
func ApplyToConfig(cfg *config.Config, gs *GeneratedSecrets) {
	if cfg.Auth.TokenHMACKey == "" && gs.Auth.TokenHMACKey != "" {
		cfg.Auth.TokenHMACKey = gs.Auth.TokenHMACKey
	}
	if cfg.Admin.APIKey == "" && gs.Admin.APIKey != "" {
		cfg.Admin.APIKey = gs.Admin.APIKey
	}
	// Apply TLS paths if not already configured
	if cfg.Gateway.TLS.CertFile == "" && gs.TLS.CertFile != "" {
		cfg.Gateway.TLS.CertFile = gs.TLS.CertFile
	}
	if cfg.Gateway.TLS.KeyFile == "" && gs.TLS.KeyFile != "" {
		cfg.Gateway.TLS.KeyFile = gs.TLS.KeyFile
	}
	if cfg.Gateway.TLS.CAFile == "" && gs.TLS.CACertFile != "" {
		cfg.Gateway.TLS.CAFile = gs.TLS.CACertFile
	}
}

// ParseAccessLevel converts a human-readable access level name to its integer value.
func ParseAccessLevel(name string) (int, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "NONE":
		return acl.AccessNone, nil
	case "READ":
		return acl.AccessRead, nil
	case "READWRITE", "RW":
		return acl.AccessReadWrite, nil
	case "MANAGE":
		return acl.AccessManage, nil
	case "ADMIN":
		return acl.AccessAdmin, nil
	case "SUPERADMIN":
		return acl.AccessSuperAdmin, nil
	default:
		return 0, fmt.Errorf("unknown access level %q (valid: NONE, READ, READWRITE, MANAGE, ADMIN, SUPERADMIN)", name)
	}
}

// CreateInitialToken creates an admin bootstrap API token in the database and
// seeds an ACL rule granting the token's principal the specified access level
// on all workspaces.
// principalType should be a valid models.PrincipalType (e.g., models.PrincipalUser).
func CreateInitialToken(ctx context.Context, db *sql.DB, cfg *config.Config, tokenName string, principalType models.PrincipalType, accessLevel int) (string, error) {
	if db == nil {
		return "", fmt.Errorf("database connection required to create initial token")
	}

	// Ensure HMAC key is initialised so the token hash is consistent
	if cfg.Auth.TokenHMACKey != "" {
		crypto.InitTokenHMAC([]byte(cfg.Auth.TokenHMACKey))
	}

	store := auth.NewAPITokenStore(db)
	result, err := store.CreateToken(
		ctx,
		tokenName,
		string(principalType), // use proper constant
		[]string{"*"},         // all workspaces
		[]string{"*"},         // all scopes
		acl.SystemPrincipal,   // created by _system principal
		nil,                   // no expiration
	)
	if err != nil {
		return "", fmt.Errorf("creating initial token: %w", err)
	}

	// Seed an ACL rule granting the _system principal the requested access level
	// on all workspaces. The token inherits permissions from its creator (_system),
	// so this rule enables the token holder to perform admin operations.
	aclService := acl.NewService(db, acl.SystemPrincipal)
	defer aclService.Close()

	_, err = aclService.GrantAccess(
		ctx,
		acl.PrincipalTypeForModel(principalType), // principal type (lowercase for DB convention)
		acl.SystemPrincipal,                      // principal ID (_system — the token's creator)
		acl.ResourceTypeWorkspace,                // resource type
		acl.WildcardAnyResource,                  // resource ID (* = all workspaces)
		accessLevel,                              // access level
		acl.SystemPrincipal,                      // granted by
		"bootstrap ACL for _system principal via init-secrets", // reason
		nil, // no expiration
	)
	if err != nil {
		return "", fmt.Errorf("creating ACL rule for initial token: %w", err)
	}

	return result.Token, nil
}

// SaveGeneratedSecrets writes the secrets struct to a YAML file at the given path.
func SaveGeneratedSecrets(path string, gs *GeneratedSecrets) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("creating secrets directory %s: %w", dir, err)
	}

	header := "# Auto-generated by aether init-secrets at " + time.Now().UTC().Format(time.RFC3339) + "\n" +
		"# Re-run init-secrets --force to regenerate (existing values will NOT be overwritten without --force)\n"

	body, err := yaml.Marshal(gs)
	if err != nil {
		return fmt.Errorf("marshaling secrets: %w", err)
	}

	content := []byte(header + string(body))

	if err := os.WriteFile(path, content, 0600); err != nil {
		return fmt.Errorf("writing secrets file %s: %w", path, err)
	}
	return nil
}

func generateRandomBase64(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func generateRandomURLSafeBase64(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}
