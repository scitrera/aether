package aether

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultConnectionOptions(t *testing.T) {
	opts := DefaultConnectionOptions()

	if opts.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d, want 5", opts.MaxRetries)
	}
	if opts.InitialBackoff != 1*time.Second {
		t.Errorf("InitialBackoff = %v, want 1s", opts.InitialBackoff)
	}
	if opts.MaxBackoff != 30*time.Second {
		t.Errorf("MaxBackoff = %v, want 30s", opts.MaxBackoff)
	}
	if opts.BackoffMultiplier != 2.0 {
		t.Errorf("BackoffMultiplier = %f, want 2.0", opts.BackoffMultiplier)
	}
	if opts.AutoReconnect != true {
		t.Errorf("AutoReconnect = %v, want true", opts.AutoReconnect)
	}
	if opts.ConnectTimeout != 30*time.Second {
		t.Errorf("ConnectTimeout = %v, want 30s", opts.ConnectTimeout)
	}
	if opts.KeepAliveInterval != 30*time.Second {
		t.Errorf("KeepAliveInterval = %v, want 30s", opts.KeepAliveInterval)
	}
}

func TestConnectionOptionFunctions(t *testing.T) {
	t.Run("WithMaxRetries", func(t *testing.T) {
		opts := DefaultConnectionOptions()
		WithMaxRetries(10)(&opts)
		if opts.MaxRetries != 10 {
			t.Errorf("MaxRetries = %d, want 10", opts.MaxRetries)
		}
	})

	t.Run("WithInitialBackoff", func(t *testing.T) {
		opts := DefaultConnectionOptions()
		WithInitialBackoff(5 * time.Second)(&opts)
		if opts.InitialBackoff != 5*time.Second {
			t.Errorf("InitialBackoff = %v, want 5s", opts.InitialBackoff)
		}
	})

	t.Run("WithMaxBackoff", func(t *testing.T) {
		opts := DefaultConnectionOptions()
		WithMaxBackoff(60 * time.Second)(&opts)
		if opts.MaxBackoff != 60*time.Second {
			t.Errorf("MaxBackoff = %v, want 60s", opts.MaxBackoff)
		}
	})

	t.Run("WithBackoffMultiplier", func(t *testing.T) {
		opts := DefaultConnectionOptions()
		WithBackoffMultiplier(1.5)(&opts)
		if opts.BackoffMultiplier != 1.5 {
			t.Errorf("BackoffMultiplier = %f, want 1.5", opts.BackoffMultiplier)
		}
	})

	t.Run("WithAutoReconnect", func(t *testing.T) {
		opts := DefaultConnectionOptions()
		WithAutoReconnect(false)(&opts)
		if opts.AutoReconnect != false {
			t.Errorf("AutoReconnect = %v, want false", opts.AutoReconnect)
		}
	})

	t.Run("WithConnectTimeout", func(t *testing.T) {
		opts := DefaultConnectionOptions()
		WithConnectTimeout(60 * time.Second)(&opts)
		if opts.ConnectTimeout != 60*time.Second {
			t.Errorf("ConnectTimeout = %v, want 60s", opts.ConnectTimeout)
		}
	})

	t.Run("WithKeepAliveInterval", func(t *testing.T) {
		opts := DefaultConnectionOptions()
		WithKeepAliveInterval(15 * time.Second)(&opts)
		if opts.KeepAliveInterval != 15*time.Second {
			t.Errorf("KeepAliveInterval = %v, want 15s", opts.KeepAliveInterval)
		}
	})
}

func TestApplyConnectionOptions(t *testing.T) {
	opts := DefaultConnectionOptions()
	ApplyConnectionOptions(&opts,
		WithMaxRetries(20),
		WithAutoReconnect(false),
		WithConnectTimeout(120*time.Second),
	)

	if opts.MaxRetries != 20 {
		t.Errorf("MaxRetries = %d, want 20", opts.MaxRetries)
	}
	if opts.AutoReconnect != false {
		t.Errorf("AutoReconnect = %v, want false", opts.AutoReconnect)
	}
	if opts.ConnectTimeout != 120*time.Second {
		t.Errorf("ConnectTimeout = %v, want 120s", opts.ConnectTimeout)
	}
	// Unchanged options should remain at defaults
	if opts.InitialBackoff != 1*time.Second {
		t.Errorf("InitialBackoff = %v, want 1s (unchanged)", opts.InitialBackoff)
	}
}

func TestClientOptions_Validate(t *testing.T) {
	tests := []struct {
		name        string
		opts        ClientOptions
		wantErr     bool
		errContains string
	}{
		{
			name: "valid options",
			opts: ClientOptions{
				ServerAddr: "localhost:50051",
			},
			wantErr: false,
		},
		{
			name:        "missing server address",
			opts:        ClientOptions{},
			wantErr:     true,
			errContains: "server address",
		},
		{
			name: "empty server address",
			opts: ClientOptions{
				ServerAddr: "",
			},
			wantErr:     true,
			errContains: "server address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("Validate() error = nil, want error containing %q", tt.errContains)
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("Validate() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("Validate() unexpected error = %v", err)
			}
		})
	}
}

func TestAgentOptions_Validate(t *testing.T) {
	tests := []struct {
		name        string
		opts        AgentOptions
		wantErr     bool
		errContains string
	}{
		{
			name: "valid options",
			opts: AgentOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				Workspace:      "prod",
				Implementation: "worker",
				Specifier:      "inst-1",
			},
			wantErr: false,
		},
		{
			name: "missing server address",
			opts: AgentOptions{
				ClientOptions:  ClientOptions{},
				Workspace:      "prod",
				Implementation: "worker",
				Specifier:      "inst-1",
			},
			wantErr:     true,
			errContains: "server address",
		},
		{
			name: "missing workspace",
			opts: AgentOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				Implementation: "worker",
				Specifier:      "inst-1",
			},
			wantErr:     true,
			errContains: "workspace",
		},
		{
			name: "missing implementation",
			opts: AgentOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				Workspace: "prod",
				Specifier: "inst-1",
			},
			wantErr:     true,
			errContains: "implementation",
		},
		{
			name: "missing specifier",
			opts: AgentOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				Workspace:      "prod",
				Implementation: "worker",
			},
			wantErr:     true,
			errContains: "specifier",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("Validate() error = nil, want error containing %q", tt.errContains)
					return
				}
				if tt.errContains != "" && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.errContains)) {
					t.Errorf("Validate() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("Validate() unexpected error = %v", err)
			}
		})
	}
}

func TestTaskOptions_Validate(t *testing.T) {
	tests := []struct {
		name        string
		opts        TaskOptions
		wantErr     bool
		errContains string
	}{
		{
			name: "valid unique task",
			opts: TaskOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				Workspace:      "prod",
				Implementation: "processor",
				Specifier:      "unique-1",
			},
			wantErr: false,
		},
		{
			name: "valid non-unique task (no specifier)",
			opts: TaskOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				Workspace:      "prod",
				Implementation: "processor",
			},
			wantErr: false,
		},
		{
			name: "missing server address",
			opts: TaskOptions{
				ClientOptions:  ClientOptions{},
				Workspace:      "prod",
				Implementation: "processor",
			},
			wantErr:     true,
			errContains: "server address",
		},
		{
			name: "missing workspace",
			opts: TaskOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				Implementation: "processor",
			},
			wantErr:     true,
			errContains: "workspace",
		},
		{
			name: "missing implementation",
			opts: TaskOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				Workspace: "prod",
			},
			wantErr:     true,
			errContains: "implementation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("Validate() error = nil, want error containing %q", tt.errContains)
					return
				}
				if tt.errContains != "" && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.errContains)) {
					t.Errorf("Validate() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("Validate() unexpected error = %v", err)
			}
		})
	}
}

func TestTaskOptions_IsUnique(t *testing.T) {
	tests := []struct {
		name      string
		specifier string
		want      bool
	}{
		{
			name:      "unique task",
			specifier: "unique-1",
			want:      true,
		},
		{
			name:      "non-unique task",
			specifier: "",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := TaskOptions{Specifier: tt.specifier}
			if got := opts.IsUnique(); got != tt.want {
				t.Errorf("IsUnique() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUserOptions_Validate(t *testing.T) {
	tests := []struct {
		name        string
		opts        UserOptions
		wantErr     bool
		errContains string
	}{
		{
			name: "valid options",
			opts: UserOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				UserID:   "alice",
				WindowID: "tab-1",
			},
			wantErr: false,
		},
		{
			name: "valid options with workspace",
			opts: UserOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				UserID:    "alice",
				WindowID:  "tab-1",
				Workspace: "prod",
			},
			wantErr: false,
		},
		{
			name: "missing user ID",
			opts: UserOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				WindowID: "tab-1",
			},
			wantErr:     true,
			errContains: "user ID",
		},
		{
			name: "missing window ID",
			opts: UserOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				UserID: "alice",
			},
			wantErr:     true,
			errContains: "window ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("Validate() error = nil, want error containing %q", tt.errContains)
					return
				}
				if tt.errContains != "" && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.errContains)) {
					t.Errorf("Validate() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("Validate() unexpected error = %v", err)
			}
		})
	}
}

func TestOrchestratorOptions_Validate(t *testing.T) {
	tests := []struct {
		name        string
		opts        OrchestratorOptions
		wantErr     bool
		errContains string
	}{
		{
			name: "valid options",
			opts: OrchestratorOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				Implementation:    "kubernetes",
				SupportedProfiles: []string{"gpu", "cpu"},
			},
			wantErr: false,
		},
		{
			name: "valid with specifier",
			opts: OrchestratorOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				Implementation:    "kubernetes",
				SupportedProfiles: []string{"gpu"},
				Specifier:         "cluster-1",
			},
			wantErr: false,
		},
		{
			name: "missing implementation",
			opts: OrchestratorOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				SupportedProfiles: []string{"gpu"},
			},
			wantErr:     true,
			errContains: "implementation",
		},
		{
			name: "missing supported profiles",
			opts: OrchestratorOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				Implementation: "kubernetes",
			},
			wantErr:     true,
			errContains: "profile",
		},
		{
			name: "empty supported profiles",
			opts: OrchestratorOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				Implementation:    "kubernetes",
				SupportedProfiles: []string{},
			},
			wantErr:     true,
			errContains: "profile",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("Validate() error = nil, want error containing %q", tt.errContains)
					return
				}
				if tt.errContains != "" && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.errContains)) {
					t.Errorf("Validate() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("Validate() unexpected error = %v", err)
			}
		})
	}
}

func TestWorkflowEngineOptions_Validate(t *testing.T) {
	tests := []struct {
		name        string
		opts        WorkflowEngineOptions
		wantErr     bool
		errContains string
	}{
		{
			name: "valid options",
			opts: WorkflowEngineOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
			},
			wantErr: false,
		},
		{
			name: "valid with specifier",
			opts: WorkflowEngineOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				Specifier: "engine-1",
			},
			wantErr: false,
		},
		{
			name: "missing server address",
			opts: WorkflowEngineOptions{
				ClientOptions: ClientOptions{},
			},
			wantErr:     true,
			errContains: "server address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("Validate() error = nil, want error containing %q", tt.errContains)
					return
				}
				if tt.errContains != "" && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.errContains)) {
					t.Errorf("Validate() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("Validate() unexpected error = %v", err)
			}
		})
	}
}

func TestMetricsBridgeOptions_Validate(t *testing.T) {
	tests := []struct {
		name        string
		opts        MetricsBridgeOptions
		wantErr     bool
		errContains string
	}{
		{
			name: "valid options",
			opts: MetricsBridgeOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
			},
			wantErr: false,
		},
		{
			name: "valid with specifier",
			opts: MetricsBridgeOptions{
				ClientOptions: ClientOptions{
					ServerAddr: "localhost:50051",
				},
				Specifier: "bridge-1",
			},
			wantErr: false,
		},
		{
			name: "missing server address",
			opts: MetricsBridgeOptions{
				ClientOptions: ClientOptions{},
			},
			wantErr:     true,
			errContains: "server address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("Validate() error = nil, want error containing %q", tt.errContains)
					return
				}
				if tt.errContains != "" && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.errContains)) {
					t.Errorf("Validate() error = %v, want error containing %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("Validate() unexpected error = %v", err)
			}
		})
	}
}

func TestKVScope_Valid(t *testing.T) {
	tests := []struct {
		name  string
		scope KVScope
		want  bool
	}{
		{"global", KVScopeGlobal, true},
		{"workspace", KVScopeWorkspace, true},
		{"user", KVScopeUser, true},
		{"user-workspace", KVScopeUserWorkspace, true},
		{"invalid scope", KVScope("invalid"), false},
		{"empty scope", KVScope(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.scope.Valid(); got != tt.want {
				t.Errorf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMessageType_Valid(t *testing.T) {
	tests := []struct {
		name string
		mt   MessageType
		want bool
	}{
		{"chat", MessageTypeChat, true},
		{"control", MessageTypeControl, true},
		{"tool call", MessageTypeToolCall, true},
		{"event", MessageTypeEvent, true},
		{"metric", MessageTypeMetric, true},
		{"invalid type", MessageType("INVALID"), false},
		{"empty type", MessageType(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.mt.Valid(); got != tt.want {
				t.Errorf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTaskAssignmentMode_Valid(t *testing.T) {
	tests := []struct {
		name string
		mode TaskAssignmentMode
		want bool
	}{
		{"self assign", TaskAssignmentSelfAssign, true},
		{"targeted", TaskAssignmentTargeted, true},
		{"pool", TaskAssignmentPool, true},
		{"invalid mode", TaskAssignmentMode("INVALID"), false},
		{"empty mode", TaskAssignmentMode(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.mode.Valid(); got != tt.want {
				t.Errorf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCredentials(t *testing.T) {
	t.Run("NewCredentials", func(t *testing.T) {
		creds := NewCredentials()
		if creds == nil {
			t.Error("NewCredentials() returned nil")
		}
		if len(creds) != 0 {
			t.Errorf("NewCredentials() len = %d, want 0", len(creds))
		}
	})

	t.Run("WithAPIKey", func(t *testing.T) {
		creds := NewCredentials().WithAPIKey("test-key")
		if creds["x-api-key"] != "test-key" {
			t.Errorf("WithAPIKey() x-api-key = %q, want %q", creds["x-api-key"], "test-key")
		}
	})

	t.Run("WithToken", func(t *testing.T) {
		creds := NewCredentials().WithToken("test-token")
		expected := "Bearer test-token"
		if creds["authorization"] != expected {
			t.Errorf("WithToken() authorization = %q, want %q", creds["authorization"], expected)
		}
	})

	t.Run("WithTenant", func(t *testing.T) {
		creds := NewCredentials().WithTenant("tenant-123")
		if creds["x-tenant-id"] != "tenant-123" {
			t.Errorf("WithTenant() x-tenant-id = %q, want %q", creds["x-tenant-id"], "tenant-123")
		}
	})

	t.Run("chained methods", func(t *testing.T) {
		creds := NewCredentials().
			WithAPIKey("key").
			WithToken("token").
			WithTenant("tenant")

		if creds["x-api-key"] != "key" {
			t.Errorf("x-api-key = %q, want %q", creds["x-api-key"], "key")
		}
		if creds["authorization"] != "Bearer token" {
			t.Errorf("authorization = %q, want %q", creds["authorization"], "Bearer token")
		}
		if creds["x-tenant-id"] != "tenant" {
			t.Errorf("x-tenant-id = %q, want %q", creds["x-tenant-id"], "tenant")
		}
	})
}

func TestTLSConfig_ToTLSConfig(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		var cfg *TLSConfig
		result, err := cfg.ToTLSConfig()
		if err != nil {
			t.Errorf("ToTLSConfig() error = %v, want nil", err)
		}
		if result != nil {
			t.Errorf("ToTLSConfig() = %v, want nil", result)
		}
	})

	t.Run("disabled TLS", func(t *testing.T) {
		cfg := &TLSConfig{Enabled: false}
		result, err := cfg.ToTLSConfig()
		if err != nil {
			t.Errorf("ToTLSConfig() error = %v, want nil", err)
		}
		if result != nil {
			t.Errorf("ToTLSConfig() = %v, want nil", result)
		}
	})

	t.Run("enabled TLS with server name", func(t *testing.T) {
		cfg := &TLSConfig{
			Enabled:    true,
			ServerName: "test-server",
		}
		result, err := cfg.ToTLSConfig()
		if err != nil {
			t.Errorf("ToTLSConfig() error = %v, want nil", err)
		}
		if result == nil {
			t.Fatal("ToTLSConfig() = nil, want non-nil")
		}
		if result.ServerName != "test-server" {
			t.Errorf("ServerName = %q, want %q", result.ServerName, "test-server")
		}
	})

	t.Run("enabled TLS with insecure skip verify", func(t *testing.T) {
		cfg := &TLSConfig{
			Enabled:            true,
			InsecureSkipVerify: true,
		}
		result, err := cfg.ToTLSConfig()
		if err != nil {
			t.Errorf("ToTLSConfig() error = %v, want nil", err)
		}
		if result == nil {
			t.Fatal("ToTLSConfig() = nil, want non-nil")
		}
		if !result.InsecureSkipVerify {
			t.Error("InsecureSkipVerify = false, want true")
		}
	})

	t.Run("invalid client certificate", func(t *testing.T) {
		cfg := &TLSConfig{
			Enabled:    true,
			ClientCert: []byte("invalid-cert"),
			ClientKey:  []byte("invalid-key"),
		}
		_, err := cfg.ToTLSConfig()
		if err == nil {
			t.Error("ToTLSConfig() error = nil, want error for invalid certificate")
		}
	})
}

func TestOptionConstantValues(t *testing.T) {
	// Verify constant values match expected strings
	t.Run("KVScope constants", func(t *testing.T) {
		if KVScopeGlobal != "global" {
			t.Errorf("KVScopeGlobal = %q, want %q", KVScopeGlobal, "global")
		}
		if KVScopeWorkspace != "workspace" {
			t.Errorf("KVScopeWorkspace = %q, want %q", KVScopeWorkspace, "workspace")
		}
		if KVScopeUser != "user" {
			t.Errorf("KVScopeUser = %q, want %q", KVScopeUser, "user")
		}
		if KVScopeUserWorkspace != "user-workspace" {
			t.Errorf("KVScopeUserWorkspace = %q, want %q", KVScopeUserWorkspace, "user-workspace")
		}
	})

	t.Run("MessageType constants", func(t *testing.T) {
		if MessageTypeChat != "CHAT" {
			t.Errorf("MessageTypeChat = %q, want %q", MessageTypeChat, "CHAT")
		}
		if MessageTypeControl != "CONTROL" {
			t.Errorf("MessageTypeControl = %q, want %q", MessageTypeControl, "CONTROL")
		}
		if MessageTypeToolCall != "TOOL_CALL" {
			t.Errorf("MessageTypeToolCall = %q, want %q", MessageTypeToolCall, "TOOL_CALL")
		}
		if MessageTypeEvent != "EVENT" {
			t.Errorf("MessageTypeEvent = %q, want %q", MessageTypeEvent, "EVENT")
		}
		if MessageTypeMetric != "METRIC" {
			t.Errorf("MessageTypeMetric = %q, want %q", MessageTypeMetric, "METRIC")
		}
	})

	t.Run("TaskAssignmentMode constants", func(t *testing.T) {
		if TaskAssignmentSelfAssign != "SELF_ASSIGN" {
			t.Errorf("TaskAssignmentSelfAssign = %q, want %q", TaskAssignmentSelfAssign, "SELF_ASSIGN")
		}
		if TaskAssignmentTargeted != "TARGETED" {
			t.Errorf("TaskAssignmentTargeted = %q, want %q", TaskAssignmentTargeted, "TARGETED")
		}
		if TaskAssignmentPool != "POOL" {
			t.Errorf("TaskAssignmentPool = %q, want %q", TaskAssignmentPool, "POOL")
		}
	})
}
