package models

import (
	"testing"
)

func TestParseSendTarget(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantImpl    string
		wantSpec    string
		wantWild    bool
		wantErr     bool
		errContains string
	}{
		{
			name:     "canonical 4-segment",
			input:    "sv::my-service::instance-1",
			wantImpl: "my-service",
			wantSpec: "instance-1",
			wantWild: false,
		},
		{
			name:     "canonical 4-segment with dots in impl",
			input:    "sv::scitrera.runtime.core::pod-1",
			wantImpl: "scitrera.runtime.core",
			wantSpec: "pod-1",
			wantWild: false,
		},
		{
			name:     "canonical 4-segment with empty specifier",
			input:    "sv::my-service::",
			wantImpl: "my-service",
			wantSpec: "",
			wantWild: false,
		},
		{
			name:     "bare 3-segment wildcard",
			input:    "sv::my-service",
			wantImpl: "my-service",
			wantSpec: "",
			wantWild: true,
		},
		{
			name:     "bare 3-segment wildcard with dotted impl",
			input:    "sv::scitrera.runtime.core",
			wantImpl: "scitrera.runtime.core",
			wantSpec: "",
			wantWild: true,
		},
		{
			name:        "wrong prefix",
			input:       "ag::workspace::impl::spec",
			wantErr:     true,
			errContains: "must start with",
		},
		{
			name:        "no separator at all",
			input:       "svmyservice",
			wantErr:     true,
			errContains: "must start with",
		},
		{
			name:        "empty string",
			input:       "",
			wantErr:     true,
			errContains: "must start with",
		},
		{
			name:        "double-colon in impl value produces too many segments",
			input:       "sv::bad::impl::extra::value",
			wantErr:     true,
			errContains: "too many segments",
		},
		{
			name:        "empty implementation in wildcard form",
			input:       "sv::",
			wantErr:     true,
			errContains: "empty implementation",
		},
		{
			name:        "empty implementation in full form",
			input:       "sv::::spec",
			wantErr:     true,
			errContains: "empty implementation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			impl, spec, isWild, err := ParseSendTarget(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseSendTarget(%q) error = nil, want error containing %q", tt.input, tt.errContains)
					return
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("ParseSendTarget(%q) error = %v, want error containing %q", tt.input, err, tt.errContains)
				}
				return
			}

			if err != nil {
				t.Errorf("ParseSendTarget(%q) unexpected error = %v", tt.input, err)
				return
			}
			if impl != tt.wantImpl {
				t.Errorf("ParseSendTarget(%q) impl = %q, want %q", tt.input, impl, tt.wantImpl)
			}
			if spec != tt.wantSpec {
				t.Errorf("ParseSendTarget(%q) specifier = %q, want %q", tt.input, spec, tt.wantSpec)
			}
			if isWild != tt.wantWild {
				t.Errorf("ParseSendTarget(%q) isWildcard = %v, want %v", tt.input, isWild, tt.wantWild)
			}
		})
	}
}
