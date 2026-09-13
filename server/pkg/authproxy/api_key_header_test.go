// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package authproxy

import (
	"net/http/httptest"
	"testing"

	"github.com/scitrera/aether/server/internal/auth"
)

func TestExplicitAPIKeyHeaderAndAuthorizationPrecedence(t *testing.T) {
	request := httptest.NewRequest("GET", "/auth/verify", nil)
	request.Header.Set("X-API-Key", "opaque-fixture")
	if got := extractCredentials(request)[auth.CredKeyAPIKey]; got != "opaque-fixture" {
		t.Fatal("explicit API-key header not forwarded to authenticator")
	}
	request.Header.Set("Authorization", "Bearer different-fixture")
	if got := extractCredentials(request)[auth.CredKeyAPIKey]; got != "different-fixture" {
		t.Fatal("Authorization must retain its established precedence")
	}
}
