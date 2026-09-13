// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	authsqlite "github.com/scitrera/aether/server/internal/auth/sqlite"
	aclstore "github.com/scitrera/aether/server/internal/storage/acl"
	aclsqlite "github.com/scitrera/aether/server/internal/storage/acl/sqlite"

	"github.com/scitrera/aether/server/pkg/crypto"
)

func TestLiteAuthVerifyUsesLiveTokenAndACLStores(t *testing.T) {
	crypto.InitTokenHMAC([]byte("synthetic-auth-verify-test-hmac-key"))
	ctx := context.Background()
	tokenDB, err := openSQLiteNative(ctx, filepath.Join(t.TempDir(), "tokens.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer tokenDB.Close()
	tokens, err := authsqlite.New(tokenDB)
	if err != nil {
		t.Fatal(err)
	}
	aclDB, err := openSQLiteNative(ctx, filepath.Join(t.TempDir(), "acl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer aclDB.Close()
	policy, err := aclsqlite.New(aclDB, nil, nil, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer policy.Close()
	_, err = policy.GrantAccess(ctx, "user", "alice@example.test", "workspace", "*", aclstore.AccessReadWrite, "fixture", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	token, err := tokens.CreateToken(ctx, "fixture", "User", []string{"*"}, nil, "alice@example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	server, listener, err := newLiteAuthVerify("127.0.0.1:0", "alpha", tokens, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	go server.Serve(listener)
	client := &http.Client{Timeout: 3 * time.Second}
	request := func(value string, status int) http.Header {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, "http://"+listener.Addr().String()+"/auth/verify", nil)
		if value != "" {
			req.Header.Set("X-API-Key", value)
		}
		req.Header.Set("X-Auth-User-ID", "forged@example.test")
		req.Header.Set("X-Auth-Tenant-ID", "beta")
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != status {
			t.Fatalf("verify status %d, want %d", response.StatusCode, status)
		}
		return response.Header
	}
	request("", http.StatusUnauthorized)
	request("invalid-fixture-token", http.StatusUnauthorized)
	headers := request(token.Token, http.StatusOK)
	if headers.Get("X-Auth-User-ID") != "alice@example.test" || headers.Get("X-Auth-Tenant-ID") != "alpha" {
		t.Fatalf("forged identity survived: %v", headers)
	}
	// The same store supplies token revocation; no process restart or copied DB.
	if err := tokens.RevokeToken(ctx, token.APIToken.ID); err != nil {
		t.Fatal(err)
	}
	request(token.Token, http.StatusUnauthorized)
	if _, _, err := newLiteAuthVerify("127.0.0.1:0", "", tokens, policy); err == nil {
		t.Fatal("missing tenant accepted")
	}
}
