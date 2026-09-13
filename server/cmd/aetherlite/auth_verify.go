// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	aclcore "github.com/scitrera/aether/server/internal/acl"
	"github.com/scitrera/aether/server/internal/auth"
	aclstore "github.com/scitrera/aether/server/internal/storage/acl"
	"github.com/scitrera/aether/server/pkg/authproxy"
	"github.com/scitrera/aether/server/pkg/models"
)

// liveACLEvaluator delegates to the gateway's own store, including its current
// ACL cache, roles, fallback policy and cluster authority decorators.
type liveACLEvaluator struct{ store aclstore.Store }

func (e liveACLEvaluator) EvaluateAccess(ctx context.Context, principal models.Identity,
	resourceType, resourceID string, requiredLevel int) (*aclcore.ACLDecision, error) {
	return e.store.CheckAccess(ctx, principal, resourceType, resourceID, "auth_verify",
		principal.Workspace, uuid.Nil, requiredLevel)
}

// newLiteAuthVerify reuses the gateway's token and ACL stores. There is no second
// database or authentication implementation. The listener is opt-in and belongs
// on the private service network, separately from public browser ingress.
func newLiteAuthVerify(address, tenant string, tokens auth.APITokenStore, policy aclstore.Store) (*http.Server, net.Listener, error) {
	if tokens == nil || policy == nil || tenant == "" {
		return nil, nil, fmt.Errorf("auth verify requires tenant, token store and ACL store")
	}
	resolver, err := authproxy.LoadSingleTenantResolverFromEnv(tenant)
	if err != nil {
		return nil, nil, err
	}
	authenticator := auth.NewCompositeAuthenticator(auth.NewAPIKeyAuthenticator(tokens))
	middleware := authproxy.NewAuthMiddlewareFull(authenticator, liveACLEvaluator{policy}, policy, resolver, tenant)
	verifier, err := authproxy.NewServer(&authproxy.Config{Mode: authproxy.ModeVerify, TenantID: tenant}, middleware)
	if err != nil {
		return nil, nil, err
	}
	// Expose only strict verification and health on this plane.
	mux := http.NewServeMux()
	mux.Handle("/auth/verify", verifier.Mux())
	mux.Handle("/healthz", verifier.Mux())
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, nil, err
	}
	return &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}, listener, nil
}
