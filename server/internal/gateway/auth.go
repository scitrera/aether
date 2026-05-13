package gateway

import (
	"context"

	pb "github.com/scitrera/aether/api/proto"
	"github.com/scitrera/aether/pkg/models"
)

// authenticateMTLS delegates to the AuthHandler.
func (s *GatewayServer) authenticateMTLS(ctx context.Context) (models.Identity, models.PrincipalType, bool, bool, error) {
	return s.authHandler.authenticateMTLS(ctx)
}

// resolveConnectionIdentity delegates to the AuthHandler.
func (s *GatewayServer) resolveConnectionIdentity(ctx context.Context, init *pb.InitConnection, certIdentity models.Identity, certPrincipalType models.PrincipalType, hasCertificate bool, isAnonymous bool) (models.Identity, error) {
	return s.authHandler.resolveConnectionIdentity(ctx, init, certIdentity, certPrincipalType, hasCertificate, isAnonymous)
}

// authenticateCredentials delegates to the AuthHandler.
func (s *GatewayServer) authenticateCredentials(ctx context.Context, init *pb.InitConnection, identity models.Identity, hasCertificate bool) (string, models.Identity, error) {
	return s.authHandler.authenticateCredentials(ctx, init, identity, hasCertificate)
}
