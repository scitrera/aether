package quota_test

import (
	"github.com/scitrera/aether/internal/gateway"
	"github.com/scitrera/aether/internal/quota"
)

// Compile-time check: MemoryQuotaManager satisfies the gateway QuotaChecker interface.
var _ gateway.QuotaChecker = (*quota.MemoryQuotaManager)(nil)
