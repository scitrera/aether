package gateway

import (
	"context"
	"strings"

	pb "github.com/scitrera/aether/api/proto"
	regstore "github.com/scitrera/aether/internal/storage/registry"
	"github.com/scitrera/aether/pkg/models"
)

// extensionMetadataHeader is the gRPC metadata key clients can use to
// declare extensions at connect-time without re-encoding the InitConnection
// proto. Comma-separated URI list; mirrors A2A's "X-A2A-Extensions" HTTP
// header convention. Header-sourced declarations are always non-required —
// the proto field is the authoritative surface for `required` semantics.
const extensionMetadataHeader = "aether-extensions"

// KnownExtensions is the server's intrinsic extension set. Adding a URI here
// declares that the gateway natively supports an extension regardless of
// any agent declaration. Negotiation at connect-time unions this with the
// requesting agent's AgentRegistration.Extensions (when the caller is an
// agent and the URI matches its own registration) so customer-declared
// extensions become first-class for sessions on that agent.
//
// Phase 6 ships with KnownExtensions intentionally empty. The wire +
// negotiation machinery is in place; concrete extensions land in later
// phases as they're specified. Adding an entry here is the only required
// action to declare gateway support — IsExtensionKnown reads from this map
// directly.
var KnownExtensions = map[string]bool{
	// (intentionally empty in Phase 6)
}

// IsExtensionKnown reports whether the gateway natively supports the URI.
// Returns true for any entry in KnownExtensions.
func IsExtensionKnown(uri string) bool { return KnownExtensions[uri] }

// serverSupportedExtensionsList returns a sorted-ish slice of the URIs the
// gateway natively supports. Returned slice is safe for the caller to
// append to — we allocate fresh each call.
func serverSupportedExtensionsList() []string {
	if len(KnownExtensions) == 0 {
		return nil
	}
	out := make([]string, 0, len(KnownExtensions))
	for uri := range KnownExtensions {
		out = append(out, uri)
	}
	return out
}

// extensionNegotiationResult bundles the outcome of negotiating a set of
// client-declared extensions against the gateway + agent registration set.
// rejectURI / rejectReason are populated only when the caller asked for an
// unsupported `required` URI; callers MUST close the connection with
// codes.FailedPrecondition when rejectURI != "".
type extensionNegotiationResult struct {
	negotiated      []*pb.NegotiatedExtension
	serverSupported []string
	activeURIs      map[string]string // uri → version, snapshotted onto ClientSession
	rejectURI       string
	rejectReason    string
}

// agentDeclaredLookup is the subset of AgentRegistration data the
// negotiator needs. Decoupled into an interface so tests can pass a fake
// without dragging in the full registry stack.
type agentDeclaredLookup interface {
	// AgentExtensions returns the URI list the agent's registration
	// declared, or nil when no registration exists for the implementation.
	// The implementation argument is the bare implementation name (no
	// ":specifier" suffix); callers should strip the specifier first.
	AgentExtensions(ctx context.Context, implementation string) []string
}

// registryAgentExtensions adapts a regstore.Store (Phase 5+ registry
// surface) to agentDeclaredLookup. Errors are swallowed — a missing or
// failed lookup just returns nil, mirroring the rest of the gateway's
// nil-tolerant treatment of optional registrations.
type registryAgentExtensions struct {
	store regstore.Store
}

func (r registryAgentExtensions) AgentExtensions(ctx context.Context, implementation string) []string {
	if r.store == nil || implementation == "" {
		return nil
	}
	reg, err := r.store.Get(ctx, implementation)
	if err != nil || reg == nil {
		return nil
	}
	return reg.Extensions
}

// negotiateExtensions runs the connect-time extension negotiation. Inputs:
//
//   - clientDecls: the union of InitConnection.Extensions and any
//     ExtensionDeclaration values synthesized from the Aether-Extensions
//     gRPC metadata header. Order is preserved on the wire (header-sourced
//     declarations follow proto-sourced ones).
//   - identity: the resolved connecting principal. For agents, the
//     implementation field is used to look up agent-declared extensions
//     that widen the supported set.
//   - lookup: source of agent-registration Extensions URIs.
//
// Behavior:
//   - Each URI is supported when it's in KnownExtensions OR (for agent
//     callers) when it appears in the agent's registration Extensions list.
//   - A `required` declaration that ends up unsupported causes negotiation
//     to fail. rejectURI / rejectReason are populated; the caller MUST
//     reject the connection. Other declarations are still negotiated and
//     surfaced in the result for diagnostic value.
//   - serverSupported is the set of native KnownExtensions URIs the client
//     did NOT declare. Agent-declared (non-native) URIs do not appear here
//     — they're only visible to clients that explicitly ask for them.
func negotiateExtensions(
	ctx context.Context,
	clientDecls []*pb.ExtensionDeclaration,
	identity models.Identity,
	lookup agentDeclaredLookup,
) extensionNegotiationResult {
	res := extensionNegotiationResult{
		activeURIs: make(map[string]string),
	}

	// Build the agent-declared set once per call. Only relevant for agent
	// principals — tasks/users/services don't carry an AgentRegistration.
	var agentDeclared map[string]struct{}
	if identity.Type == models.PrincipalAgent && lookup != nil && identity.Implementation != "" {
		impl := identity.Implementation
		// Defensive: strip a trailing ":specifier" if it sneaks in. The
		// agent registry stores by implementation name only.
		if idx := strings.LastIndex(impl, ":"); idx > 0 {
			impl = impl[:idx]
		}
		uris := lookup.AgentExtensions(ctx, impl)
		if len(uris) > 0 {
			agentDeclared = make(map[string]struct{}, len(uris))
			for _, u := range uris {
				agentDeclared[u] = struct{}{}
			}
		}
	}

	declaredByClient := make(map[string]struct{}, len(clientDecls))
	for _, decl := range clientDecls {
		if decl == nil || decl.Uri == "" {
			continue
		}
		declaredByClient[decl.Uri] = struct{}{}
		supported := false
		if KnownExtensions[decl.Uri] {
			supported = true
		} else if _, ok := agentDeclared[decl.Uri]; ok {
			supported = true
		}

		nx := &pb.NegotiatedExtension{
			Uri:       decl.Uri,
			Version:   decl.Version,
			Supported: supported,
		}
		if !supported {
			nx.RejectionReason = "extension URI not supported by this gateway"
			if decl.Required && res.rejectURI == "" {
				res.rejectURI = decl.Uri
				res.rejectReason = "extension required but not supported: " + decl.Uri
			}
		} else {
			res.activeURIs[decl.Uri] = decl.Version
		}
		res.negotiated = append(res.negotiated, nx)
	}

	// server_supported_extensions: URIs the gateway natively supports but
	// the client did not list. Skip agent-declared (non-native) URIs —
	// they're attached to a specific agent's identity, not gateway-wide.
	for _, uri := range serverSupportedExtensionsList() {
		if _, ok := declaredByClient[uri]; !ok {
			res.serverSupported = append(res.serverSupported, uri)
		}
	}

	return res
}

// parseExtensionMetadataHeader expands the Aether-Extensions gRPC metadata
// values into ExtensionDeclaration entries. Values are comma-separated
// URI lists; whitespace around URIs is trimmed. Header-sourced declarations
// are always non-required and carry no version / json_schema (those fields
// stay zero). Returns nil when no header values are supplied.
func parseExtensionMetadataHeader(values []string) []*pb.ExtensionDeclaration {
	if len(values) == 0 {
		return nil
	}
	var out []*pb.ExtensionDeclaration
	for _, v := range values {
		for _, raw := range strings.Split(v, ",") {
			uri := strings.TrimSpace(raw)
			if uri == "" {
				continue
			}
			out = append(out, &pb.ExtensionDeclaration{Uri: uri})
		}
	}
	return out
}
