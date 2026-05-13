package proxysidecar

import (
	"github.com/rs/zerolog/log"
	pb "github.com/scitrera/aether/api/proto"
)

// describeSandboxIdentity stringifies the sandbox's claimed client_type for
// audit logs. The function intentionally avoids structured fields the
// gateway has not authenticated — it returns a short label suitable for
// logger payloads only.
func describeSandboxIdentity(init *pb.InitConnection) string {
	if init == nil {
		return "<nil>"
	}
	switch v := init.GetClientType().(type) {
	case *pb.InitConnection_Agent:
		ag := v.Agent
		return "agent:" + ag.GetWorkspace() + "/" + ag.GetImplementation() + "/" + ag.GetSpecifier()
	case *pb.InitConnection_Task:
		t := v.Task
		return "task:" + t.GetWorkspace() + "/" + t.GetImplementation() + "/" + t.GetUniqueSpecifier()
	case *pb.InitConnection_User:
		u := v.User
		return "user:" + u.GetUserId() + "/" + u.GetWindowId()
	case *pb.InitConnection_Orchestrator:
		o := v.Orchestrator
		return "orchestrator:" + o.GetImplementation() + "/" + o.GetSpecifier()
	case *pb.InitConnection_WorkflowEngine:
		return "workflow_engine:" + v.WorkflowEngine.GetInstanceId()
	case *pb.InitConnection_MetricsBridge:
		return "metrics_bridge:" + v.MetricsBridge.GetInstanceId()
	case *pb.InitConnection_Bridge:
		b := v.Bridge
		return "bridge:" + b.GetImplementation() + "/" + b.GetSpecifier()
	case *pb.InitConnection_Service:
		s := v.Service
		return "service:" + s.GetImplementation() + "/" + s.GetSpecifier()
	case nil:
		return "<unset>"
	default:
		return "<unknown>"
	}
}

// rewriteInitConnection replaces the sandbox's claimed identity and
// credentials with the sidecar's configured Service identity and gateway
// API key. The original claim is logged so operators can correlate
// sandbox attempts with the rewritten upstream init.
//
// Returns a new InitConnection — the input is not mutated so the caller
// may inspect the original after the rewrite.
func rewriteInitConnection(in *pb.InitConnection, cfg *Config, apiKey string) *pb.InitConnection {
	out := &pb.InitConnection{
		ClientType: &pb.InitConnection_Service{
			Service: &pb.ServiceIdentity{
				Implementation: cfg.Service.Implementation,
				Specifier:      cfg.Service.Specifier,
			},
		},
		Credentials: map[string]string{},
	}
	if apiKey != "" {
		out.Credentials["api_key"] = apiKey
	}

	claimed := describeSandboxIdentity(in)
	log.Info().
		Str("sandbox_claim", claimed).
		Str("sidecar_identity", "service:"+cfg.Service.Implementation+"/"+cfg.Service.Specifier).
		Msg("relay: rewriting InitConnection identity")

	// Discard sandbox-supplied credentials and resume id outright. A
	// sandbox is not allowed to take over an existing session lock by
	// claiming a session id we never gave it.
	if in != nil && in.GetResumeSessionId() != "" {
		log.Warn().
			Str("sandbox_claim", claimed).
			Str("resume_session_id", in.GetResumeSessionId()).
			Msg("relay: discarding sandbox-supplied resume_session_id")
	}
	return out
}
