# Admin UI and API Reference

The Aether Gateway ships with an embedded administrative web interface and a REST API. Both are served by a separate HTTP server that runs alongside the main gRPC gateway. This document covers access, authentication, available features, all REST endpoints, and the WebSocket monitoring interface.

## Accessing the Admin UI

By default the admin server listens on port **31880**. Open a browser to:

```
http://localhost:31880
```

The UI is a single-page application embedded in the gateway binary. It loads from the embedded filesystem automatically; no separate deployment step is required.

The admin API is served under the `/api/v1` path prefix (versioned). Two additional endpoints, `/health` and `/info`, are registered unversioned directly on the server root and require no authentication. All API endpoints return JSON.

### Default Port

The port is set in the YAML configuration file:

```yaml
admin:
  enabled: true
  port: 31880
```

It can be overridden at startup with the `--admin-port` flag (or the `AETHER_ADMIN_PORT` environment variable). Note this is distinct from the ops/metrics port (default 9090, see below) — don't confuse the two:

```bash
./gateway --config configs/dev.yaml --admin-port 8888
```

## Authentication

The admin API requires authentication in all production deployments. Two mechanisms are supported.

### API Key (recommended)

Set an API key in the configuration:

```yaml
admin:
  api_key: "your-secret-key"
```

All requests to `/api/v1/**` must include:

```
Authorization: Bearer your-secret-key
```

If `api_key` is set but TLS is not enabled, the gateway logs a warning that the key will be transmitted in plaintext. Enable TLS for production deployments.

### Insecure Mode (development only)

If no API key is configured, the gateway will refuse to start unless the insecure flag is explicitly set:

```bash
AETHER_ALLOW_DEV_MODE=1 ./gateway --dev --insecure-admin
```

**Note:** `--insecure-admin` additionally requires the `AETHER_ALLOW_DEV_MODE` environment variable to be set to any non-empty value; without it the gateway process exits immediately at startup with `--insecure-admin requires AETHER_ALLOW_DEV_MODE env var to be set; this flag is NOT for production use`, even if `--dev` is also passed.

This is equivalent to setting `InsecureNoAuth: true` in the server configuration. A warning is logged:

```
WRN admin API is running without authentication (InsecureNoAuth=true); NOT FOR PRODUCTION
```

Never use insecure mode in a production environment.

### TLS

The admin server supports HTTPS independently of the gRPC server's mTLS configuration:

```yaml
admin:
  tls_cert_file: "/path/to/cert.pem"
  tls_key_file:  "/path/to/key.pem"
```

When both files are set, the admin server starts with TLS and the API key is transmitted encrypted.

### WebSocket Authentication

The WebSocket endpoint at `/api/v1/ws/events` is under the `/api/v1` subrouter and inherits the API key middleware. Clients that cannot set the `Authorization` header in the WebSocket upgrade request (browser `WebSocket` API) may pass the token via the `Sec-WebSocket-Protocol` subprotocol:

```javascript
const ws = new WebSocket("ws://localhost:31880/api/v1/ws/events", ["auth", "Bearer your-secret-key"]);
```

The server echoes back the `auth` subprotocol in the upgrade response to confirm the method was accepted.

## Rate Limiting

All requests are subject to IP-based rate limiting. The defaults are 10 requests per second with a burst of 20. These can be adjusted in configuration:

```yaml
admin:
  rate_limit: 10        # requests/second per IP
  rate_limit_burst: 20  # burst allowance
```

Health probe endpoints (`/health/*`) are exempt from rate limiting.

## CORS

Cross-origin requests are controlled by:

```yaml
admin:
  cors_origin: "https://your-dashboard.example.com"
```

Setting `cors_origin: "*"` is permitted but disables WebSocket connections in production mode (to prevent cross-site WebSocket hijacking). Omitting `cors_origin` restricts the UI to same-origin requests.

## Health Probes and Prometheus Metrics (Separate Ops Server)

`/health/live`, `/health/ready`, `/health/startup`, and `GET /metrics` are **not** served by the admin server described above — they run on a dedicated ops server, on its own port, unauthenticated and not subject to rate limiting. This isolates health/metrics scraping from the admin API's auth and rate-limit policy.

The ops port defaults to **9090** and is configured independently of the admin port via `gateway.ops_port` in the YAML config, the `--ops-port` flag, or the `AETHER_OPS_PORT` environment variable. See the [Monitoring Guide](monitoring.md) for full metric details.

| Endpoint | Method | Description |
|---|---|---|
| `/health/live` | GET | Always returns `200 OK` while the process is running. Use as liveness probe. |
| `/health/ready` | GET | Returns `200` when dependencies (Redis, RabbitMQ) are reachable; `503` otherwise. Use as readiness probe. |
| `/health/startup` | GET | Returns `200` once the ops server has been marked ready by the gateway; `503` before that. Use as startup probe. |
| `/metrics` | GET | Prometheus scrape endpoint, unauthenticated by convention. |

In production, network-isolate the ops port so that only the Prometheus scraper and orchestrator (Kubernetes) can reach it. Do not expose it through the public load balancer.

Note that the admin server also exposes an unversioned, unauthenticated `GET /health` (dependency health as JSON, not the K8s-probe format above) and `GET /info` on the admin port — see [Health and Info](#health-and-info) below.

## REST API Reference

All endpoints below are prefixed with `/api/v1` and require the `Authorization: Bearer <key>` header unless otherwise noted.

### Health and Info

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Gateway health status including dependency checks. Unversioned (no `/api/v1` prefix) and exempt from API key auth. |
| GET | `/info` | Gateway identity, version, and build information. Unversioned (no `/api/v1` prefix) and exempt from API key auth. |
| GET | `/api/v1/stats` | Active connection counts and message routing statistics. |

### Connections

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/connections` | List all active connections. Filter by `?type=agent` or `?workspace=my-workspace`. |
| GET | `/api/v1/connections/{session_id}` | Get details for a specific session. |
| DELETE | `/api/v1/connections/{session_id}` | Force-disconnect a session. Releases the distributed lock and terminates the gRPC stream. |

### Tasks

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/tasks` | List tasks. Filter by `?status=pending`, `?workspace=ws`, `?type=unique`. |
| GET | `/api/v1/tasks/{task_id}` | Get a specific task record. |
| POST | `/api/v1/tasks/{task_id}/retry` | Schedule a failed task for retry. |
| POST | `/api/v1/tasks/{task_id}/cancel` | Cancel a pending or running task. |

### Workspaces

| Method | Path | Body fields | Description |
|---|---|---|---|
| GET | `/api/v1/workspaces` | — | List all workspaces. |
| POST | `/api/v1/workspaces` | `workspace_id`, `display_name`, `description`, `tenant_id`, `metadata` | Create a workspace. |
| GET | `/api/v1/workspaces/{workspace_id}` | — | Get workspace details. |
| PUT | `/api/v1/workspaces/{workspace_id}` | `display_name`, `description`, `tenant_id`, `metadata` | Update workspace metadata. |
| DELETE | `/api/v1/workspaces/{workspace_id}` | — | Delete a workspace. |
| GET | `/api/v1/workspaces/{workspace_id}/message-flow` | — | Get recent message flow graph for the workspace. |

### Agents and Orchestration

| Method | Path | Body fields | Description |
|---|---|---|---|
| GET | `/api/v1/agents` | — | List registered agent implementations. |
| POST | `/api/v1/agents` | `implementation`, `description`, `launch_params` | Register an agent implementation. |
| GET | `/api/v1/agents/{implementation}` | — | Get agent registration details. |
| PUT | `/api/v1/agents/{implementation}` | `description`, `launch_params` | Update agent registration. |
| DELETE | `/api/v1/agents/{implementation}` | — | Remove an agent registration. |
| POST | `/api/v1/agents/{implementation}/launch` | `specifier`, `workspace` | Manually trigger an agent launch via the registered Orchestrator. Defaults: `specifier="default"`, `workspace="default"`. |
| GET | `/api/v1/orchestrators` | — | List registered Orchestrator profiles. |

### KV Store

The KV store is organized into scopes. Common scopes are `global` and workspace names.

| Method | Path | Body fields | Description |
|---|---|---|---|
| GET | `/api/v1/kv` | — | List keys. Filter by `?scope=global&prefix=my/`. Defaults to `scope=global`. |
| GET | `/api/v1/kv/{scope}/{key}` | — | Get a KV entry. The key supports slashes (e.g., `/api/v1/kv/global/demo/setting`). |
| PUT | `/api/v1/kv/{scope}/{key}` | `value`, `ttl` (seconds, optional) | Set a KV entry. A `ttl` of `0` means no expiration. |
| DELETE | `/api/v1/kv/{scope}/{key}` | — | Delete a KV entry. |

### ACL Management

Access control rules govern which principals can perform which operations on which resources.

**Rules**

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/acl/rules` | List ACL rules. Filter by `?principal_type=agent&principal_id=...&resource_type=...&resource_id=...`. |
| POST | `/api/v1/acl/rules` | Grant access. Body: `principal_type`, `principal_id`, `resource_type`, `resource_id`, `granted_by`. All fields required. |
| GET | `/api/v1/acl/rules/{rule_id}` | Get a specific rule. Query params: `principal_type`, `principal_id`, `resource_type`, `resource_id`. |
| DELETE | `/api/v1/acl/rules/{rule_id}` | Revoke access. Query params: `principal_type`, `principal_id`, `resource_type`, `resource_id`. |

**Audit Log**

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/acl/audit` | Query the ACL decision audit log. Filter by `principal_type`, `principal_id`, `resource_type`, `resource_id`, `decision`, `workspace`, `limit`. |

**Fallback Policy**

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/acl/fallback-policy` | Get the fallback policy for a rule category. Query param: `rule_category`. |
| PUT | `/api/v1/acl/fallback-policy` | Set the fallback policy. Body: `rule_category`, `updated_by`. |

**Maintenance**

| Method | Path | Description |
|---|---|---|
| POST | `/api/v1/acl/cleanup/expired-rules` | Delete all expired ACL rules. Returns `count` of deleted rules. |
| POST | `/api/v1/acl/cleanup/audit-logs` | Delete ACL audit log entries older than the retention window. Query param: `?retention_days=90` (default 90). Returns `count` of deleted entries. |

**Authority Grants**

Authority grants model delegated authority (subject → delegate, issued by a principal, optionally chained via `parent_grant_id`) with expiration and hop-count limits.

| Method | Path | Body fields | Description |
|---|---|---|---|
| GET | `/api/v1/acl/authority-grants` | — | List grants. Filter by `root_grant_id`, `subject_type`, `subject_id`, `delegate_type`, `delegate_id`, `audience_type`, `audience_id`, `include_revoked`, `active_only`, `limit`, `offset`. |
| POST | `/api/v1/acl/authority-grants` | `subject`, `delegate`, `issued_by` (each `{principal_type, principal_id}`), `root_subject`, `parent_grant_id`, `may_delegate`, `remaining_hops`, `workspace_scope`, `expires_at` | Create a grant. `subject`, `delegate`, `issued_by`, and `expires_at` are required. |
| GET | `/api/v1/acl/authority-grants/{grant_id}` | — | Get a specific grant. |
| POST | `/api/v1/acl/authority-grants/{grant_id}/renew` | `expires_at` | Extend a grant's expiration. `expires_at` is required. |
| POST | `/api/v1/acl/authority-grants/{grant_id}/revoke` | — | Revoke a grant. |

**Groups**

| Method | Path | Body fields | Description |
|---|---|---|---|
| GET | `/api/v1/acl/groups` | — | List ACL groups. |
| POST | `/api/v1/acl/groups` | `name`, `description`, `created_by`, `metadata` | Create a group. `name` is required. |
| GET | `/api/v1/acl/groups/{name}` | — | Get a group. |
| DELETE | `/api/v1/acl/groups/{name}` | — | Delete a group. |
| GET | `/api/v1/acl/groups/{name}/members` | — | List group members. |
| POST | `/api/v1/acl/groups/{name}/members` | `member_type`, `member_id`, `granted_by`, `expires_at` | Add a member. `member_type` and `member_id` are required. |
| DELETE | `/api/v1/acl/groups/{name}/members` | — | Remove a member. |

**Roles**

| Method | Path | Body fields | Description |
|---|---|---|---|
| GET | `/api/v1/acl/roles` | — | List ACL roles. |
| POST | `/api/v1/acl/roles` | `name`, `description`, `created_by`, `metadata` | Create a role. `name` is required. |
| GET | `/api/v1/acl/roles/{name}` | — | Get a role. |
| DELETE | `/api/v1/acl/roles/{name}` | — | Delete a role. |
| GET | `/api/v1/acl/roles/{name}/assignments` | — | List role assignments. |
| POST | `/api/v1/acl/roles/{name}/assignments` | `assignee_type`, `assignee_id`, `granted_by`, `expires_at` | Assign the role. `assignee_type` and `assignee_id` are required. |
| DELETE | `/api/v1/acl/roles/{name}/assignments` | — | Unassign the role. |

**Principals**

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/acl/principals/{type}/{id}/groups` | List the groups a principal belongs to. |
| GET | `/api/v1/acl/principals/{type}/{id}/roles` | List the roles assigned to a principal. |
| GET | `/api/v1/acl/principals/{type}/{id}/effective` | Explain the principal's effective access (resolved rules, group/role membership). |

### API Token Management

API tokens are used by clients to authenticate gRPC connections via the API key auth method. Token secrets are returned only at creation time and are not stored in plaintext.

| Method | Path | Body fields | Description |
|---|---|---|---|
| GET | `/api/v1/tokens` | — | List all tokens (metadata only, no secret values). |
| POST | `/api/v1/tokens` | `name`, `principal_type`, `workspace_patterns`, `scopes`, `expires_in_hours`, `created_by` | Create a token. Returns the plaintext token value once. Defaults: `workspace_patterns=["*"]`, `scopes=["connect"]`, `created_by="admin"`. |
| GET | `/api/v1/tokens/{token_id}` | — | Get token metadata. |
| POST | `/api/v1/tokens/{token_id}/revoke` | — | Revoke a token without deleting its record. |
| DELETE | `/api/v1/tokens/{token_id}` | — | Permanently delete a token record. |

### Messaging

| Method | Path | Body fields | Description |
|---|---|---|---|
| POST | `/api/v1/messages/send` | `target_topic`, `payload`, `message_type` | Inject a message directly into the routing layer. `message_type` must be one of `CHAT`, `CONTROL`, `TOOL_CALL`, `EVENT`, `METRIC`, or empty (defaults to `CHAT`). `target_topic` is required. Note: there is no `source_topic` body field — the request does not accept a spoofed source. |

### Workspace Rate Limits

Per-workspace message rate limits (messages/second), separate from the admin API's own IP-based rate limiting described above.

| Method | Path | Body fields | Description |
|---|---|---|---|
| GET | `/api/v1/rate-limits` | — | List all configured workspace rate limits. |
| GET | `/api/v1/workspaces/{workspace_id}/rate-limit` | — | Get the rate limit for a workspace. |
| PUT | `/api/v1/workspaces/{workspace_id}/rate-limit` | `messages_per_second` | Set the rate limit for a workspace. Must be `>= 0`. |
| DELETE | `/api/v1/workspaces/{workspace_id}/rate-limit` | — | Remove the workspace-specific rate limit (falls back to the default). |

## WebSocket Monitoring

The WebSocket endpoint provides a real-time stream of gateway events and per-topic message monitoring.

**Endpoint:** `ws://localhost:31880/api/v1/ws/events`

Authentication follows the same API key rules as the REST API. See the WebSocket Authentication section above for browser compatibility.

### Server-to-Client Messages

All messages from the server are JSON objects with a `type` field.

**Gateway events** are emitted for system-level occurrences (connections, disconnections, errors):

```json
{
  "type": "event",
  "event": { ... }
}
```

**Topic monitor messages** are delivered when you subscribe to a topic:

```json
{
  "type": "monitor_message",
  "topic": "ag.hello.greeter.agent-a",
  "message": {
    "source_topic": "ag.hello.greeter.agent-b",
    "payload": "...",
    "message_type": "CHAT"
  }
}
```

**Subscription confirmations:**

```json
{ "type": "monitor_subscribed",   "topic": "ag.hello.greeter.agent-a" }
{ "type": "monitor_unsubscribed", "topic": "ag.hello.greeter.agent-a" }
```

**Errors:**

```json
{ "type": "error", "error": "topic is required for subscribe_monitor" }
```

### Client-to-Server Messages

Clients can subscribe to live message traffic on any topic.

**Subscribe to a topic:**

```json
{
  "action": "subscribe_monitor",
  "topic": "ag.hello.greeter.agent-a"
}
```

**Unsubscribe:**

```json
{
  "action": "unsubscribe_monitor",
  "topic": "ag.hello.greeter.agent-a"
}
```

Multiple topic subscriptions can be active on a single WebSocket connection. Each is managed independently and can be cancelled without affecting others.

### Example: Monitor a Topic with curl and websocat

```bash
# Install websocat: https://github.com/vi/websocat
websocat -H "Authorization: Bearer your-secret-key" \
  "ws://localhost:31880/api/v1/ws/events"

# After connecting, send:
{"action":"subscribe_monitor","topic":"ag.hello.greeter.agent-a"}
```

## See Also

- [Quickstart](quickstart.md) — start the gateway and connect your first agents
- [Horizontal Scaling](horizontal-scaling.md) — deploy multiple gateway instances
- [Monitoring](monitoring.md) — Prometheus metrics, OTLP export, and the ops server's health/metrics endpoints
- [Error Codes](error-codes.md) — protocol-level error reference
- [Specification](specification.md) — full system specification
