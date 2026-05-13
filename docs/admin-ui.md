# Admin UI and API Reference

The Aether Gateway ships with an embedded administrative web interface and a REST API. Both are served by a separate HTTP server that runs alongside the main gRPC gateway. This document covers access, authentication, available features, all REST endpoints, and the WebSocket monitoring interface.

## Accessing the Admin UI

By default the admin server listens on port **31880**. Open a browser to:

```
http://localhost:31880
```

The UI is a single-page application embedded in the gateway binary. It loads from the embedded filesystem automatically; no separate deployment step is required.

The admin API is served under the `/api` path prefix. All API endpoints return JSON.

### Default Port

The port is set in the YAML configuration file:

```yaml
admin:
  enabled: true
  port: 31880
```

It can be overridden at startup with the `--admin-port` flag:

```bash
./gateway --config configs/dev.yaml --admin-port 9090
```

## Authentication

The admin API requires authentication in all production deployments. Two mechanisms are supported.

### API Key (recommended)

Set an API key in the configuration:

```yaml
admin:
  api_key: "your-secret-key"
```

All requests to `/api/**` (except `/api/health`) must include:

```
Authorization: Bearer your-secret-key
```

If `api_key` is set but TLS is not enabled, the gateway logs a warning that the key will be transmitted in plaintext. Enable TLS for production deployments.

### Insecure Mode (development only)

If no API key is configured, the gateway will refuse to start unless the insecure flag is explicitly set:

```bash
./gateway --dev --insecure-admin
```

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

The WebSocket endpoint at `/api/ws/events` is under the `/api` subrouter and inherits the API key middleware. Clients that cannot set the `Authorization` header in the WebSocket upgrade request (browser `WebSocket` API) may pass the token via the `Sec-WebSocket-Protocol` subprotocol:

```javascript
const ws = new WebSocket("ws://localhost:31880/api/ws/events", ["auth", "Bearer your-secret-key"]);
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

## Health Probes

The following endpoints are unauthenticated and not subject to rate limiting. They are intended for Kubernetes liveness/readiness/startup probes.

| Endpoint | Method | Description |
|---|---|---|
| `/health/live` | GET | Always returns `200 OK` while the process is running. Use as liveness probe. |
| `/health/ready` | GET | Returns `200` when Redis and RabbitMQ are reachable; `503` otherwise. Use as readiness probe. |
| `/health/startup` | GET | Returns `200` once routes are registered and the server is accepting connections; `503` before that. Use as startup probe. |

## Prometheus Metrics

The Prometheus scrape endpoint is unauthenticated by convention:

```
GET /metrics
```

In production, isolate the admin port so that only the Prometheus scraper can reach it. Do not expose it through the public load balancer.

## REST API Reference

All endpoints below are prefixed with `/api` and require the `Authorization: Bearer <key>` header unless otherwise noted.

### Health and Info

| Method | Path | Description |
|---|---|---|
| GET | `/api/health` | Gateway health status including dependency checks. Exempt from API key auth. |
| GET | `/api/info` | Gateway identity, version, and build information. |
| GET | `/api/stats` | Active connection counts and message routing statistics. |

### Connections

| Method | Path | Description |
|---|---|---|
| GET | `/api/connections` | List all active connections. Filter by `?type=agent` or `?workspace=my-workspace`. |
| GET | `/api/connections/{session_id}` | Get details for a specific session. |
| DELETE | `/api/connections/{session_id}` | Force-disconnect a session. Releases the distributed lock and terminates the gRPC stream. |

### Tasks

| Method | Path | Description |
|---|---|---|
| GET | `/api/tasks` | List tasks. Filter by `?status=pending`, `?workspace=ws`, `?type=unique`. |
| GET | `/api/tasks/{task_id}` | Get a specific task record. |
| POST | `/api/tasks/{task_id}/retry` | Schedule a failed task for retry. |
| POST | `/api/tasks/{task_id}/cancel` | Cancel a pending or running task. |

### Workspaces

| Method | Path | Body fields | Description |
|---|---|---|---|
| GET | `/api/workspaces` | — | List all workspaces. |
| POST | `/api/workspaces` | `workspace_id`, `display_name`, `description`, `tenant_id`, `metadata` | Create a workspace. |
| GET | `/api/workspaces/{workspace_id}` | — | Get workspace details. |
| PUT | `/api/workspaces/{workspace_id}` | `display_name`, `description`, `tenant_id`, `metadata` | Update workspace metadata. |
| DELETE | `/api/workspaces/{workspace_id}` | — | Delete a workspace. |
| GET | `/api/workspaces/{workspace_id}/message-flow` | — | Get recent message flow graph for the workspace. |

### Agents and Orchestration

| Method | Path | Body fields | Description |
|---|---|---|---|
| GET | `/api/agents` | — | List registered agent implementations. |
| POST | `/api/agents` | `implementation`, `description`, `launch_params` | Register an agent implementation. |
| GET | `/api/agents/{implementation}` | — | Get agent registration details. |
| PUT | `/api/agents/{implementation}` | `description`, `launch_params` | Update agent registration. |
| DELETE | `/api/agents/{implementation}` | — | Remove an agent registration. |
| POST | `/api/agents/{implementation}/launch` | `specifier`, `workspace` | Manually trigger an agent launch via the registered Orchestrator. Defaults: `specifier="default"`, `workspace="default"`. |
| GET | `/api/orchestrators` | — | List registered Orchestrator profiles. |

### KV Store

The KV store is organized into scopes. Common scopes are `global` and workspace names.

| Method | Path | Body fields | Description |
|---|---|---|---|
| GET | `/api/kv` | — | List keys. Filter by `?scope=global&prefix=my/`. Defaults to `scope=global`. |
| GET | `/api/kv/{scope}/{key}` | — | Get a KV entry. The key supports slashes (e.g., `/api/kv/global/demo/setting`). |
| PUT | `/api/kv/{scope}/{key}` | `value`, `ttl` (seconds, optional) | Set a KV entry. A `ttl` of `0` means no expiration. |
| DELETE | `/api/kv/{scope}/{key}` | — | Delete a KV entry. |

### ACL Management

Access control rules govern which principals can perform which operations on which resources.

**Rules**

| Method | Path | Description |
|---|---|---|
| GET | `/api/acl/rules` | List ACL rules. Filter by `?principal_type=agent&principal_id=...&resource_type=...&resource_id=...`. |
| POST | `/api/acl/rules` | Grant access. Body: `principal_type`, `principal_id`, `resource_type`, `resource_id`, `granted_by`. All fields required. |
| GET | `/api/acl/rules/{rule_id}` | Get a specific rule. Query params: `principal_type`, `principal_id`, `resource_type`, `resource_id`. |
| DELETE | `/api/acl/rules/{rule_id}` | Revoke access. Query params: `principal_type`, `principal_id`, `resource_type`, `resource_id`. |

**Audit Log**

| Method | Path | Description |
|---|---|---|
| GET | `/api/acl/audit` | Query the ACL decision audit log. Filter by `principal_type`, `principal_id`, `resource_type`, `resource_id`, `decision`, `workspace`, `limit`. |

**Fallback Policy**

| Method | Path | Description |
|---|---|---|
| GET | `/api/acl/fallback-policy` | Get the fallback policy for a rule category. Query param: `rule_category`. |
| PUT | `/api/acl/fallback-policy` | Set the fallback policy. Body: `rule_category`, `updated_by`. |

**Maintenance**

| Method | Path | Description |
|---|---|---|
| POST | `/api/acl/cleanup/expired-rules` | Delete all expired ACL rules. Returns `count` of deleted rules. |
| POST | `/api/acl/cleanup/audit-logs` | Delete ACL audit log entries older than the retention window. Query param: `?retention_days=90` (default 90). Returns `count` of deleted entries. |

### API Token Management

API tokens are used by clients to authenticate gRPC connections via the API key auth method. Token secrets are returned only at creation time and are not stored in plaintext.

| Method | Path | Body fields | Description |
|---|---|---|---|
| GET | `/api/tokens` | — | List all tokens (metadata only, no secret values). |
| POST | `/api/tokens` | `name`, `principal_type`, `workspace_patterns`, `scopes`, `expires_in_hours`, `created_by` | Create a token. Returns the plaintext token value once. Defaults: `workspace_patterns=["*"]`, `scopes=["connect"]`, `created_by="admin"`. |
| GET | `/api/tokens/{token_id}` | — | Get token metadata. |
| POST | `/api/tokens/{token_id}/revoke` | — | Revoke a token without deleting its record. |
| DELETE | `/api/tokens/{token_id}` | — | Permanently delete a token record. |

### Messaging

| Method | Path | Body fields | Description |
|---|---|---|---|
| POST | `/api/messages/send` | `target_topic`, `payload`, `message_type`, `source_topic` | Inject a message directly into the routing layer. `message_type` must be one of `CHAT`, `CONTROL`, `TOOL_CALL`, `EVENT`, `METRIC`, or empty (defaults to `CHAT`). |

## WebSocket Monitoring

The WebSocket endpoint provides a real-time stream of gateway events and per-topic message monitoring.

**Endpoint:** `ws://localhost:31880/api/ws/events`

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
  "ws://localhost:31880/api/ws/events"

# After connecting, send:
{"action":"subscribe_monitor","topic":"ag.hello.greeter.agent-a"}
```

## See Also

- [Quickstart](quickstart.md) — start the gateway and connect your first agents
- [Horizontal Scaling](horizontal-scaling.md) — deploy multiple gateway instances
- [Error Codes](error-codes.md) — protocol-level error reference
- [Specification](specification.md) — full system specification
