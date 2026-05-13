# Environment Variables Reference

This document is the canonical reference for every environment variable
recognised by an Aether OSS deployment. It covers the gateway, AetherLite,
the auth-proxy, the workflow server, the messaging bridge, and the proxy
sidecar.

Aether follows two naming conventions on purpose:

- Aether-specific variables use an `AETHER_*` prefix (or service-scoped
  prefixes such as `AETHERLITE_*`, `MSGBRIDGE_*`, `WORKFLOW_*`,
  `AUTH_PROXY_*`, `PROXY_SIDECAR_*`). These are reserved by Aether and will
  not collide with PaaS-injected variables.
- Backing-service and ecosystem variables use the conventional unprefixed
  names (`PORT`, `POSTGRES_*`, `REDIS_*`, `STREAM_URL`, `AMQP_URL`,
  `DATABASE_URL`, `OTEL_*`). These are kept unprefixed for portability
  across PaaS targets (Heroku, Railway, Cloud Run, Fly.io) and to honour
  the standard semantics tooling already expects.

Precedence (highest first) for every binary is: explicit CLI flag >
environment variable > YAML config file > compiled-in default. AetherLite
adds one more layer (CLI flag > `AETHERLITE_*` env > compiled-in default
for the flag value), as documented in that section.

For setup walkthroughs, see `docs/quickstart.md`,
`docs/aetherlite.md`, and the per-binary guides under `server/docs/`.

## Naming Conventions

- `AETHER_*` — variables read by the gateway / shared aether server
  packages (auth, ACL, TLS, config validation, tracing knobs).
- `AETHERLITE_*` — flag defaults for the AetherLite single-binary mode.
  Every CLI flag has a matching `AETHERLITE_*` env var.
- `MSGBRIDGE_*`, `WORKFLOW_*`, `AUTH_PROXY_*`, `PROXY_SIDECAR_*` —
  service-scoped overrides for the corresponding `cmd/*` binary.
- `PORT`, `POSTGRES_*`, `REDIS_*`, `STREAM_URL`, `AMQP_URL`,
  `DATABASE_URL`, `OTEL_*` — cloud / ecosystem conventions, intentionally
  unprefixed so the same image runs cleanly on a PaaS.
- `DISCORD_*`, `TEAMS_*`, `SMTP_*` — credentials for third-party
  messaging-platform integrations consumed by the messaging bridge.

## Quick Reference

| Section | Binary | Description |
|---|---|---|
| [Gateway — Identity & Process](#gateway-identity--process) | `gateway` | Process identity, ports, dev-mode flags |
| [Gateway — Admin API](#gateway-admin-api) | `gateway` | Admin REST API + TLS |
| [Gateway — Authentication & TLS](#gateway-authentication--tls) | `gateway` | mTLS, token HMAC, auth modes |
| [Gateway — ACL](#gateway-acl) | `gateway` | ACL enforcement |
| [Gateway — Audit](#gateway-audit) | `gateway` | Audit logging knobs |
| [Gateway — Storage](#gateway-storage-postgresql-redis-rabbitmq) | `gateway` | Postgres, Redis, RabbitMQ |
| [Gateway — Tracing & Logging](#gateway-tracing--logging) | `gateway` / `all` | OpenTelemetry, log level/format |
| [Gateway — Proxy data plane](#gateway-proxy-data-plane) | `gateway` | Proxy bypass kill switch |
| [AetherLite](#aetherlite-cmdaetherlite) | `aetherlite` | Embedded single-binary flag overrides |
| [Migrate](#migrate-cmdmigrate) | `migrate` | Standalone DB migration tool |
| [Auth Proxy](#auth-proxy-cmdauth-proxy) | `auth-proxy` | Reverse-proxy auth gateway |
| [Auth Proxy — Login flow](#auth-proxy-browser-login-flow) | `auth-proxy` | Browser OIDC login + session cookies |
| [Workflow](#workflow-cmdworkflow) | `workflow` | Workflow engine server |
| [Msgbridge](#msgbridge-cmdmsgbridge) | `msgbridge` | Discord / Teams / Email bridge |
| [Proxy Sidecar](#proxy-sidecar-cmdproxy-sidecar) | `proxy-sidecar` | Terminator / initiator / relay sidecar |
| [Bridge Integrations](#bridge-integrations-discord--teams--smtp) | `msgbridge` | External platform credentials |
| [Reserved / Deprecated](#reserved--deprecated) | — | Renamed variable migration table |

## Gateway (`cmd/gateway`)

The gateway is configured primarily via YAML
(`server/configs/dev.yaml` is the example). Every variable below applies
to the `gateway` binary at config-load time via
`Config.ApplyEnvOverrides()` in `server/internal/config/gateway.go`.
AetherLite reads the same gateway env vars because it embeds the same
config object; AetherLite-specific overrides are documented in their own
section.

### Gateway — Identity & process

| Variable | Type | Default | Description |
|---|---|---|---|
| `AETHER_GATEWAY_ID` | string | `gateway-default` (dev) / required in prod | Stable identifier for this gateway instance. Used in audit records, task claims, and admin state. Renamed from `GATEWAY_ID`. |
| `PORT` | int | `50051` | gRPC listen port. Unprefixed by design for PaaS compatibility. |
| `AETHER_DEV_MODE` | bool | `false` | Gates several "fail-closed" validations: allows missing token HMAC key, allows `verify_signature:false`, allows plaintext gRPC with credential auth. **NOT FOR PRODUCTION.** |
| `AETHER_ALLOW_DEV_MODE` | bool | `false` | Required opt-in for `--insecure-admin` and for `admin.cors_origin="*"` to be accepted by the config validator. **NOT FOR PRODUCTION.** |
| `AETHER_AUTO_TLS` | bool | `false` | When `true`, the gateway auto-generates a self-signed CA + server cert into `secrets-dir/tls/` if no TLS files are configured. Dev convenience; **NOT FOR PRODUCTION** unless the generated CA is trust-rooted externally. |

### Gateway — Admin API

| Variable | Type | Default | Description |
|---|---|---|---|
| `AETHER_ADMIN_PORT` | int | `31880` | Admin REST API + UI listen port. Renamed from `ADMIN_PORT`. |
| `AETHER_ADMIN_ENABLED` | bool | `true` (dev) | Toggles the admin server. Renamed from `ADMIN_ENABLED`. |
| `AETHER_ADMIN_API_KEY` | string | (none) | Shared-secret API key for the admin REST surface. Must be at least 16 characters when set. Renamed from `ADMIN_API_KEY`. |
| `AETHER_ADMIN_TLS_CERT_FILE` | path | (none) | TLS certificate path for HTTPS on the admin server. Renamed from `ADMIN_TLS_CERT_FILE`. |
| `AETHER_ADMIN_TLS_KEY_FILE` | path | (none) | TLS private key path for HTTPS on the admin server. Both cert+key must be set together. Renamed from `ADMIN_TLS_KEY_FILE`. |

### Gateway — Authentication & TLS

| Variable | Type | Default | Description |
|---|---|---|---|
| `AETHER_AUTH_MODES` | comma-list | (from YAML; e.g. `mtls,api_key,task_token,oauth`) | Comma-separated list of enabled auth modes. Renamed from `AUTH_MODES`. |
| `AETHER_TOKEN_HMAC_KEY` | string (>= 32 bytes) | (none, required outside dev) | HMAC-SHA256 key used to hash stored API tokens. Required when `api_key` auth is enabled in production. |
| `AETHER_TLS_CERT_FILE` | path | (none) | gRPC server TLS certificate. |
| `AETHER_TLS_KEY_FILE` | path | (none) | gRPC server TLS private key. |
| `AETHER_TLS_CA_FILE` | path | (none) | CA bundle used to verify client certificates when client auth is enabled. |
| `AETHER_TLS_CLIENT_AUTH` | enum (`require`/`request`/`none`) | (from YAML; defaults to `request` when auto-TLS engaged) | mTLS client-auth policy. |

### Gateway — ACL

| Variable | Type | Default | Description |
|---|---|---|---|
| `AETHER_ACL_REQUIRED` | bool | `false` | Strict mode. When `true`, ACL service is required and Postgres must be configured. Renamed from `ACL_REQUIRED`. |

### Gateway — Audit

Audit knobs are read in two places: by the unified config loader (which
wins) and by `audit.LoadConfigFromEnv()` for standalone callers. Defaults
come from `internal/audit/types.go`.

| Variable | Type | Default | Description |
|---|---|---|---|
| `AETHER_AUDIT_ENABLED` | bool | `false` (dev YAML enables it) | Master switch for audit logging. Renamed from `AUDIT_ENABLED`. |
| `AETHER_AUDIT_EVENT_TYPES` | comma-list or `all` | (all event types) | Filters which event types are captured. Valid types: `connection`, `auth`, `message`, `kv`, `task`, `admin`, `acl`. Renamed from `AUDIT_EVENT_TYPES`. |
| `AETHER_AUDIT_VERBOSITY_LEVEL` | enum (`low`/`medium`/`high`) | `low` | Controls payload detail for message events. Renamed from `AUDIT_VERBOSITY_LEVEL`. |
| `AETHER_AUDIT_BATCH_SIZE` | int > 0 | `100` | Number of events buffered before a flush. Renamed from `AUDIT_BATCH_SIZE`. |
| `AETHER_AUDIT_FLUSH_PERIOD` | duration | `5s` | Maximum interval between flushes when the batch is not full. Renamed from `AUDIT_FLUSH_PERIOD`. |
| `AETHER_AUDIT_RETENTION_DAYS` | int > 0 | `90` | Retention window for batched writes. Renamed from `AUDIT_RETENTION_DAYS`. |
| `AETHER_AUDIT_CHANNEL_BUFFER` | int > 0 | `1000` | Size of the async event channel buffer. Renamed from `AUDIT_CHANNEL_BUFFER`. |

### Gateway — Storage (PostgreSQL, Redis, RabbitMQ)

These variables follow the cloud convention of being unprefixed so the
gateway image can drop straight into PaaS targets that inject
`POSTGRES_*`, `REDIS_*`, `STREAM_URL`, and `AMQP_URL` for you.

| Variable | Type | Default | Description |
|---|---|---|---|
| `POSTGRES_HOST` | string | `localhost` (dev) | PostgreSQL host. Leaving unset disables Postgres-backed features (task store, ACL, audit, orchestration). |
| `POSTGRES_PORT` | int | `5432` (dev) | PostgreSQL port. |
| `POSTGRES_USER` | string | `aether` (dev fallback, with warning) | PostgreSQL user. |
| `POSTGRES_PASSWORD` | string | `aether_dev` (dev fallback, with warning) | PostgreSQL password. |
| `POSTGRES_DATABASE` | string | `aether` (dev) | PostgreSQL database name. |
| `REDIS_ADDR` | string `host:port` | `localhost:56379` (dev) | Redis address. Setting this overrides `redis.cluster` to a single-node entry. |
| `REDIS_PASSWORD` | string | (none) | Redis password. |
| `STREAM_URL` | URL (`rabbitmq-stream://`) | `rabbitmq-stream://guest:guest@localhost:55552` (dev) | RabbitMQ Streams endpoint. |
| `AMQP_URL` | URL (`amqp://` or `amqps://`) | `amqp://guest:guest@localhost:55672/` (dev) | RabbitMQ AMQP endpoint (used for delayed-exchange orchestration). |
| `AETHER_FANIN_SHARDS` | int ≥ 1 | `1` | Number of fan-in shards for `event::receiver{N}` and `metric::receiver{N}` topics. Today only shard 0 is used (stub always returns 0). Future releases will use fnv64 hashing to distribute workspaces across N shards, enabling N parallel Workflow Engine and Metrics Bridge instances. |

### Gateway — Tracing & Logging

| Variable | Type | Default | Description |
|---|---|---|---|
| `AETHER_LOG_LEVEL` | enum (`debug`/`info`/`warn`/`error`) | `info` | Structured logger level. Applies to gateway, AetherLite, msgbridge, workflow, and proxy-sidecar. Renamed from `LOG_LEVEL`. |
| `AETHER_LOG_FORMAT` | enum (`json`/`console`) | TTY auto-detect | Forces structured (`json`) or human (`console`) log output, overriding the stderr TTY detection. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | URL | (unset) | Standard OpenTelemetry endpoint. When set, the gateway exports OTLP traces and the zerolog→OTLP log bridge is activated. Unprefixed per OpenTelemetry convention. |

### Gateway — Proxy data plane

| Variable | Type | Default | Description |
|---|---|---|---|
| `AETHER_PROXY_LOCAL_BYPASS_DISABLED` | bool (`1`/`true`) | unset → bypass enabled | Emergency kill switch for the proxy local-bypass optimisation. Forces every proxy hop through the RabbitMQ data plane. |

## AetherLite (`cmd/aetherlite`)

AetherLite is the single-binary mode (gateway + workflow + msgbridge in
one process with embedded SQLite + Badger). It honours every gateway
variable above (it embeds the same config object) and adds a parallel
set of `AETHERLITE_*` variables that set the **default value of the
matching CLI flag**. Precedence at the call site is therefore:

> explicit CLI flag > `AETHERLITE_*` env var > compiled-in default

If you want to override a setting that the gateway already exposes via
`AETHER_*` / `POSTGRES_*` / etc., set that variable directly — those win
during `ApplyEnvOverrides()`.

| Variable | Type | Default | Maps to flag |
|---|---|---|---|
| `AETHERLITE_CONFIG` | path | (empty) | `--config` |
| `AETHERLITE_DATA_DIR` | path | `./aether-lite-data` | `--data-dir` |
| `AETHERLITE_PORT` | int | `50051` | `--port` |
| `AETHERLITE_ADMIN_PORT` | int | `31880` | `--admin-port` |
| `AETHERLITE_DEV` | bool | `false` | `--dev` |
| `AETHERLITE_INSECURE_ADMIN` | bool | `false` | `--insecure-admin` (requires `AETHER_ALLOW_DEV_MODE=true`). **NOT FOR PRODUCTION.** |
| `AETHERLITE_WORKFLOW` | bool | `true` | `--workflow` (toggles embedded workflow server) |
| `AETHERLITE_WORKFLOW_CONFIG` | path | (empty) | `--workflow-config` |
| `AETHERLITE_WORKFLOW_ADMIN_PORT` | int | `31881` | `--workflow-admin-port` |
| `AETHERLITE_MSGBRIDGE` | bool | `false` | `--msgbridge` (toggles embedded messaging bridge) |
| `AETHERLITE_MSGBRIDGE_CONFIG` | path | (empty) | `--msgbridge-config` |
| `AETHERLITE_MSGBRIDGE_ADMIN_PORT` | int | `31882` | `--msgbridge-admin-port` |

## Migrate (`cmd/migrate`)

The standalone migration runner. Use it to apply embedded SQL migrations
against an external PostgreSQL when you do not want gateway startup to
run migrations implicitly.

| Variable | Type | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | postgres DSN | `postgres://postgres:postgres@localhost:5432/aether?sslmode=disable` | PostgreSQL connection string. The unprefixed name matches the convention used by Heroku, Railway, and most ORMs. Overridden by the `--db` flag. |

## Auth Proxy (`cmd/auth-proxy`)

The standalone auth-proxy reads `AUTH_PROXY_*` variables in
`pkg/authproxy/config.go`. `AETHER_DEV_MODE` is also honoured to permit
disabling JWT signature verification during development.

| Variable | Type | Default | Description |
|---|---|---|---|
| `AUTH_PROXY_MODE` | enum (`proxy`/`verify`) | `proxy` | `proxy` runs reverse-proxy mode (inject headers and forward). `verify` runs auth-verify mode for nginx `auth_request` / Envoy `ext_authz`. |
| `AUTH_PROXY_LISTEN_ADDR` | host:port | `:8080` | HTTP listen address. |
| `AUTH_PROXY_BACKEND_URL` | URL | `http://localhost:61001` | Upstream backend URL in proxy mode. |
| `AUTH_PROXY_TENANT_ID` | string | `default` | Tenant identifier injected into identity headers. |
| `AUTH_PROXY_DB_URL` | postgres DSN | **required** | Database URL for the token / ACL store. Startup fails if empty. |
| `AUTH_PROXY_REDIS_ADDR` | host:port | (none) | Optional Redis used by the token cache and (by default) the session store. |
| `AUTH_PROXY_LOG_LEVEL` | enum (`debug`/`info`/`warn`/`error`) | `info` | Structured logger level. |
| `AUTH_PROXY_CORS_ORIGIN` | string | (none) | CORS `Access-Control-Allow-Origin` value. |
| `AUTH_PROXY_TOKEN_HMAC_KEY` | string | (none) | Optional HMAC-SHA256 key for token-hash verification (matches gateway). |
| `AUTH_PROXY_SECRETS_FILE` | path | (none) | Path to a JSON secrets file (alternative to env-only configuration). |
| `AUTH_PROXY_TLS_CERT_FILE` | path | (none) | TLS certificate for HTTPS listener. |
| `AUTH_PROXY_TLS_KEY_FILE` | path | (none) | TLS private key for HTTPS listener. |
| `AUTH_PROXY_OAUTH_ISSUER` | URL | (none) | OAuth issuer URL for bearer-token validation. |
| `AUTH_PROXY_OAUTH_JWKS_URL` | URL | (none) | JWKS endpoint for bearer-token validation. |
| `AUTH_PROXY_OAUTH_AUDIENCE` | string | (none) | Expected `aud` claim on bearer tokens. |
| `AUTH_PROXY_OAUTH_VERIFY_SIGNATURE` | bool | `true` | Setting `false` requires `AETHER_DEV_MODE=true` or startup will fail. |
| `AUTH_PROXY_ENTRA_TENANT_ID` | string | (none) | Microsoft Entra (Azure AD) tenant ID. |
| `AUTH_PROXY_ENTRA_CLIENT_ID` | string | (none) | Microsoft Entra client ID. |
| `AUTH_PROXY_ENTRA_ALLOWED_TENANTS` | comma-list | (none) | Whitelist of acceptable Entra `tid` values. |
| `AUTH_PROXY_ENTRA_VERIFY_SIGNATURE` | bool | `true` | Setting `false` requires `AETHER_DEV_MODE=true` or startup will fail. |
| `AUTH_PROXY_ALLOWED_EMAIL_DOMAINS` | comma-list | (none) | Single-tenant email-domain allow-list applied during login. |

### Auth Proxy — Browser login flow

The login flow is **enabled iff** `AUTH_PROXY_LOGIN_PROVIDERS` is non-empty.
Each provider name maps to a set of per-provider variables (the provider
name is upper-cased and `-` is replaced with `_` when forming the key).
Example: `AUTH_PROXY_LOGIN_PROVIDERS=azure,google` reads variables under
`AUTH_PROXY_LOGIN_AZURE_*` and `AUTH_PROXY_LOGIN_GOOGLE_*`.

| Variable | Type | Default | Description |
|---|---|---|---|
| `AUTH_PROXY_LOGIN_PROVIDERS` | comma-list | (empty → login disabled) | List of OIDC provider names to mount. |
| `AUTH_PROXY_LOGIN_<NAME>_ISSUER` | URL | (none) | OIDC issuer URL for provider `<NAME>`. |
| `AUTH_PROXY_LOGIN_<NAME>_CLIENT_ID` | string | (none) | OAuth client ID. |
| `AUTH_PROXY_LOGIN_<NAME>_CLIENT_SECRET` | string | (none) | OAuth client secret. |
| `AUTH_PROXY_LOGIN_<NAME>_REDIRECT_URL` | URL | (none) | OAuth redirect URL handled by `/auth/callback/<name>`. |
| `AUTH_PROXY_LOGIN_<NAME>_SCOPES` | comma-list | (provider default) | Extra OAuth scopes. |
| `AUTH_PROXY_LOGIN_<NAME>_ALLOWED_TENANTS` | comma-list | (none) | Per-provider tenant allow-list. |
| `AUTH_PROXY_SESSION_COOKIE_NAME` | string | `aether_session` | Session cookie name. |
| `AUTH_PROXY_SESSION_COOKIE_DOMAIN` | string | (none) | Session cookie Domain attribute. |
| `AUTH_PROXY_SESSION_COOKIE_SECURE` | bool | `true` | Session cookie Secure attribute. |
| `AUTH_PROXY_SESSION_COOKIE_SAMESITE` | enum (`lax`/`strict`/`none`) | `lax` | Session cookie SameSite attribute. |
| `AUTH_PROXY_SESSION_TTL` | duration | `24h` | Session lifetime. |
| `AUTH_PROXY_SESSION_STORE` | enum (`redis`/`jwt`) | `redis` | Backing store for session state. |
| `AUTH_PROXY_SESSION_JWT_SIGNING_KEY` | string (>= 32 bytes) | (none) | HS256 signing key (required when `SESSION_STORE=jwt`). |
| `AUTH_PROXY_SESSION_REDIS_ADDR` | host:port | falls back to `AUTH_PROXY_REDIS_ADDR` | Redis address for session storage. |
| `AUTH_PROXY_SESSION_REDIS_PASSWORD` | string | (none) | Redis password for session storage. |
| `AUTH_PROXY_SESSION_REDIS_DB` | int | `0` | Redis logical DB index. |
| `AUTH_PROXY_SESSION_REDIS_PREFIX` | string | `auth-session:` | Redis key prefix. |

## Workflow (`cmd/workflow`)

The workflow engine binary reuses the cloud-convention `POSTGRES_*`,
`REDIS_*` and `AETHER_LOG_LEVEL` (via `LOG_LEVEL` today; see Reserved
section) variables and adds workflow-specific knobs.

| Variable | Type | Default | Description |
|---|---|---|---|
| `WORKFLOW_MODE` | enum (`standard`/`lite`) | `standard` | `standard` uses Postgres + Redis; `lite` uses SQLite only. |
| `SQLITE_PATH` | path | `workflow.db` | SQLite database path when running in lite mode. |
| `AETHER_ADDRESS` | host:port | `localhost:50051` (dev) | Gateway address the workflow server connects to. |
| `AETHER_WORKSPACE` | string | `_system` (dev) | Workspace the workflow engine attaches itself to. |
| `AETHER_API_KEY` | string | (none) | API key credential for the gateway connection. |
| `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DATABASE` | — | dev defaults match gateway | See gateway storage section. |
| `REDIS_ADDR` / `REDIS_PASSWORD` | — | `localhost:6379` (dev) | See gateway storage section. |
| `WORKFLOW_ADMIN_ENABLED` | bool | `true` | Toggles the workflow admin REST surface. |
| `WORKFLOW_ADMIN_PORT` | int | `31881` | Workflow admin port. |
| `WORKFLOW_ADMIN_API_KEY` | string | (none) | API key for the workflow admin surface. |

## Msgbridge (`cmd/msgbridge`)

The messaging bridge connects Discord, Microsoft Teams, and SMTP-based
email to Aether. It accepts the standard `AETHER_*` + `POSTGRES_*`
connection variables plus its own admin and platform variables.

| Variable | Type | Default | Description |
|---|---|---|---|
| `AETHER_ADDRESS` | host:port | `localhost:50051` (dev) | Gateway address. |
| `AETHER_IMPLEMENTATION` | string | `aether-msgbridge` (dev) | Bridge implementation identifier. |
| `AETHER_SPECIFIER` | string | `instance-1` (dev) | Bridge specifier (unique per instance). |
| `AETHER_API_KEY` | string | (none) | API key for the gateway connection. |
| `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DATABASE` | — | dev defaults match gateway | See gateway storage section. |
| `MSGBRIDGE_ADMIN_ENABLED` | bool | `true` | Toggles the msgbridge admin REST surface. |
| `MSGBRIDGE_ADMIN_PORT` | int | `31882` | Msgbridge admin port. |
| `MSGBRIDGE_ADMIN_API_KEY` | string | (none) | API key for the msgbridge admin surface. |

## Proxy Sidecar (`cmd/proxy-sidecar`)

The proxy sidecar runs one or more surfaces (terminator, initiator,
relay) that share a single gateway connection.

| Variable | Type | Default | Description |
|---|---|---|---|
| `AETHER_ADDRESS` | host:port | (from YAML) | Gateway address. |
| `AETHER_API_KEY` | string | (none) | Long-lived service API key. Mutually exclusive with `AETHER_TASK_TOKEN`. |
| `AETHER_TASK_TOKEN` | string | (none) | Per-task token issued via `CreateTask`. Mutually exclusive with `AETHER_API_KEY`. |
| `AETHER_TENANT_ID` | string | (none) | Tenant identifier propagated in identity headers. |
| `PROXY_SIDECAR_LISTEN` | host:port | (from YAML) | Initiator HTTP listener bind address. |
| `PROXY_SIDECAR_TARGET` | string (topic) | (from YAML) | Initiator target service topic. |
| `PROXY_SIDECAR_RELAY_LISTEN` | `unix://path` or `host:port` | (from YAML) | Relay listen address (UDS preferred for sandbox isolation). |

## Bridge Integrations (Discord / Teams / SMTP)

Credentials for external platforms consumed by the messaging bridge.
These names are intentionally unprefixed to match each vendor's standard
ergonomics.

### Discord

| Variable | Type | Default | Description |
|---|---|---|---|
| `DISCORD_BOT_TOKEN` | string (secret) | (none) | Discord bot token used to log into the Gateway API. |
| `DISCORD_APPLICATION_ID` | string | (none) | Discord application ID for slash-command registration. |

### Microsoft Teams

| Variable | Type | Default | Description |
|---|---|---|---|
| `TEAMS_APP_ID` | string | (none) | Bot Framework / Teams app ID. |
| `TEAMS_APP_PASSWORD` | string (secret) | (none) | Bot Framework app password (client secret). |
| `TEAMS_TENANT_ID` | string | (none) | Azure AD tenant the Teams app is registered in. |
| `TEAMS_WEBHOOK_PORT` | int | `8081` | Inbound webhook listen port. |

### Email (SMTP outbound)

| Variable | Type | Default | Description |
|---|---|---|---|
| `SMTP_HOST` | string | (none) | SMTP server hostname. |
| `SMTP_PORT` | int | (none) | SMTP server port (typically `587` STARTTLS or `465` TLS). |
| `SMTP_USERNAME` | string | (none) | SMTP auth username. |
| `SMTP_PASSWORD` | string (secret) | (none) | SMTP auth password. |
| `SMTP_FROM_ADDRESS` | email | (none) | Default `From:` header on outbound mail. |

## Reserved / Deprecated

The variables below have been renamed to live under the `AETHER_*`
namespace. The old names are **no longer read** by the current code
path. Update your deployment manifests, Helm values, and `.env` files.

| Old name | New name | Notes |
|---|---|---|
| `GATEWAY_ID` | `AETHER_GATEWAY_ID` | Identity & process section. |
| `ADMIN_PORT` | `AETHER_ADMIN_PORT` | Admin API section. |
| `ADMIN_ENABLED` | `AETHER_ADMIN_ENABLED` | Admin API section. |
| `ADMIN_API_KEY` | `AETHER_ADMIN_API_KEY` | Admin API section. |
| `ADMIN_TLS_CERT_FILE` | `AETHER_ADMIN_TLS_CERT_FILE` | Admin API section. |
| `ADMIN_TLS_KEY_FILE` | `AETHER_ADMIN_TLS_KEY_FILE` | Admin API section. |
| `ACL_REQUIRED` | `AETHER_ACL_REQUIRED` | ACL section. |
| `AUTH_MODES` | `AETHER_AUTH_MODES` | Authentication section. |
| `LOG_LEVEL` | `AETHER_LOG_LEVEL` | Applies to gateway, msgbridge, workflow, proxy-sidecar. |
| `AUDIT_ENABLED` | `AETHER_AUDIT_ENABLED` | Audit section. |
| `AUDIT_BATCH_SIZE` | `AETHER_AUDIT_BATCH_SIZE` | Audit section. |
| `AUDIT_CHANNEL_BUFFER` | `AETHER_AUDIT_CHANNEL_BUFFER` | Audit section. |
| `AUDIT_EVENT_TYPES` | `AETHER_AUDIT_EVENT_TYPES` | Audit section. |
| `AUDIT_FLUSH_PERIOD` | `AETHER_AUDIT_FLUSH_PERIOD` | Audit section. |
| `AUDIT_RETENTION_DAYS` | `AETHER_AUDIT_RETENTION_DAYS` | Audit section. |
| `AUDIT_VERBOSITY_LEVEL` | `AETHER_AUDIT_VERBOSITY_LEVEL` | Audit section. |
