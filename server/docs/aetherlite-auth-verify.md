# Private token verification with AetherLite

Set `AETHERLITE_AUTH_VERIFY_LISTEN` (or `--auth-verify-listen`) to a private HTTP
listener address to expose `/auth/verify` and `/healthz`. The listener is disabled
by default and requires API-key authentication. It uses AetherLite's existing
token and ACL stores, including policy updates; it does not create another auth
database.

Bind a host installation to loopback, for example `127.0.0.1:8090`. Inside a
container, a listener such as `:8090` belongs on the private service network;
publish only the application's intended public ingress. The listener has no
login, session-management or metrics endpoints.

Callers may supply an opaque API key in `X-API-Key`. An explicit `Authorization`
header takes precedence. Existing verification identity, delegation and resource
access checks continue to apply.
