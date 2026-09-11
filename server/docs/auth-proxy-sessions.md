# Browser sessions

`SessionStore` handles cookie issuance, lookup and logout. The Redis/Valkey store
also implements the optional `SessionManager` interface for per-user listing,
single revocation and bulk revocation. JWT stores intentionally do not implement
that interface. Applications must expose management only behind their own
administrative authentication, authorization, CSRF protection and audit trail.

`ListSessions(ctx, subject, limit, offset)` returns only management ID, provider,
creation and expiry. The subject is normalized email when available, otherwise
UserID. Email normalization trims/lowercases and converts IDNA domains. Management
IDs are SHA-256 digests of 256-bit random cookies, never bearer credentials.
The store does not track presence or last activity. Pagination (limit 1–200,
offset 0–1000000) is a live view ordered by expiry then management ID.

`RevokeSession` is scoped to the subject and idempotent. `RevokeAllSessions`
atomically advances the subject's generation; later logins remain possible.
Every lookup verifies that generation. Session creation and indexing use a Lua
script to share an atomic boundary with bulk revocation. Redis failure never
returns an authenticated session or a successful empty inventory.

## Upgrade and rollback

**This storage change applies to every Redis/Valkey browser-session deployment,
even when no application exposes `SessionManager`.** JWT mode is unaffected.
Older replicas cannot read the new session records or enforce their revocation
generations. Mixing versions can cause intermittent authentication failures and
allow legacy sessions to authenticate after bulk revocation.

For an upgrade that preserves valid browser sessions:

1. Drain and stop all replicas running the older session store, including
   applications that embed this package. Do this before the new version serves
   requests; do not use a rolling deployment that serves both versions together.
2. Deploy the new version to every replica, using the same Redis/Valkey primary,
   logical DB and `AUTH_PROXY_SESSION_REDIS_PREFIX` (or library constructor
   prefix) as before. Restore traffic only after all serving replicas are upgraded.
3. Verify login, logout, and any application's session-management flow. Legacy
   sessions under `<prefix><cookie>` migrate atomically when used and then appear
   in the index. The inventory does not include unused legacy sessions, but bulk
   revocation invalidates those sessions too.

For rollback, restoring a stale backup, or recovery from acknowledged-write loss:

1. Drain and stop all serving replicas.
2. Choose a fresh, never-used session prefix and configure it on every replica
   before restoring traffic. For the standalone auth-proxy, set
   `AUTH_PROXY_SESSION_REDIS_PREFIX`, for example `auth-session:recovery-20260911:`.
   Library users must pass the new prefix to `NewRedisOpaqueSessionStore`.
3. Require all users to sign in again. Keep the old session records out of the new
   prefix, and do not later reuse the old prefix or reset generation counters
   while its session records remain. Restoring old records or losing revocation
   state can otherwise make revoked sessions valid again.

Previously accepted requests and downstream/IdP sessions are outside this store's
revocation boundary.

## Storage lifetime

Finite session records expire at their session lifetime. Creation and legacy
migration prune expired index entries, so retention does not depend on anyone
calling `ListSessions`. Logout and individual revocation set the index lifetime
to the latest remaining finite expiry; an empty index is removed. An index stays
nonexpiring while it contains a nonexpiring session.

A user generation counter persists after bulk revocation. Bulk-revoked records
expire naturally; explicitly nonexpiring records (zero `ExpiresAt` in library
calls) remain stored. Keep generation state for as long as records under that
prefix can remain. Use persistence, `noeviction` and sufficient capacity. All
replicas must use the same primary, DB and prefix. The implementation targets a
single Redis/Valkey endpoint, not Redis Cluster.

## Testing

Tests use miniredis by default. Set `AUTH_TEST_REDIS_ADDR` to exercise the same
suite against real Redis/Valkey; each test owns a unique key prefix. Run
`go test -race ./pkg/authproxy/login ./pkg/authproxy ./internal/auth` from `server`.
