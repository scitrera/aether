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

Legacy sessions under `<prefix><cookie>` migrate atomically when used and then
appear in the index. Bulk revocation also invalidates unindexed legacy sessions.
Upgrade all replicas together: old implementations do not check generations.
Use a fresh prefix when rolling back, restoring stale backups, or recovering
from acknowledged-write loss; retained session records must not outlive a reset
generation. Previously accepted requests and downstream/IdP sessions are outside
this store's revocation boundary.

Records/indexes use the session expiry; a user generation counter persists after
bulk revocation. Bulk-revoked records expire naturally. Nonexpiring sessions,
explicitly requested by library callers with a zero ExpiresAt, remain stored.
Use persistence, `noeviction` and sufficient capacity. All replicas must use the
same primary, DB and prefix. The implementation targets a single Redis/Valkey
endpoint, not Redis Cluster.

Tests use miniredis by default. Set `AUTH_TEST_REDIS_ADDR` to exercise the same
suite against real Redis/Valkey; each test owns a unique key prefix. Run
`go test -race ./pkg/authproxy/login ./pkg/authproxy ./internal/auth` from `server`.
