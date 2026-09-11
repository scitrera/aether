package login

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/net/idna"
)

// SessionInfo deliberately excludes credentials and provider claims. ID is a
// management handle, never a cookie value. A session is valid, not necessarily
// online. ExpiresAt is zero for callers that explicitly create timeless sessions.
type SessionInfo struct {
	ID        string    `json:"id"`
	Provider  string    `json:"provider"`
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// SessionPage is a bounded live view, rather than a snapshot across requests.
type SessionPage struct {
	Sessions []SessionInfo `json:"sessions"`
	HasMore  bool          `json:"has_more"`
}

// SessionManager is an optional capability; stateless JWT stores do not support
// it. Subject is the email when present, otherwise UserID. Email-shaped subjects
// are normalized; other UserIDs remain case sensitive.
// RevokeAll invalidates sessions created before its atomic generation change;
// a later login remains possible. It also invalidates legacy, unindexed sessions.
type SessionManager interface {
	ListSessions(context.Context, string, int, int) (*SessionPage, error)
	RevokeSession(context.Context, string, string) error
	RevokeAllSessions(context.Context, string) error
}

type storedSession struct {
	SessionData
	SubjectKey string `json:"_subject_key"`
	Generation string `json:"_generation,omitempty"`
}

func digest(value string) string {
	h := sha256.Sum256([]byte(value))
	return hex.EncodeToString(h[:])
}

func sessionSubject(data *SessionData) string {
	if data.Email != "" {
		return normalizeSubject(data.Email)
	}
	return data.UserID
}

func normalizeSubject(subject string) string {
	if at := strings.LastIndex(subject, "@"); at >= 0 {
		subject = strings.ToLower(strings.TrimSpace(subject))
		at = strings.LastIndex(subject, "@")
		domain, err := idna.Lookup.ToASCII(strings.TrimSuffix(subject[at+1:], "."))
		if err == nil {
			subject = subject[:at+1] + domain
		}
	}
	return subject
}

func (s *RedisOpaqueSessionStore) subjectKey(subject string) string {
	return s.prefix + "v2:user:" + digest(normalizeSubject(subject))
}

func (s *RedisOpaqueSessionStore) sessionKey(id string) string {
	return s.prefix + "v2:session:" + id
}

// Creation and indexing share a generation with revoke-all atomically. Indexes
// expire with their longest-lived session; old session records retain their TTL
// after bulk revocation. Generation counters intentionally do not expire.
var createSession = redis.NewScript(`
local generation = redis.call('GET', KEYS[2]) or '0'
if ARGV[4] == 'legacy' then
  if generation ~= '0' or redis.call('GET', KEYS[3]) ~= ARGV[5] then return 0 end
end
-- Preserve the original JSON: Lua cjson would turn empty claim arrays into
-- objects if it decoded and re-encoded the complete session record.
local record = '{"_generation":' .. cjson.encode(generation) .. ',' .. string.sub(ARGV[1], 2)
local ttl = tonumber(ARGV[2])
if ttl > 0 then
  redis.call('SET', KEYS[1], record, 'PX', ttl)
else
  redis.call('SET', KEYS[1], record)
end
local index = ARGV[7] .. ':index:' .. generation
local previous = redis.call('PTTL', index)
redis.call('ZADD', index, ARGV[3], ARGV[6])
if ttl == 0 or previous == -1 then redis.call('PERSIST', index)
elseif previous < ttl then redis.call('PEXPIRE', index, ttl) end
if ARGV[4] == 'legacy' then redis.call('DEL', KEYS[3]) end
return 1
`)

func (s *RedisOpaqueSessionStore) persist(ctx context.Context, id string, data *SessionData, legacy string) (bool, error) {
	if data == nil {
		return false, errors.New("nil session")
	}
	var ttl int64
	score := "+inf"
	if !data.ExpiresAt.IsZero() {
		ttl = time.Until(data.ExpiresAt).Milliseconds()
		if ttl <= 0 {
			return false, errors.New("session ExpiresAt is in the past")
		}
		score = strconv.FormatInt(data.ExpiresAt.UnixMilli(), 10)
	}
	subject := s.subjectKey(sessionSubject(data))
	payload, err := json.Marshal(storedSession{SessionData: *data, SubjectKey: subject})
	if err != nil {
		return false, fmt.Errorf("marshal session: %w", err)
	}
	mode := "new"
	if legacy != "" {
		mode = "legacy"
	}
	result, err := createSession.Run(ctx, s.client, []string{s.sessionKey(digest(id)), subject + ":generation", s.prefix + id}, payload, ttl, score, mode, legacy, digest(id), subject).Int()
	return result == 1, err
}

// New implements SessionStore. The returned 256-bit random value is a bearer
// credential. Redis stores only its digest, which is safe for management APIs.
func (s *RedisOpaqueSessionStore) New(ctx context.Context, data *SessionData) (string, error) {
	id, err := newOpaqueID(s.idLen)
	if err != nil {
		return "", err
	}
	if _, err = s.persist(ctx, id, data, ""); err != nil {
		return "", err
	}
	return id, nil
}

func (s *RedisOpaqueSessionStore) managed(ctx context.Context, id string) (*storedSession, error) {
	payload, err := s.client.Get(ctx, s.sessionKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var record storedSession
	if err = json.Unmarshal(payload, &record); err != nil {
		return nil, err
	}
	if record.IsExpired() {
		return nil, nil
	}
	generation, err := s.generation(ctx, record.SubjectKey)
	if err != nil {
		return nil, err
	}
	if record.Generation != generation {
		return nil, nil
	}
	return &record, nil
}

func (s *RedisOpaqueSessionStore) generation(ctx context.Context, subjectKey string) (string, error) {
	value, err := s.client.Get(ctx, subjectKey+":generation").Result()
	if errors.Is(err, redis.Nil) {
		return "0", nil
	}
	return value, err
}

func validManagementID(id string) bool {
	if len(id) != 64 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil && id == strings.ToLower(id)
}

// Get checks the current generation on every request. Pre-management Redis
// sessions are migrated atomically on use, unless revoke-all invalidated them.
// Deploy all replicas together: older Aether versions cannot enforce generations.
func (s *RedisOpaqueSessionStore) Get(ctx context.Context, id string) (*SessionData, error) {
	if !validManagementID(id) {
		return nil, nil
	}
	record, err := s.managed(ctx, digest(id))
	if err != nil {
		return nil, err
	}
	if record != nil {
		return &record.SessionData, nil
	}
	payload, err := s.client.Get(ctx, s.prefix+id).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var data SessionData
	if err = json.Unmarshal([]byte(payload), &data); err != nil {
		return nil, err
	}
	if data.IsExpired() {
		return nil, nil
	}
	ok, err := s.persist(ctx, id, &data, payload)
	if err != nil {
		return nil, err
	}
	if !ok {
		// Another request may have completed the same migration.
		record, err = s.managed(ctx, digest(id))
		if err != nil || record == nil {
			return nil, err
		}
		return &record.SessionData, nil
	}
	return &data, nil
}

// Delete removes the session presented by a bearer cookie, including legacy keys.
func (s *RedisOpaqueSessionStore) Delete(ctx context.Context, id string) error {
	if !validManagementID(id) {
		return nil
	}
	return s.remove(ctx, digest(id), s.prefix+id)
}

var deleteSession = redis.NewScript(`
local raw = redis.call('GET', KEYS[1])
if raw then
  local record = cjson.decode(raw)
  redis.call('ZREM', record['_subject_key'] .. ':index:' .. record['_generation'], ARGV[1])
end
redis.call('DEL', KEYS[1])
if #KEYS > 1 then redis.call('DEL', KEYS[2]) end
return 1
`)

func (s *RedisOpaqueSessionStore) remove(ctx context.Context, id string, legacyKey string) error {
	keys := []string{s.sessionKey(id)}
	if legacyKey != "" {
		keys = append(keys, legacyKey)
	}
	return deleteSession.Run(ctx, s.client, keys, id).Err()
}

// ListSessions returns unexpired sessions ordered by expiry, then ID.
func (s *RedisOpaqueSessionStore) ListSessions(ctx context.Context, subject string, limit, offset int) (*SessionPage, error) {
	if limit < 1 || limit > 200 || offset < 0 || offset > 1000000 {
		return nil, errors.New("invalid session pagination")
	}
	key := s.subjectKey(subject)
	generation, err := s.generation(ctx, key)
	if err != nil {
		return nil, err
	}
	index := key + ":index:" + generation
	if err = s.client.ZRemRangeByScore(ctx, index, "-inf", strconv.FormatInt(time.Now().UnixMilli(), 10)).Err(); err != nil {
		return nil, err
	}
	ids, err := s.client.ZRange(ctx, index, int64(offset), int64(offset+limit)).Result()
	if err != nil {
		return nil, err
	}
	page := &SessionPage{Sessions: []SessionInfo{}, HasMore: len(ids) > limit}
	if page.HasMore {
		ids = ids[:limit]
	}
	for _, id := range ids {
		record, err := s.managed(ctx, id)
		if err != nil {
			return nil, err
		}
		if record == nil || record.SubjectKey != key {
			continue
		}
		page.Sessions = append(page.Sessions, SessionInfo{ID: id, Provider: record.Provider, IssuedAt: record.IssuedAt, ExpiresAt: record.ExpiresAt})
	}
	return page, nil
}

// RevokeSession is idempotent. A handle from another subject never revokes that
// subject's session, and management handles cannot be used as bearer cookies.
func (s *RedisOpaqueSessionStore) RevokeSession(ctx context.Context, subject, id string) error {
	if !validManagementID(id) {
		return errors.New("invalid session management ID")
	}
	record, err := s.managed(ctx, id)
	if err != nil || record == nil {
		return err
	}
	if record.SubjectKey != s.subjectKey(subject) {
		return nil
	}
	return s.remove(ctx, id, "")
}

var revokeAllSessions = redis.NewScript(`
local generation = redis.call('GET', KEYS[1]) or '0'
redis.call('INCR', KEYS[1])
redis.call('DEL', ARGV[1] .. ':index:' .. generation)
return 1
`)

// RevokeAllSessions invalidates the subject's current generation atomically.
func (s *RedisOpaqueSessionStore) RevokeAllSessions(ctx context.Context, subject string) error {
	key := s.subjectKey(subject)
	return revokeAllSessions.Run(ctx, s.client, []string{key + ":generation"}, key).Err()
}

var _ SessionManager = (*RedisOpaqueSessionStore)(nil)
