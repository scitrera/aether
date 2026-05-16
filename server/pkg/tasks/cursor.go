// Phase 4: cursor pagination helpers for ListTasksPage.
//
// Cursor format: base64url("<unix_micros>|<task_id>"). The decimal-encoded
// unix_micros plus the task ID together form the stable ordering key, and
// pagination order is (updated_at DESC, task_id DESC). Encoding is opaque
// to clients; the format must round-trip through encode/decode but is not
// part of any external contract.
package tasks

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// EncodePageToken builds an opaque cursor for ListTasksPage. The token
// encodes the (updated_at, task_id) tuple of the last row returned on the
// current page; passing it back resumes strictly after that row under the
// ORDER BY (updated_at DESC, task_id DESC) ordering.
func EncodePageToken(unixMicros int64, taskID string) string {
	raw := strconv.FormatInt(unixMicros, 10) + "|" + taskID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodePageToken reverses EncodePageToken. Returns an error on any decode
// failure so callers can surface "invalid page_token" to the client without
// exposing the cursor format.
func DecodePageToken(token string) (int64, string, error) {
	if token == "" {
		return 0, "", errors.New("empty page_token")
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, "", fmt.Errorf("base64 decode: %w", err)
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return 0, "", errors.New("malformed page_token")
	}
	micros, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("parse unix_micros: %w", err)
	}
	if parts[1] == "" {
		return 0, "", errors.New("malformed page_token: empty task_id")
	}
	return micros, parts[1], nil
}
