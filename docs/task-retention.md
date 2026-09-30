# Task retention and durable application results

By default cleanup purges completed tasks after seven days, failed tasks after
fourteen days, and cancelled tasks after seven days. These windows and the purge
interval are operator-configurable under `cleanup`.

Applications whose published results, duplicate-submission protection, or retry
controls depend on task history can set this metadata at creation:

```json
{"aether.retain_terminal": "true"}
```

The value is the literal string `"true"` (matching the task protocol metadata
map). Both PostgreSQL and AetherLite SQLite exempt these tasks from automatic
terminal-task purging. Existing terminal records can opt in through an authorized
metadata update before they expire. This does not recover an already-purged row.
Removing the key or changing its value restores normal age-based purging.

Retention preserves the original status, scope, subject, input and checkpoint
fields. It does not mark a task completed, renew task authority, allow a completed
task to run again, or change task access checks. A stored output or draft alone
is not evidence that its task completed successfully.

Use this option for low-volume, application-owned durable records. Rows and their
associated task history remain on disk until retention is released or an operator
removes them; this is not a compact receipt or an automatic storage limit. Store
large documents and scratch files outside task payloads. Applications should plan
retention with their artifact lifecycle and preserve any required completion proof
before releasing task retention. Ordinary transient tasks keep the default policy.
