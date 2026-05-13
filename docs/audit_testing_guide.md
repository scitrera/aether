# Comprehensive Audit Logging - Testing Guide

This guide explains how to manually test the comprehensive audit logging feature.

## Overview

The comprehensive audit logging system records all security-relevant events including:
- Connection lifecycle (connect, disconnect)
- Authentication (mTLS, token validation)
- Identity resolution
- Lock acquisition/rejection
- Session registration
- Message routing
- KV operations
- Admin actions

## Prerequisites

1. **Infrastructure Running:**
   - PostgreSQL (localhost:5432)
   - Redis (localhost:6379)
   - RabbitMQ Streams (localhost:5552)

2. **Database Migration:**
   ```bash
   # Ensure migration 005 has been applied
   migrate -path migrations -database "postgres://aether:aether_dev@localhost:5432/aether?sslmode=disable" up
   ```

3. **Tools:**
   - `psql` - PostgreSQL client
   - `python3` - Python 3.7+ with Aether client

## Automated Test

Run the automated integration test:

```bash
./scripts/test_connection_audit.sh
```

This script will:
1. Check prerequisites
2. Clear old test audit logs
3. Create and run a test client
4. Verify audit events were logged
5. Display results

## Manual Testing

### 1. Start the Gateway

```bash
# Set audit logging environment variables
export AETHER_AUDIT_ENABLED=true
export AETHER_AUDIT_VERBOSITY_LEVEL=high
export AETHER_AUDIT_EVENT_TYPES=all
export AETHER_AUDIT_BATCH_SIZE=10
export AETHER_AUDIT_FLUSH_PERIOD=2s

# Start gateway
go run ./cmd/gateway
```

You should see:
```
Audit logger configuration:
  Enabled: true
  Event types: [connection auth message kv admin acl]
  Verbosity: high
  Batch size: 10
  Flush period: 2s
  Retention: 90 days
  Channel buffer: 1000
Audit logger initialized (migration: 005_comprehensive_audit_schema.sql)
```

### 2. Connect a Test Client

In another terminal:

```bash
cd python-client
python3 example.py
```

Or create a simple test client:

```python
#!/usr/bin/env python3
import sys
import time
sys.path.insert(0, './python-client')

from scitrera_aether_client import AgentClient

client = AgentClient(
    workspace="test-workspace",
    implementation="test-agent",
    specifier="v1"
)

print("Connecting to gateway...")
client.connect("localhost:50051")
print("Connected!")

time.sleep(2)

print("Disconnecting...")
client.disconnect()
print("Done!")
```

### 3. Query Audit Logs

Connect to PostgreSQL:

```bash
PGPASSWORD=aether_dev psql -h localhost -p 5432 -U aether -d aether
```

#### View All Connection Events

```sql
SELECT
    audit_id,
    timestamp,
    event_type,
    operation,
    actor_type,
    actor_id,
    success,
    error_message,
    metadata::text
FROM comprehensive_audit_log
WHERE event_type IN ('connection', 'auth')
ORDER BY timestamp DESC
LIMIT 20;
```

#### Expected Events for a Connection

For each client connection, you should see:

1. **mTLS Authentication** (if using client certs)
   ```sql
   event_type: auth
   operation: auth_mtls_success
   success: true
   ```

2. **Identity Resolution**
   ```sql
   event_type: auth
   operation: identity_resolved
   success: true
   ```

3. **Lock Acquisition**
   ```sql
   event_type: connection
   operation: lock_acquired
   success: true
   metadata: {"workspace": "test-workspace", "resumed": false}
   ```

4. **Session Registration**
   ```sql
   event_type: connection
   operation: session_registered
   success: true
   metadata: {"workspace": "test-workspace"}
   ```

5. **Connection Close** (when disconnected)
   ```sql
   event_type: connection
   operation: connection_closed
   success: true
   metadata: {"reason": "graceful_close", "workspace": "test-workspace"}
   ```

#### Check Event Counts by Type

```sql
SELECT
    event_type,
    operation,
    COUNT(*) as count,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as failed
FROM comprehensive_audit_log
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY event_type, operation
ORDER BY event_type, operation;
```

#### View Failed Operations

```sql
SELECT
    timestamp,
    event_type,
    operation,
    actor_type,
    actor_id,
    error_message,
    metadata::text
FROM comprehensive_audit_log
WHERE success = false
ORDER BY timestamp DESC
LIMIT 10;
```

#### View Metadata Details

```sql
SELECT
    operation,
    metadata
FROM comprehensive_audit_log
WHERE event_type = 'connection'
AND timestamp > NOW() - INTERVAL '10 minutes'
ORDER BY timestamp DESC;
```

### 4. Test Duplicate Connection (Lock Rejection)

Start a client:
```bash
python3 test_client.py
# Keep it running
```

In another terminal, try to connect with the same identity:
```bash
python3 test_client.py
# Should fail with "identity already connected"
```

Query for lock rejection:
```sql
SELECT
    timestamp,
    operation,
    success,
    error_message,
    metadata
FROM comprehensive_audit_log
WHERE operation = 'lock_acquired'
AND success = false
ORDER BY timestamp DESC
LIMIT 5;
```

### 5. Verify Metadata Fields

Check that metadata contains expected fields:

```sql
-- Lock acquisition metadata
SELECT metadata
FROM comprehensive_audit_log
WHERE operation = 'lock_acquired'
AND success = true
LIMIT 1;

-- Expected: {"workspace": "...", "resumed": false, "resume_session_id": ""}
```

```sql
-- Connection close metadata
SELECT metadata
FROM comprehensive_audit_log
WHERE operation = 'connection_closed'
LIMIT 1;

-- Expected: {"reason": "graceful_close", "workspace": "..."}
-- Possible reasons: "graceful_close", "error", "admin_disconnect"
```

## Testing Different Scenarios

### Scenario 1: Normal Connection Flow
1. Start gateway
2. Connect client
3. Verify: auth_mtls_success → lock_acquired → session_registered
4. Disconnect client
5. Verify: connection_closed with reason="graceful_close"

### Scenario 2: Duplicate Connection Attempt
1. Connect client A (should succeed)
2. Connect client B with same identity (should fail)
3. Verify: lock_acquired with success=false for client B

### Scenario 3: Crashed Connection
1. Connect client
2. Kill client process (SIGKILL)
3. Wait for connection timeout
4. Verify: connection_closed with reason="error"

### Scenario 4: Admin Disconnect
1. Connect client
2. Use admin API to disconnect session
3. Verify: connection_closed with reason="admin_disconnect"

## Testing KV Operations Audit Events

### ⚠️ Security: KV Value Protection

**IMPORTANT**: For security and compliance reasons, KV audit logs do NOT store actual values (old_value, new_value). This prevents sensitive data like passwords, API keys, and tokens from being exposed in audit logs.

**What is logged**:
- ✅ Key name
- ✅ Scope
- ✅ Workspace
- ✅ Value sizes (old_value_size, new_value_size)
- ✅ Whether value changed (value_changed)
- ✅ Whether it's an update vs new entry (is_update)
- ✅ TTL information
- ✅ Success/failure status

**What is NOT logged**:
- ❌ Actual old value (old_value)
- ❌ Actual new value (new_value)

This design ensures compliance with security standards (SOC2, HIPAA, PCI-DSS) that require protection of sensitive data, while still providing comprehensive audit trails for forensic analysis.

### Automated KV Test

Run the automated KV operations test:

```bash
./scripts/test_kv_audit.sh
```

This script will:
1. Check prerequisites
2. Clear old test audit logs
3. Create and run a test client that performs various KV operations
4. Verify all KV audit events were logged correctly
5. Validate metadata fields (before/after values, TTL, etc.)
6. Display results

### Manual KV Testing

#### 1. Start Gateway with High Verbosity

```bash
export AETHER_AUDIT_ENABLED=true
export AETHER_AUDIT_VERBOSITY_LEVEL=high
export AETHER_AUDIT_EVENT_TYPES=all

go run ./cmd/gateway
```

#### 2. Run KV Operations Test Client

Create a test client that performs various KV operations:

```python
#!/usr/bin/env python3
import sys
import time
sys.path.insert(0, './python-client')

from scitrera_aether_client import AgentClient

client = AgentClient(
    workspace="kv-test",
    implementation="kv-agent",
    specifier="v1"
)

print("Connecting...")
client.connect("localhost:50051")
time.sleep(1)

# Test PUT (create)
print("PUT test-key = value-1")
client.kv_put("test-key", "value-1", scope="address")
time.sleep(1)

# Test GET (success)
print("GET test-key")
value = client.kv_get("test-key", scope="address")
print(f"  Retrieved: {value}")
time.sleep(1)

# Test PUT (update)
print("PUT test-key = value-2 (update)")
client.kv_put("test-key", "value-2", scope="address")
time.sleep(1)

# Test PUT with TTL
print("PUT ttl-key = expires (TTL: 300s)")
client.kv_put("ttl-key", "expires", scope="address", ttl=300)
time.sleep(1)

# Test LIST
print("LIST keys")
keys = client.kv_list(scope="address")
print(f"  Keys: {keys}")
time.sleep(1)

# Test GET (failure - non-existent key)
print("GET non-existent-key (should fail)")
try:
    value = client.kv_get("non-existent-key", scope="address")
except Exception as e:
    print(f"  Failed (expected): {e}")
time.sleep(1)

# Test DELETE
print("DELETE test-key")
client.kv_delete("test-key", scope="address")
time.sleep(1)

# Test GET after DELETE (should fail)
print("GET test-key (should fail after delete)")
try:
    value = client.kv_get("test-key", scope="address")
except Exception as e:
    print(f"  Failed (expected): {e}")
time.sleep(1)

# Wait for audit flush
time.sleep(3)

print("Disconnecting...")
client.disconnect()
print("Done!")
```

#### 3. Query KV Audit Logs

Connect to PostgreSQL and query KV events:

```sql
-- View all KV operation events
SELECT
    audit_id,
    timestamp,
    operation,
    actor_id,
    resource_id,
    success,
    metadata->>'scope' as scope,
    metadata->>'key' as key,
    metadata->>'old_value' as old_value,
    metadata->>'new_value' as new_value,
    metadata->>'result_count' as result_count
FROM comprehensive_audit_log
WHERE event_type = 'kv'
ORDER BY timestamp DESC
LIMIT 20;
```

#### Expected KV Events

For the test client above, you should see:

1. **KV PUT (create)**
   ```sql
   operation: kv_put
   success: true
   metadata: {
     "scope": "address",
     "key": "test-key",
     "old_value": "",
     "new_value": "value-1",
     "new_value_size": 7,
     "is_update": false
   }
   ```

2. **KV GET (success)**
   ```sql
   operation: kv_get
   success: true
   metadata: {
     "scope": "address",
     "key": "test-key",
     "value_length": 7
   }
   ```

3. **KV PUT (update)**
   ```sql
   operation: kv_put
   success: true
   metadata: {
     "scope": "address",
     "key": "test-key",
     "old_value": "value-1",
     "old_value_size": 7,
     "new_value": "value-2",
     "new_value_size": 7,
     "is_update": true
   }
   ```

4. **KV PUT with TTL**
   ```sql
   operation: kv_put
   success: true
   metadata: {
     "scope": "address",
     "key": "ttl-key",
     "new_value": "expires",
     "ttl_seconds": 300,
     "is_update": false
   }
   ```

5. **KV LIST**
   ```sql
   operation: kv_list
   success: true
   metadata: {
     "scope": "address",
     "result_count": 2
   }
   ```

6. **KV GET (failure)**
   ```sql
   operation: kv_get
   success: false
   error_message: "key not found: non-existent-key"
   metadata: {
     "scope": "address",
     "key": "non-existent-key"
   }
   ```

7. **KV DELETE**
   ```sql
   operation: kv_delete
   success: true
   metadata: {
     "scope": "address",
     "key": "test-key",
     "old_value": "value-2",
     "old_value_size": 7
   }
   ```

#### Verify Metadata Fields

Check that KV operations include all expected metadata:

```sql
-- Check PUT operations include before/after values
SELECT
    operation,
    metadata->>'old_value' as old_value,
    metadata->>'new_value' as new_value,
    metadata->>'is_update' as is_update
FROM comprehensive_audit_log
WHERE operation = 'kv_put'
ORDER BY timestamp DESC;
```

```sql
-- Check DELETE operations include old_value
SELECT
    operation,
    metadata->>'key' as key,
    metadata->>'old_value' as old_value
FROM comprehensive_audit_log
WHERE operation = 'kv_delete'
ORDER BY timestamp DESC;
```

```sql
-- Check LIST operations include result_count
SELECT
    operation,
    metadata->>'scope' as scope,
    metadata->>'result_count' as result_count
FROM comprehensive_audit_log
WHERE operation = 'kv_list'
ORDER BY timestamp DESC;
```

```sql
-- Check PUT with TTL includes ttl_seconds
SELECT
    operation,
    metadata->>'key' as key,
    metadata->>'ttl_seconds' as ttl_seconds
FROM comprehensive_audit_log
WHERE operation = 'kv_put'
AND metadata->>'ttl_seconds' IS NOT NULL
ORDER BY timestamp DESC;
```

#### Verify Success/Failure Status

```sql
-- Count successful vs failed operations
SELECT
    operation,
    COUNT(*) as total,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as failed
FROM comprehensive_audit_log
WHERE event_type = 'kv'
GROUP BY operation
ORDER BY operation;
```

```sql
-- View failed operations with error messages
SELECT
    timestamp,
    operation,
    metadata->>'key' as key,
    error_message
FROM comprehensive_audit_log
WHERE event_type = 'kv'
AND success = false
ORDER BY timestamp DESC;
```

### KV Testing Scenarios

#### Scenario 1: PUT Create vs Update
1. PUT a new key
2. Verify: is_update=false, old_value=""
3. PUT the same key with new value
4. Verify: is_update=true, old_value contains previous value

#### Scenario 2: Different Scopes
Test KV operations with different scopes:
- workspace scope
- address scope
- global scope (if permissions allow)

```sql
-- View operations by scope
SELECT
    metadata->>'scope' as scope,
    operation,
    COUNT(*) as count
FROM comprehensive_audit_log
WHERE event_type = 'kv'
GROUP BY metadata->>'scope', operation
ORDER BY scope, operation;
```

#### Scenario 3: Failed Operations
1. GET non-existent key (should fail)
2. Verify: success=false, error_message present
3. DELETE non-existent key (should fail)
4. Verify: success=false, error_message present

```sql
-- View all failed KV operations
SELECT
    timestamp,
    operation,
    metadata->>'key' as key,
    error_message
FROM comprehensive_audit_log
WHERE event_type = 'kv'
AND success = false
ORDER BY timestamp DESC
LIMIT 10;
```

### KV Audit Success Criteria

✅ All KV operations logged (GET, PUT, DELETE, LIST)
✅ PUT operations include before/after values
✅ PUT updates set is_update=true and include old_value
✅ DELETE operations include old_value
✅ LIST operations include result_count
✅ PUT with TTL includes ttl_seconds
✅ Failed operations have success=false
✅ Failed operations include error_message
✅ All operations include scope metadata
✅ All operations include session_id for correlation

## Testing Message Routing Audit Events

### Automated Message Routing Test

Run the automated message routing audit test:

```bash
./scripts/test_message_routing_audit.sh
```

This script will:
1. Check prerequisites
2. Test message routing with three verbosity levels (low, medium, high)
3. Verify audit events for each verbosity level
4. Validate metadata fields based on verbosity configuration
5. Display results with pass/fail summary

**Note:** This test requires you to restart the gateway for each verbosity level.

### Manual Message Routing Testing

#### 1. Understanding Verbosity Levels

Message routing audit supports three verbosity levels:

- **Low**: Basic routing metadata only
  - `from`: Source topic
  - `to`: Target topic
  - `message_type`: Type of message (CHAT, CONTROL, TOOL_CALL, etc.)

- **Medium**: Low metadata + message size
  - All fields from Low
  - `message_size`: Size of message payload in bytes

- **High**: Medium metadata + message content
  - All fields from Medium
  - `message_content`: Message payload content (truncated to 1KB to prevent log bloat)

#### 2. Test Low Verbosity

Start gateway with low verbosity:

```bash
export AETHER_AUDIT_ENABLED=true
export AETHER_AUDIT_VERBOSITY_LEVEL=low
export AETHER_AUDIT_EVENT_TYPES=all

go run ./cmd/gateway
```

Create two test clients to send messages:

**Receiver Client** (`receiver.py`):
```python
#!/usr/bin/env python3
import sys
import time
sys.path.insert(0, './python-client')

from scitrera_aether_client import AgentClient

client = AgentClient(
    workspace="msg-test",
    implementation="receiver-agent",
    specifier="v1"
)

def message_handler(msg):
    print(f"Received: {len(msg.get('payload', b''))} bytes")

print("Connecting receiver...")
client.connect("localhost:50051")
client.start_receiving(message_handler)
print("Receiver ready!")

# Wait for messages
time.sleep(30)

client.disconnect()
print("Receiver disconnected")
```

**Sender Client** (`sender.py`):
```python
#!/usr/bin/env python3
import sys
import time
sys.path.insert(0, './python-client')

from scitrera_aether_client import AgentClient

client = AgentClient(
    workspace="msg-test",
    implementation="sender-agent",
    specifier="v1"
)

print("Connecting sender...")
client.connect("localhost:50051")
time.sleep(2)

# Send messages
target = "ag.msg-test.receiver-agent.v1"

print("Sending short message...")
client.send_message(target, b"Hello!", message_type="CHAT")
time.sleep(1)

print("Sending medium message...")
client.send_message(target, b"M" * 500, message_type="CHAT")
time.sleep(1)

print("Sending large message...")
client.send_message(target, b"L" * 2048, message_type="CHAT")
time.sleep(1)

print("Sending CONTROL message...")
client.send_message(target, b"STOP", message_type="CONTROL")
time.sleep(3)

client.disconnect()
print("Sender disconnected")
```

#### 3. Query Message Routing Audit Logs

Connect to PostgreSQL and query message events:

```sql
-- View all message routing events
SELECT
    audit_id,
    timestamp,
    operation,
    actor_id,
    resource_id,
    success,
    metadata->>'from' as from_topic,
    metadata->>'to' as to_topic,
    metadata->>'message_type' as msg_type,
    metadata->>'message_size' as msg_size,
    CASE
        WHEN metadata->>'message_content' IS NOT NULL
        THEN substring(metadata->>'message_content', 1, 50) || '...'
        ELSE NULL
    END as content_preview
FROM comprehensive_audit_log
WHERE event_type = 'message'
AND workspace = 'msg-test'
ORDER BY timestamp;
```

#### 4. Verify Expected Events

For each message sent, you should see TWO audit events:

1. **message_received** - Logged after ACL check
   - operation: `message_received`
   - actor_type: Source agent type
   - actor_id: Source agent identity
   - resource_id: Target topic
   - success: `true`

2. **message_routed** - Logged after successful publish
   - operation: `message_routed`
   - actor_type: Source agent type
   - actor_id: Source agent identity
   - resource_id: Target topic
   - success: `true`

If routing fails, you'll see:

3. **message_route_failed** - Logged on ACL denial or publish error
   - operation: `message_route_failed`
   - success: `false`
   - error_message: Reason for failure

#### 5. Test Medium Verbosity

Restart gateway with medium verbosity:

```bash
export AETHER_AUDIT_VERBOSITY_LEVEL=medium
go run ./cmd/gateway
```

Clear old audit logs:
```sql
DELETE FROM comprehensive_audit_log WHERE workspace = 'msg-test';
```

Run the same sender/receiver test, then query with message_size:

```sql
-- Verify message_size is logged for medium verbosity
SELECT
    timestamp,
    operation,
    metadata->>'message_size' as msg_size,
    metadata->>'message_content' as content
FROM comprehensive_audit_log
WHERE event_type = 'message'
AND workspace = 'msg-test'
AND operation = 'message_received'
ORDER BY timestamp;
```

Expected results:
- `message_size` should be populated with byte counts
- `message_content` should be NULL (not logged at medium verbosity)

#### 6. Test High Verbosity

Restart gateway with high verbosity:

```bash
export AETHER_AUDIT_VERBOSITY_LEVEL=high
go run ./cmd/gateway
```

Clear old audit logs and run the same test again.

Query with message_content:

```sql
-- Verify message_content is logged for high verbosity
SELECT
    timestamp,
    operation,
    metadata->>'message_size' as msg_size,
    LENGTH(metadata->>'message_content') as content_length,
    substring(metadata->>'message_content', 1, 100) as content_preview
FROM comprehensive_audit_log
WHERE event_type = 'message'
AND workspace = 'msg-test'
AND operation = 'message_received'
ORDER BY timestamp;
```

Expected results:
- `message_size` should be populated
- `message_content` should contain message payload
- For messages > 1KB, `content_length` should be ≤ 1024 (truncated)

#### 7. Test Failed Routing

To test failed routing audit events, try sending to an unauthorized topic:

```python
# In sender client, try to send to a topic you don't have permission for
# This will depend on your ACL configuration
client.send_message("ga.other-workspace", b"test", message_type="CHAT")
```

Then query for failed operations:

```sql
-- View failed message routing operations
SELECT
    timestamp,
    operation,
    actor_id,
    resource_id,
    error_message,
    metadata->>'from' as from_topic,
    metadata->>'to' as to_topic,
    metadata->>'denied_reason' as reason
FROM comprehensive_audit_log
WHERE event_type = 'message'
AND success = false
ORDER BY timestamp DESC;
```

### Message Routing Audit Success Criteria

✅ message_received events logged after ACL check
✅ message_routed events logged after successful publish
✅ message_route_failed events logged on ACL denial or publish error
✅ All events include from, to, message_type metadata
✅ Low verbosity: Only basic metadata (no size or content)
✅ Medium verbosity: Includes message_size
✅ High verbosity: Includes message_content (truncated to 1KB)
✅ Failed operations have success=false with error_message
✅ All operations include session_id for correlation
✅ task_id included in metadata for correlation with task system

### Verbosity Level Comparison

Query to compare metadata across verbosity levels:

```sql
-- Compare metadata fields across test runs
SELECT
    operation,
    CASE
        WHEN metadata->>'message_content' IS NOT NULL THEN 'high'
        WHEN metadata->>'message_size' IS NOT NULL THEN 'medium'
        ELSE 'low'
    END as detected_verbosity,
    COUNT(*) as event_count
FROM comprehensive_audit_log
WHERE event_type = 'message'
AND workspace = 'msg-test'
GROUP BY operation, detected_verbosity
ORDER BY operation, detected_verbosity;
```

## Performance Testing

Check that audit logging doesn't block operations:

```bash
# Run gateway with audit logging
time go run ./cmd/gateway

# Connect multiple clients simultaneously
for i in {1..10}; do
  python3 test_client.py &
done
wait

# Query audit logs
psql -c "SELECT COUNT(*) FROM comprehensive_audit_log WHERE timestamp > NOW() - INTERVAL '1 minute';"
```

Expected:
- All clients should connect within reasonable time
- All events should be logged (may take a few seconds for batch flush)

## Audit Configuration Testing

Test different configuration options:

### Test 1: Disable Audit Logging
```bash
export AETHER_AUDIT_ENABLED=false
go run ./cmd/gateway
# Connect client - no events should be logged
```

### Test 2: Selective Event Types
```bash
export AETHER_AUDIT_EVENT_TYPES=connection,auth
go run ./cmd/gateway
# Only connection and auth events should be logged
```

### Test 3: Different Flush Periods
```bash
export AETHER_AUDIT_FLUSH_PERIOD=1s
go run ./cmd/gateway
# Events should appear in database within 1-2 seconds
```

## Troubleshooting

### No audit events logged
- Check audit logger initialization: Look for "Audit logger initialized" in gateway logs
- Check database: Verify comprehensive_audit_log table exists
- Check configuration: Ensure AETHER_AUDIT_ENABLED=true
- Wait for flush: Events are batched, wait for AETHER_AUDIT_FLUSH_PERIOD

### Events missing metadata
- Check implementation: Ensure metadata is being set in audit event creation
- Query: `SELECT metadata FROM comprehensive_audit_log WHERE metadata IS NULL;`

### Performance issues
- Reduce batch size: `export AETHER_AUDIT_BATCH_SIZE=50`
- Increase flush period: `export AETHER_AUDIT_FLUSH_PERIOD=10s`
- Check async channel: Events should not block operations

## Using Analysis Views

The migration includes helpful views:

### Daily Summary
```sql
SELECT * FROM audit_daily_summary
WHERE event_date = CURRENT_DATE;
```

### Failed Operations
```sql
SELECT * FROM audit_failed_operations
WHERE timestamp > NOW() - INTERVAL '1 day';
```

### Actor Activity
```sql
SELECT * FROM audit_actor_activity
ORDER BY event_count DESC
LIMIT 20;
```

### Workspace Summary
```sql
SELECT * FROM audit_workspace_summary
ORDER BY event_count DESC;
```

## Cleanup

Clear test data:
```sql
DELETE FROM comprehensive_audit_log
WHERE workspace = 'test-workspace';
```

Test retention cleanup:
```sql
-- Insert old test record
INSERT INTO comprehensive_audit_log (timestamp, event_type, operation, actor_type, actor_id, workspace)
VALUES (NOW() - INTERVAL '100 days', 'connection', 'test', 'agent', 'test', 'test');

-- Run cleanup (manually)
SELECT cleanup_old_comprehensive_audit_logs(90);

-- Verify old record was deleted
SELECT COUNT(*) FROM comprehensive_audit_log
WHERE workspace = 'test' AND operation = 'test';
```

## Success Criteria

✅ All connection events logged correctly
✅ Metadata fields populated
✅ Session IDs tracked across events
✅ Success/failure status accurate
✅ Error messages captured for failures
✅ Timestamps reasonable
✅ No performance degradation
✅ Batch processing working
✅ Retention cleanup functional

---

## 4. Testing Admin Action Audit Events

### Automated Test

Run the automated admin action audit test:

```bash
./scripts/test_admin_audit.sh
```

This script will:
1. Check prerequisites (psql, curl, admin API connectivity)
2. Clear old admin audit logs
3. Perform various admin actions via the API
4. Verify audit events were logged with correct metadata
5. Display results and sample logs

### Manual Testing

#### Prerequisites

1. **Start Gateway with Admin UI:**
   ```bash
   export AETHER_AUDIT_ENABLED=true
   export AETHER_AUDIT_VERBOSITY_LEVEL=high
   go run ./cmd/gateway
   ```

2. **Verify Admin API is Running:**
   ```bash
   curl http://localhost:8080/api/health
   # Should return: {"status":"healthy"}
   ```

#### Test Scenarios

### Admin Operations Tested

The admin audit logging tracks three types of operations:

1. **State Queries** (`admin_state_query`): Read-only operations
   - List connections (`GET /api/connections`)
   - Get specific connection (`GET /api/connections/{session_id}`)

2. **Session Disconnect** (`admin_session_disconnect`): Administrative control
   - Disconnect session (`DELETE /api/connections/{session_id}`)

3. **Config Changes** (`admin_config_change`): State-modifying operations
   - KV operations: Set value (`PUT /api/kv/{scope}/{key}`), Delete key (`DELETE /api/kv/{scope}/{key}`)
   - Agent operations: Create, Update, Delete, Launch agent
   - Task operations: Retry task, Cancel task
   - Send message via admin API

#### Test 1: State Query Operations

Query the connections list via admin API:

```bash
# List all connections
curl http://localhost:8080/api/connections

# Get specific connection (may fail if session doesn't exist, but should still be audited)
curl http://localhost:8080/api/connections/some-session-id
```

Wait 2-5 seconds for batch flush, then verify audit logs:

```sql
-- View state query operations
SELECT
    audit_id,
    TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as time,
    operation,
    actor_type,
    actor_id,
    success,
    metadata->>'query_type' as query_type,
    metadata->>'remote_addr' as remote_addr
FROM comprehensive_audit_log
WHERE event_type = 'admin'
AND operation = 'admin_state_query'
ORDER BY timestamp DESC
LIMIT 5;
```

Expected results:
- `operation`: `admin_state_query`
- `actor_type`: `admin_ui`
- `actor_id`: `unknown` (prepared for future authentication)
- `success`: `true` (queries should succeed even if result is empty)
- `metadata.query_type`: `list_connections` or `get_connection`
- `metadata.remote_addr`: Client IP address

#### Test 2: Session Disconnect Operations

Disconnect a session via admin API:

```bash
# First, list connections to get a valid session ID
SESSION_ID=$(curl -s http://localhost:8080/api/connections | jq -r '.[0].session_id')

# Disconnect the session
curl -X DELETE http://localhost:8080/api/connections/$SESSION_ID
```

Wait 2-5 seconds for batch flush, then verify:

```sql
-- View session disconnect operations
SELECT
    audit_id,
    TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as time,
    operation,
    actor_type,
    success,
    metadata->>'session_id' as session_id,
    metadata->>'remote_addr' as remote_addr,
    error_message
FROM comprehensive_audit_log
WHERE event_type = 'admin'
AND operation = 'admin_session_disconnect'
ORDER BY timestamp DESC
LIMIT 5;
```

Expected results:
- `operation`: `admin_session_disconnect`
- `actor_type`: `admin_ui`
- `success`: `true` on successful disconnect, `false` if session not found
- `metadata.session_id`: The session ID that was disconnected
- `error_message`: Populated if disconnect failed

#### Test 3: KV Config Change Operations

Set and delete KV values via admin API:

```bash
# Set a KV value
curl -X PUT http://localhost:8080/api/kv/workspace:test-ws/admin-test-key \
  -H "Content-Type: application/json" \
  -d '{"value": "test-value", "ttl": 3600}'

# Delete the KV value
curl -X DELETE http://localhost:8080/api/kv/workspace:test-ws/admin-test-key
```

Wait 2-5 seconds for batch flush, then verify:

```sql
-- View KV config change operations
SELECT
    audit_id,
    TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as time,
    operation,
    success,
    metadata->>'action' as action,
    metadata->>'scope' as scope,
    metadata->>'key' as key,
    metadata->>'ttl_seconds' as ttl,
    metadata->>'remote_addr' as remote_addr
FROM comprehensive_audit_log
WHERE event_type = 'admin'
AND operation = 'admin_config_change'
AND metadata->>'action' IN ('kv_set', 'kv_delete')
ORDER BY timestamp DESC
LIMIT 10;
```

Expected results for KV Set:
- `operation`: `admin_config_change`
- `metadata.action`: `kv_set`
- `metadata.scope`: The KV scope (e.g., `workspace:test-ws`)
- `metadata.key`: The KV key
- `metadata.ttl_seconds`: TTL if specified

Expected results for KV Delete:
- `operation`: `admin_config_change`
- `metadata.action`: `kv_delete`
- `metadata.scope`: The KV scope
- `metadata.key`: The KV key

#### Test 4: Agent Config Change Operations

Create, update, and delete agents via admin API:

```bash
# Create agent
curl -X POST http://localhost:8080/api/agents \
  -H "Content-Type: application/json" \
  -d '{
    "implementation": "test-agent-audit",
    "display_name": "Test Agent",
    "description": "Test agent for audit logging",
    "orchestrator_id": "local"
  }'

# Update agent
curl -X PUT http://localhost:8080/api/agents/test-agent-audit \
  -H "Content-Type: application/json" \
  -d '{
    "display_name": "Updated Test Agent",
    "description": "Updated description"
  }'

# Delete agent
curl -X DELETE http://localhost:8080/api/agents/test-agent-audit
```

Wait 2-5 seconds for batch flush, then verify:

```sql
-- View agent config change operations
SELECT
    audit_id,
    TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as time,
    operation,
    success,
    metadata->>'action' as action,
    metadata->>'implementation' as implementation,
    metadata->>'remote_addr' as remote_addr,
    error_message
FROM comprehensive_audit_log
WHERE event_type = 'admin'
AND operation = 'admin_config_change'
AND metadata->>'action' LIKE 'agent_%'
ORDER BY timestamp DESC
LIMIT 10;
```

Expected results:
- `operation`: `admin_config_change`
- `metadata.action`: `agent_create`, `agent_update`, or `agent_delete`
- `metadata.implementation`: Agent implementation name
- `success`: `true` on success, `false` on failure
- `error_message`: Populated on failure

For agent launch operations:

```sql
-- View agent launch operations
SELECT
    audit_id,
    TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as time,
    operation,
    success,
    metadata->>'action' as action,
    metadata->>'implementation' as implementation,
    metadata->>'workspace' as workspace,
    metadata->>'specifier' as specifier
FROM comprehensive_audit_log
WHERE event_type = 'admin'
AND metadata->>'action' = 'agent_launch'
ORDER BY timestamp DESC;
```

#### Test 5: Task Operations

Test task retry and cancel operations:

```bash
# Retry a task (may fail if task doesn't exist)
curl -X POST http://localhost:8080/api/tasks/some-task-id/retry

# Cancel a task (may fail if task doesn't exist)
curl -X POST http://localhost:8080/api/tasks/some-task-id/cancel
```

Verify audit logs:

```sql
-- View task operations
SELECT
    audit_id,
    TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as time,
    operation,
    success,
    metadata->>'action' as action,
    metadata->>'task_id' as task_id,
    error_message
FROM comprehensive_audit_log
WHERE event_type = 'admin'
AND metadata->>'action' IN ('task_retry', 'task_cancel')
ORDER BY timestamp DESC;
```

#### Verify Actor Identity

All admin operations should include actor identity:

```sql
-- Verify all admin events have correct actor_type
SELECT
    actor_type,
    COUNT(*) as event_count
FROM comprehensive_audit_log
WHERE event_type = 'admin'
GROUP BY actor_type;
```

Expected: All events should have `actor_type = 'admin_ui'`

```sql
-- Verify all admin events include remote_addr
SELECT
    COUNT(*) as total_events,
    SUM(CASE WHEN metadata ? 'remote_addr' THEN 1 ELSE 0 END) as with_remote_addr
FROM comprehensive_audit_log
WHERE event_type = 'admin';
```

Expected: `total_events` should equal `with_remote_addr`

#### Verify Metadata Fields

Check that all required metadata fields are present:

```sql
-- Check metadata completeness for different action types
SELECT
    metadata->>'action' as action,
    COUNT(*) as total,
    SUM(CASE WHEN metadata ? 'remote_addr' THEN 1 ELSE 0 END) as has_remote_addr,
    SUM(CASE WHEN metadata ? 'query_type' THEN 1 ELSE 0 END) as has_query_type,
    SUM(CASE WHEN metadata ? 'scope' THEN 1 ELSE 0 END) as has_scope,
    SUM(CASE WHEN metadata ? 'implementation' THEN 1 ELSE 0 END) as has_implementation
FROM comprehensive_audit_log
WHERE event_type = 'admin'
GROUP BY metadata->>'action'
ORDER BY action;
```

### Admin Audit Success Criteria

✅ **State Query Operations:**
- List connections and get connection operations are audited
- `operation` = `admin_state_query`
- `metadata.query_type` indicates type of query
- `success` field reflects operation status

✅ **Session Disconnect Operations:**
- Session disconnect attempts are audited
- `operation` = `admin_session_disconnect`
- `metadata.session_id` included
- Both successful and failed disconnects logged

✅ **Config Change Operations:**
- All config-changing operations are audited
- `operation` = `admin_config_change`
- `metadata.action` indicates specific action (e.g., `kv_set`, `agent_create`)
- Context fields included (scope, key, implementation, etc.)

✅ **Actor Identity:**
- All admin events have `actor_type` = `admin_ui`
- `actor_id` = `unknown` (prepared for future authentication)
- `metadata.remote_addr` included for all operations

✅ **Success/Failure Tracking:**
- `success` field correctly reflects operation outcome
- `error_message` populated for failed operations

✅ **Metadata Completeness:**
- All operations include `remote_addr`
- Action-specific metadata included (query_type, scope, key, implementation, etc.)

### Quick SQL Queries for Admin Audit

```sql
-- Summary of admin operations
SELECT
    operation,
    metadata->>'action' as action,
    COUNT(*) as total,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as failed
FROM comprehensive_audit_log
WHERE event_type = 'admin'
GROUP BY operation, metadata->>'action'
ORDER BY operation, action;
```

```sql
-- Recent admin activity
SELECT
    TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as time,
    operation,
    metadata->>'action' as action,
    success,
    CASE
        WHEN metadata ? 'session_id' THEN metadata->>'session_id'
        WHEN metadata ? 'key' THEN metadata->>'key'
        WHEN metadata ? 'implementation' THEN metadata->>'implementation'
        ELSE NULL
    END as target,
    metadata->>'remote_addr' as from_addr
FROM comprehensive_audit_log
WHERE event_type = 'admin'
ORDER BY timestamp DESC
LIMIT 20;
```

```sql
-- Failed admin operations
SELECT
    TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as time,
    operation,
    metadata->>'action' as action,
    error_message,
    metadata->>'remote_addr' as from_addr
FROM comprehensive_audit_log
WHERE event_type = 'admin'
AND success = false
ORDER BY timestamp DESC;
```

---

## 5. Testing Log Retention and Cleanup

### Automated Test

Run the automated retention and cleanup test:

```bash
./scripts/test_retention_cleanup.sh
```

This script will:
1. Check prerequisites (psql, cleanup function)
2. Insert test audit records with various timestamps (old and recent)
3. Run the cleanup function with a 30-day retention period
4. Verify old logs are deleted and recent logs are retained
5. Display detailed results and statistics

### Manual Testing

#### Prerequisites

1. **PostgreSQL Database:**
   ```bash
   # Ensure PostgreSQL is running and audit table exists
   psql -h localhost -U aether -d aether -c "\d comprehensive_audit_log"
   ```

2. **Cleanup Function:**
   ```bash
   # Verify cleanup function exists
   psql -h localhost -U aether -d aether -c "\df cleanup_old_comprehensive_audit_logs"
   ```

#### Test Scenarios

### Test 1: Insert Old Test Records

Create test records with old timestamps:

```sql
-- Insert very old records (100 days ago)
INSERT INTO comprehensive_audit_log (
    timestamp, event_type, actor_type, actor_id, operation,
    gateway_id, success, metadata
)
SELECT
    NOW() - INTERVAL '100 days',
    'connection',
    'agent',
    'old_test_' || i,
    'connection_established',
    'test-gateway',
    true,
    '{"test": "old_record"}'::jsonb
FROM generate_series(1, 10) i;

-- Insert borderline old records (31 days ago)
INSERT INTO comprehensive_audit_log (
    timestamp, event_type, actor_type, actor_id, operation,
    gateway_id, success, metadata
)
SELECT
    NOW() - INTERVAL '31 days',
    'connection',
    'agent',
    'borderline_old_' || i,
    'connection_established',
    'test-gateway',
    true,
    '{"test": "borderline_old_record"}'::jsonb
FROM generate_series(1, 10) i;

-- Insert borderline recent records (29 days ago)
INSERT INTO comprehensive_audit_log (
    timestamp, event_type, actor_type, actor_id, operation,
    gateway_id, success, metadata
)
SELECT
    NOW() - INTERVAL '29 days',
    'connection',
    'agent',
    'borderline_recent_' || i,
    'connection_established',
    'test-gateway',
    true,
    '{"test": "borderline_recent_record"}'::jsonb
FROM generate_series(1, 10) i;

-- Insert recent records (10 days ago)
INSERT INTO comprehensive_audit_log (
    timestamp, event_type, actor_type, actor_id, operation,
    gateway_id, success, metadata
)
SELECT
    NOW() - INTERVAL '10 days',
    'connection',
    'agent',
    'recent_test_' || i,
    'connection_established',
    'test-gateway',
    true,
    '{"test": "recent_record"}'::jsonb
FROM generate_series(1, 10) i;
```

### Test 2: Verify Test Records Were Inserted

Check that all test records exist:

```sql
-- Count total test records
SELECT COUNT(*) as total_test_records
FROM comprehensive_audit_log
WHERE actor_id LIKE '%test_%';

-- Count by age category
SELECT
    CASE
        WHEN timestamp < NOW() - INTERVAL '50 days' THEN 'very_old (100 days)'
        WHEN timestamp < NOW() - INTERVAL '30 days' THEN 'borderline_old (31 days)'
        WHEN timestamp >= NOW() - INTERVAL '30 days' AND timestamp < NOW() - INTERVAL '20 days' THEN 'borderline_recent (29 days)'
        ELSE 'recent (10 days)'
    END as age_category,
    COUNT(*) as record_count
FROM comprehensive_audit_log
WHERE actor_id LIKE '%test_%'
GROUP BY age_category
ORDER BY MIN(timestamp);
```

Expected results:
- `very_old (100 days)`: 10 records
- `borderline_old (31 days)`: 10 records
- `borderline_recent (29 days)`: 10 records
- `recent (10 days)`: 10 records
- Total: 40 test records

### Test 3: Run Cleanup Function

Run the cleanup function with a 30-day retention period:

```sql
-- Run cleanup and get count of deleted records
SELECT cleanup_old_comprehensive_audit_logs(30) as deleted_count;
```

The function will:
- Delete all audit logs with `timestamp < NOW() - INTERVAL '30 days'`
- Return the count of deleted records

Expected results:
- The function should delete records older than 30 days
- The return value indicates how many records were deleted (will include non-test records)

### Test 4: Verify Old Records Were Deleted

Check that old test records are gone:

```sql
-- Count remaining test records
SELECT COUNT(*) as remaining_test_records
FROM comprehensive_audit_log
WHERE actor_id LIKE '%test_%';

-- Count old records (should be 0)
SELECT COUNT(*) as old_records_remaining
FROM comprehensive_audit_log
WHERE actor_id LIKE '%test_%'
AND timestamp < NOW() - INTERVAL '30 days';

-- Count recent records (should be 20)
SELECT COUNT(*) as recent_records_remaining
FROM comprehensive_audit_log
WHERE actor_id LIKE '%test_%'
AND timestamp >= NOW() - INTERVAL '30 days';
```

Expected results:
- `old_records_remaining`: 0 (all old records deleted)
- `recent_records_remaining`: 20 (borderline_recent + recent records)
- Total remaining: 20 test records

### Test 5: Verify Retention Boundary Precision

Check that the boundary is enforced precisely at 30 days:

```sql
-- Show records near the boundary
SELECT
    actor_id,
    timestamp,
    AGE(NOW(), timestamp) as age,
    CASE
        WHEN timestamp < NOW() - INTERVAL '30 days' THEN 'should_be_deleted'
        ELSE 'should_be_retained'
    END as expected_status,
    CASE
        WHEN actor_id LIKE '%test_%' THEN 'exists'
        ELSE 'not_a_test_record'
    END as actual_status
FROM comprehensive_audit_log
WHERE actor_id LIKE '%test_%'
ORDER BY timestamp;
```

Expected results:
- Records at 31 days (borderline_old): Deleted (not in results)
- Records at 29 days (borderline_recent): Retained
- The boundary is precisely enforced at 30 days

### Test 6: Test Different Retention Periods

Test cleanup with different retention periods:

```sql
-- Test with 7-day retention (more aggressive)
-- First, insert fresh test records
INSERT INTO comprehensive_audit_log (
    timestamp, event_type, actor_type, actor_id, operation,
    gateway_id, success, metadata
)
VALUES
    (NOW() - INTERVAL '10 days', 'connection', 'agent', 'test_10d', 'test', 'gw', true, '{}'),
    (NOW() - INTERVAL '5 days', 'connection', 'agent', 'test_5d', 'test', 'gw', true, '{}'),
    (NOW() - INTERVAL '2 days', 'connection', 'agent', 'test_2d', 'test', 'gw', true, '{}');

-- Run 7-day cleanup
SELECT cleanup_old_comprehensive_audit_logs(7) as deleted_count;

-- Verify only recent records remain
SELECT
    actor_id,
    AGE(NOW(), timestamp) as age
FROM comprehensive_audit_log
WHERE actor_id LIKE 'test_%'
ORDER BY timestamp DESC;
```

Expected: Only `test_5d` and `test_2d` should remain (< 7 days old)

### Test 7: Background Cleanup Job

The gateway runs a background cleanup job daily. To test it manually:

1. **Start Gateway:**
   ```bash
   export AETHER_AUDIT_RETENTION_DAYS=30
   go run ./cmd/gateway
   ```

2. **Check Logs:**
   Look for log messages about cleanup job:
   ```
   [INFO] Audit log cleanup job started (runs daily)
   [INFO] [Audit] Cleanup: purged N old audit log entries
   ```

3. **Verify in Database:**
   After 24 hours, the cleanup job should have run automatically and deleted old records.

For immediate testing, you can modify the ticker interval in `cmd/gateway/main.go`:
```go
// Change from 24 hours to 1 minute for testing
ticker := time.NewTicker(1 * time.Minute)
```

### Retention Policy Verification

Check the current retention policy and log statistics:

```sql
-- Show oldest and newest audit logs
SELECT
    MIN(timestamp) as oldest_log,
    MAX(timestamp) as newest_log,
    AGE(NOW(), MIN(timestamp)) as oldest_age,
    COUNT(*) as total_logs
FROM comprehensive_audit_log;

-- Count logs by age buckets
SELECT
    CASE
        WHEN timestamp >= NOW() - INTERVAL '7 days' THEN '0-7 days'
        WHEN timestamp >= NOW() - INTERVAL '30 days' THEN '7-30 days'
        WHEN timestamp >= NOW() - INTERVAL '90 days' THEN '30-90 days'
        WHEN timestamp >= NOW() - INTERVAL '180 days' THEN '90-180 days'
        ELSE '180+ days'
    END as age_bucket,
    COUNT(*) as log_count,
    MIN(timestamp) as oldest_in_bucket,
    MAX(timestamp) as newest_in_bucket
FROM comprehensive_audit_log
GROUP BY age_bucket
ORDER BY MIN(timestamp) DESC;
```

### Retention Policy Recommendations

Different compliance requirements may need different retention periods:

- **GDPR**: 6 months to 2 years
- **SOC2**: 1 year minimum
- **HIPAA**: 6 years
- **PCI-DSS**: 1 year minimum
- **General Security**: 90 days

Configure retention via environment variable:

```bash
# 90-day retention (default)
export AETHER_AUDIT_RETENTION_DAYS=90

# 1-year retention (SOC2, PCI-DSS)
export AETHER_AUDIT_RETENTION_DAYS=365

# 6-year retention (HIPAA)
export AETHER_AUDIT_RETENTION_DAYS=2190
```

### Cleanup Performance Testing

Test cleanup performance with large datasets:

```sql
-- Insert many old records for performance testing
INSERT INTO comprehensive_audit_log (
    timestamp, event_type, actor_type, actor_id, operation,
    gateway_id, success, metadata
)
SELECT
    NOW() - INTERVAL '100 days',
    'connection',
    'agent',
    'perf_test_' || i,
    'test',
    'test-gateway',
    true,
    '{"test": "performance"}'::jsonb
FROM generate_series(1, 100000) i;

-- Measure cleanup performance
EXPLAIN ANALYZE
SELECT cleanup_old_comprehensive_audit_logs(30);
```

Expected:
- Cleanup should complete in seconds even with large datasets
- The function uses the timestamp index for efficient deletion

### Retention Cleanup Success Criteria

✅ **Cleanup Function:**
- `cleanup_old_comprehensive_audit_logs(retention_days)` function exists
- Function deletes logs older than retention period
- Function returns correct count of deleted records
- Function uses indexed timestamp column for efficient deletion

✅ **Retention Boundary:**
- Boundary is enforced precisely at the specified retention period
- Records exactly at the boundary (e.g., 30.0 days old) are retained
- Records older than the boundary are deleted

✅ **Background Job:**
- Gateway starts background cleanup job on startup
- Job runs every 24 hours
- Job uses retention period from audit configuration
- Job logs cleanup results (number of records purged)

✅ **Configuration:**
- Retention period configurable via `AETHER_AUDIT_RETENTION_DAYS` environment variable
- Default retention is 90 days
- Custom retention periods work correctly (7, 30, 90, 365, etc.)

✅ **Recent Records Preserved:**
- Records newer than retention period are not deleted
- All recent test records remain after cleanup
- Production audit logs are preserved according to policy

✅ **Performance:**
- Cleanup completes quickly even with large datasets (100k+ records)
- Cleanup uses timestamp index for efficient deletion
- Cleanup runs in background without blocking operations

### Cleanup Test SQL Queries

```sql
-- Quick test: Insert old record and verify cleanup
DO $$
BEGIN
    -- Insert old test record
    INSERT INTO comprehensive_audit_log (timestamp, event_type, operation, actor_type, actor_id, gateway_id)
    VALUES (NOW() - INTERVAL '100 days', 'connection', 'test', 'agent', 'cleanup_test', 'gw');

    -- Verify it exists
    IF NOT EXISTS (SELECT 1 FROM comprehensive_audit_log WHERE actor_id = 'cleanup_test') THEN
        RAISE EXCEPTION 'Test record not inserted';
    END IF;

    -- Run cleanup
    PERFORM cleanup_old_comprehensive_audit_logs(90);

    -- Verify it was deleted
    IF EXISTS (SELECT 1 FROM comprehensive_audit_log WHERE actor_id = 'cleanup_test') THEN
        RAISE EXCEPTION 'Test record was not deleted by cleanup';
    END IF;

    RAISE NOTICE 'Cleanup test PASSED';
END $$;
```

### Cleanup Troubleshooting

**Issue: Cleanup not deleting old records**
- Check that the `timestamp` column has the correct values
- Verify retention period: `SELECT NOW() - INTERVAL '30 days';`
- Check function exists: `\df cleanup_old_comprehensive_audit_logs`
- Run manually: `SELECT cleanup_old_comprehensive_audit_logs(30);`

**Issue: Too many/too few records deleted**
- Verify the retention period parameter
- Check the timestamp calculation: records with `timestamp < NOW() - retention_days` are deleted
- Query to see what would be deleted:
  ```sql
  SELECT COUNT(*) FROM comprehensive_audit_log
  WHERE timestamp < NOW() - INTERVAL '30 days';
  ```

**Issue: Background job not running**
- Check gateway logs for "Audit log cleanup job started"
- Verify the job goroutine is running (check for context cancellation)
- Wait for the full 24-hour interval (or modify for testing)

**Issue: Performance degradation during cleanup**
- Check that the timestamp index exists: `\d comprehensive_audit_log`
- Use EXPLAIN ANALYZE to check query plan
- Consider running cleanup during off-peak hours
- Adjust cleanup interval if needed

---

## Next Steps

After verifying all audit logging features:
1. ✅ Test connection audit events (subtask-9-1) - COMPLETED
2. ✅ Test KV operation audit events (subtask-9-2) - COMPLETED
3. ✅ Test message routing audit events (subtask-9-3) - COMPLETED
4. ✅ Test admin action audit events (subtask-9-4) - COMPLETED
5. Test log retention and cleanup (subtask-9-5) - IN PROGRESS
