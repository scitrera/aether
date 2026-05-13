# Horizontal Scaling Architecture

> **Note: AetherLite is single-node only.** If you are running `./aetherlite` or `./gateway --lite`, horizontal scaling is not supported — all state is held in local Badger and SQLite databases that cannot be shared across processes. To scale horizontally, use the full Aether stack with Redis, RabbitMQ, and PostgreSQL as described in this document. See [aetherlite.md](aetherlite.md) for AetherLite limitations.

This document describes how to deploy Aether Gateway in a horizontally scaled configuration with session affinity for high availability and load distribution.

## Overview

Aether Gateway is designed to scale horizontally with multiple instances running simultaneously. The architecture leverages:

- **Stateless Gateway Instances**: All session state stored in external Redis
- **Shared Message Backbone**: RabbitMQ Streams accessible by all gateway instances
- **Distributed Locking**: Redis-based locks ensure identity uniqueness across instances
- **Session Affinity**: Load balancer routing to minimize reconnection overhead

This design allows you to:
- Run 3+ gateway instances for high availability
- Scale horizontally to handle thousands of concurrent agent connections
- Achieve zero-downtime deployments with rolling updates
- Survive individual gateway instance failures with automatic failover

## Architecture Diagram

```
┌─────────────┐
│   Clients   │
│ (Agents,    │
│  Tasks,     │
│  Users)     │
└──────┬──────┘
       │
       ▼
┌──────────────────────────────────────┐
│   Load Balancer (Session Affinity)   │
│   - ClientIP or Cookie-based         │
│   - Health checks on all instances   │
└──────┬───────────────────────────────┘
       │
       ├─────────┬─────────┬─────────┐
       ▼         ▼         ▼         ▼
  ┌─────────┐ ┌─────────┐ ┌─────────┐
  │Gateway-1│ │Gateway-2│ │Gateway-N│
  │ ID: gw1 │ │ ID: gw2 │ │ ID: gwN │
  └────┬────┘ └────┬────┘ └────┬────┘
       │           │           │
       └───────────┴───────────┘
                   │
       ┌───────────┴───────────┐
       │                       │
       ▼                       ▼
┌─────────────┐       ┌──────────────┐
│    Redis    │       │  RabbitMQ    │
│             │       │  Streams     │
│ - Locks     │       │              │
│ - Sessions  │       │ - Messages   │
│ - KV Store  │       │ - Topics     │
└─────────────┘       └──────────────┘
```

## How Distributed Locks Enable Horizontal Scaling

### The Connection = Lock = Heartbeat Paradigm

Each gateway instance enforces identity uniqueness using Redis distributed locks:

1. **Lock Acquisition**: When a client connects with identity `ag.workspace1.impl.spec`, the gateway attempts:
   ```
   Redis SetNX: session:ag.workspace1.impl.spec = sessionID + gatewayID + timestamp
   ```

2. **Lock Enforcement**: If the key already exists (another instance or connection holds it), the connection is rejected with `DuplicateIdentityError`.

3. **Lock Ownership**: The lock contains:
   - Session ID (unique per connection)
   - Gateway ID (which instance owns the lock)
   - Timestamp (for timeout detection)

4. **Lock Release**: When the gRPC connection closes (clean disconnect or network failure), the gateway releases the lock:
   ```
   Redis DEL: session:ag.workspace1.impl.spec
   ```

### Cross-Instance Lock Coordination

Because locks are in Redis (external to any single gateway), they coordinate across all instances:

- **Gateway-1** holds lock for `ag.ws1.agent-a`
- **Gateway-2** cannot accept a duplicate connection for `ag.ws1.agent-a`
- If **Gateway-1** crashes, its locks auto-expire (TTL-based)
- Client reconnects to **Gateway-2**, which acquires the now-available lock

### Lock Lifecycle

```
Client Connect → Gateway Acquires Lock → Active Session
                                              │
                                              ▼
Client Disconnect ────────────────→ Gateway Releases Lock
     OR                                       │
Gateway Crash + TTL Expires ────────────────┘
```

## Session Affinity Configuration

### Why Session Affinity?

Without session affinity, each client reconnection might route to a different gateway instance. This causes:
- Unnecessary lock churn (release on old instance, acquire on new instance)
- Message redelivery overhead (new consumer offset initialization)
- Increased latency during reconnection

**Session affinity** routes reconnecting clients to the same gateway instance when possible, reducing overhead.

### Session Affinity Mechanisms

#### 1. **Kubernetes Service (ClientIP)**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: aether-gateway
spec:
  sessionAffinity: ClientIP
  sessionAffinityConfig:
    clientIP:
      timeoutSeconds: 10800  # 3 hours
  selector:
    app: aether-gateway
  ports:
  - port: 50051
    protocol: TCP
```

**How it works:**
- Kubernetes tracks client source IP
- Routes all connections from same IP to same gateway pod
- Timeout ensures affinity expires after 3 hours of inactivity

**Limitations:**
- Multiple clients behind NAT share the same IP (over-affinity)
- IP changes (mobile clients) break affinity

#### 2. **Nginx Ingress (Cookie-based)**

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: aether-gateway
  annotations:
    nginx.ingress.kubernetes.io/affinity: "cookie"
    nginx.ingress.kubernetes.io/affinity-mode: "persistent"
    nginx.ingress.kubernetes.io/session-cookie-name: "aether-gateway-affinity"
    nginx.ingress.kubernetes.io/session-cookie-max-age: "10800"
    nginx.ingress.kubernetes.io/session-cookie-expires: "10800"
spec:
  rules:
  - host: gateway.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: aether-gateway
            port:
              number: 50051
```

**How it works:**
- Nginx sets a cookie on first connection
- Cookie contains hashed gateway pod identifier
- Subsequent connections use cookie to route to same pod
- More precise than ClientIP (per-client vs per-IP)

**Advantages:**
- Works across IP changes
- Fine-grained (per-client, not per-IP)
- Standard HTTP cookie mechanism

#### 3. **AWS Application Load Balancer (Target Group Stickiness)**

```bash
aws elbv2 create-target-group \
  --name aether-gateway-tg \
  --protocol TCP \
  --port 50051 \
  --vpc-id vpc-xxxxx \
  --target-type ip \
  --health-check-protocol TCP \
  --health-check-port 50051

aws elbv2 modify-target-group-attributes \
  --target-group-arn arn:aws:elasticloadbalancing:... \
  --attributes Key=stickiness.enabled,Value=true \
              Key=stickiness.type,Value=source_ip \
              Key=stickiness.lb_cookie.duration_seconds,Value=10800
```

**Note**: For gRPC (HTTP/2), use Network Load Balancer (NLB) with source IP stickiness instead of ALB.

### Session Affinity Trade-offs

| Mechanism | Precision | Failover | Cross-IP Support |
|-----------|-----------|----------|------------------|
| ClientIP  | Low (IP)  | Immediate | No |
| Cookie    | High (client) | Immediate | Yes |
| Source IP (NLB) | Low (IP) | Immediate | No |

**Recommendation**: Use cookie-based affinity for external clients, ClientIP for internal services.

## Failover Behavior

### Automatic Failover on Gateway Crash

When a gateway instance crashes or becomes unavailable:

1. **TCP Connection Breaks**: Client's gRPC stream connection fails with network error
2. **Client Reconnects**: Client initiates new connection to load balancer
3. **Load Balancer Routes**:
   - If session affinity target is unavailable (failed health check), route to healthy instance
   - If session affinity expired, distribute via round-robin or least-connections
4. **New Gateway Acquires Lock**:
   - Old lock may still exist (if crash prevented cleanup)
   - Old lock has TTL and expires within 60 seconds
   - New gateway either acquires immediately (if old lock expired) or rejects with `DuplicateIdentityError`
5. **Client Retries**: If rejected, client waits and retries exponentially (up to old lock TTL expiration)

### Graceful Shutdown (Zero-Downtime Deploys)

For rolling updates or planned maintenance:

1. **Signal SIGTERM**: Kubernetes sends SIGTERM to gateway pod
2. **Stop Accepting New Connections**: Gateway stops accepting new `InitConnection` requests
3. **Drain Existing Connections**: Gateway allows active streams to complete (configurable timeout: 30s-120s)
4. **Release All Locks**: On connection close, gateway releases all Redis locks
5. **Exit**: Gateway process terminates
6. **Clients Reconnect**: Clients reconnect to remaining healthy instances

**Configuration**:
```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      terminationGracePeriodSeconds: 120  # Allow 2 minutes for drain
      containers:
      - name: gateway
        lifecycle:
          preStop:
            exec:
              command: ["/bin/sh", "-c", "sleep 10"]  # Brief delay for load balancer de-registration
```

### Message Durability During Failover

**RabbitMQ Streams ensure no message loss**:

1. **Before Failover**: Gateway-1 consuming from stream at offset 1000
2. **Gateway-1 Crashes**: Connection lost, consumer offset remains at 1000
3. **Client Reconnects to Gateway-2**: Gateway-2 creates new consumer
4. **Consumer Offset Restored**:
   - If client provides `last_processed_offset`, resume from that offset
   - Otherwise, resume from last committed offset (may receive duplicates)
5. **At-Least-Once Delivery**: Client may receive duplicate messages (offsets 999-1001)

**Client Responsibility**: Implement idempotent message handling or track `message_id` for deduplication.

## Production Deployment Checklist

### Prerequisites
- [ ] Redis cluster with high availability (Sentinel or Cluster mode)
- [ ] RabbitMQ Streams cluster with 3+ nodes
- [ ] Load balancer with health checks configured
- [ ] Kubernetes cluster (for K8s deployment) or Docker Swarm/Nomad

### Gateway Configuration
- [ ] Set `AETHER_GATEWAY_ID` to unique value per instance (e.g., Pod name)
- [ ] Set `REDIS_ADDR` to Redis cluster endpoint
- [ ] Set `STREAM_URL` to RabbitMQ Streams cluster URL
- [ ] Configure `PORT` (default 50051 for gRPC)
- [ ] Set lock TTL via `LOCK_TTL_SECONDS` (default: 60)

### Load Balancer Configuration
- [ ] Enable session affinity (ClientIP or Cookie)
- [ ] Configure health checks on port 50051 (gRPC) or 8080 (HTTP health endpoint)
- [ ] Set health check interval: 10 seconds
- [ ] Set health check timeout: 5 seconds
- [ ] Set unhealthy threshold: 2 consecutive failures
- [ ] Set healthy threshold: 2 consecutive successes

### Scaling Configuration
- [ ] Deploy minimum 3 gateway instances for high availability
- [ ] Configure horizontal pod autoscaling based on:
  - CPU usage (target: 70%)
  - Memory usage (target: 80%)
  - gRPC connection count (target: 500 per instance)

### Monitoring & Alerting
- [ ] Monitor active connection count per instance
- [ ] Monitor Redis lock acquisition failures
- [ ] Monitor RabbitMQ stream consumer lag
- [ ] Alert on gateway instance crashes
- [ ] Alert on sustained high connection churn (frequent reconnections)

## Testing Multi-Instance Setup

### Local Testing with Docker Compose

Use the provided `deployments/docker-compose/multi-instance.yaml`:

```bash
# Start 3 gateway instances + Redis + RabbitMQ + Nginx LB
docker-compose -f deployments/docker-compose/multi-instance.yaml up

# Verify all instances are healthy
docker-compose -f deployments/docker-compose/multi-instance.yaml ps

# Run integration tests
./scripts/test_multi_instance.sh
```

### Kubernetes Testing

```bash
# Deploy to test namespace
kubectl apply -f deployments/k8s/gateway/ -n aether-test

# Verify 3 replicas running
kubectl get pods -n aether-test -l app=aether-gateway

# Test connection to load balancer
grpcurl -plaintext gateway.aether-test.svc.cluster.local:50051 list

# Simulate failover by deleting one pod
kubectl delete pod <gateway-pod-name> -n aether-test

# Verify clients reconnect to remaining pods
kubectl logs -n aether-test -l app=aether-gateway --tail=100
```

### Manual Failover Testing

1. **Connect a client** to the load balancer
2. **Identify which gateway** the client connected to (check logs or connection metadata)
3. **Kill that gateway instance** (`kubectl delete pod` or `docker-compose stop`)
4. **Verify client reconnects** to a different instance within 5-10 seconds
5. **Send messages** before, during, and after failover - verify none are lost

## Troubleshooting

### Problem: Clients Cannot Connect (DuplicateIdentityError)

**Symptoms**: All connection attempts rejected with "identity already in use"

**Cause**: Stale lock in Redis from crashed gateway that didn't release lock

**Solution**:
```bash
# Check for stale locks
redis-cli KEYS "session:*"

# Inspect lock content
redis-cli GET "session:ag.workspace1.impl.spec"

# If timestamp is old (>60s) and gateway no longer exists, manually delete
redis-cli DEL "session:ag.workspace1.impl.spec"
```

**Prevention**: Ensure `LOCK_TTL_SECONDS` is set (default 60). Locks auto-expire even if not explicitly released.

### Problem: Uneven Load Distribution

**Symptoms**: One gateway has 90% of connections, others idle

**Cause**: Session affinity + no connection churn = initial distribution persists

**Solution**:
- This is expected behavior with session affinity
- Connections naturally rebalance as clients disconnect/reconnect
- For immediate rebalancing: rolling restart of gateway instances

### Problem: High Connection Churn

**Symptoms**: Clients constantly reconnecting, high CPU on gRPC handshake

**Cause**: Session affinity not working, every reconnect hits different instance

**Solution**:
- Verify session affinity configuration in load balancer
- Check load balancer logs to confirm affinity cookies/IP tracking
- Ensure client source IP is stable (not changing per request)

### Problem: Message Loss During Failover

**Symptoms**: Messages sent before failover not received after reconnection

**Cause**: RabbitMQ Streams consumer offset not properly restored

**Solution**:
- Verify client tracks and provides `last_processed_offset` on reconnection
- Check RabbitMQ consumer offset tracking (should be per-consumer-name)
- Ensure gateway creates consumers with persistent offset storage

## Performance Characteristics

### Connection Capacity

- **Single gateway instance**: 1,000-2,000 concurrent connections (depends on message throughput)
- **3-instance cluster**: 3,000-6,000 concurrent connections
- **Bottleneck**: Usually RabbitMQ Streams throughput, not gateway CPU

### Failover Time

- **Detection**: 5-10 seconds (health check interval + timeout)
- **Reconnection**: 1-3 seconds (client reconnection backoff)
- **Lock acquisition**: <100ms (Redis round-trip)
- **Total failover time**: 6-13 seconds

### Resource Usage Per Instance

- **CPU**: 0.5-1 core at 1,000 connections, 2-4 cores at 2,000 connections
- **Memory**: 512MB base + ~1MB per 10 concurrent connections
- **Network**: Depends on message throughput; ~10Mbps per 100 msg/s

## See Also

- [Load Balancer Setup Guide](load-balancer-setup.md) - Detailed configuration for various load balancers
- [CLAUDE.md](../CLAUDE.md) - System architecture overview
- [specification.md](specification.md) - Full protocol specification
- [Kubernetes Deployment Manifests](../deployments/k8s/gateway/) - Production-ready K8s configs
