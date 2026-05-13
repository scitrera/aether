# Aether Gateway Multi-Instance Docker Compose

This directory contains Docker Compose configuration for testing Aether Gateway horizontal scaling with session affinity.

## Architecture

The setup includes:

- **3 Gateway Instances** (`gateway-1`, `gateway-2`, `gateway-3`)
  - Each with unique `AETHER_GATEWAY_ID`
  - Ports: 50051-50053 (gRPC), 8081-8083 (HTTP), 9091-9093 (Prometheus)

- **Nginx Load Balancer**
  - Port 50050 (gRPC load balanced endpoint)
  - Session affinity using `ip_hash` (clients stick to same gateway)
  - Health checks on all backend instances

- **Infrastructure Services**
  - **PostgreSQL**: Task persistence and metadata (port 5432)
  - **Redis (Valkey)**: Session registry, KV store, checkpoints (port 6379)
  - **RabbitMQ Streams**: Message routing backbone (ports 5552, 5672, 15672)

## Usage

### Start all services:
```bash
docker compose -f deployments/docker-compose/multi-instance.yaml up
```

### Start in detached mode:
```bash
docker compose -f deployments/docker-compose/multi-instance.yaml up -d
```

### View logs:
```bash
docker compose -f deployments/docker-compose/multi-instance.yaml logs -f
docker compose -f deployments/docker-compose/multi-instance.yaml logs -f gateway-1
```

### Stop all services:
```bash
docker compose -f deployments/docker-compose/multi-instance.yaml down
```

### Clean up volumes:
```bash
docker compose -f deployments/docker-compose/multi-instance.yaml down -v
```

## Testing Session Affinity

1. **Connect a client** to the load balancer on port 50050
2. **Check which gateway handled the connection** (inspect logs)
3. **Disconnect and reconnect** - should route to same gateway (session affinity)
4. **Kill a gateway instance** - client should failover to another instance
5. **Verify no message loss** during failover

Example using Python client:
```bash
# Connect to load balancer
python python-client/example.py --host localhost --port 50050
```

## Testing Failover

```bash
# Connect client to load balancer (port 50050)
# Note which gateway it connects to from logs

# Kill that gateway instance
docker stop aether-gateway-1

# Client should automatically reconnect to gateway-2 or gateway-3
# Verify messages are not lost

# Restart the killed instance
docker start aether-gateway-1
```

## Monitoring

- **Gateway Health Checks**: http://localhost:8081/health/live (gateway-1)
- **Load Balancer Health**: http://localhost:8080/health
- **Nginx Status**: http://localhost:8080/nginx_status
- **RabbitMQ Management**: http://localhost:15672 (guest/guest)
- **Prometheus Metrics**: http://localhost:9091/metrics (gateway-1)

## Configuration Files

- `multi-instance.yaml` - Main docker-compose configuration
- `nginx/nginx.conf` - Nginx load balancer with gRPC and session affinity
- `nginx/rabbitmq.conf` - RabbitMQ configuration for streams

## Session Affinity

The nginx load balancer uses `ip_hash` for session affinity:
- Same client IP → always routes to same gateway instance
- Reduces reconnection churn and improves performance
- Automatic failover if gateway instance becomes unhealthy

## Notes

- All services use health checks for proper startup ordering
- PostgreSQL and RabbitMQ data persists in Docker volumes
- Redis runs in ephemeral mode (no persistence) for testing
- mTLS is disabled for local testing
- Gateway IDs are static: `gateway-1`, `gateway-2`, `gateway-3`

## Building Custom Gateway Image

If you need to build a custom gateway image:

```bash
# From repository root
docker build -t scitrera/aether-gateway:latest .

# Or let docker-compose build it
docker compose -f deployments/docker-compose/multi-instance.yaml build
```
