# Load Balancer Setup Guide

This guide provides detailed configuration instructions for setting up various load balancers with session affinity for Aether Gateway horizontal scaling.

## Overview

Aether Gateway requires a load balancer with:
- **gRPC/HTTP2 support** for bidirectional streaming
- **Session affinity** (sticky sessions) to route reconnecting clients to the same gateway instance
- **Health checks** to detect and route around failed instances
- **TLS termination** (optional, recommended for production)

This guide covers:
1. [Kubernetes Nginx Ingress](#kubernetes-nginx-ingress)
2. [AWS Application/Network Load Balancer](#aws-load-balancers)
3. [GCP Load Balancer](#gcp-load-balancer)
4. [Bare Metal Nginx](#bare-metal-nginx)

## Kubernetes Nginx Ingress

### Prerequisites

- Kubernetes cluster with Nginx Ingress Controller installed
- TLS certificate (optional, for HTTPS/TLS)
- DNS record pointing to ingress controller

### Installation

If Nginx Ingress Controller is not already installed:

```bash
# Using Helm
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update
helm install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace ingress-nginx \
  --create-namespace

# Verify installation
kubectl get pods -n ingress-nginx
```

### Configuration

#### 1. Create TLS Secret (Optional)

```bash
# Create TLS secret from certificate files
kubectl create secret tls aether-gateway-tls \
  --cert=path/to/cert.pem \
  --key=path/to/key.pem \
  -n aether
```

#### 2. Deploy Ingress Resource

Create `deployments/k8s/gateway/ingress.yaml`:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: aether-gateway
  namespace: aether
  annotations:
    # Session affinity using cookies
    nginx.ingress.kubernetes.io/affinity: "cookie"
    nginx.ingress.kubernetes.io/affinity-mode: "persistent"
    nginx.ingress.kubernetes.io/session-cookie-name: "aether-gateway-affinity"
    nginx.ingress.kubernetes.io/session-cookie-max-age: "10800"  # 3 hours
    nginx.ingress.kubernetes.io/session-cookie-expires: "10800"
    nginx.ingress.kubernetes.io/session-cookie-path: "/"
    nginx.ingress.kubernetes.io/session-cookie-samesite: "Strict"

    # gRPC backend configuration
    nginx.ingress.kubernetes.io/backend-protocol: "GRPC"
    nginx.ingress.kubernetes.io/grpc-backend: "true"

    # Timeouts for long-lived connections
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
    nginx.ingress.kubernetes.io/proxy-connect-timeout: "10"

    # SSL/TLS configuration
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    nginx.ingress.kubernetes.io/force-ssl-redirect: "true"
spec:
  ingressClassName: nginx
  tls:
  - hosts:
    - gateway.example.com
    secretName: aether-gateway-tls
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

#### 3. Apply Configuration

```bash
kubectl apply -f deployments/k8s/gateway/ingress.yaml

# Verify ingress is created
kubectl get ingress -n aether

# Check ingress details
kubectl describe ingress aether-gateway -n aether
```

#### 4. Test Connection

```bash
# Test gRPC connection through ingress
grpcurl gateway.example.com:443 list

# Connect with Python client
python python-client/example.py --host gateway.example.com --port 443 --tls
```

### Session Affinity Configuration

The key annotations for session affinity are:

- `nginx.ingress.kubernetes.io/affinity: "cookie"` - Enable cookie-based affinity
- `nginx.ingress.kubernetes.io/session-cookie-name` - Name of the sticky session cookie
- `nginx.ingress.kubernetes.io/session-cookie-max-age` - Cookie lifetime in seconds (3 hours recommended)

**How it works:**
1. Client connects through ingress
2. Nginx sets a cookie with hashed backend pod identifier
3. Client includes cookie in subsequent requests
4. Nginx routes to the same backend pod based on cookie value

### Troubleshooting

**Problem:** Connections not sticky (reconnections go to different pods)

**Solution:**
```bash
# Check if affinity annotation is present
kubectl get ingress aether-gateway -n aether -o yaml | grep affinity

# Verify nginx controller version supports affinity
kubectl exec -n ingress-nginx <nginx-controller-pod> -- nginx -V 2>&1 | grep affinity

# Check nginx config for sticky sessions
kubectl exec -n ingress-nginx <nginx-controller-pod> -- cat /etc/nginx/nginx.conf | grep sticky
```

**Problem:** gRPC errors "unimplemented" or "protocol error"

**Solution:**
- Ensure `nginx.ingress.kubernetes.io/backend-protocol: "GRPC"` annotation is set
- Verify Nginx Ingress Controller version is 0.48.0+ (gRPC support required)

## AWS Load Balancers

AWS offers two load balancers suitable for gRPC:
- **Application Load Balancer (ALB)** - HTTP/2 and gRPC support, cookie-based stickiness
- **Network Load Balancer (NLB)** - TCP-based, source IP stickiness, lower latency

### Option 1: Application Load Balancer (ALB)

**Best for:** HTTP/2 gRPC with TLS termination and cookie-based stickiness

#### 1. Create Target Group

```bash
# Create target group for gRPC
aws elbv2 create-target-group \
  --name aether-gateway-tg \
  --protocol HTTP \
  --protocol-version GRPC \
  --port 50051 \
  --vpc-id vpc-xxxxx \
  --target-type ip \
  --health-check-enabled \
  --health-check-protocol HTTP \
  --health-check-path /grpc.health.v1.Health/Check \
  --health-check-interval-seconds 10 \
  --health-check-timeout-seconds 5 \
  --healthy-threshold-count 2 \
  --unhealthy-threshold-count 2
```

#### 2. Enable Stickiness

```bash
# Enable cookie-based stickiness (3 hours)
aws elbv2 modify-target-group-attributes \
  --target-group-arn arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/aether-gateway-tg/xxx \
  --attributes \
    Key=stickiness.enabled,Value=true \
    Key=stickiness.type,Value=app_cookie \
    Key=stickiness.app_cookie.cookie_name,Value=AETHER_GATEWAY_AFFINITY \
    Key=stickiness.app_cookie.duration_seconds,Value=10800
```

#### 3. Create Application Load Balancer

```bash
# Create ALB
aws elbv2 create-load-balancer \
  --name aether-gateway-alb \
  --subnets subnet-xxxxx subnet-yyyyy \
  --security-groups sg-xxxxx \
  --scheme internet-facing \
  --type application \
  --ip-address-type ipv4

# Create HTTPS listener (port 443)
aws elbv2 create-listener \
  --load-balancer-arn arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/aether-gateway-alb/xxx \
  --protocol HTTPS \
  --port 443 \
  --certificates CertificateArn=arn:aws:acm:us-east-1:123456789012:certificate/xxx \
  --default-actions Type=forward,TargetGroupArn=arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/aether-gateway-tg/xxx
```

#### 4. Register Targets

```bash
# Register EKS pod IPs (or EC2 instances) to target group
aws elbv2 register-targets \
  --target-group-arn arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/aether-gateway-tg/xxx \
  --targets Id=10.0.1.10 Id=10.0.2.20 Id=10.0.3.30
```

### Option 2: Network Load Balancer (NLB)

**Best for:** High throughput, low latency, source IP preservation

#### 1. Create Target Group

```bash
# Create TCP target group
aws elbv2 create-target-group \
  --name aether-gateway-nlb-tg \
  --protocol TCP \
  --port 50051 \
  --vpc-id vpc-xxxxx \
  --target-type ip \
  --health-check-protocol TCP \
  --health-check-port 50051 \
  --health-check-interval-seconds 10
```

#### 2. Enable Source IP Stickiness

```bash
# Enable source IP stickiness
aws elbv2 modify-target-group-attributes \
  --target-group-arn arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/aether-gateway-nlb-tg/xxx \
  --attributes \
    Key=stickiness.enabled,Value=true \
    Key=stickiness.type,Value=source_ip
```

#### 3. Create Network Load Balancer

```bash
# Create NLB
aws elbv2 create-load-balancer \
  --name aether-gateway-nlb \
  --subnets subnet-xxxxx subnet-yyyyy \
  --scheme internet-facing \
  --type network \
  --ip-address-type ipv4

# Create TCP listener (port 50051)
aws elbv2 create-listener \
  --load-balancer-arn arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/aether-gateway-nlb/xxx \
  --protocol TCP \
  --port 50051 \
  --default-actions Type=forward,TargetGroupArn=arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/aether-gateway-nlb-tg/xxx
```

### Kubernetes Integration (AWS Load Balancer Controller)

For automatic integration with Kubernetes:

#### 1. Install AWS Load Balancer Controller

```bash
# Install via Helm
helm repo add eks https://aws.github.io/eks-charts
helm install aws-load-balancer-controller eks/aws-load-balancer-controller \
  -n kube-system \
  --set clusterName=<cluster-name> \
  --set serviceAccount.create=false \
  --set serviceAccount.name=aws-load-balancer-controller
```

#### 2. Update Service Annotations

```yaml
apiVersion: v1
kind: Service
metadata:
  name: aether-gateway
  namespace: aether
  annotations:
    # Use NLB
    service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
    service.beta.kubernetes.io/aws-load-balancer-scheme: "internet-facing"
    service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: "ip"

    # Health check configuration
    service.beta.kubernetes.io/aws-load-balancer-healthcheck-protocol: "tcp"
    service.beta.kubernetes.io/aws-load-balancer-healthcheck-interval: "10"

    # Cross-zone load balancing
    service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled: "true"
spec:
  type: LoadBalancer
  sessionAffinity: ClientIP
  sessionAffinityConfig:
    clientIP:
      timeoutSeconds: 10800  # 3 hours
  selector:
    app: aether-gateway
  ports:
  - port: 50051
    targetPort: 50051
    protocol: TCP
```

## GCP Load Balancer

Google Cloud Platform offers **Cloud Load Balancing** with gRPC support.

### Prerequisites

- GKE cluster or Compute Engine instances
- gcloud CLI configured
- TLS certificate uploaded to Google Certificate Manager

### Setup Steps

#### 1. Create Backend Service

```bash
# Create health check
gcloud compute health-checks create tcp aether-gateway-hc \
  --port=50051 \
  --check-interval=10s \
  --timeout=5s \
  --unhealthy-threshold=2 \
  --healthy-threshold=2

# Create backend service with session affinity
gcloud compute backend-services create aether-gateway-backend \
  --protocol=HTTP2 \
  --health-checks=aether-gateway-hc \
  --session-affinity=CLIENT_IP \
  --affinity-cookie-ttl=10800 \
  --global \
  --enable-cdn=false
```

#### 2. Add Instance Groups

```bash
# Create instance group (managed or unmanaged)
gcloud compute instance-groups unmanaged create aether-gateway-ig \
  --zone=us-central1-a

# Add instances to group
gcloud compute instance-groups unmanaged add-instances aether-gateway-ig \
  --instances=gateway-1,gateway-2,gateway-3 \
  --zone=us-central1-a

# Set named port
gcloud compute instance-groups unmanaged set-named-ports aether-gateway-ig \
  --named-ports=grpc:50051 \
  --zone=us-central1-a

# Add backend to service
gcloud compute backend-services add-backend aether-gateway-backend \
  --instance-group=aether-gateway-ig \
  --instance-group-zone=us-central1-a \
  --balancing-mode=UTILIZATION \
  --max-utilization=0.8 \
  --global
```

#### 3. Create URL Map and Target Proxy

```bash
# Create URL map
gcloud compute url-maps create aether-gateway-map \
  --default-service=aether-gateway-backend

# Create target HTTPS proxy
gcloud compute target-https-proxies create aether-gateway-proxy \
  --url-map=aether-gateway-map \
  --ssl-certificates=<certificate-name>
```

#### 4. Create Forwarding Rule

```bash
# Reserve static IP
gcloud compute addresses create aether-gateway-ip --global

# Create forwarding rule
gcloud compute forwarding-rules create aether-gateway-https-rule \
  --address=aether-gateway-ip \
  --global \
  --target-https-proxy=aether-gateway-proxy \
  --ports=443
```

### GKE Integration (Google Cloud Load Balancer Ingress)

For GKE clusters, use built-in Ingress:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: aether-gateway
  namespace: aether
  annotations:
    kubernetes.io/ingress.class: "gce"
    kubernetes.io/ingress.allow-http: "false"

    # Session affinity
    cloud.google.com/backend-config: '{"default": "aether-gateway-backendconfig"}'

    # TLS certificate
    ingress.gcp.kubernetes.io/pre-shared-cert: "<certificate-name>"
spec:
  rules:
  - host: gateway.example.com
    http:
      paths:
      - path: /*
        pathType: ImplementationSpecific
        backend:
          service:
            name: aether-gateway
            port:
              number: 50051
---
apiVersion: cloud.google.com/v1
kind: BackendConfig
metadata:
  name: aether-gateway-backendconfig
  namespace: aether
spec:
  sessionAffinity:
    affinityType: "CLIENT_IP"
    affinityCookieTtlSec: 10800
  healthCheck:
    checkIntervalSec: 10
    port: 50051
    type: TCP
  timeoutSec: 3600
```

## Bare Metal Nginx

For on-premises or non-cloud deployments, use standalone nginx as a load balancer.

### Prerequisites

- nginx 1.13.10+ (for gRPC support)
- TLS certificate and key files
- Multiple gateway instances running on different ports or servers

### Installation

```bash
# Install nginx on Ubuntu/Debian
sudo apt update
sudo apt install nginx

# Install nginx on RHEL/CentOS
sudo yum install nginx

# Verify version (must be 1.13.10+)
nginx -v
```

### Configuration

#### 1. Create nginx Configuration

Create `/etc/nginx/conf.d/aether-gateway.conf`:

```nginx
# Upstream backend servers
upstream aether_gateway_backend {
    # Session affinity using client IP hash
    ip_hash;

    # Gateway instances
    server 10.0.1.10:50051 max_fails=3 fail_timeout=30s;
    server 10.0.2.20:50051 max_fails=3 fail_timeout=30s;
    server 10.0.3.30:50051 max_fails=3 fail_timeout=30s;

    # Health check (requires nginx Plus or lua module)
    # check interval=10s rise=2 fall=3 timeout=5s;

    # Keepalive connections to backends
    keepalive 32;
}

# HTTP/2 server block for gRPC
server {
    listen 443 ssl http2;
    server_name gateway.example.com;

    # TLS configuration
    ssl_certificate /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    ssl_prefer_server_ciphers on;

    # gRPC-specific settings
    grpc_read_timeout 3600s;
    grpc_send_timeout 3600s;
    grpc_connect_timeout 10s;

    # Buffer settings for streaming
    grpc_buffer_size 16k;

    # Logging
    access_log /var/log/nginx/aether-gateway-access.log;
    error_log /var/log/nginx/aether-gateway-error.log warn;

    # gRPC pass to backend
    location / {
        grpc_pass grpc://aether_gateway_backend;

        # Error handling
        error_page 502 = /error502grpc;
        error_page 503 = /error503grpc;
        error_page 504 = /error504grpc;
    }

    # Error pages for gRPC
    location = /error502grpc {
        internal;
        default_type application/grpc;
        add_header grpc-status 14;
        add_header grpc-message "Bad Gateway";
        return 204;
    }

    location = /error503grpc {
        internal;
        default_type application/grpc;
        add_header grpc-status 14;
        add_header grpc-message "Service Unavailable";
        return 204;
    }

    location = /error504grpc {
        internal;
        default_type application/grpc;
        add_header grpc-status 14;
        add_header grpc-message "Gateway Timeout";
        return 204;
    }
}

# Redirect HTTP to HTTPS
server {
    listen 80;
    server_name gateway.example.com;
    return 301 https://$server_name$request_uri;
}
```

#### 2. Alternative: Cookie-Based Session Affinity

For more precise session affinity than `ip_hash`, use the `sticky` module (requires nginx Plus or third-party module):

```nginx
upstream aether_gateway_backend {
    # Cookie-based sticky sessions
    sticky cookie aether_gateway_affinity expires=3h path=/;

    server 10.0.1.10:50051;
    server 10.0.2.20:50051;
    server 10.0.3.30:50051;
}
```

**Note:** Cookie-based stickiness requires the nginx sticky module. Install it:

```bash
# For Ubuntu/Debian (requires nginx-extras package)
sudo apt install nginx-extras

# Or compile nginx with sticky module
# https://github.com/yaoweibin/nginx_upstream_hash
```

#### 3. Test Configuration

```bash
# Test configuration syntax
sudo nginx -t

# Reload nginx
sudo systemctl reload nginx

# Check status
sudo systemctl status nginx
```

#### 4. Verify Load Balancing

```bash
# Test gRPC connection
grpcurl -insecure gateway.example.com:443 list

# Monitor nginx logs
sudo tail -f /var/log/nginx/aether-gateway-access.log
```

### Health Checks

Nginx Open Source doesn't have native health checks. Options:

#### Option 1: Use max_fails and fail_timeout

```nginx
upstream aether_gateway_backend {
    server 10.0.1.10:50051 max_fails=3 fail_timeout=30s;
    server 10.0.2.20:50051 max_fails=3 fail_timeout=30s;
}
```

#### Option 2: External Health Check Script

Create `/usr/local/bin/nginx-healthcheck.sh`:

```bash
#!/bin/bash
# Check each backend and update nginx config if needed

BACKENDS=("10.0.1.10:50051" "10.0.2.20:50051" "10.0.3.30:50051")

for backend in "${BACKENDS[@]}"; do
    if ! nc -z ${backend/:/ }; then
        echo "Backend $backend is down"
        # Update nginx config to mark backend as down
        # Or use DNS-based failover
    fi
done
```

Run as cron job:
```bash
* * * * * /usr/local/bin/nginx-healthcheck.sh
```

#### Option 3: Upgrade to nginx Plus

nginx Plus includes native health checks:

```nginx
upstream aether_gateway_backend {
    zone aether_gateway 64k;
    server 10.0.1.10:50051;
    server 10.0.2.20:50051;
}

match grpc_check {
    status 200;
    header Content-Type = "application/grpc";
}

server {
    location / {
        grpc_pass grpc://aether_gateway_backend;
        health_check interval=10s fails=3 passes=2 match=grpc_check;
    }
}
```

## Load Balancer Comparison

| Feature | Nginx Ingress (K8s) | AWS ALB | AWS NLB | GCP LB | Nginx (Bare Metal) |
|---------|---------------------|---------|---------|--------|-------------------|
| **Session Affinity** | Cookie | Cookie | Source IP | Client IP / Cookie | IP Hash / Cookie |
| **gRPC Support** | ✅ Native | ✅ Native | ✅ (TCP) | ✅ Native | ✅ Native |
| **Health Checks** | ✅ HTTP/gRPC | ✅ HTTP | ✅ TCP | ✅ TCP/HTTP | ⚠️ Passive |
| **TLS Termination** | ✅ | ✅ | ❌ | ✅ | ✅ |
| **Setup Complexity** | Medium | High | Medium | High | Low |
| **Cost** | Cluster cost | $$ | $ | $$ | Free |
| **Best For** | Kubernetes | Full features | High throughput | GKE integration | On-prem |

## Testing Session Affinity

Verify session affinity is working correctly:

### 1. Connect Multiple Clients

```bash
# Run 10 clients
for i in {1..10}; do
  python python-client/example.py --host gateway.example.com --session-id "client-$i" &
done

# Check which gateway each client connected to
# (Gateway should log connection with gateway ID)
```

### 2. Verify Reconnection Affinity

```bash
# Connect client
python python-client/example.py --host gateway.example.com --session-id "test-client"

# Note which gateway instance handled the connection (from logs)

# Disconnect and reconnect within affinity timeout
python python-client/example.py --host gateway.example.com --session-id "test-client"

# Should connect to the SAME gateway instance
```

### 3. Test Failover

```bash
# Connect client and note gateway instance
python python-client/example.py --host gateway.example.com

# Kill that gateway instance
# Client should automatically reconnect to a different instance
```

## Production Recommendations

1. **Use cookie-based affinity** for external clients (more precise than IP-based)
2. **Set affinity timeout to 3 hours** (10800 seconds) to balance stickiness and distribution
3. **Enable health checks** with 10s interval, 2 consecutive failures to mark unhealthy
4. **Configure TLS** for production deployments
5. **Monitor connection distribution** to ensure load is reasonably balanced
6. **Test failover scenarios** before production deployment
7. **Use at least 3 gateway instances** for high availability

## Troubleshooting

### Session Affinity Not Working

**Symptoms:** Every reconnection goes to a different gateway instance

**Diagnosis:**
```bash
# Check load balancer access logs for affinity cookie/IP tracking
# For nginx:
sudo grep -i "sticky" /var/log/nginx/aether-gateway-access.log

# For AWS ALB:
aws elbv2 describe-target-group-attributes --target-group-arn <arn>

# For Kubernetes:
kubectl get ingress aether-gateway -o yaml | grep -i affinity
```

**Solution:**
- Verify session affinity configuration is applied
- Check affinity cookie is being set (inspect HTTP headers)
- Ensure client is sending cookie on subsequent requests
- For IP-based affinity, verify client IP is not changing

### High Latency Through Load Balancer

**Symptoms:** Direct connection to gateway is fast, but load balancer connection is slow

**Diagnosis:**
- Check load balancer health check frequency (too frequent = overhead)
- Verify connection keepalive settings
- Check for SSL/TLS negotiation overhead

**Solution:**
```nginx
# For nginx, enable keepalive to backends
upstream aether_gateway_backend {
    keepalive 32;
    keepalive_requests 1000;
}
```

### Uneven Load Distribution

**Symptoms:** One gateway has 90% of connections, others idle

**Cause:** Session affinity working as intended - initial distribution persists

**Solution:**
- Expected behavior with long-lived connections
- Connections rebalance naturally as clients disconnect/reconnect
- For immediate rebalancing: perform rolling restart of gateway instances

## See Also

- [Horizontal Scaling Architecture](horizontal-scaling.md) - Overview of multi-instance deployment
- [Kubernetes Deployment Manifests](../deployments/k8s/gateway/) - Production K8s configs
- [specification.md](specification.md) - Aether protocol specification
