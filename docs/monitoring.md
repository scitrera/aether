# Aether Gateway Monitoring Guide

This guide covers Prometheus metrics exposed by Aether Gateway, recommended dashboard panels, alerting rules, and integration with observability stacks.

## Metrics Endpoint

Aether Gateway exposes Prometheus metrics at:

```
GET http://localhost:9090/metrics
```

The metrics port and endpoint are configured in the admin server. In production, this port should be network-isolated (internal only) to prevent metrics exposure on the public load balancer.

**Important:** The `/metrics` endpoint is intentionally unauthenticated per Prometheus convention. Restrict network access at the infrastructure level (firewall, VPC, etc.).

## Available Metrics

### Connection Metrics

These metrics track client connection lifecycle and multiplicity.

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `aether_active_connections` | Gauge | `workspace`, `principal_type` | Currently active client connections broken down by workspace and principal type (agent, task, user, orchestrator, workflow_engine, metrics_bridge) |
| `aether_admin_active_connections_total` | Gauge | | Total number of active gRPC connections across all workspaces and principal types (updated every 5 seconds) |
| `aether_connection_duration_seconds` | Histogram | `workspace`, `principal_type` | Duration of individual client connections in seconds. Buckets: 1s to ~4.5h (exponential, base 2) |
| `aether_connection_attempts_total` | Counter | `workspace`, `principal_type`, `status` | Total connection attempts by outcome (success, failure, duplicate, auth_failed) |

**Example queries:**
```promql
# Current connections by workspace
aether_active_connections{workspace="default"}

# Connection success rate
sum(rate(aether_connection_attempts_total{status="success"}[5m])) /
sum(rate(aether_connection_attempts_total[5m]))

# Average connection lifetime (using histogram)
histogram_quantile(0.5, rate(aether_connection_duration_seconds_bucket[5m]))
```

### Message Routing Metrics

These metrics monitor message throughput, errors, and latency.

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `aether_messages_routed_total` | Counter | `workspace`, `message_type` | Total messages successfully routed (CHAT, CONTROL, TOOL_CALL, EVENT, METRIC) |
| `aether_message_errors_total` | Counter | `workspace`, `error_type` | Total message routing errors by type (invalid_topic, unauthorized, offline_target, etc.) |
| `aether_message_routing_latency_seconds` | Histogram | `workspace` | Latency of message routing from receive to publish (time spent in gateway). Buckets: 0.1ms to ~3.2s (exponential, base 2) |

**Example queries:**
```promql
# Messages routed per second by workspace
rate(aether_messages_routed_total[1m])

# Error rate (%)
sum(rate(aether_message_errors_total[5m])) /
sum(rate(aether_messages_routed_total[5m]) + rate(aether_message_errors_total[5m]))

# Message routing p99 latency
histogram_quantile(0.99, rate(aether_message_routing_latency_seconds_bucket[5m]))
```

### KV Store Metrics

These metrics track configuration store operations and performance.

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `aether_kv_operations_total` | Counter | `operation`, `scope`, `status` | Total KV operations (get, set, delete, list) by scope (tenant, workspace, address) and outcome (success, failure) |
| `aether_kv_operation_latency_seconds` | Histogram | `operation`, `scope` | Latency of KV store operations. Buckets: 0.1ms to ~800ms (exponential, base 2) |

**Example queries:**
```promql
# KV operation success rate
sum(rate(aether_kv_operations_total{status="success"}[5m])) /
sum(rate(aether_kv_operations_total[5m]))

# KV get latency p95
histogram_quantile(0.95, rate(aether_kv_operation_latency_seconds_bucket{operation="get"}[5m]))
```

### Authentication & Authorization Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `aether_auth_attempts_total` | Counter | `method`, `status` | Authentication attempts by method (api_key, token, mTLS) and outcome (success, failure, invalid) |

### Orchestration Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `aether_orchestration_triggers_total` | Counter | `workspace` | Total times orchestration was triggered to launch offline agents or tasks |

**Example query:**
```promql
# Orchestration triggers per minute
rate(aether_orchestration_triggers_total[1m])
```

### Topic & Subscription Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `aether_topic_subscriptions_active` | Gauge | | Current number of active topic subscriptions across all clients |

---

## Key Dashboard Panels

### 1. Connection Monitoring

**Active Connections by Principal Type**
```promql
aether_active_connections
```
- **Type:** Stacked area chart
- **Time range:** Last 1 hour
- **Alerts:** Spike in connections, unexpected principal type mix

**Connection Success Rate**
```promql
100 * (
  sum(rate(aether_connection_attempts_total{status="success"}[5m]))
  /
  sum(rate(aether_connection_attempts_total[5m]))
)
```
- **Type:** Stat (percentage)
- **Alert threshold:** < 99%
- **Description:** Percentage of successful connection attempts

**Average Connection Duration**
```promql
histogram_quantile(0.50, rate(aether_connection_duration_seconds_bucket[5m]))
```
- **Type:** Graph with multiple quantiles
- **Quantiles:** p50, p95, p99
- **Alert threshold:** Sudden drops may indicate instability

---

### 2. Message Throughput & Quality

**Messages Routed per Second**
```promql
sum(rate(aether_messages_routed_total[1m])) by (workspace)
```
- **Type:** Stacked bar chart or time series
- **Breakdown:** By message type for deeper analysis
```promql
sum(rate(aether_messages_routed_total[1m])) by (workspace, message_type)
```

**Message Error Rate (%)**
```promql
100 * (
  sum(rate(aether_message_errors_total[5m])) by (workspace)
  /
  (sum(rate(aether_messages_routed_total[5m])) by (workspace) +
   sum(rate(aether_message_errors_total[5m])) by (workspace))
)
```
- **Type:** Graph with threshold line
- **Alert threshold:** > 5%
- **Breakdown:** By error type to identify patterns

---

### 3. Latency Analysis

**Message Routing Latency Percentiles**
```promql
# p50, p95, p99 in milliseconds
histogram_quantile(0.50, rate(aether_message_routing_latency_seconds_bucket[5m])) * 1000
histogram_quantile(0.95, rate(aether_message_routing_latency_seconds_bucket[5m])) * 1000
histogram_quantile(0.99, rate(aether_message_routing_latency_seconds_bucket[5m])) * 1000
```
- **Type:** Graph with three series
- **Alert threshold:** p99 > 500ms may indicate bottleneck

**KV Operation Latency by Type**
```promql
histogram_quantile(0.95, rate(aether_kv_operation_latency_seconds_bucket[5m])) by (operation) * 1000
```
- **Type:** Graph
- **Operations:** get, set, delete, list
- **Alert threshold:** Sudden increase in p95 latency

---

### 4. Orchestration Health

**Orchestration Triggers per Minute**
```promql
sum(rate(aether_orchestration_triggers_total[1m])) by (workspace)
```
- **Type:** Bar chart
- **Description:** How often agents/tasks are being spun up (indicates usage patterns)

**Active Topic Subscriptions**
```promql
aether_topic_subscriptions_active
```
- **Type:** Stat (number)
- **Comparison:** Trend over time

---

## Alerting Rules

Place these rules in your Prometheus alerting configuration:

### High Message Error Rate

```yaml
- alert: AetherHighMessageErrorRate
  expr: |
    (
      sum(rate(aether_message_errors_total[5m])) by (workspace)
      /
      (sum(rate(aether_messages_routed_total[5m])) by (workspace) +
       sum(rate(aether_message_errors_total[5m])) by (workspace))
    ) > 0.05
  for: 5m
  labels:
    severity: warning
    component: aether
  annotations:
    summary: "High message error rate in {{ $labels.workspace }}"
    description: "Message error rate is {{ humanizePercentage $value }} in workspace {{ $labels.workspace }}. Check gateway logs for routing issues."
```

### Connection Failure Rate

```yaml
- alert: AetherHighConnectionFailureRate
  expr: |
    (
      sum(rate(aether_connection_attempts_total{status!="success"}[5m])) by (workspace)
      /
      sum(rate(aether_connection_attempts_total[5m])) by (workspace)
    ) > 0.10
  for: 5m
  labels:
    severity: warning
    component: aether
  annotations:
    summary: "High connection failure rate in {{ $labels.workspace }}"
    description: "Connection failure rate is {{ humanizePercentage $value }} in workspace {{ $labels.workspace }}. Check Redis locks and network connectivity."
```

### Message Routing Latency Spike

```yaml
- alert: AetherHighRoutingLatency
  expr: |
    histogram_quantile(0.99, rate(aether_message_routing_latency_seconds_bucket[5m])) > 0.5
  for: 3m
  labels:
    severity: warning
    component: aether
  annotations:
    summary: "High message routing latency (p99 > 500ms)"
    description: "Message routing p99 latency is {{ humanizeDuration $value }}. Check RabbitMQ performance and gateway CPU/memory."
```

### KV Store Degradation

```yaml
- alert: AetherKVOperationFailureRate
  expr: |
    (
      sum(rate(aether_kv_operations_total{status="failure"}[5m])) by (scope)
      /
      sum(rate(aether_kv_operations_total[5m])) by (scope)
    ) > 0.05
  for: 5m
  labels:
    severity: warning
    component: aether
  annotations:
    summary: "High KV operation failure rate in {{ $labels.scope }}"
    description: "KV failure rate is {{ humanizePercentage $value }} in scope {{ $labels.scope }}. Check Redis connectivity."
```

### No Active Connections (Potential Outage)

```yaml
- alert: AetherNoActiveConnections
  expr: aether_admin_active_connections_total == 0
  for: 2m
  labels:
    severity: critical
    component: aether
  annotations:
    summary: "No active connections to Aether Gateway"
    description: "Gateway has zero active connections. Check if gateway is running and if clients can reach it."
```

### Authentication Failures

```yaml
- alert: AetherHighAuthFailureRate
  expr: |
    (
      sum(rate(aether_auth_attempts_total{status!="success"}[5m]))
      /
      sum(rate(aether_auth_attempts_total[5m]))
    ) > 0.25
  for: 5m
  labels:
    severity: warning
    component: aether
  annotations:
    summary: "High authentication failure rate"
    description: "Authentication failure rate is {{ humanizePercentage $value }}. Check API keys and token configuration."
```

---

## Grafana Dashboard JSON

Import this dashboard into Grafana for a complete monitoring view:

```json
{
  "annotations": {
    "list": [
      {
        "builtIn": 1,
        "datasource": "-- Grafana --",
        "enable": true,
        "hide": true,
        "iconColor": "rgba(0, 211, 255, 1)",
        "name": "Annotations & Alerts",
        "type": "dashboard"
      }
    ]
  },
  "editable": true,
  "gnetId": null,
  "graphTooltip": 0,
  "id": null,
  "links": [],
  "panels": [
    {
      "datasource": "Prometheus",
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 10,
            "gradientMode": "none",
            "hideFrom": {
              "tooltip": false,
              "viz": false,
              "legend": false
            },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": {
              "type": "linear"
            },
            "showPoints": "never",
            "spanNulls": false,
            "stacking": {
              "group": "A",
              "mode": "normal"
            },
            "thresholdsStyle": {
              "mode": "off"
            }
          },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              }
            ]
          },
          "unit": "short"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 0,
        "y": 0
      },
      "id": 2,
      "options": {
        "legend": {
          "calcs": [],
          "displayMode": "list",
          "placement": "bottom"
        },
        "tooltip": {
          "mode": "single"
        }
      },
      "pluginVersion": "8.0.0",
      "targets": [
        {
          "expr": "aether_active_connections",
          "legendFormat": "{{ principal_type }} - {{ workspace }}",
          "refId": "A"
        }
      ],
      "title": "Active Connections",
      "type": "timeseries"
    },
    {
      "datasource": "Prometheus",
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 10,
            "gradientMode": "none",
            "hideFrom": {
              "tooltip": false,
              "viz": false,
              "legend": false
            },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": {
              "type": "linear"
            },
            "showPoints": "never",
            "spanNulls": false,
            "stacking": {
              "group": "A",
              "mode": "normal"
            },
            "thresholdsStyle": {
              "mode": "off"
            }
          },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              }
            ]
          },
          "unit": "msg/s"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 12,
        "y": 0
      },
      "id": 3,
      "options": {
        "legend": {
          "calcs": [],
          "displayMode": "list",
          "placement": "bottom"
        },
        "tooltip": {
          "mode": "single"
        }
      },
      "pluginVersion": "8.0.0",
      "targets": [
        {
          "expr": "sum(rate(aether_messages_routed_total[1m])) by (workspace)",
          "legendFormat": "{{ workspace }}",
          "refId": "A"
        }
      ],
      "title": "Message Throughput (per second)",
      "type": "timeseries"
    },
    {
      "datasource": "Prometheus",
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 10,
            "gradientMode": "none",
            "hideFrom": {
              "tooltip": false,
              "viz": false,
              "legend": false
            },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": {
              "type": "linear"
            },
            "showPoints": "never",
            "spanNulls": false,
            "stacking": {
              "group": "A",
              "mode": "normal"
            },
            "thresholdsStyle": {
              "mode": "off"
            }
          },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              },
              {
                "color": "red",
                "value": 0.05
              }
            ]
          },
          "unit": "percentunit"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 0,
        "y": 8
      },
      "id": 4,
      "options": {
        "legend": {
          "calcs": [],
          "displayMode": "list",
          "placement": "bottom"
        },
        "tooltip": {
          "mode": "single"
        }
      },
      "pluginVersion": "8.0.0",
      "targets": [
        {
          "expr": "sum(rate(aether_message_errors_total[5m])) by (workspace) / (sum(rate(aether_messages_routed_total[5m])) by (workspace) + sum(rate(aether_message_errors_total[5m])) by (workspace))",
          "legendFormat": "{{ workspace }}",
          "refId": "A"
        }
      ],
      "title": "Message Error Rate",
      "type": "timeseries"
    },
    {
      "datasource": "Prometheus",
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 10,
            "gradientMode": "none",
            "hideFrom": {
              "tooltip": false,
              "viz": false,
              "legend": false
            },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": {
              "type": "linear"
            },
            "showPoints": "never",
            "spanNulls": false,
            "stacking": {
              "group": "A",
              "mode": "normal"
            },
            "thresholdsStyle": {
              "mode": "off"
            }
          },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              },
              {
                "color": "yellow",
                "value": 0.1
              },
              {
                "color": "red",
                "value": 0.5
              }
            ]
          },
          "unit": "s"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 12,
        "y": 8
      },
      "id": 5,
      "options": {
        "legend": {
          "calcs": [],
          "displayMode": "list",
          "placement": "bottom"
        },
        "tooltip": {
          "mode": "single"
        }
      },
      "pluginVersion": "8.0.0",
      "targets": [
        {
          "expr": "histogram_quantile(0.50, rate(aether_message_routing_latency_seconds_bucket[5m]))",
          "legendFormat": "p50",
          "refId": "A"
        },
        {
          "expr": "histogram_quantile(0.95, rate(aether_message_routing_latency_seconds_bucket[5m]))",
          "legendFormat": "p95",
          "refId": "B"
        },
        {
          "expr": "histogram_quantile(0.99, rate(aether_message_routing_latency_seconds_bucket[5m]))",
          "legendFormat": "p99",
          "refId": "C"
        }
      ],
      "title": "Message Routing Latency (percentiles)",
      "type": "timeseries"
    },
    {
      "datasource": "Prometheus",
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 10,
            "gradientMode": "none",
            "hideFrom": {
              "tooltip": false,
              "viz": false,
              "legend": false
            },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": {
              "type": "linear"
            },
            "showPoints": "never",
            "spanNulls": false,
            "stacking": {
              "group": "A",
              "mode": "normal"
            },
            "thresholdsStyle": {
              "mode": "off"
            }
          },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              }
            ]
          },
          "unit": "ops/s"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 0,
        "y": 16
      },
      "id": 6,
      "options": {
        "legend": {
          "calcs": [],
          "displayMode": "list",
          "placement": "bottom"
        },
        "tooltip": {
          "mode": "single"
        }
      },
      "pluginVersion": "8.0.0",
      "targets": [
        {
          "expr": "sum(rate(aether_kv_operations_total{status=\"success\"}[1m])) by (operation)",
          "legendFormat": "{{ operation }}",
          "refId": "A"
        }
      ],
      "title": "KV Operations (per second)",
      "type": "timeseries"
    },
    {
      "datasource": "Prometheus",
      "fieldConfig": {
        "defaults": {
          "color": {
            "mode": "palette-classic"
          },
          "custom": {
            "axisLabel": "",
            "axisPlacement": "auto",
            "barAlignment": 0,
            "drawStyle": "line",
            "fillOpacity": 10,
            "gradientMode": "none",
            "hideFrom": {
              "tooltip": false,
              "viz": false,
              "legend": false
            },
            "lineInterpolation": "linear",
            "lineWidth": 1,
            "pointSize": 5,
            "scaleDistribution": {
              "type": "linear"
            },
            "showPoints": "never",
            "spanNulls": false,
            "stacking": {
              "group": "A",
              "mode": "normal"
            },
            "thresholdsStyle": {
              "mode": "off"
            }
          },
          "mappings": [],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              },
              {
                "color": "yellow",
                "value": 0.05
              },
              {
                "color": "red",
                "value": 0.1
              }
            ]
          },
          "unit": "percentunit"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 12,
        "y": 16
      },
      "id": 7,
      "options": {
        "legend": {
          "calcs": [],
          "displayMode": "list",
          "placement": "bottom"
        },
        "tooltip": {
          "mode": "single"
        }
      },
      "pluginVersion": "8.0.0",
      "targets": [
        {
          "expr": "sum(rate(aether_kv_operations_total{status=\"failure\"}[5m])) by (scope) / sum(rate(aether_kv_operations_total[5m])) by (scope)",
          "legendFormat": "{{ scope }}",
          "refId": "A"
        }
      ],
      "title": "KV Operation Failure Rate",
      "type": "timeseries"
    }
  ],
  "schemaVersion": 27,
  "style": "dark",
  "tags": [
    "aether",
    "gateway",
    "monitoring"
  ],
  "templating": {
    "list": []
  },
  "time": {
    "from": "now-6h",
    "to": "now"
  },
  "timepicker": {},
  "timezone": "",
  "title": "Aether Gateway Monitoring",
  "uid": "aether-gateway",
  "version": 1
}
```

---

## Scrape Configuration

Add this job to your Prometheus `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'aether-gateway'
    static_configs:
      - targets: ['localhost:9090']  # Adjust port to match admin server port
    scrape_interval: 30s
    scrape_timeout: 10s
    # Optional: authentication if you expose metrics on a secure endpoint
    # basic_auth:
    #   username: 'prometheus'
    #   password: 'secret'
    # Optional: TLS verification
    # scheme: https
    # tls_config:
    #   ca_file: '/etc/prometheus/ca.crt'
    #   cert_file: '/etc/prometheus/client.crt'
    #   key_file: '/etc/prometheus/client.key'
    #   insecure_skip_verify: false
```

---

## Health Check Endpoints

Aether Gateway exposes Kubernetes-compatible health probes on the admin HTTP server:

| Endpoint | Purpose | Returns |
|----------|---------|---------|
| `GET /health/live` | Liveness probe | 200 OK if process is running |
| `GET /health/ready` | Readiness probe | 200 OK if dependencies (Redis, RabbitMQ) are accessible |
| `GET /health/startup` | Startup probe | 200 OK once initialization is complete |

Use these for container orchestration health checks:

**Kubernetes Example:**
```yaml
containers:
- name: aether-gateway
  livenessProbe:
    httpGet:
      path: /health/live
      port: 9090
    initialDelaySeconds: 10
    periodSeconds: 10
  readinessProbe:
    httpGet:
      path: /health/ready
      port: 9090
    initialDelaySeconds: 5
    periodSeconds: 10
  startupProbe:
    httpGet:
      path: /health/startup
      port: 9090
    initialDelaySeconds: 0
    periodSeconds: 2
    failureThreshold: 30
```

---

## Observability Best Practices

### 1. Retention & Storage
- **Metrics retention:** 15 days minimum for operational debugging
- **Dashboard dashboard:** Pin key metrics to main dashboard for quick visibility
- **Correlation:** Use timestamps to correlate metrics with logs for root cause analysis

### 2. Alerting Strategy
- **Warning alerts** (> 5m): High error rate, elevated latency
- **Critical alerts** (2m): No connections, dependency failures, auth failures
- **Investigation:** Every alert should have a runbook or troubleshooting link

### 3. Baselines & Thresholds
- **Connection patterns:** Baseline expected connections per principal type
- **Message rate:** Understand normal throughput for each workspace
- **Latency:** Establish p99 baselines during peak load testing

### 4. Dashboard Organization
Group panels by concern:
1. **System Health** (connections, subscriptions, overall active status)
2. **Throughput** (messages/sec, error rates)
3. **Performance** (latency percentiles, KV operation times)
4. **Orchestration** (trigger rates, resource usage)

### 5. Metering Integration
Usage metering (billable connections, message byte counts, KV operations by scope) is planned but not yet integrated. Use Prometheus metrics as the current source of truth for capacity planning.

---

## Troubleshooting Guide

### High Message Error Rate
1. Check `message_errors_total` by `error_type` to identify failure category
2. Examine gateway logs for specific error messages
3. Verify RabbitMQ Streams health (producer/consumer status)
4. Check Redis for lock contention issues

### Latency Spikes
1. Compare message routing latency with RabbitMQ broker latency
2. Check gateway CPU/memory utilization (OS metrics)
3. Monitor KV operation latency separately (Redis latency)
4. Correlate with burst in connection attempts or message volume

### Connection Failures
1. Check `connection_attempts_total{status!="success"}` by principal type
2. Verify auth configuration (API keys, tokens)
3. Monitor Redis lock contention (`connection_attempts_total{status="duplicate"}`)
4. Check network connectivity between clients and gateway

### No Active Connections
1. Verify gateway process is running (`/health/live`)
2. Check gateway accessibility (firewall, load balancer)
3. Examine client connection logs for error messages
4. Verify Redis and RabbitMQ are healthy

---

## Related Documentation

- [Admin UI Guide](admin-ui.md) - Real-time monitoring dashboard
- [Error Codes](error-codes.md) - Message error types and meanings
- [Horizontal Scaling](horizontal-scaling.md) - Multi-instance deployment
- [CLAUDE.md](../CLAUDE.md) - System architecture and concepts
