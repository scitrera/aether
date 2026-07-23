# Aether Gateway Monitoring Guide

This guide covers Prometheus metrics exposed by Aether Gateway, recommended dashboard panels, alerting rules, and integration with observability stacks.

## Metrics Endpoint

Aether Gateway exposes Prometheus metrics at:

```
GET http://localhost:9090/metrics
```

Metrics are served by a dedicated ops server that is separate from the admin UI/API server (see [Admin UI Guide](admin-ui.md)) — it exists purely for Kubernetes health probes and the Prometheus scrape endpoint, with no authentication. The port defaults to 9090 and is configured via `gateway.ops_port` in the YAML config, the `--ops-port` flag, or the `AETHER_OPS_PORT` environment variable. In production, this port should be network-isolated (internal only) to prevent metrics exposure on the public load balancer.

**Important:** The `/metrics` endpoint is intentionally unauthenticated per Prometheus convention. Restrict network access at the infrastructure level (firewall, VPC, etc.).

## Available Metrics

### Connection Metrics

These metrics track client connection lifecycle and multiplicity.

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `aether_active_connections` | Gauge | `workspace`, `principal_type` | Currently active client connections broken down by workspace and principal type (agent, task, user, orchestrator, workflow_engine, metrics_bridge) |
| `aether_admin_active_connections_total` | Gauge | | Total number of active gRPC connections across all workspaces and principal types (updated every 5 seconds) |
| `aether_connection_duration_seconds` | Histogram | `workspace`, `principal_type` | Duration of individual client connections in seconds. Buckets: 1s to ~4.5h (exponential, base 2) |
| `aether_connection_attempts_total` | Counter | `workspace`, `principal_type`, `status` | Total connection attempts by outcome (`success`, `lock_failed`, `quota_exceeded`, `acl_denied`) |

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
| `aether_message_errors_total` | Counter | `workspace`, `error_type` | Total message routing errors by type (`payload_too_large`, `rate_limited`, `workspace_rate_limited`, `permission_denied`, `cross_workspace_broadcast_denied`, `sv_wildcard_unavailable`, `metric_validation`, `publish_failed`) |
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
| `aether_kv_operations_total` | Counter | `operation`, `scope`, `status` | Total KV operations by proto `OpType` name (`GET`, `PUT`, `LIST`, `DELETE`, `INCREMENT`, `DECREMENT`, `COMPARE_AND_SET`, `COMPARE_AND_DELETE`, etc.), scope, and outcome (`ok`, `error`) |
| `aether_kv_operation_latency_seconds` | Histogram | `operation`, `scope` | Latency of KV store operations. Buckets: 0.1ms to ~800ms (exponential, base 2) |

**Example queries:**
```promql
# KV operation success rate
sum(rate(aether_kv_operations_total{status="ok"}[5m])) /
sum(rate(aether_kv_operations_total[5m]))

# KV get latency p95
histogram_quantile(0.95, rate(aether_kv_operation_latency_seconds_bucket{operation="GET"}[5m]))
```

### Redis & RabbitMQ Backend Metrics

These metrics track the health of the two stateful backends the gateway depends on (not exposed by AetherLite, which uses Badger/JetStream instead).

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `aether_redis_operations_total` | Counter | `operation`, `status` | Total Redis session/lock operations (`lock_acquire`, `lock_refresh`, `lock_release`, `session_register`, `session_unregister`) by outcome (`success`, `failure`) |
| `aether_session_lock_duration_seconds` | Histogram | | Duration of session lock acquisition. Buckets: 0.1ms to ~3.2s (exponential, base 2) |
| `aether_circuit_breaker_state` | Gauge | `subsystem` | Circuit breaker state: 0=closed, 1=open, 2=half-open |
| `aether_rabbitmq_publish_total` | Counter | `status` | Total RabbitMQ stream publish attempts by outcome |
| `aether_rabbitmq_publish_duration_seconds` | Histogram | | Duration of RabbitMQ stream publish operations. Buckets: 0.1ms to ~3.2s (exponential, base 2) |

**Example queries:**
```promql
# Redis operation failure rate
sum(rate(aether_redis_operations_total{status="failure"}[5m])) /
sum(rate(aether_redis_operations_total[5m]))

# Circuit breaker currently open (1) for any subsystem
aether_circuit_breaker_state == 1
```

**Note:** There is currently no dedicated authentication-attempt metric (`aether_auth_attempts_total` does not exist in the codebase). Credential authentication (mTLS/API key/OAuth) failures short-circuit the connection before any Prometheus counter is incremented, so they are only visible in gateway logs, not in `aether_connection_attempts_total` — that metric only covers post-authentication outcomes (lock, quota, ACL).

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
      sum(rate(aether_kv_operations_total{status="error"}[5m])) by (scope)
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

### Redis Lock Failures

There is no dedicated authentication-attempt metric (see the note in [Redis & RabbitMQ Backend Metrics](#redis--rabbitmq-backend-metrics)). The closest actionable signal for identity/session problems is a spike in Redis lock failures, which surfaces both stale-lock contention and Redis connectivity issues:

```yaml
- alert: AetherHighRedisLockFailureRate
  expr: |
    (
      sum(rate(aether_redis_operations_total{operation="lock_acquire", status="failure"}[5m]))
      /
      sum(rate(aether_redis_operations_total{operation="lock_acquire"}[5m]))
    ) > 0.10
  for: 5m
  labels:
    severity: warning
    component: aether
  annotations:
    summary: "High Redis lock acquisition failure rate"
    description: "Lock acquisition failure rate is {{ humanizePercentage $value }}. Check for stale locks (DuplicateIdentityError) and Redis connectivity/latency."
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
          "expr": "sum(rate(aether_kv_operations_total{status=\"ok\"}[1m])) by (operation)",
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
          "expr": "sum(rate(aether_kv_operations_total{status=\"error\"}[5m])) by (scope) / sum(rate(aether_kv_operations_total[5m])) by (scope)",
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
      - targets: ['localhost:9090']  # Adjust port to match the ops server port (gateway.ops_port / AETHER_OPS_PORT)
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

## OTLP Traces & Metrics Export

In addition to the Prometheus `/metrics` endpoint, both `gateway` and `aetherlite` can export traces and metrics directly via OTLP/gRPC to an OpenTelemetry Collector.

Both exporters are gated on the same environment variable, `OTEL_EXPORTER_OTLP_ENDPOINT`. When it is unset, tracing and metrics export are both disabled and the SDKs fall back to no-op providers — there is no separate on/off flag for each signal.

```bash
# Typical OTLP/gRPC collector endpoint (insecure/plaintext for http:// scheme)
export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4317"
```

- **Traces**: exported via `otlptracegrpc`, batched, and tagged with `service.name` (`aether-gateway` or `aether-lite`).
- **Metrics**: exported via `otlpmetricgrpc` with a periodic reader (~60s interval by default), tagged with `service.name`, `service.namespace=scitrera`, and `service.version`.
- **Go runtime metrics**: `InitMeter` also starts the OpenTelemetry Go runtime instrumentation, so `go_*`/process metrics (GC, goroutines, memory) are included in the OTLP metrics stream alongside the application-level counters/histograms.
- The OTLP metrics exporter is a separate signal path from the Prometheus `aether_*` metrics described above — both can be enabled simultaneously and carry overlapping but not identical data (OTLP export includes Go runtime metrics that are not registered with the Prometheus registry).
- Both `otlptracegrpc` and `otlpmetricgrpc` connect to the **same** endpoint over gRPC (typically port 4317); do not point `OTEL_EXPORTER_OTLP_ENDPOINT` at an OTLP/HTTP port (4318) — the gRPC exporters will fail against it.

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
A dedicated `aether_metering_*` metric namespace is already exposed for usage-based billing: `aether_metering_messages_routed_total`, `aether_metering_bytes_routed_total`, `aether_metering_kv_operations_total`, `aether_metering_checkpoint_operations_total`, and `aether_metering_task_operations_total` are actively incremented in the routing and orchestration paths (labeled by `workspace` and operation type). `aether_metering_active_connections` is defined but not currently updated by any code path. A billing pipeline that consumes these counters is not yet built — use them as the current source of truth for capacity planning and manual usage analysis in the meantime.

---

## Troubleshooting Guide

### High Message Error Rate
1. Check `aether_message_errors_total` by `error_type` to identify failure category
2. Examine gateway logs for specific error messages
3. Verify RabbitMQ Streams health via `aether_rabbitmq_publish_total{status="failure"}` and producer/consumer status
4. Check Redis for lock contention issues via `aether_redis_operations_total{status="failure"}`

### Latency Spikes
1. Compare message routing latency with RabbitMQ broker latency
2. Check gateway CPU/memory utilization (OS metrics) and Go runtime metrics (`go_memstats_*`, `go_goroutines`) exported via OTLP — see [OTLP Traces & Metrics Export](#otlp-traces--metrics-export)
3. Monitor KV operation latency separately (Redis latency)
4. Correlate with burst in connection attempts or message volume

### Connection Failures
1. Check `aether_connection_attempts_total{status!="success"}` by principal type
2. Verify auth configuration (API keys, tokens) — credential authentication failures are not counted in this metric, so check gateway logs directly
3. Monitor Redis lock contention (`aether_connection_attempts_total{status="lock_failed"}`, `aether_redis_operations_total{operation="lock_acquire", status="failure"}`)
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
