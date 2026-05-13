#!/usr/bin/env bash
# Wire-level proxy load test driver.
#
# Brings up its own ephemeral RabbitMQ Streams + Valkey/Redis containers
# on dedicated ports (advertised at 127.0.0.1 to avoid host-name collisions
# with any operator-managed dev stacks), starts a real Aether gateway and
# two proxy-sidecar terminators, then runs the harness binary at
# server/cmd/loadtest/proxy.
#
# Usage:
#   bash server/scripts/run_proxy_load_test.sh           # full run
#   bash server/scripts/run_proxy_load_test.sh --check   # build only, skip infra/run
#   bash server/scripts/run_proxy_load_test.sh --no-infra-up
#                                                       # skip starting RMQ/Redis
#                                                       # (assume already reachable
#                                                       #  on the configured ports)
#
# Env knobs:
#   RMQ_STREAM_PORT  RMQ_AMQP_PORT  RMQ_MGMT_PORT  REDIS_PORT
#       Override the dedicated test ports (defaults: 56552, 56673, 56674, 56380).
#       These are deliberately distinct from the standard dev ports to avoid
#       clobbering an operator-managed RMQ/Redis instance.
#   CALLERS  DURATION  RAMP  TEARDOWN_WAIT  TARGET_MIBPS
#       Harness behavior (defaults: 100 / 60s / 5s / 30s / 1.0).
#
# Hard rules: never modifies the user's git state, never runs `git stash`.
set -euo pipefail

# -----------------------------------------------------------------------------
# Resolve repo paths.
# -----------------------------------------------------------------------------
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SERVER_DIR="$( cd "${SCRIPT_DIR}/.." && pwd )"
REPO_ROOT="$( cd "${SERVER_DIR}/.." && pwd )"

GO="${GO:-/home/drew/sdk/go1.25.5/bin/go}"
if [[ ! -x "$GO" ]]; then
  GO="$(command -v go || true)"
fi
if [[ -z "$GO" || ! -x "$GO" ]]; then
  echo "ERROR: no usable 'go' found. Set GO=/path/to/go and re-run." >&2
  exit 2
fi

CHECK_ONLY=0
NO_INFRA_UP=0
for arg in "$@"; do
  case "$arg" in
    --check) CHECK_ONLY=1 ;;
    --no-infra-up) NO_INFRA_UP=1 ;;
    -h|--help)
      sed -n '2,28p' "${BASH_SOURCE[0]}"
      exit 0
      ;;
  esac
done

LOG_DIR="${LOG_DIR:-${SERVER_DIR}/.proxy_load_logs}"
mkdir -p "${LOG_DIR}"

GATEWAY_LOG="${LOG_DIR}/gateway.log"
SIDECAR_A_LOG="${LOG_DIR}/sidecar-a.log"
SIDECAR_B_LOG="${LOG_DIR}/sidecar-b.log"
HARNESS_LOG="${LOG_DIR}/harness.log"

GATEWAY_BIN="${LOG_DIR}/gateway"
SIDECAR_BIN="${LOG_DIR}/proxy-sidecar"
HARNESS_BIN="${LOG_DIR}/proxy-load"

GATEWAY_CFG="${LOG_DIR}/gateway.yaml"
RMQ_CONF="${LOG_DIR}/rabbitmq.conf"

# Test-only ports — distinct from standard dev so a parallel operator stack
# is unaffected.
RMQ_STREAM_PORT="${RMQ_STREAM_PORT:-56552}"
RMQ_AMQP_PORT="${RMQ_AMQP_PORT:-56673}"
RMQ_MGMT_PORT="${RMQ_MGMT_PORT:-56674}"
REDIS_PORT="${REDIS_PORT:-56380}"

# Container names — single-instance, removed on teardown.
RMQ_CTR="aether-loadtest-rmq"
REDIS_CTR="aether-loadtest-redis"

GATEWAY_PID=""
SIDECAR_A_PID=""
SIDECAR_B_PID=""
INFRA_STARTED=0

# -----------------------------------------------------------------------------
# trap for guaranteed teardown — kills processes and removes containers we
# brought up.
# -----------------------------------------------------------------------------
cleanup() {
  set +e
  for var in SIDECAR_A_PID SIDECAR_B_PID GATEWAY_PID; do
    pid="${!var:-}"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "[driver] stopping $var=$pid"
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done
  sleep 1
  for var in SIDECAR_A_PID SIDECAR_B_PID GATEWAY_PID; do
    pid="${!var:-}"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done
  if [[ "${INFRA_STARTED}" -eq 1 ]]; then
    echo "[driver] removing test containers..."
    docker rm -f "${RMQ_CTR}" "${REDIS_CTR}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

# -----------------------------------------------------------------------------
# Reachability checks.
# -----------------------------------------------------------------------------
check_tcp() {
  local host="$1" port="$2" label="$3"
  if (echo > "/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
    echo "[driver] ${label} reachable at ${host}:${port}"
    return 0
  fi
  echo "ERROR: ${label} not reachable at ${host}:${port}" >&2
  return 1
}

# -----------------------------------------------------------------------------
# Spin up dedicated test infra containers (RabbitMQ Streams + Valkey).
# -----------------------------------------------------------------------------
start_test_infra() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker is required to bring up RMQ + Valkey for the load test" >&2
    exit 2
  fi

  # Make sure no stale containers from a prior run are around.
  docker rm -f "${RMQ_CTR}" "${REDIS_CTR}" >/dev/null 2>&1 || true

  echo "[driver] starting RabbitMQ Streams (${RMQ_CTR}) on streams=${RMQ_STREAM_PORT} amqp=${RMQ_AMQP_PORT} mgmt=${RMQ_MGMT_PORT}..."
  cat > "${RMQ_CONF}" <<EOF
loopback_users = none
stream.advertised_host = 127.0.0.1
stream.advertised_port = ${RMQ_STREAM_PORT}
EOF
  docker run -d --rm --name "${RMQ_CTR}" \
    -p "${RMQ_STREAM_PORT}:5552" \
    -p "${RMQ_AMQP_PORT}:5672" \
    -p "${RMQ_MGMT_PORT}:15672" \
    -v "${RMQ_CONF}:/etc/rabbitmq/rabbitmq.conf:ro" \
    ghcr.io/scitrera/rabbitmq-stream:4-management >/dev/null

  echo "[driver] starting Valkey (${REDIS_CTR}) on ${REDIS_PORT}..."
  docker run -d --rm --name "${REDIS_CTR}" \
    -p "${REDIS_PORT}:6379" \
    valkey/valkey:latest \
    valkey-server --bind 0.0.0.0 --protected-mode no --appendonly no --save "" >/dev/null

  INFRA_STARTED=1

  # Wait for RMQ Streams to accept connections.
  echo "[driver] waiting for RMQ Streams to be reachable..."
  local tries=0
  while ! (echo > "/dev/tcp/127.0.0.1/${RMQ_STREAM_PORT}") >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [[ "$tries" -gt 60 ]]; then
      echo "ERROR: RMQ Streams did not open port ${RMQ_STREAM_PORT} within 60s" >&2
      docker logs "${RMQ_CTR}" 2>&1 | tail -n 40 >&2 || true
      exit 2
    fi
    sleep 1
  done
  # Port-open is necessary but not sufficient — the rabbitmq_stream plugin
  # also has to finish loading and respond to commandPeerProperties.  Poll
  # the management API until the cluster reports ready, then give the
  # plugin a couple more seconds to settle.
  echo "[driver] waiting for RabbitMQ management api on :${RMQ_MGMT_PORT}..."
  tries=0
  while ! curl -fsS -u guest:guest "http://127.0.0.1:${RMQ_MGMT_PORT}/api/overview" >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [[ "$tries" -gt 60 ]]; then
      echo "ERROR: RabbitMQ management API never became reachable at :${RMQ_MGMT_PORT}" >&2
      docker logs "${RMQ_CTR}" 2>&1 | tail -n 60 >&2 || true
      exit 2
    fi
    sleep 1
  done
  sleep 3

  echo "[driver] waiting for Valkey..."
  tries=0
  while ! (echo > "/dev/tcp/127.0.0.1/${REDIS_PORT}") >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [[ "$tries" -gt 30 ]]; then
      echo "ERROR: Valkey did not open port ${REDIS_PORT} within 30s" >&2
      docker logs "${REDIS_CTR}" 2>&1 | tail -n 40 >&2 || true
      exit 2
    fi
    sleep 1
  done

  echo "[driver] test infra up: rmq_stream=${RMQ_STREAM_PORT} rmq_amqp=${RMQ_AMQP_PORT} redis=${REDIS_PORT}"
}

# -----------------------------------------------------------------------------
# Generate a self-contained gateway config that points at the test ports.
# -----------------------------------------------------------------------------
write_gateway_config() {
  cat > "${GATEWAY_CFG}" <<EOF
# Auto-generated by run_proxy_load_test.sh — do not edit by hand.
gateway:
  port: 50051
  ops_port: 9090
  gateway_id: "gateway-proxyload-1"

admin:
  enabled: true
  port: 31880
  cors_origin: "*"

auth:
  modes:
    - mtls
    - task_token
    - api_key
  mtls:
    required: false
    mode: relaxed
  api_key: { }
  oauth:
    verify_signature: false
    providers: [ ]

# PostgreSQL deliberately omitted — task/orchestration/ACL/audit features
# degrade gracefully and the harness exercises only the proxy fast path.

redis:
  cluster:
    - "127.0.0.1:${REDIS_PORT}"
  password: ""
  db: 0

rabbitmq:
  stream_url: "rabbitmq-stream://guest:guest@127.0.0.1:${RMQ_STREAM_PORT}"
  amqp_url: "amqp://guest:guest@127.0.0.1:${RMQ_AMQP_PORT}/"

audit:
  # Audit batched writes target Postgres; we don't run Postgres in this
  # harness, so disable audit to avoid the nil-DB panic on flush.
  enabled: false
  event_types: [ connection, auth, message, kv, admin, acl ]
  verbosity: low
  batch_size: 100
  flush_period: "5s"
  retention_days: 90
  channel_buffer: 1000

cleanup:
  task_purge_interval: "0s"
  reconciliation_interval: "0s"

checkpoint:
  default_ttl: "3600s"

shutdown:
  graceful_timeout: "10s"

log_level: "debug"
EOF
}

# -----------------------------------------------------------------------------
# Build phase.
# -----------------------------------------------------------------------------
build_all() {
  echo "[driver] building gateway, proxy-sidecar, and harness..."
  ( cd "${SERVER_DIR}" && "${GO}" build -o "${GATEWAY_BIN}"  ./cmd/gateway )
  ( cd "${SERVER_DIR}" && "${GO}" build -o "${SIDECAR_BIN}"  ./cmd/proxy-sidecar )
  ( cd "${SERVER_DIR}" && "${GO}" build -o "${HARNESS_BIN}"  ./cmd/loadtest/proxy )
  echo "[driver] build done: ${GATEWAY_BIN}, ${SIDECAR_BIN}, ${HARNESS_BIN}"
}

# -----------------------------------------------------------------------------
# Boot subprocesses.
# -----------------------------------------------------------------------------
start_gateway() {
  echo "[driver] starting gateway (logs: ${GATEWAY_LOG})..."
  AETHER_ALLOW_DEV_MODE=true AETHER_DEV_MODE=true \
    "${GATEWAY_BIN}" --dev --insecure-admin --config "${GATEWAY_CFG}" \
    >"${GATEWAY_LOG}" 2>&1 &
  GATEWAY_PID=$!
  echo "[driver] gateway pid=${GATEWAY_PID}"

  local tries=0
  while ! (echo > /dev/tcp/127.0.0.1/50051) >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [[ "$tries" -gt 60 ]]; then
      echo "ERROR: gateway did not open port 50051 within 30s; tail of log:" >&2
      tail -n 60 "${GATEWAY_LOG}" >&2
      exit 2
    fi
    sleep 0.5
  done
  echo "[driver] gateway listening on :50051"
}

start_sidecar() {
  local label="$1" cfg="$2" log="$3"
  # Echo status to stderr so command substitution captures only the PID.
  echo "[driver] starting sidecar ${label} (cfg: ${cfg}, logs: ${log})..." >&2
  "${SIDECAR_BIN}" --config "${cfg}" >"${log}" 2>&1 &
  local pid=$!
  echo "[driver] sidecar ${label} pid=${pid}" >&2
  printf '%s' "$pid"
}

# -----------------------------------------------------------------------------
# Phase: --check (build only, no infra/run).
# -----------------------------------------------------------------------------
if [[ "${CHECK_ONLY}" -eq 1 ]]; then
  echo "[driver] --check mode: build only, no infra/run"
  build_all
  echo "[driver] --check OK"
  exit 0
fi

# -----------------------------------------------------------------------------
# Full run.
# -----------------------------------------------------------------------------
build_all

if [[ "${NO_INFRA_UP}" -eq 1 ]]; then
  echo "[driver] --no-infra-up: skipping container start; assuming infra reachable"
  check_tcp 127.0.0.1 "${RMQ_STREAM_PORT}" "RabbitMQ Streams" || exit 2
  check_tcp 127.0.0.1 "${RMQ_AMQP_PORT}"   "RabbitMQ AMQP"     || exit 2
  check_tcp 127.0.0.1 "${REDIS_PORT}"      "Redis/Valkey"      || exit 2
else
  start_test_infra
fi

write_gateway_config
start_gateway

SIDECAR_A_PID="$(start_sidecar a "${SCRIPT_DIR}/proxy_load/sidecar-a.yaml" "${SIDECAR_A_LOG}")"
SIDECAR_B_PID="$(start_sidecar b "${SCRIPT_DIR}/proxy_load/sidecar-b.yaml" "${SIDECAR_B_LOG}")"

echo "[driver] waiting 4s for sidecars to register..."
sleep 4

if ! kill -0 "${SIDECAR_A_PID}" 2>/dev/null; then
  echo "ERROR: sidecar A exited early; tail of log:" >&2
  tail -n 60 "${SIDECAR_A_LOG}" >&2
  exit 2
fi
if ! kill -0 "${SIDECAR_B_PID}" 2>/dev/null; then
  echo "ERROR: sidecar B exited early; tail of log:" >&2
  tail -n 60 "${SIDECAR_B_LOG}" >&2
  exit 2
fi

DOC="${SERVER_DIR}/docs/proxy-load-test-results.md"

CALLERS="${CALLERS:-100}"
DURATION="${DURATION:-60s}"
RAMP="${RAMP:-5s}"
TEARDOWN_WAIT="${TEARDOWN_WAIT:-30s}"
TARGET_MIBPS="${TARGET_MIBPS:-1.0}"

echo "[driver] running harness (callers=${CALLERS}, duration=${DURATION})"
"${HARNESS_BIN}" \
  --gateway 127.0.0.1:50051 \
  --gateway-pid "${GATEWAY_PID}" \
  --audit-log "${GATEWAY_LOG}" \
  --append-doc "${DOC}" \
  --callers "${CALLERS}" \
  --duration "${DURATION}" \
  --ramp "${RAMP}" \
  --teardown-wait "${TEARDOWN_WAIT}" \
  --target-mibps "${TARGET_MIBPS}" \
  | tee "${HARNESS_LOG}"

echo "[driver] running auth-proxy regression tests..."
( cd "${SERVER_DIR}" && "${GO}" test ./internal/authproxy/... )

echo "[driver] done. logs: ${LOG_DIR}"
